package core

import (
	"context"
	"fmt"
	"io"
	"log"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// Transport adalah interface yang harus diimplementasikan oleh setiap
// opsi transport. Menangani komunikasi antar peer.
type Transport interface {
	// Start memulai transport dan discovery.
	Start(ctx context.Context) error
	// Stop menghentikan transport.
	Stop() error
	// SelfID mengembalikan ID peer ini.
	SelfID() string
	// Peers mengembalikan daftar peer yang dikenal.
	Peers() []PeerInfo
	// SendMeta mengirim metadata file ke peer.
	SendMeta(peer PeerInfo, meta FileMeta) error
	// SendFile mengirim konten file ke peer.
	SendFile(peer PeerInfo, meta FileMeta, data io.Reader) error
	// ReceiveMeta mengembalikan channel metadata file yang diterima.
	ReceiveMeta() <-chan FileMeta
	// ReceiveFile mengembalikan channel file yang diterima.
	ReceiveFile() <-chan FileTransfer
	// ResolveFile meminta konten file dari peer tertentu.
	ResolveFile(peer PeerInfo, meta FileMeta) (io.ReadCloser, error)
	// BroadcastMeta mengirim metadata ke semua peer.
	BroadcastMeta(meta FileMeta) error
	// BroadcastSnapshot mengirim snapshot index ke semua peer.
	BroadcastSnapshot(snapshot map[string]FileMeta) error
	// ReceiveSnapshot mengembalikan channel snapshot yang diterima.
	ReceiveSnapshot() <-chan map[string]FileMeta
}

// Option adalah fungsi konfigurasi untuk Syncer.
type Option func(*Syncer)

// WithConflictStrategy mengatur strategi resolusi konflik.
func WithConflictStrategy(strategy string) Option {
	return func(s *Syncer) {
		s.conflictStrategy = strategy
	}
}

// WithDeviceName mengatur nama device untuk identifikasi peer.
func WithDeviceName(name string) Option {
	return func(s *Syncer) {
		s.deviceName = name
	}
}

// Syncer adalah mesin sinkronisasi utama. Menghubungkan file watcher
// dengan transport untuk menyebarkan perubahan ke peer lain.
type Syncer struct {
	root    string
	index   *FileIndex
	watcher *Watcher
	trans   Transport

	deviceName       string
	conflictStrategy string // "last-writer-wins" | "manual"
	events           chan SyncEvent
	pending          map[string]FileMeta // file yang menunggu dikirim
	mu               sync.Mutex
}

// SyncEvent merepresentasikan event sinkronisasi.
type SyncEvent struct {
	Type      string // "file-received" | "file-sent" | "peer-joined" | "peer-left" | "conflict"
	FilePath  string
	PeerID    string
	Timestamp time.Time
	Error     error
}

// NewSyncer membuat Syncer baru.
func NewSyncer(root string, trans Transport, opts ...Option) (*Syncer, error) {
	absRoot, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("resolve root path: %w", err)
	}

	// Pastikan direktori ada
	if err := os.MkdirAll(absRoot, 0755); err != nil {
		return nil, fmt.Errorf("create root dir: %w", err)
	}

	index := NewFileIndex(absRoot)

	s := &Syncer{
		root:             absRoot,
		index:            index,
		trans:            trans,
		watcher:          NewWatcher(absRoot, index),
		conflictStrategy: "last-writer-wins",
		events:           make(chan SyncEvent, 100),
		pending:          make(map[string]FileMeta),
	}

	for _, opt := range opts {
		opt(s)
	}

	return s, nil
}

// Events mengembalikan channel event sinkronisasi.
func (s *Syncer) Events() <-chan SyncEvent { return s.events }

// Index mengembalikan file index.
func (s *Syncer) Index() *FileIndex { return s.index }

// Root mengembalikan path root.
func (s *Syncer) Root() string { return s.root }

// Peers mengembalikan daftar peer.
func (s *Syncer) Peers() []PeerInfo { return s.trans.Peers() }

// SelfID mengembalikan ID peer ini.
func (s *Syncer) SelfID() string { return s.trans.SelfID() }

// Run menjalankan loop sinkronisasi utama. Blocking sampai context
// di-cancel.
func (s *Syncer) Run(ctx context.Context) error {
	// Scan awal untuk membangun index
	log.Println("Menjalankan scan awal...")
	files, err := ScanDirectory(s.root)
	if err != nil {
		return fmt.Errorf("initial scan: %w", err)
	}
	for path, meta := range files {
		s.index.Set(path, meta)
	}
	log.Printf("Scan awal selesai: %d file", len(files))

	// Start transport
	if err := s.trans.Start(ctx); err != nil {
		return fmt.Errorf("start transport: %w", err)
	}
	log.Printf("Transport siap, device: %s, ID: %s", s.deviceName, s.trans.SelfID())

	// Broadcast snapshot ke peer yang sudah ada
	if err := s.trans.BroadcastSnapshot(s.index.Snapshot()); err != nil {
		log.Printf("Broadcast snapshot awal gagal: %v", err)
	}

	// Start file watcher
	if err := s.watcher.Start(ctx); err != nil {
		return fmt.Errorf("start watcher: %w", err)
	}

	log.Println("Sinkronisasi berjalan. Menunggu perubahan...")

	for {
		select {
		case <-ctx.Done():
			return ctx.Err()

		case change := <-s.watcher.Events():
			s.handleLocalChange(ctx, change)

		case meta := <-s.trans.ReceiveMeta():
			s.handleRemoteMeta(ctx, meta)

		case transfer := <-s.trans.ReceiveFile():
			s.handleRemoteFile(ctx, transfer)

		case snapshot := <-s.trans.ReceiveSnapshot():
			s.handleRemoteSnapshot(ctx, snapshot)
		}
	}
}

func (s *Syncer) handleLocalChange(ctx context.Context, change FileChange) {
	log.Printf("Perubahan lokal: %s %s", change.Type, change.Meta.Path)

	switch change.Type {
	case ChangeCreated, ChangeModified:
		// Jika file, broadcast ke semua peer
		if !change.Meta.IsDir {
			if err := s.trans.BroadcastMeta(change.Meta); err != nil {
				log.Printf("Broadcast meta gagal: %v", err)
			}
			s.emitEvent("file-sent", change.Meta.Path, "")
		}
	case ChangeDeleted:
		// Broadcast metadata dengan hash kosong menandakan delete
		delMeta := FileMeta{Path: change.Meta.Path, Hash: ""}
		if err := s.trans.BroadcastMeta(delMeta); err != nil {
			log.Printf("Broadcast delete gagal: %v", err)
		}
	}
}

func (s *Syncer) handleRemoteMeta(ctx context.Context, meta FileMeta) {
	log.Printf("Meta diterima: %s (hash=%s)", meta.Path, shortHash(meta.Hash))

	// Jika hash kosong, ini adalah sinyal hapus
	if meta.Hash == "" {
		localPath := filepath.Join(s.root, meta.Path)
		if err := os.RemoveAll(localPath); err != nil {
			log.Printf("Hapus file gagal: %v", err)
		}
		s.index.Delete(meta.Path)
		s.emitEvent("file-received", meta.Path, "DELETE")
		return
	}

	// Cek apakah file sudah sama
	localMeta, exists := s.index.Get(meta.Path)
	if exists && localMeta.Hash == meta.Hash {
		return // Sudah sinkron
	}

	// Resolusi konflik: jika file lokal lebih baru, skip
	if exists {
		switch s.conflictStrategy {
		case "last-writer-wins":
			if localMeta.ModTime >= meta.ModTime {
				return // File lokal lebih baru, skip
			}
		default:
			// Tandai sebagai konflik
			s.emitEvent("conflict", meta.Path, "")
			return
		}
	}

	// Minta file dari pengirim
	peer := s.findPeerByMeta(meta)
	if peer == nil {
		log.Printf("Tidak ada peer yang dikenal untuk file: %s", meta.Path)
		return
	}

	reader, err := s.trans.ResolveFile(*peer, meta)
	if err != nil {
		log.Printf("Gagal meminta file %s: %v", meta.Path, err)
		return
	}

	// Simpan file
	localPath := filepath.Join(s.root, meta.Path)
	if err := os.MkdirAll(filepath.Dir(localPath), 0755); err != nil {
		log.Printf("Buat direktori gagal: %v", err)
		reader.Close()
		return
	}

	dst, err := os.Create(localPath)
	if err != nil {
		log.Printf("Buat file gagal: %v", err)
		reader.Close()
		return
	}

	written, err := io.Copy(dst, reader)
	reader.Close()
	dst.Close()
	if err != nil {
		log.Printf("Simpan file gagal: %v", err)
		return
	}

	// Update index
	meta.Size = written
	s.index.Set(meta.Path, meta)

	log.Printf("File diterima: %s (%d bytes)", meta.Path, written)
	s.emitEvent("file-received", meta.Path, "")
}

func (s *Syncer) handleRemoteFile(ctx context.Context, transfer FileTransfer) {
	// Logika sama seperti handleRemoteMeta untuk transfer langsung
	defer transfer.Data.Close()

	if transfer.Error != nil {
		log.Printf("Error transfer file %s dari %s: %v", transfer.Meta.Path, transfer.PeerID, transfer.Error)
		return
	}

	localPath := filepath.Join(s.root, transfer.Meta.Path)
	if err := os.MkdirAll(filepath.Dir(localPath), 0755); err != nil {
		log.Printf("Buat direktori gagal: %v", err)
		return
	}

	dst, err := os.Create(localPath)
	if err != nil {
		log.Printf("Buat file gagal: %v", err)
		return
	}
	defer dst.Close()

	written, err := io.Copy(dst, transfer.Data)
	if err != nil {
		log.Printf("Simpan file gagal: %v", err)
		return
	}

	transfer.Meta.Size = written
	s.index.Set(transfer.Meta.Path, transfer.Meta)

	log.Printf("File diterima (push): %s (%d bytes)", transfer.Meta.Path, written)
	s.emitEvent("file-received", transfer.Meta.Path, transfer.PeerID)
}

func (s *Syncer) handleRemoteSnapshot(ctx context.Context, remote map[string]FileMeta) {
	log.Printf("Snapshot diterima: %d file", len(remote))

	local := s.index.Snapshot()
	root := s.root

	// Cari file yang ada di remote tapi tidak di lokal atau berbeda
	for path, rMeta := range remote {
		if rMeta.IsDir {
			continue
		}

		lMeta, exists := local[path]
		if !exists || lMeta.Hash != rMeta.Hash {
			// Butuh file ini dari remote — push request via meta broadcast
			// (this peer is the "newer" one for this file)
			localPath := filepath.Join(root, path)
			if _, err := os.Stat(localPath); os.IsNotExist(err) {
				log.Printf("File hilang, request dari remote: %s", path)
				s.trans.BroadcastMeta(rMeta)
			}
		}
	}
}

func (s *Syncer) findPeerByMeta(meta FileMeta) *PeerInfo {
	// Implementasi sederhana: ambil peer pertama yang dikenal
	peers := s.trans.Peers()
	if len(peers) > 0 {
		return &peers[0]
	}
	return nil
}

func (s *Syncer) emitEvent(eventType, filePath, peerID string) {
	evt := SyncEvent{
		Type:      eventType,
		FilePath:  filePath,
		PeerID:    peerID,
		Timestamp: time.Now(),
	}
	select {
	case s.events <- evt:
	default:
	}
}

func shortHash(h string) string {
	if len(h) > 12 {
		return h[:12]
	}
	return h
}

// SyncNow memicu sinkronisasi penuh dengan semua peer.
func (s *Syncer) SyncNow(ctx context.Context) error {
	snapshot := s.index.Snapshot()
	return s.trans.BroadcastSnapshot(snapshot)
}
