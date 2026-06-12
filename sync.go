package main

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

// ---- transport interface ----

type Transport interface {
	Start(ctx context.Context) error
	Stop() error
	SelfID() string
	Peers() []PeerInfo
	SendMeta(peer PeerInfo, meta FileMeta) error
	SendFile(peer PeerInfo, meta FileMeta, data io.Reader) error
	ReceiveMeta() <-chan FileMeta
	ReceiveFile() <-chan FileTransfer
	ResolveFile(peer PeerInfo, meta FileMeta) (io.ReadCloser, error)
	BroadcastMeta(meta FileMeta) error
	BroadcastSnapshot(snapshot map[string]FileMeta) error
	ReceiveSnapshot() <-chan map[string]FileMeta
}

// ---- syncer ----

type Option func(*Syncer)

func WithConflictStrategy(strategy string) Option {
	return func(s *Syncer) {
		s.conflictStrategy = strategy
	}
}

func WithDeviceName(name string) Option {
	return func(s *Syncer) {
		s.deviceName = name
	}
}

type Syncer struct {
	root    string
	index   *FileIndex
	watcher *Watcher
	trans   Transport

	deviceName       string
	conflictStrategy string
	events           chan SyncEvent
	pending          map[string]FileMeta
	mu               sync.Mutex
}

type SyncEvent struct {
	Type      string
	FilePath  string
	PeerID    string
	Timestamp time.Time
	Error     error
}

func NewSyncer(root string, trans Transport, opts ...Option) (*Syncer, error) {
	absRoot, err := filepath.Abs(root)
	if err != nil {
		return nil, fmt.Errorf("resolve root path: %w", err)
	}

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

func (s *Syncer) Events() <-chan SyncEvent        { return s.events }
func (s *Syncer) Index() *FileIndex                { return s.index }
func (s *Syncer) Root() string                     { return s.root }
func (s *Syncer) Peers() []PeerInfo                { return s.trans.Peers() }
func (s *Syncer) SelfID() string                   { return s.trans.SelfID() }

func (s *Syncer) Run(ctx context.Context) error {
	log.Println("Running initial scan...")
	files, err := ScanDirectory(s.root)
	if err != nil {
		return fmt.Errorf("initial scan: %w", err)
	}
	for path, meta := range files {
		s.index.Set(path, meta)
	}
	log.Printf("Initial scan complete: %d files", len(files))

	if err := s.trans.Start(ctx); err != nil {
		return fmt.Errorf("start transport: %w", err)
	}
	log.Printf("Transport ready, device: %s, ID: %s", s.deviceName, s.trans.SelfID())

	if err := s.trans.BroadcastSnapshot(s.index.Snapshot()); err != nil {
		log.Printf("Initial snapshot broadcast failed: %v", err)
	}

	if err := s.watcher.Start(ctx); err != nil {
		return fmt.Errorf("start watcher: %w", err)
	}

	log.Println("Sync running. Waiting for changes...")

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
	log.Printf("Local change: %s %s", change.Type, change.Meta.Path)

	switch change.Type {
	case ChangeCreated, ChangeModified:
		if !change.Meta.IsDir {
			if err := s.trans.BroadcastMeta(change.Meta); err != nil {
				log.Printf("Broadcast meta failed: %v", err)
			}
			s.emitEvent("file-sent", change.Meta.Path, "")
		}
	case ChangeDeleted:
		delMeta := FileMeta{Path: change.Meta.Path, Hash: ""}
		if err := s.trans.BroadcastMeta(delMeta); err != nil {
			log.Printf("Broadcast delete failed: %v", err)
		}
	}
}

func (s *Syncer) handleRemoteMeta(ctx context.Context, meta FileMeta) {
	log.Printf("Meta received: %s (hash=%s)", meta.Path, shortHash(meta.Hash))

	if meta.Hash == "" {
		localPath := filepath.Join(s.root, meta.Path)
		if err := os.RemoveAll(localPath); err != nil {
			log.Printf("Remove file failed: %v", err)
		}
		s.index.Delete(meta.Path)
		s.emitEvent("file-received", meta.Path, "DELETE")
		return
	}

	localMeta, exists := s.index.Get(meta.Path)
	if exists && localMeta.Hash == meta.Hash {
		return
	}

	if exists {
		switch s.conflictStrategy {
		case "last-writer-wins":
			if localMeta.ModTime >= meta.ModTime {
				return
			}
		default:
			s.emitEvent("conflict", meta.Path, "")
			return
		}
	}

	peer := s.findPeerByMeta(meta)
	if peer == nil {
		log.Printf("No known peer for file: %s", meta.Path)
		return
	}

	reader, err := s.trans.ResolveFile(*peer, meta)
	if err != nil {
		log.Printf("Failed to request file %s: %v", meta.Path, err)
		return
	}

	localPath := filepath.Join(s.root, meta.Path)
	if err := os.MkdirAll(filepath.Dir(localPath), 0755); err != nil {
		log.Printf("Create directory failed: %v", err)
		reader.Close()
		return
	}

	dst, err := os.Create(localPath)
	if err != nil {
		log.Printf("Create file failed: %v", err)
		reader.Close()
		return
	}

	written, err := io.Copy(dst, reader)
	reader.Close()
	dst.Close()
	if err != nil {
		log.Printf("Save file failed: %v", err)
		return
	}

	meta.Size = written
	s.index.Set(meta.Path, meta)

	log.Printf("File received: %s (%d bytes)", meta.Path, written)
	s.emitEvent("file-received", meta.Path, "")
}

func (s *Syncer) handleRemoteFile(ctx context.Context, transfer FileTransfer) {
	defer transfer.Data.Close()

	if transfer.Error != nil {
		log.Printf("File transfer error %s from %s: %v", transfer.Meta.Path, transfer.PeerID, transfer.Error)
		return
	}

	localPath := filepath.Join(s.root, transfer.Meta.Path)
	if err := os.MkdirAll(filepath.Dir(localPath), 0755); err != nil {
		log.Printf("Create directory failed: %v", err)
		return
	}

	dst, err := os.Create(localPath)
	if err != nil {
		log.Printf("Create file failed: %v", err)
		return
	}
	defer dst.Close()

	written, err := io.Copy(dst, transfer.Data)
	if err != nil {
		log.Printf("Save file failed: %v", err)
		return
	}

	transfer.Meta.Size = written
	s.index.Set(transfer.Meta.Path, transfer.Meta)

	log.Printf("File received (push): %s (%d bytes)", transfer.Meta.Path, written)
	s.emitEvent("file-received", transfer.Meta.Path, transfer.PeerID)
}

func (s *Syncer) handleRemoteSnapshot(ctx context.Context, remote map[string]FileMeta) {
	log.Printf("Snapshot received: %d files", len(remote))

	local := s.index.Snapshot()
	root := s.root

	for path, rMeta := range remote {
		if rMeta.IsDir {
			continue
		}

		lMeta, exists := local[path]
		if !exists || lMeta.Hash != rMeta.Hash {
			localPath := filepath.Join(root, path)
			if _, err := os.Stat(localPath); os.IsNotExist(err) {
				log.Printf("File missing, requesting from remote: %s", path)
				s.trans.BroadcastMeta(rMeta)
			}
		}
	}
}

func (s *Syncer) findPeerByMeta(meta FileMeta) *PeerInfo {
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

func (s *Syncer) SyncNow(ctx context.Context) error {
	snapshot := s.index.Snapshot()
	return s.trans.BroadcastSnapshot(snapshot)
}
