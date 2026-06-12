// Package core menyediakan mesin sinkronisasi file yang digunakan oleh
// semua opsi transport. Mencakup file index, file watcher, dan logika
// sinkronisasi utama.
package core

import (
	"crypto/sha256"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// FileMeta menyimpan metadata sebuah file.
type FileMeta struct {
	Path    string `json:"path"`
	Size    int64  `json:"size"`
	ModTime int64  `json:"mod_time"` // Unix nano
	Hash    string `json:"hash"`     // SHA256 hex
	IsDir   bool   `json:"is_dir"`
}

// FileTransfer merepresentasikan file yang sedang dikirim/diterima.
type FileTransfer struct {
	Meta   FileMeta
	Data   io.ReadCloser
	PeerID string
	Error  error
}

// ChangeType mendeskripsikan jenis perubahan file.
type ChangeType int

const (
	ChangeCreated ChangeType = iota
	ChangeModified
	ChangeDeleted
)

func (ct ChangeType) String() string {
	switch ct {
	case ChangeCreated:
		return "created"
	case ChangeModified:
		return "modified"
	case ChangeDeleted:
		return "deleted"
	default:
		return "unknown"
	}
}

// FileChange merepresentasikan satu perubahan file.
type FileChange struct {
	Type ChangeType
	Meta FileMeta
}

// PeerInfo menyimpan informasi tentang peer lain.
type PeerInfo struct {
	ID      string `json:"id"`
	Name    string `json:"name"`
	Address string `json:"address"` // host:port
}

// PeerState menyimpan state sinkronisasi dengan satu peer.
type PeerState struct {
	Peer       PeerInfo
	LastSync   time.Time
	RemoteHash string // hash snapshot terakhir dari remote
}

// FileIndex adalah thread-safe index dari semua file yang di-track.
// Menyimpan hash SHA256 untuk mendeteksi perubahan konten.
type FileIndex struct {
	mu     sync.RWMutex
	files  map[string]FileMeta // key = relative path
	root   string
}

// NewFileIndex membuat FileIndex baru untuk direktori root.
func NewFileIndex(root string) *FileIndex {
	return &FileIndex{
		files: make(map[string]FileMeta),
		root:  root,
	}
}

// Root mengembalikan path root.
func (fi *FileIndex) Root() string { return fi.root }

// Set memperbarui atau menambahkan file ke index.
func (fi *FileIndex) Set(path string, meta FileMeta) {
	fi.mu.Lock()
	defer fi.mu.Unlock()
	fi.files[path] = meta
}

// Get mengambil metadata file dari index.
func (fi *FileIndex) Get(path string) (FileMeta, bool) {
	fi.mu.RLock()
	defer fi.mu.RUnlock()
	m, ok := fi.files[path]
	return m, ok
}

// Delete menghapus file dari index.
func (fi *FileIndex) Delete(path string) {
	fi.mu.Lock()
	defer fi.mu.Unlock()
	delete(fi.files, path)
}

// Snapshot mengembalikan salinan semua file yang di-track.
func (fi *FileIndex) Snapshot() map[string]FileMeta {
	fi.mu.RLock()
	defer fi.mu.RUnlock()
	snap := make(map[string]FileMeta, len(fi.files))
	for k, v := range fi.files {
		snap[k] = v
	}
	return snap
}

// AllFiles mengembalikan semua file yang di-track.
func (fi *FileIndex) AllFiles() []FileMeta {
	fi.mu.RLock()
	defer fi.mu.RUnlock()
	files := make([]FileMeta, 0, len(fi.files))
	for _, v := range fi.files {
		files = append(files, v)
	}
	return files
}

// HashFile menghitung SHA256 hash dari sebuah file.
func HashFile(path string) (string, error) {
	f, err := os.Open(path)
	if err != nil {
		return "", fmt.Errorf("open file for hash: %w", err)
	}
	defer f.Close()

	h := sha256.New()
	if _, err := io.Copy(h, f); err != nil {
		return "", fmt.Errorf("hash file: %w", err)
	}
	return hex.EncodeToString(h.Sum(nil)), nil
}

// ScanDirectory memindai direktori dan mengembalikan semua file beserta hash-nya.
func ScanDirectory(root string) (map[string]FileMeta, error) {
	files := make(map[string]FileMeta)

	err := filepath.Walk(root, func(absPath string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}

		// Skip root directory itself
		if absPath == root {
			return nil
		}

		relPath, err := filepath.Rel(root, absPath)
		if err != nil {
			return err
		}

		meta := FileMeta{
			Path:    relPath,
			Size:    info.Size(),
			ModTime: info.ModTime().UnixNano(),
			IsDir:   info.IsDir(),
		}

		if !info.IsDir() {
			hash, err := HashFile(absPath)
			if err != nil {
				return err
			}
			meta.Hash = hash
		}

		files[relPath] = meta
		return nil
	})

	return files, err
}
