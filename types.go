package main

import (
	"crypto/sha256"
	"encoding/binary"
	"encoding/hex"
	"fmt"
	"io"
	"os"
	"path/filepath"
	"sync"
	"time"
)

// ---- types ----

type FileMeta struct {
	Path    string `json:"path"`
	Size    int64  `json:"size"`
	ModTime int64  `json:"mod_time"`
	Hash    string `json:"hash"`
	IsDir   bool   `json:"is_dir"`
}

type FileTransfer struct {
	Meta   FileMeta
	Data   io.ReadCloser
	PeerID string
	Error  error
}

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

type FileChange struct {
	Type ChangeType
	Meta FileMeta
}

type PeerInfo struct {
	ID      string `json:"id"`
	Name    string `json:"name"`
	Address string `json:"address"`
}

type PeerState struct {
	Peer       PeerInfo
	LastSync   time.Time
	RemoteHash string
}

type FileIndex struct {
	mu    sync.RWMutex
	files map[string]FileMeta
	root  string
}

func NewFileIndex(root string) *FileIndex {
	return &FileIndex{
		files: make(map[string]FileMeta),
		root:  root,
	}
}

func (fi *FileIndex) Root() string { return fi.root }

func (fi *FileIndex) Set(path string, meta FileMeta) {
	fi.mu.Lock()
	defer fi.mu.Unlock()
	fi.files[path] = meta
}

func (fi *FileIndex) Get(path string) (FileMeta, bool) {
	fi.mu.RLock()
	defer fi.mu.RUnlock()
	m, ok := fi.files[path]
	return m, ok
}

func (fi *FileIndex) Delete(path string) {
	fi.mu.Lock()
	defer fi.mu.Unlock()
	delete(fi.files, path)
}

func (fi *FileIndex) Snapshot() map[string]FileMeta {
	fi.mu.RLock()
	defer fi.mu.RUnlock()
	snap := make(map[string]FileMeta, len(fi.files))
	for k, v := range fi.files {
		snap[k] = v
	}
	return snap
}

func (fi *FileIndex) AllFiles() []FileMeta {
	fi.mu.RLock()
	defer fi.mu.RUnlock()
	files := make([]FileMeta, 0, len(fi.files))
	for _, v := range fi.files {
		files = append(files, v)
	}
	return files
}

// ---- hash & scan ----

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

func ScanDirectory(root string) (map[string]FileMeta, error) {
	files := make(map[string]FileMeta)

	err := filepath.Walk(root, func(absPath string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
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

// ---- binary I/O ----

func ReadInt32(r io.Reader) (int32, error) {
	var v int32
	err := binary.Read(r, binary.LittleEndian, &v)
	return v, err
}

func WriteInt32(w io.Writer, v int32) error {
	return binary.Write(w, binary.LittleEndian, v)
}

func ReadExact(r io.Reader, n int) ([]byte, error) {
	buf := make([]byte, n)
	_, err := io.ReadFull(r, buf)
	return buf, err
}

func WriteExact(w io.Writer, data []byte) error {
	_, err := w.Write(data)
	return err
}

func ReadMsg(r io.Reader) ([]byte, error) {
	length, err := ReadInt32(r)
	if err != nil {
		return nil, fmt.Errorf("read msg length: %w", err)
	}
	return ReadExact(r, int(length))
}

func WriteMsg(w io.Writer, data []byte) error {
	if err := WriteInt32(w, int32(len(data))); err != nil {
		return fmt.Errorf("write msg length: %w", err)
	}
	return WriteExact(w, data)
}
