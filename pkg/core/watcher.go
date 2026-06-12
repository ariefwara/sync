package core

import (
	"context"
	"log"
	"os"
	"path/filepath"
	"time"

	"github.com/fsnotify/fsnotify"
)

// Watcher memonitor filesystem untuk perubahan dan melaporkannya
// melalui channel. Menggunakan fsnotify dengan fallback polling.
type Watcher struct {
	root    string
	index   *FileIndex
	events  chan FileChange
	pollInterval time.Duration
}

// NewWatcher membuat Watcher baru.
func NewWatcher(root string, index *FileIndex) *Watcher {
	return &Watcher{
		root:         root,
		index:        index,
		events:       make(chan FileChange, 100),
		pollInterval: 10 * time.Second,
	}
}

// Events mengembalikan channel perubahan file.
func (w *Watcher) Events() <-chan FileChange { return w.events }

// Start memulai monitoring. Menggunakan fsnotify untuk event real-time,
// dilengkapi periodic scan untuk mendeteksi perubahan yang terlewat.
func (w *Watcher) Start(ctx context.Context) error {
	watcher, err := fsnotify.NewWatcher()
	if err != nil {
		return err
	}

	// Watch root directory dan semua subdirectory
	err = filepath.Walk(w.root, func(path string, info os.FileInfo, err error) error {
		if err != nil {
			return err
		}
		if info.IsDir() {
			return watcher.Add(path)
		}
		return nil
	})
	if err != nil {
		watcher.Close()
		return err
	}

	// Periodic polling goroutine
	go func() {
		ticker := time.NewTicker(w.pollInterval)
		defer ticker.Stop()
		for {
			select {
			case <-ctx.Done():
				watcher.Close()
				return
			case <-ticker.C:
				w.pollChanges()
			case event, ok := <-watcher.Events:
				if !ok {
					return
				}
				w.handleEvent(event)
			case err, ok := <-watcher.Errors:
				if !ok {
					return
				}
				log.Printf("fsnotify error: %v", err)
			}
		}
	}()

	return nil
}

func (w *Watcher) handleEvent(event fsnotify.Event) {
	relPath, err := filepath.Rel(w.root, event.Name)
	if err != nil {
		return
	}

	if event.Has(fsnotify.Create) || event.Has(fsnotify.Write) {
		info, err := os.Stat(event.Name)
		if err != nil {
			return
		}

		meta := FileMeta{
			Path:    relPath,
			Size:    info.Size(),
			ModTime: info.ModTime().UnixNano(),
			IsDir:   info.IsDir(),
		}

		if !info.IsDir() {
			hash, err := HashFile(event.Name)
			if err != nil {
				return
			}
			meta.Hash = hash
		}

		oldMeta, exists := w.index.Get(relPath)

		// Only report if content actually changed
		if exists && oldMeta.Hash == meta.Hash && !meta.IsDir {
			return
		}

		w.index.Set(relPath, meta)

		changeType := ChangeModified
		if !exists {
			changeType = ChangeCreated
		}

		w.events <- FileChange{Type: changeType, Meta: meta}

		// If new directory, add it to fsnotify watch
		if info.IsDir() && changeType == ChangeCreated {
			if w2, err := fsnotify.NewWatcher(); err == nil {
				w2.Add(event.Name)
			}
		}
	}

	if event.Has(fsnotify.Remove) || event.Has(fsnotify.Rename) {
		w.index.Delete(relPath)
		w.events <- FileChange{
			Type: ChangeDeleted,
			Meta: FileMeta{Path: relPath},
		}
	}
}

// pollChanges melakukan scan penuh untuk mendeteksi perubahan yang
// mungkin terlewat oleh fsnotify (misalnya, file yang dimodifikasi
// terlalu cepat).
func (w *Watcher) pollChanges() {
	current, err := ScanDirectory(w.root)
	if err != nil {
		log.Printf("scan error: %v", err)
		return
	}

	snapshot := w.index.Snapshot()

	// Detect new and modified files
	for path, meta := range current {
		oldMeta, exists := snapshot[path]
		if !exists || oldMeta.Hash != meta.Hash {
			w.index.Set(path, meta)
			changeType := ChangeModified
			if !exists {
				changeType = ChangeCreated
			}
			w.events <- FileChange{Type: changeType, Meta: meta}
		}
	}

	// Detect deleted files (not in current scan but still in index)
	for path := range snapshot {
		if _, exists := current[path]; !exists {
			if !snapshot[path].IsDir { // Skip directories; Walk handles them
				w.index.Delete(path)
				w.events <- FileChange{
					Type: ChangeDeleted,
					Meta: FileMeta{Path: path},
				}
			}
		}
	}
}
