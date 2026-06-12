// sync — sinkronisasi folder P2P via LAN.
//
// Penggunaan:
//   sync .          sync folder saat ini
//   sync /path      sync folder tertentu
//
// Nama komputer diambil otomatis dari OS. Jika port sudah dipakai
// (instance lain sudah jalan), maka yang kedua langsung exit.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/ariefwara/sync/pkg/core"
	"github.com/ariefwara/sync/pkg/transport/lan"
)

func main() {
	// Tentukan folder: argumen pertama, atau current dir
	syncDir := "."
	if len(os.Args) > 1 && !strings.HasPrefix(os.Args[1], "-") {
		syncDir = os.Args[1]
	}

	absDir, err := filepath.Abs(syncDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}

	// Cek apakah direktori valid
	if info, err := os.Stat(absDir); err != nil {
		fmt.Fprintf(os.Stderr, "error: direktori %s tidak ditemukan\n", absDir)
		os.Exit(1)
	} else if !info.IsDir() {
		fmt.Fprintf(os.Stderr, "error: %s bukan direktori\n", absDir)
		os.Exit(1)
	}

	hostname, _ := os.Hostname()

	trans := lan.NewTransport(absDir, hostname)
	syncer, err := core.NewSyncer(absDir, trans, core.WithDeviceName(hostname))
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	// Trap Ctrl+C
	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		fmt.Println("\nberhenti.")
		cancel()
	}()

	fmt.Printf("sync — mensinkronkan %s\n", absDir)
	fmt.Printf("      device: %s\n", hostname)
	fmt.Println("      menunggu peer di LAN...")
	fmt.Println()

	// Monitor event
	go func() {
		for evt := range syncer.Events() {
			switch evt.Type {
			case "file-sent":
				fmt.Printf("  ↑ %s\n", evt.FilePath)
			case "file-received":
				fmt.Printf("  ↓ %s\n", evt.FilePath)
			case "peer-joined":
				fmt.Printf("  + peer bergabung\n")
			case "peer-left":
				fmt.Printf("  - peer pergi\n")
			}
		}
	}()

	if err := syncer.Run(ctx); err != nil && err != context.Canceled {
		// Cek apakah error karena port already in use
		if strings.Contains(err.Error(), "address already in use") ||
			strings.Contains(err.Error(), "EADDRINUSE") {
			fmt.Fprintf(os.Stderr, "sync sudah berjalan di folder ini (port 43211 sudah dipakai)\n")
			os.Exit(1)
		}
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}
