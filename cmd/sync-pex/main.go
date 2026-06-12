// sync-pex: Sinkronisasi folder via Peer Exchange (PEX).
//
// Peer ditemukan dengan saling bertukar daftar peer yang dikenal.
// Mirip mekanisme PEX di BitTorrent. Bisa bekerja di internet.
//
// Penggunaan:
//   sync-pex -dir /path/to/sync -name "My Laptop" -peers 192.168.1.5:43215
//
// Peer pertama:
//   sync-pex -dir /path/to/sync -name "Server"
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"

	"github.com/ariefwara/sync/pkg/core"
	"github.com/ariefwara/sync/pkg/transport/pex"
)

func main() {
	syncDir := flag.String("dir", ".", "Direktori yang akan disinkronkan")
	devName := flag.String("name", "", "Nama device (default: hostname)")
	peersFlag := flag.String("peers", "", "Peer awal (host:port, pisahkan dengan koma)")
	flag.Parse()

	if *devName == "" {
		hostname, _ := os.Hostname()
		*devName = hostname
	}

	absDir, err := filepath.Abs(*syncDir)
	if err != nil {
		log.Fatalf("Direktori tidak valid: %v", err)
	}

	var initialPeers []string
	if *peersFlag != "" {
		initialPeers = strings.Split(*peersFlag, ",")
		for i, p := range initialPeers {
			initialPeers[i] = strings.TrimSpace(p)
		}
	}

	trans := pex.NewTransport(absDir, *devName, initialPeers)
	syncer, err := core.NewSyncer(absDir, trans, core.WithDeviceName(*devName))
	if err != nil {
		log.Fatalf("Gagal inisialisasi syncer: %v", err)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		fmt.Println("\nMenghentikan sync-pex...")
		cancel()
	}()

	fmt.Printf("=== sync-pex (Peer Exchange) ===\n")
	fmt.Printf("Device : %s\n", *devName)
	fmt.Printf("ID     : %s\n", syncer.SelfID())
	fmt.Printf("Folder : %s\n", absDir)
	if len(initialPeers) > 0 {
		fmt.Printf("Peers  : %v\n", initialPeers)
	} else {
		fmt.Println("Mode: first peer (tunggu koneksi masuk)")
	}
	fmt.Println()

	go func() {
		for evt := range syncer.Events() {
			log.Printf("[%s] %s", evt.Type, evt.FilePath)
		}
	}()

	if err := syncer.Run(ctx); err != nil && err != context.Canceled {
		log.Fatalf("Sync error: %v", err)
	}
}
