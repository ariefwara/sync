// sync-mdns: Sinkronisasi folder via mDNS/DNS-SD Discovery.
//
// Peer ditemukan melalui multicast DNS (standar Apple Bonjour/Avahi).
// Bekerja dalam LAN, bisa lintas subnet.
//
// Penggunaan:
//   sync-mdns -dir /path/to/sync -name "My Laptop"
package main

import (
	"context"
	"flag"
	"fmt"
	"log"
	"os"
	"os/signal"
	"path/filepath"
	"syscall"

	"github.com/ariefwara/sync/pkg/core"
	"github.com/ariefwara/sync/pkg/transport/mdns"
)

func main() {
	syncDir := flag.String("dir", ".", "Direktori yang akan disinkronkan")
	devName := flag.String("name", "", "Nama device (default: hostname)")
	flag.Parse()

	if *devName == "" {
		hostname, _ := os.Hostname()
		*devName = hostname
	}

	absDir, err := filepath.Abs(*syncDir)
	if err != nil {
		log.Fatalf("Direktori tidak valid: %v", err)
	}

	trans := mdns.NewTransport(absDir, *devName)
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
		fmt.Println("\nMenghentikan sync-mdns...")
		cancel()
	}()

	fmt.Printf("=== sync-mdns ===\n")
	fmt.Printf("Device : %s\n", *devName)
	fmt.Printf("ID     : %s\n", syncer.SelfID())
	fmt.Printf("Folder : %s\n", absDir)
	fmt.Println("Menunggu peer via mDNS...")
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
