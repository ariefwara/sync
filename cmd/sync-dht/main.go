// sync-dht: Sinkronisasi folder via Kademlia DHT Discovery.
//
// Peer ditemukan melalui Distributed Hash Table (Kademlia).
// Bekerja di internet tanpa server khusus.
//
// Penggunaan:
//   sync-dht -dir /path/to/sync -name "My Laptop" -bootstrap 192.168.1.5:43212
//
// Node pertama di jaringan:
//   sync-dht -dir /path/to/sync -name "Server"
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
	"github.com/ariefwara/sync/pkg/transport/dht"
)

func main() {
	syncDir := flag.String("dir", ".", "Direktori yang akan disinkronkan")
	devName := flag.String("name", "", "Nama device (default: hostname)")
	bootstrapFlag := flag.String("bootstrap", "", "Node bootstrap (host:port, pisahkan dengan koma untuk multiple)")
	flag.Parse()

	if *devName == "" {
		hostname, _ := os.Hostname()
		*devName = hostname
	}

	absDir, err := filepath.Abs(*syncDir)
	if err != nil {
		log.Fatalf("Direktori tidak valid: %v", err)
	}

	var bootstraps []string
	if *bootstrapFlag != "" {
		bootstraps = strings.Split(*bootstrapFlag, ",")
		for i, bs := range bootstraps {
			bootstraps[i] = strings.TrimSpace(bs)
		}
	}

	trans := dht.NewTransport(absDir, *devName, bootstraps)
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
		fmt.Println("\nMenghentikan sync-dht...")
		cancel()
	}()

	fmt.Printf("=== sync-dht (Kademlia DHT) ===\n")
	fmt.Printf("Device  : %s\n", *devName)
	fmt.Printf("Node ID : %s\n", trans.NodeID())
	fmt.Printf("Folder  : %s\n", absDir)
	fmt.Printf("DHT Port: %d\n", dht.DHTPort())
	if len(bootstraps) > 0 {
		fmt.Printf("Bootstrap: %v\n", bootstraps)
	} else {
		fmt.Println("Mode: first node in DHT network")
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
