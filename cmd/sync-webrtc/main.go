// sync-webrtc: Sinkronisasi folder via WebRTC P2P.
//
// Menggunakan WebRTC DataChannel untuk transfer file P2P.
// Bisa menembus NAT/firewall. Signaling via stdin/stdout
// (copy-paste SDP).
//
// Penggunaan:
//   Peer A: sync-webrtc -dir /path/to/sync -name "Laptop A" -offer
//   Peer B: sync-webrtc -dir /path/to/sync -name "Laptop B"
//
// Copy SDP Offer dari Peer A, paste ke Peer B, lalu copy
// SDP Answer dari Peer B kembali ke Peer A.
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
	"github.com/ariefwara/sync/pkg/transport/webrtc"
)

func main() {
	syncDir := flag.String("dir", ".", "Direktori yang akan disinkronkan")
	devName := flag.String("name", "", "Nama device (default: hostname)")
	makeOffer := flag.Bool("offer", false, "Jadikan peer ini sebagai offerer")
	peerName := flag.String("peer", "", "Nama peer target")
	stunFlag := flag.String("stun", "stun:stun.l.google.com:19302", "STUN server (pisahkan dengan koma)")
	flag.Parse()

	if *devName == "" {
		hostname, _ := os.Hostname()
		*devName = hostname
	}
	if *peerName == "" {
		*peerName = "remote-peer"
	}
	*peerName = fmt.Sprintf("%s-%d", *peerName, os.Getpid()%1000)

	absDir, err := filepath.Abs(*syncDir)
	if err != nil {
		log.Fatalf("Direktori tidak valid: %v", err)
	}

	stunServers := strings.Split(*stunFlag, ",")
	for i, s := range stunServers {
		stunServers[i] = strings.TrimSpace(s)
	}

	trans := webrtc.NewTransport(absDir, *devName, stunServers)
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
		fmt.Println("\nMenghentikan sync-webrtc...")
		cancel()
	}()

	fmt.Printf("=== sync-webrtc (WebRTC P2P) ===\n")
	fmt.Printf("Device : %s\n", *devName)
	fmt.Printf("Folder : %s\n", absDir)
	fmt.Printf("STUN   : %v\n", stunServers)
	fmt.Println()

	// Start signaling console
	trans.StartSignalingConsole(ctx)

	// Jika offerer, buat koneksi
	if *makeOffer {
		fmt.Println("Membuat koneksi...")
		if err := trans.ConnectToPeer(ctx, *peerName, *peerName); err != nil {
			log.Printf("Koneksi gagal: %v", err)
		}
	}

	// Monitor incoming signaling dan accept connection
	go func() {
		for msg := range trans.SignalingOutput() {
			if msg.Type == "offer" {
				fmt.Println("\nMenerima SDP offer, membuat answer...")
				if err := trans.AcceptConnection(ctx, msg); err != nil {
					log.Printf("Accept koneksi gagal: %v", err)
				}
			}
		}
	}()

	go func() {
		for evt := range syncer.Events() {
			log.Printf("[%s] %s", evt.Type, evt.FilePath)
		}
	}()

	if err := syncer.Run(ctx); err != nil && err != context.Canceled {
		log.Fatalf("Sync error: %v", err)
	}
}
