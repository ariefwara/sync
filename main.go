// lan-sync — P2P folder sync over LAN.
//
// Usage:
//   lan-sync .          sync current directory
//   lan-sync /path      sync a specific directory
//
// Hostname is auto-detected from the OS. If the port is already in use
// (another instance is running), the second one exits immediately.
package main

import (
	"context"
	"fmt"
	"os"
	"os/signal"
	"path/filepath"
	"strings"
	"syscall"
)

func main() {
	syncDir := "."
	if len(os.Args) > 1 && !strings.HasPrefix(os.Args[1], "-") {
		syncDir = os.Args[1]
	}

	absDir, err := filepath.Abs(syncDir)
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}

	if info, err := os.Stat(absDir); err != nil {
		fmt.Fprintf(os.Stderr, "error: directory %s not found\n", absDir)
		os.Exit(1)
	} else if !info.IsDir() {
		fmt.Fprintf(os.Stderr, "error: %s is not a directory\n", absDir)
		os.Exit(1)
	}

	hostname, _ := os.Hostname()

	trans := NewTransport(absDir, hostname)
	syncer, err := NewSyncer(absDir, trans, WithDeviceName(hostname))
	if err != nil {
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()

	sigCh := make(chan os.Signal, 1)
	signal.Notify(sigCh, syscall.SIGINT, syscall.SIGTERM)
	go func() {
		<-sigCh
		fmt.Println("\nshutdown.")
		cancel()
	}()

	fmt.Printf("lan-sync — syncing %s\n", absDir)
	fmt.Printf("      device: %s\n", hostname)
	fmt.Println("      waiting for peers on LAN...")
	fmt.Println()

	go func() {
		for evt := range syncer.Events() {
			switch evt.Type {
			case "file-sent":
				fmt.Printf("  ↑ %s\n", evt.FilePath)
			case "file-received":
				fmt.Printf("  ↓ %s\n", evt.FilePath)
			case "peer-joined":
				fmt.Printf("  + peer joined\n")
			case "peer-left":
				fmt.Printf("  - peer left\n")
			}
		}
	}()

	if err := syncer.Run(ctx); err != nil && err != context.Canceled {
		if strings.Contains(err.Error(), "address already in use") ||
			strings.Contains(err.Error(), "EADDRINUSE") {
			fmt.Fprintf(os.Stderr, "lan-sync is already running (port 43211 is in use)\n")
			os.Exit(1)
		}
		fmt.Fprintf(os.Stderr, "error: %v\n", err)
		os.Exit(1)
	}
}
