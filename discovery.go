package main

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"sync"
	"time"
)

const (
	discoveryPort = 43210
	transferPort  = 43211
	broadcastAddr = "255.255.255.255"
	pingInterval  = 5 * time.Second
	peerTimeout   = 30 * time.Second
	acceptTimeout = 1 * time.Second
)

type DiscoveryMessage struct {
	Type    string `json:"type"`
	ID      string `json:"id"`
	Name    string `json:"name"`
	Address string `json:"address"`
}

type LanTransport struct {
	id      string
	name    string
	syncDir string

	peers   map[string]PeerInfo
	peersMu sync.RWMutex

	metaCh     chan FileMeta
	fileCh     chan FileTransfer
	snapshotCh chan map[string]FileMeta

	udpConn *net.UDPConn
	tcpLn   net.Listener

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func NewTransport(syncDir, deviceName string) *LanTransport {
	hostname, _ := os.Hostname()
	id := fmt.Sprintf("lan-%s-%d", hostname, time.Now().UnixNano())
	return &LanTransport{
		id:         id,
		name:       deviceName,
		syncDir:    syncDir,
		peers:      make(map[string]PeerInfo),
		metaCh:     make(chan FileMeta, 100),
		fileCh:     make(chan FileTransfer, 50),
		snapshotCh: make(chan map[string]FileMeta, 10),
	}
}

func (t *LanTransport) SelfID() string                                  { return t.id }
func (t *LanTransport) ReceiveMeta() <-chan FileMeta                    { return t.metaCh }
func (t *LanTransport) ReceiveFile() <-chan FileTransfer                { return t.fileCh }
func (t *LanTransport) ReceiveSnapshot() <-chan map[string]FileMeta     { return t.snapshotCh }

func (t *LanTransport) Peers() []PeerInfo {
	t.peersMu.RLock()
	defer t.peersMu.RUnlock()
	list := make([]PeerInfo, 0, len(t.peers))
	for _, p := range t.peers {
		list = append(list, p)
	}
	return list
}

func (t *LanTransport) Start(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	t.cancel = cancel

	udpAddr, err := net.ResolveUDPAddr("udp", fmt.Sprintf(":%d", discoveryPort))
	if err != nil {
		return fmt.Errorf("resolve UDP: %w", err)
	}
	udpConn, err := net.ListenUDP("udp", udpAddr)
	if err != nil {
		return fmt.Errorf("listen UDP: %w", err)
	}
	t.udpConn = udpConn

	tcpLn, err := net.Listen("tcp", fmt.Sprintf(":%d", transferPort))
	if err != nil {
		udpConn.Close()
		return fmt.Errorf("listen TCP: %w", err)
	}
	t.tcpLn = tcpLn

	t.wg.Add(3)
	go t.broadcastPings(ctx)
	go t.listenDiscovery(ctx)
	go t.listenTransfers(ctx)

	return nil
}

func (t *LanTransport) Stop() error {
	if t.cancel != nil {
		t.cancel()
	}
	t.wg.Wait()
	if t.udpConn != nil {
		t.udpConn.Close()
	}
	if t.tcpLn != nil {
		t.tcpLn.Close()
	}
	return nil
}

func (t *LanTransport) broadcastPings(ctx context.Context) {
	defer t.wg.Done()

	localAddr := t.getLocalIP()
	msg := DiscoveryMessage{
		Type:    "ping",
		ID:      t.id,
		Name:    t.name,
		Address: fmt.Sprintf("%s:%d", localAddr, transferPort),
	}
	data, _ := json.Marshal(msg)

	bcastAddr, err := net.ResolveUDPAddr("udp", fmt.Sprintf("%s:%d", broadcastAddr, discoveryPort))
	if err != nil {
		log.Printf("resolve broadcast addr: %v", err)
		return
	}

	t.sendDiscovery(data, bcastAddr)

	ticker := time.NewTicker(pingInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			t.sendDiscovery(data, bcastAddr)
		}
	}
}

func (t *LanTransport) sendDiscovery(data []byte, addr *net.UDPAddr) {
	conn, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		return
	}
	defer conn.Close()
	conn.Write(data)
}

func (t *LanTransport) listenDiscovery(ctx context.Context) {
	defer t.wg.Done()

	buf := make([]byte, 2048)
	for {
		select {
		case <-ctx.Done():
			return
		default:
			t.udpConn.SetReadDeadline(time.Now().Add(1 * time.Second))
			n, addr, err := t.udpConn.ReadFromUDP(buf)
			if err != nil {
				if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					continue
				}
				return
			}

			var msg DiscoveryMessage
			if err := json.Unmarshal(buf[:n], &msg); err != nil {
				continue
			}
			if msg.ID == t.id {
				continue
			}

			switch msg.Type {
			case "ping":
				resp := DiscoveryMessage{
					Type:    "pong",
					ID:      t.id,
					Name:    t.name,
					Address: fmt.Sprintf("%s:%d", t.getLocalIP(), transferPort),
				}
				respData, _ := json.Marshal(resp)
				t.udpConn.WriteToUDP(respData, addr)
				t.addPeer(msg.ID, msg.Name, msg.Address)
			case "pong":
				t.addPeer(msg.ID, msg.Name, msg.Address)
			}
		}
	}
}

func (t *LanTransport) listenTransfers(ctx context.Context) {
	defer t.wg.Done()

	for {
		select {
		case <-ctx.Done():
			return
		default:
			tcpLn := t.tcpLn.(*net.TCPListener)
			tcpLn.SetDeadline(time.Now().Add(acceptTimeout))
			conn, err := tcpLn.Accept()
			if err != nil {
				if netErr, ok := err.(net.Error); ok && netErr.Timeout() {
					continue
				}
				return
			}
			go t.handleTransfer(conn)
		}
	}
}

func (t *LanTransport) handleTransfer(conn net.Conn) {
	defer conn.Close()

	headerBuf, err := ReadMsg(conn)
	if err != nil {
		return
	}

	var msg struct {
		Type     string              `json:"type"`
		Meta     FileMeta            `json:"meta,omitempty"`
		Snapshot map[string]FileMeta `json:"snapshot,omitempty"`
	}
	if err := json.Unmarshal(headerBuf, &msg); err != nil {
		return
	}

	switch msg.Type {
	case "meta":
		t.metaCh <- msg.Meta
	case "file":
		t.fileCh <- FileTransfer{
			Meta: msg.Meta,
			Data: io.NopCloser(conn),
		}
	case "snapshot":
		t.snapshotCh <- msg.Snapshot
	case "request":
		localPath := t.syncDir + "/" + msg.Meta.Path
		t.sendFileResponse(conn, msg.Meta, localPath)
	}
}

func (t *LanTransport) sendFileResponse(conn net.Conn, meta FileMeta, localPath string) {
	f, err := os.Open(localPath)
	if err != nil {
		return
	}
	defer f.Close()

	resp := struct {
		Type string   `json:"type"`
		Meta FileMeta `json:"meta"`
	}{"file-response", meta}
	respData, _ := json.Marshal(resp)

	WriteMsg(conn, respData)
	io.Copy(conn, f)
}

func (t *LanTransport) SendMeta(peer PeerInfo, meta FileMeta) error {
	conn, err := net.Dial("tcp", peer.Address)
	if err != nil {
		return err
	}
	defer conn.Close()

	msg := struct {
		Type string   `json:"type"`
		Meta FileMeta `json:"meta"`
	}{"meta", meta}
	data, _ := json.Marshal(msg)
	return WriteMsg(conn, data)
}

func (t *LanTransport) SendFile(peer PeerInfo, meta FileMeta, data io.Reader) error {
	conn, err := net.Dial("tcp", peer.Address)
	if err != nil {
		return err
	}
	defer conn.Close()

	msg := struct {
		Type string   `json:"type"`
		Meta FileMeta `json:"meta"`
	}{"file", meta}
	msgData, _ := json.Marshal(msg)
	if err := WriteMsg(conn, msgData); err != nil {
		return err
	}
	_, err = io.Copy(conn, data)
	return err
}

func (t *LanTransport) ResolveFile(peer PeerInfo, meta FileMeta) (io.ReadCloser, error) {
	conn, err := net.Dial("tcp", peer.Address)
	if err != nil {
		return nil, err
	}

	msg := struct {
		Type string   `json:"type"`
		Meta FileMeta `json:"meta"`
	}{"request", meta}
	data, _ := json.Marshal(msg)
	if err := WriteMsg(conn, data); err != nil {
		conn.Close()
		return nil, err
	}

	_, err = ReadMsg(conn)
	if err != nil {
		conn.Close()
		return nil, err
	}

	return conn, nil
}

func (t *LanTransport) BroadcastMeta(meta FileMeta) error {
	for _, p := range t.Peers() {
		if err := t.SendMeta(p, meta); err != nil {
			log.Printf("Broadcast meta ke %s gagal: %v", p.ID, err)
		}
	}
	return nil
}

func (t *LanTransport) BroadcastSnapshot(snapshot map[string]FileMeta) error {
	for _, peer := range t.Peers() {
		conn, err := net.Dial("tcp", peer.Address)
		if err != nil {
			continue
		}

		msg := struct {
			Type     string              `json:"type"`
			Snapshot map[string]FileMeta `json:"snapshot"`
		}{"snapshot", snapshot}
		data, _ := json.Marshal(msg)
		WriteMsg(conn, data)
		conn.Close()
	}
	return nil
}

func (t *LanTransport) addPeer(id, name, address string) {
	t.peersMu.Lock()
	defer t.peersMu.Unlock()
	if id == t.id {
		return
	}
	t.peers[id] = PeerInfo{ID: id, Name: name, Address: address}
	log.Printf("Peer ditemukan: %s (%s) @ %s", name, id[:8], address)
}

func (t *LanTransport) getLocalIP() string {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		return "127.0.0.1"
	}
	defer conn.Close()
	return conn.LocalAddr().(*net.UDPAddr).IP.String()
}
