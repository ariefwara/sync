package lan

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

	"github.com/ariefwara/sync/pkg/core"
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

type Transport struct {
	id      string
	name    string
	syncDir string

	peers   map[string]core.PeerInfo
	peersMu sync.RWMutex

	metaCh      chan core.FileMeta
	fileCh      chan core.FileTransfer
	snapshotCh  chan map[string]core.FileMeta

	udpConn *net.UDPConn
	tcpLn   net.Listener

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func NewTransport(syncDir, deviceName string) *Transport {
	hostname, _ := os.Hostname()
	id := fmt.Sprintf("lan-%s-%d", hostname, time.Now().UnixNano())
	return &Transport{
		id:         id,
		name:       deviceName,
		syncDir:    syncDir,
		peers:      make(map[string]core.PeerInfo),
		metaCh:     make(chan core.FileMeta, 100),
		fileCh:     make(chan core.FileTransfer, 50),
		snapshotCh: make(chan map[string]core.FileMeta, 10),
	}
}

func (t *Transport) SelfID() string                                    { return t.id }
func (t *Transport) ReceiveMeta() <-chan core.FileMeta                 { return t.metaCh }
func (t *Transport) ReceiveFile() <-chan core.FileTransfer             { return t.fileCh }
func (t *Transport) ReceiveSnapshot() <-chan map[string]core.FileMeta  { return t.snapshotCh }

func (t *Transport) Peers() []core.PeerInfo {
	t.peersMu.RLock()
	defer t.peersMu.RUnlock()
	list := make([]core.PeerInfo, 0, len(t.peers))
	for _, p := range t.peers {
		list = append(list, p)
	}
	return list
}

func (t *Transport) Start(ctx context.Context) error {
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

func (t *Transport) Stop() error {
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

func (t *Transport) broadcastPings(ctx context.Context) {
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

	// Kirim segera
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

func (t *Transport) sendDiscovery(data []byte, addr *net.UDPAddr) {
	conn, err := net.DialUDP("udp", nil, addr)
	if err != nil {
		return
	}
	defer conn.Close()
	conn.Write(data)
}

func (t *Transport) listenDiscovery(ctx context.Context) {
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

func (t *Transport) listenTransfers(ctx context.Context) {
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

func (t *Transport) handleTransfer(conn net.Conn) {
	defer conn.Close()

	headerBuf, err := core.ReadMsg(conn)
	if err != nil {
		return
	}

	var msg struct {
		Type     string                    `json:"type"`
		Meta     core.FileMeta             `json:"meta,omitempty"`
		Snapshot map[string]core.FileMeta  `json:"snapshot,omitempty"`
	}
	if err := json.Unmarshal(headerBuf, &msg); err != nil {
		return
	}

	switch msg.Type {
	case "meta":
		t.metaCh <- msg.Meta
	case "file":
		t.fileCh <- core.FileTransfer{
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

func (t *Transport) sendFileResponse(conn net.Conn, meta core.FileMeta, localPath string) {
	f, err := os.Open(localPath)
	if err != nil {
		return
	}
	defer f.Close()

	resp := struct {
		Type string       `json:"type"`
		Meta core.FileMeta `json:"meta"`
	}{"file-response", meta}
	respData, _ := json.Marshal(resp)

	core.WriteMsg(conn, respData)
	io.Copy(conn, f)
}

func (t *Transport) SendMeta(peer core.PeerInfo, meta core.FileMeta) error {
	conn, err := net.Dial("tcp", peer.Address)
	if err != nil {
		return err
	}
	defer conn.Close()

	msg := struct {
		Type string       `json:"type"`
		Meta core.FileMeta `json:"meta"`
	}{"meta", meta}
	data, _ := json.Marshal(msg)
	return core.WriteMsg(conn, data)
}

func (t *Transport) SendFile(peer core.PeerInfo, meta core.FileMeta, data io.Reader) error {
	conn, err := net.Dial("tcp", peer.Address)
	if err != nil {
		return err
	}
	defer conn.Close()

	msg := struct {
		Type string       `json:"type"`
		Meta core.FileMeta `json:"meta"`
	}{"file", meta}
	msgData, _ := json.Marshal(msg)
	if err := core.WriteMsg(conn, msgData); err != nil {
		return err
	}
	_, err = io.Copy(conn, data)
	return err
}

func (t *Transport) ResolveFile(peer core.PeerInfo, meta core.FileMeta) (io.ReadCloser, error) {
	conn, err := net.Dial("tcp", peer.Address)
	if err != nil {
		return nil, err
	}

	msg := struct {
		Type string       `json:"type"`
		Meta core.FileMeta `json:"meta"`
	}{"request", meta}
	data, _ := json.Marshal(msg)
	if err := core.WriteMsg(conn, data); err != nil {
		conn.Close()
		return nil, err
	}

	// Baca response header
	_, err = core.ReadMsg(conn)
	if err != nil {
		conn.Close()
		return nil, err
	}

	return conn, nil
}

func (t *Transport) BroadcastMeta(meta core.FileMeta) error {
	for _, p := range t.Peers() {
		if err := t.SendMeta(p, meta); err != nil {
			log.Printf("Broadcast meta ke %s gagal: %v", p.ID, err)
		}
	}
	return nil
}

func (t *Transport) BroadcastSnapshot(snapshot map[string]core.FileMeta) error {
	for _, peer := range t.Peers() {
		conn, err := net.Dial("tcp", peer.Address)
		if err != nil {
			continue
		}

		msg := struct {
			Type     string                    `json:"type"`
			Snapshot map[string]core.FileMeta  `json:"snapshot"`
		}{"snapshot", snapshot}
		data, _ := json.Marshal(msg)
		core.WriteMsg(conn, data)
		conn.Close()
	}
	return nil
}

func (t *Transport) addPeer(id, name, address string) {
	t.peersMu.Lock()
	defer t.peersMu.Unlock()
	if id == t.id {
		return
	}
	t.peers[id] = core.PeerInfo{ID: id, Name: name, Address: address}
	log.Printf("Peer ditemukan: %s (%s) @ %s", name, id[:8], address)
}

func (t *Transport) getLocalIP() string {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		return "127.0.0.1"
	}
	defer conn.Close()
	return conn.LocalAddr().(*net.UDPAddr).IP.String()
}
