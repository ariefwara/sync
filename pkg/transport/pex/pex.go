package pex

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
	pexPort       = 43214
	transferPort  = 43215
	pexInterval   = 60 * time.Second
	peerTimeout   = 5 * time.Minute
	maxPeersInPEX = 50
	acceptTimeout = 1 * time.Second
	dialTimeout   = 5 * time.Second
)

type PEXMessage struct {
	Type       string                    `json:"type"`
	SenderID   string                    `json:"sender_id"`
	SenderName string                    `json:"sender_name"`
	Peers      []PeerAddr                `json:"peers,omitempty"`
	Meta       *core.FileMeta            `json:"meta,omitempty"`
	Snapshot   map[string]core.FileMeta  `json:"snapshot,omitempty"`
}

type PeerAddr struct {
	ID      string `json:"id"`
	Name    string `json:"name"`
	Address string `json:"address"`
}

type Transport struct {
	id      string
	name    string
	syncDir string

	knownPeers map[string]PeerAddr
	peersMu    sync.RWMutex
	lastSeen   map[string]time.Time

	metaCh     chan core.FileMeta
	fileCh     chan core.FileTransfer
	snapshotCh chan map[string]core.FileMeta

	tcpLn  net.Listener
	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func NewTransport(syncDir, deviceName string, initialPeers []string) *Transport {
	hostname, _ := os.Hostname()
	id := fmt.Sprintf("pex-%s-%d", hostname, time.Now().UnixNano())

	t := &Transport{
		id:         id,
		name:       deviceName,
		syncDir:    syncDir,
		knownPeers: make(map[string]PeerAddr),
		lastSeen:   make(map[string]time.Time),
		metaCh:     make(chan core.FileMeta, 100),
		fileCh:     make(chan core.FileTransfer, 50),
		snapshotCh: make(chan map[string]core.FileMeta, 10),
	}

	for i, addr := range initialPeers {
		pid := fmt.Sprintf("pex-initial-%d", i)
		t.knownPeers[pid] = PeerAddr{
			ID:      pid,
			Name:    fmt.Sprintf("peer-%d", i),
			Address: addr,
		}
	}

	return t
}

func (t *Transport) SelfID() string                                    { return t.id }
func (t *Transport) ReceiveMeta() <-chan core.FileMeta                 { return t.metaCh }
func (t *Transport) ReceiveFile() <-chan core.FileTransfer             { return t.fileCh }
func (t *Transport) ReceiveSnapshot() <-chan map[string]core.FileMeta  { return t.snapshotCh }

func (t *Transport) Peers() []core.PeerInfo {
	t.peersMu.RLock()
	defer t.peersMu.RUnlock()

	now := time.Now()
	list := make([]core.PeerInfo, 0, len(t.knownPeers))
	for _, p := range t.knownPeers {
		if lastSeen, ok := t.lastSeen[p.ID]; ok {
			if now.Sub(lastSeen) < peerTimeout {
				list = append(list, core.PeerInfo{
					ID:      p.ID,
					Name:    p.Name,
					Address: p.Address,
				})
			}
		}
	}
	return list
}

func (t *Transport) AddPeer(id, name, address string) {
	t.peersMu.Lock()
	defer t.peersMu.Unlock()
	t.knownPeers[id] = PeerAddr{ID: id, Name: name, Address: address}
	t.lastSeen[id] = time.Now()
	log.Printf("Peer ditambahkan: %s (%s) @ %s", name, id[:8], address)
}

func (t *Transport) Start(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	t.cancel = cancel

	tcpLn, err := net.Listen("tcp", fmt.Sprintf(":%d", transferPort))
	if err != nil {
		return fmt.Errorf("listen TCP: %w", err)
	}
	t.tcpLn = tcpLn

	t.wg.Add(3)
	go t.listenTransfers(ctx)
	go t.pexLoop(ctx)
	go t.cleanupLoop(ctx)

	log.Printf("PEX transport siap: %s @ %s:%d", t.name, t.getLocalIP(), transferPort)
	return nil
}

func (t *Transport) Stop() error {
	if t.cancel != nil {
		t.cancel()
	}
	t.wg.Wait()
	if t.tcpLn != nil {
		t.tcpLn.Close()
	}
	return nil
}

func (t *Transport) pexLoop(ctx context.Context) {
	defer t.wg.Done()

	t.doPEX()

	ticker := time.NewTicker(pexInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			t.doPEX()
		}
	}
}

func (t *Transport) doPEX() {
	peers := t.getKnownPeers()
	if len(peers) > maxPeersInPEX {
		peers = peers[:maxPeersInPEX]
	}

	msg := PEXMessage{
		Type:       "pex",
		SenderID:   t.id,
		SenderName: t.name,
		Peers:      peers,
	}

	for _, p := range t.getActivePeers() {
		conn, err := net.DialTimeout("tcp", p.Address, dialTimeout)
		if err != nil {
			continue
		}

		data, _ := json.Marshal(msg)
		core.WriteMsg(conn, data)
		conn.Close()
		t.markActive(p.ID)
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
			go t.handleConnection(conn)
		}
	}
}

func (t *Transport) handleConnection(conn net.Conn) {
	defer conn.Close()

	buf, err := core.ReadMsg(conn)
	if err != nil {
		return
	}

	var msg PEXMessage
	if err := json.Unmarshal(buf, &msg); err != nil {
		return
	}

	if msg.SenderID != t.id {
		t.AddPeer(msg.SenderID, msg.SenderName, conn.RemoteAddr().String())
	}

	switch msg.Type {
	case "pex":
		for _, p := range msg.Peers {
			if _, exists := t.knownPeers[p.ID]; !exists && p.ID != t.id {
				t.AddPeer(p.ID, p.Name, p.Address)
				log.Printf("Peer baru dari PEX: %s @ %s", p.Name, p.Address)
			}
		}
	case "meta":
		if msg.Meta != nil {
			t.metaCh <- *msg.Meta
		}
	case "snapshot":
		if msg.Snapshot != nil {
			t.snapshotCh <- msg.Snapshot
		}
	case "request":
		if msg.Meta != nil {
			localPath := t.syncDir + "/" + msg.Meta.Path
			t.sendFileResponse(conn, *msg.Meta, localPath)
		}
	}
}

func (t *Transport) sendFileResponse(conn net.Conn, meta core.FileMeta, localPath string) {
	f, err := os.Open(localPath)
	if err != nil {
		return
	}
	defer f.Close()

	resp := PEXMessage{
		Type:       "file",
		SenderID:   t.id,
		SenderName: t.name,
		Meta:       &meta,
	}
	respData, _ := json.Marshal(resp)
	core.WriteMsg(conn, respData)
	io.Copy(conn, f)
}

func (t *Transport) cleanupLoop(ctx context.Context) {
	defer t.wg.Done()
	ticker := time.NewTicker(peerTimeout / 2)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			now := time.Now()
			t.peersMu.Lock()
			for id, last := range t.lastSeen {
				if now.Sub(last) > peerTimeout {
					delete(t.knownPeers, id)
					delete(t.lastSeen, id)
					log.Printf("Peer dihapus (timeout): %s", id[:8])
				}
			}
			t.peersMu.Unlock()
		}
	}
}

// ---- core.Transport interface ----

func (t *Transport) SendMeta(peer core.PeerInfo, meta core.FileMeta) error {
	conn, err := net.DialTimeout("tcp", peer.Address, dialTimeout)
	if err != nil {
		return err
	}
	defer conn.Close()

	msg := PEXMessage{
		Type:       "meta",
		SenderID:   t.id,
		SenderName: t.name,
		Meta:       &meta,
	}
	data, _ := json.Marshal(msg)
	return core.WriteMsg(conn, data)
}

func (t *Transport) SendFile(peer core.PeerInfo, meta core.FileMeta, data io.Reader) error {
	conn, err := net.DialTimeout("tcp", peer.Address, dialTimeout)
	if err != nil {
		return err
	}
	defer conn.Close()

	msg := PEXMessage{
		Type:       "file",
		SenderID:   t.id,
		SenderName: t.name,
		Meta:       &meta,
	}
	msgData, _ := json.Marshal(msg)
	if err := core.WriteMsg(conn, msgData); err != nil {
		return err
	}
	_, err = io.Copy(conn, data)
	return err
}

func (t *Transport) ResolveFile(peer core.PeerInfo, meta core.FileMeta) (io.ReadCloser, error) {
	conn, err := net.DialTimeout("tcp", peer.Address, dialTimeout)
	if err != nil {
		return nil, err
	}

	msg := PEXMessage{
		Type:       "request",
		SenderID:   t.id,
		SenderName: t.name,
		Meta:       &meta,
	}
	data, _ := json.Marshal(msg)
	if err := core.WriteMsg(conn, data); err != nil {
		conn.Close()
		return nil, err
	}

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
		conn, err := net.DialTimeout("tcp", peer.Address, dialTimeout)
		if err != nil {
			continue
		}
		msg := PEXMessage{
			Type:       "snapshot",
			SenderID:   t.id,
			SenderName: t.name,
			Snapshot:   snapshot,
		}
		data, _ := json.Marshal(msg)
		core.WriteMsg(conn, data)
		conn.Close()
		t.markActive(peer.ID)
	}
	return nil
}

// ---- helpers ----

func (t *Transport) getKnownPeers() []PeerAddr {
	t.peersMu.RLock()
	defer t.peersMu.RUnlock()
	list := make([]PeerAddr, 0, len(t.knownPeers))
	for _, p := range t.knownPeers {
		list = append(list, p)
	}
	return list
}

func (t *Transport) getActivePeers() []PeerAddr {
	t.peersMu.RLock()
	defer t.peersMu.RUnlock()
	now := time.Now()
	list := make([]PeerAddr, 0, len(t.knownPeers))
	for _, p := range t.knownPeers {
		if lastSeen, ok := t.lastSeen[p.ID]; ok && now.Sub(lastSeen) < peerTimeout {
			list = append(list, p)
		}
	}
	return list
}

func (t *Transport) markActive(id string) {
	t.peersMu.Lock()
	t.lastSeen[id] = time.Now()
	t.peersMu.Unlock()
}

func (t *Transport) getLocalIP() string {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		return "127.0.0.1"
	}
	defer conn.Close()
	return conn.LocalAddr().(*net.UDPAddr).IP.String()
}
