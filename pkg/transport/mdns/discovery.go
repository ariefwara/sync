package mdns

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
	"github.com/hashicorp/mdns"
)

const (
	serviceName  = "_sync-folder._tcp"
	transferPort = 43211
	queryInterval = 30 * time.Second
	peerTimeout   = 60 * time.Second
	domain        = "local"
	acceptTimeout = 1 * time.Second
)

type Transport struct {
	id        string
	name      string
	syncDir   string

	peers     map[string]core.PeerInfo
	peersMu   sync.RWMutex
	peerTrack map[string]time.Time

	metaCh     chan core.FileMeta
	fileCh     chan core.FileTransfer
	snapshotCh chan map[string]core.FileMeta

	server *mdns.Server
	tcpLn  net.Listener

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func NewTransport(syncDir, deviceName string) *Transport {
	hostname, _ := os.Hostname()
	id := fmt.Sprintf("mdns-%s-%d", hostname, time.Now().UnixNano())
	return &Transport{
		id:         id,
		name:       deviceName,
		syncDir:    syncDir,
		peers:      make(map[string]core.PeerInfo),
		peerTrack:  make(map[string]time.Time),
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

	localIP := t.getLocalIP()

	// Daftarkan service mDNS menggunakan API yang benar
	info := []string{fmt.Sprintf("id=%s", t.id), fmt.Sprintf("name=%s", t.name)}
	mdnsService, err := mdns.NewMDNSService(
		t.name,
		serviceName,
		domain,
		"",
		transferPort,
		nil,
		info,
	)
	if err != nil {
		return fmt.Errorf("buat mDNS service: %w", err)
	}

	server, err := mdns.NewServer(&mdns.Config{Zone: mdnsService})
	if err != nil {
		return fmt.Errorf("start mDNS server: %w", err)
	}
	t.server = server

	tcpLn, err := net.Listen("tcp", fmt.Sprintf(":%d", transferPort))
	if err != nil {
		server.Shutdown()
		return fmt.Errorf("listen TCP: %w", err)
	}
	t.tcpLn = tcpLn

	t.wg.Add(3)
	go t.queryPeers(ctx)
	go t.cleanupPeers(ctx)
	go t.listenTransfers(ctx)

	log.Printf("mDNS transport siap: %s @ %s:%d", t.name, localIP, transferPort)
	return nil
}

func (t *Transport) Stop() error {
	if t.cancel != nil {
		t.cancel()
	}
	t.wg.Wait()
	if t.server != nil {
		t.server.Shutdown()
	}
	if t.tcpLn != nil {
		t.tcpLn.Close()
	}
	return nil
}

func (t *Transport) queryPeers(ctx context.Context) {
	defer t.wg.Done()
	t.doQuery()

	ticker := time.NewTicker(queryInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			t.doQuery()
		}
	}
}

func (t *Transport) doQuery() {
	entriesCh := make(chan *mdns.ServiceEntry, 10)
	go func() {
		for entry := range entriesCh {
			t.handleServiceEntry(entry)
		}
	}()

	mdns.Query(&mdns.QueryParam{
		Service:   serviceName,
		Domain:    domain,
		Timeout:   time.Second * 5,
		Entries:   entriesCh,
	})
}

func (t *Transport) handleServiceEntry(entry *mdns.ServiceEntry) {
	if entry.AddrV4 == nil {
		return
	}

	var peerID, peerName string
	for _, info := range entry.InfoFields {
		if len(info) > 3 && info[:3] == "id=" {
			peerID = info[3:]
		}
		if len(info) > 5 && info[:5] == "name=" {
			peerName = info[5:]
		}
	}
	if peerID == "" || peerID == t.id {
		return
	}
	if peerName == "" {
		peerName = entry.Name
	}

	address := fmt.Sprintf("%s:%d", entry.AddrV4, entry.Port)

	t.peersMu.Lock()
	t.peers[peerID] = core.PeerInfo{ID: peerID, Name: peerName, Address: address}
	t.peerTrack[peerID] = time.Now()
	t.peersMu.Unlock()

	log.Printf("Peer ditemukan (mDNS): %s (%s) @ %s", peerName, peerID[:8], address)
}

func (t *Transport) cleanupPeers(ctx context.Context) {
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
			for id, lastSeen := range t.peerTrack {
				if now.Sub(lastSeen) > peerTimeout {
					delete(t.peers, id)
					delete(t.peerTrack, id)
				}
			}
			t.peersMu.Unlock()
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
	json.Unmarshal(headerBuf, &msg)

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

func (t *Transport) getLocalIP() string {
	conn, err := net.Dial("udp", "8.8.8.8:80")
	if err != nil {
		return "127.0.0.1"
	}
	defer conn.Close()
	return conn.LocalAddr().(*net.UDPAddr).IP.String()
}
