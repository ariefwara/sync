package dht

import (
	"context"
	"crypto/ed25519"
	"crypto/rand"
	"crypto/sha256"
	"encoding/hex"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"net"
	"os"
	"sort"
	"sync"
	"time"

	"github.com/ariefwara/sync/pkg/core"
)

const (
	dhtPort       = 43212
	transferPort  = 43213
	k             = 20
	alpha         = 3
	bucketBits    = 8
	bootstrapInterval = 60 * time.Second
	peerTimeout   = 10 * time.Minute
	nodeLifetime  = 60 * time.Minute
	acceptTimeout = 1 * time.Second
)

// DHTPort mengembalikan port DHT.
func DHTPort() int { return dhtPort }

// TransferPort mengembalikan port transfer file.
func TransferPort() int { return transferPort }

const (
	rpcPing     = "ping"
	rpcPong     = "pong"
	rpcFindNode = "find_node"
	rpcFindPeer = "find_peer"
	rpcPeerHere = "peer_here"
)

type RPCHeader struct {
	Type    string `json:"type"`
	Sender  string `json:"sender"`
	Target  string `json:"target"`
	Address string `json:"address"`
}

type DHTNode struct {
	ID       string    `json:"id"`
	Address  string    `json:"address"`
	LastSeen time.Time `json:"last_seen"`
	IsPeer   bool      `json:"is_peer"`
}

type RoutingTable struct {
	mu      sync.RWMutex
	selfID  string
	buckets [][]DHTNode
}

func newRoutingTable(selfID string) *RoutingTable {
	rt := &RoutingTable{
		selfID:  selfID,
		buckets: make([][]DHTNode, 256/bucketBits),
	}
	for i := range rt.buckets {
		rt.buckets[i] = make([]DHTNode, 0, k)
	}
	return rt
}

func (rt *RoutingTable) bucketIndex(nodeID string) int {
	dist := xorDistance(rt.selfID, nodeID)
	for i := 0; i < len(dist)*8; i++ {
		if dist[i/8]&(1<<(7-uint(i%8))) != 0 {
			bucket := i / bucketBits
			if bucket >= len(rt.buckets) {
				return len(rt.buckets) - 1
			}
			return bucket
		}
	}
	return 0
}

func (rt *RoutingTable) insert(node DHTNode) {
	rt.mu.Lock()
	defer rt.mu.Unlock()

	idx := rt.bucketIndex(node.ID)
	bucket := rt.buckets[idx]

	for i, n := range bucket {
		if n.ID == node.ID {
			bucket[i].LastSeen = time.Now()
			bucket[i].Address = node.Address
			return
		}
	}

	if len(bucket) < k {
		rt.buckets[idx] = append(bucket, node)
		return
	}

	oldest := 0
	for i, n := range bucket {
		if n.LastSeen.Before(bucket[oldest].LastSeen) {
			oldest = i
		}
	}
	bucket[oldest] = node
	rt.buckets[idx] = bucket
}

func (rt *RoutingTable) findClosest(targetID string, count int) []DHTNode {
	rt.mu.RLock()
	defer rt.mu.RUnlock()

	var all []DHTNode
	for _, bucket := range rt.buckets {
		all = append(all, bucket...)
	}

	sort.Slice(all, func(i, j int) bool {
		di := xorDistance(targetID, all[i].ID)
		dj := xorDistance(targetID, all[j].ID)
		return compareBytes(di, dj) < 0
	})

	if len(all) > count {
		all = all[:count]
	}
	return all
}

func (rt *RoutingTable) allPeers() []DHTNode {
	rt.mu.RLock()
	defer rt.mu.RUnlock()

	var peers []DHTNode
	for _, bucket := range rt.buckets {
		for _, n := range bucket {
			if n.IsPeer {
				peers = append(peers, n)
			}
		}
	}
	return peers
}

func xorDistance(a, b string) []byte {
	ab, _ := hex.DecodeString(a)
	bb, _ := hex.DecodeString(b)
	maxLen := len(ab)
	if len(bb) > maxLen {
		maxLen = len(bb)
	}
	result := make([]byte, maxLen)
	for i := 0; i < maxLen; i++ {
		var va, vb byte
		if i < len(ab) {
			va = ab[i]
		}
		if i < len(bb) {
			vb = bb[i]
		}
		result[i] = va ^ vb
	}
	return result
}

func compareBytes(a, b []byte) int {
	for i := 0; i < len(a) && i < len(b); i++ {
		if a[i] < b[i] {
			return -1
		}
		if a[i] > b[i] {
			return 1
		}
	}
	return len(a) - len(b)
}

type Transport struct {
	id           string
	name         string
	syncDir      string
	nodeID       string
	privateKey   ed25519.PrivateKey

	routingTable *RoutingTable
	bootstraps   []string

	metaCh     chan core.FileMeta
	fileCh     chan core.FileTransfer
	snapshotCh chan map[string]core.FileMeta
	peerCh     chan core.PeerInfo

	udpConn *net.UDPConn
	tcpLn   net.Listener

	cancel context.CancelFunc
	wg     sync.WaitGroup
}

func NewTransport(syncDir, deviceName string, bootstraps []string) *Transport {
	pub, priv, err := ed25519.GenerateKey(rand.Reader)
	if err != nil {
		panic(err)
	}
	hash := sha256.Sum256(pub)
	nodeID := hex.EncodeToString(hash[:])
	hostname, _ := os.Hostname()
	id := fmt.Sprintf("dht-%s-%d", hostname, time.Now().UnixNano())

	return &Transport{
		id:           id,
		name:         deviceName,
		syncDir:      syncDir,
		nodeID:       nodeID,
		privateKey:   priv,
		routingTable: newRoutingTable(nodeID),
		bootstraps:   bootstraps,
		metaCh:       make(chan core.FileMeta, 100),
		fileCh:       make(chan core.FileTransfer, 50),
		snapshotCh:   make(chan map[string]core.FileMeta, 10),
		peerCh:       make(chan core.PeerInfo, 50),
	}
}

func (t *Transport) SelfID() string                                    { return t.id }
func (t *Transport) NodeID() string                                    { return t.nodeID }
func (t *Transport) ReceiveMeta() <-chan core.FileMeta                 { return t.metaCh }
func (t *Transport) ReceiveFile() <-chan core.FileTransfer             { return t.fileCh }
func (t *Transport) ReceiveSnapshot() <-chan map[string]core.FileMeta  { return t.snapshotCh }

func (t *Transport) Peers() []core.PeerInfo {
	nodes := t.routingTable.allPeers()
	peers := make([]core.PeerInfo, 0, len(nodes))
	for _, n := range nodes {
		if n.IsPeer {
			peers = append(peers, core.PeerInfo{
				ID:      n.ID,
				Name:    fmt.Sprintf("dht-node-%s", n.ID[:8]),
				Address: n.Address,
			})
		}
	}
	return peers
}

func (t *Transport) Start(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	t.cancel = cancel

	udpAddr, err := net.ResolveUDPAddr("udp", fmt.Sprintf(":%d", dhtPort))
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

	t.wg.Add(4)
	go t.handleRPCServer(ctx)
	go t.bootstrap(ctx)
	go t.maintainRoutingTable(ctx)
	go t.listenTransfers(ctx)

	log.Printf("DHT transport siap: node=%s", t.nodeID[:12])
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

func (t *Transport) handleRPCServer(ctx context.Context) {
	defer t.wg.Done()

	buf := make([]byte, 4096)
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

			var header RPCHeader
			if err := json.Unmarshal(buf[:n], &header); err != nil {
				continue
			}
			if header.Sender == t.nodeID {
				continue
			}

			t.routingTable.insert(DHTNode{
				ID:       header.Sender,
				Address:  header.Address,
				LastSeen: time.Now(),
			})

			switch header.Type {
			case rpcPing:
				t.sendRPC(addr, RPCHeader{
					Type:    rpcPong,
					Sender:  t.nodeID,
					Address: fmt.Sprintf("%s:%d", t.getLocalIP(), dhtPort),
				})
			case rpcFindNode:
				closest := t.routingTable.findClosest(header.Target, k)
				nodesJSON, _ := json.Marshal(closest)
				t.sendRPC(addr, RPCHeader{
					Type:    rpcFindNode,
					Sender:  t.nodeID,
					Target:  header.Target,
					Address: fmt.Sprintf("%s:%d|nodes=%s", t.getLocalIP(), dhtPort, string(nodesJSON)),
				})
			case rpcFindPeer:
				peers := t.routingTable.allPeers()
				peersJSON, _ := json.Marshal(peers)
				t.sendRPC(addr, RPCHeader{
					Type:    rpcPeerHere,
					Sender:  t.nodeID,
					Address: fmt.Sprintf("%s:%d|peers=%s", t.getLocalIP(), dhtPort, string(peersJSON)),
				})
			}
		}
	}
}

func (t *Transport) sendRPC(addr *net.UDPAddr, header RPCHeader) {
	data, _ := json.Marshal(header)
	t.udpConn.WriteToUDP(data, addr)
}

func (t *Transport) bootstrap(ctx context.Context) {
	defer t.wg.Done()

	if len(t.bootstraps) == 0 {
		log.Println("Tidak ada bootstrap — menjadi node pertama")
		t.routingTable.insert(DHTNode{
			ID:       t.nodeID,
			Address:  fmt.Sprintf("%s:%d", t.getLocalIP(), dhtPort),
			LastSeen: time.Now(),
			IsPeer:   true,
		})
		return
	}

	t.doBootstrap()
	ticker := time.NewTicker(bootstrapInterval)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			t.doBootstrap()
		}
	}
}

func (t *Transport) doBootstrap() {
	for _, bs := range t.bootstraps {
		addr, err := net.ResolveUDPAddr("udp", bs)
		if err != nil {
			continue
		}
		t.sendRPC(addr, RPCHeader{
			Type:    rpcPing,
			Sender:  t.nodeID,
			Address: fmt.Sprintf("%s:%d", t.getLocalIP(), dhtPort),
		})
		t.sendRPC(addr, RPCHeader{
			Type:    rpcFindNode,
			Sender:  t.nodeID,
			Target:  t.nodeID,
			Address: fmt.Sprintf("%s:%d", t.getLocalIP(), dhtPort),
		})
	}
}

func (t *Transport) maintainRoutingTable(ctx context.Context) {
	defer t.wg.Done()
	ticker := time.NewTicker(30 * time.Second)
	defer ticker.Stop()

	for {
		select {
		case <-ctx.Done():
			return
		case <-ticker.C:
			nodes := t.routingTable.findClosest(t.nodeID, k)
			for _, n := range nodes {
				if n.ID == t.nodeID {
					continue
				}
				addr, err := net.ResolveUDPAddr("udp", n.Address)
				if err != nil {
					continue
				}
				t.sendRPC(addr, RPCHeader{
					Type:    rpcPing,
					Sender:  t.nodeID,
					Address: fmt.Sprintf("%s:%d", t.getLocalIP(), dhtPort),
				})
			}
		}
	}
}

// ---- TCP Transfer ----

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
