// Package webrtc menyediakan transport berbasis WebRTC untuk
// sinkronisasi P2P yang bisa menembus NAT/firewall.
//
// Cara kerja:
//  1. Setiap peer membuat WebRTC DataChannel
//  2. Signaling dilakukan via stdin/stdout (manual copy-paste SDP)
//     atau otomatis via shared signaling channel (opsional)
//  3. File di-chunk dan dikirim melalui DataChannel
//  4. STUN server publik untuk NAT traversal
//
// Kelebihan: Tembus NAT, compatible dengan browser, enkripsi built-in
// Kekurangan: Setup signaling agak rumit, butuh STUN server publik
package webrtc

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log"
	"os"
	"strings"
	"sync"
	"time"

	"github.com/ariefwara/sync/pkg/core"
	"github.com/pion/webrtc/v3"
)

const (
	chunkSize    = 16384 // 16KB per chunk
	dataChanLabel = "sync-data"
	metaChanLabel = "sync-meta"
)

// SignalingMessage digunakan untuk pertukaran SDP via stdin/stdout.
type SignalingMessage struct {
	Type string `json:"type"` // "offer" | "answer" | "ice"
	Data string `json:"data"` // SDP atau ICE candidate
}

// PeerConnection mewakili satu koneksi WebRTC ke peer.
type PeerConnection struct {
	ID       string
	Name     string
	PeerConn *webrtc.PeerConnection
	DataChan *webrtc.DataChannel
	MetaChan *webrtc.DataChannel
}

// Transport mengimplementasikan core.Transport menggunakan WebRTC.
type Transport struct {
	id       string
	name     string
	syncDir  string

	stunServers []string

	peers    map[string]*PeerConnection
	peersMu  sync.RWMutex

	metaCh     chan core.FileMeta
	fileCh     chan core.FileTransfer
	snapshotCh chan map[string]core.FileMeta

	// Signaling
	signalingIn  chan SignalingMessage
	signalingOut chan SignalingMessage

	// WebRTC
	settings *webrtc.SettingEngine
	api      *webrtc.API

	cancel    context.CancelFunc
	wg        sync.WaitGroup
}

// NewTransport membuat transport WebRTC baru.
// stunServers: daftar STUN server (default: stun:stun.l.google.com:19302)
func NewTransport(syncDir, deviceName string, stunServers []string) *Transport {
	hostname, _ := os.Hostname()
	id := fmt.Sprintf("wrtc-%s-%d", hostname, time.Now().UnixNano())

	if len(stunServers) == 0 {
		stunServers = []string{"stun:stun.l.google.com:19302"}
	}

	return &Transport{
		id:           id,
		name:         deviceName,
		syncDir:      syncDir,
		stunServers:  stunServers,
		peers:        make(map[string]*PeerConnection),
		metaCh:       make(chan core.FileMeta, 100),
		fileCh:       make(chan core.FileTransfer, 50),
		snapshotCh:   make(chan map[string]core.FileMeta, 10),
		signalingIn:  make(chan SignalingMessage, 50),
		signalingOut: make(chan SignalingMessage, 50),
	}
}

func (t *Transport) SelfID() string                                { return t.id }
func (t *Transport) ReceiveMeta() <-chan core.FileMeta             { return t.metaCh }
func (t *Transport) ReceiveFile() <-chan core.FileTransfer         { return t.fileCh }
func (t *Transport) ReceiveSnapshot() <-chan map[string]core.FileMeta { return t.snapshotCh }

// SignalingInput mengembalikan channel untuk menerima signaling message.
func (t *Transport) SignalingInput() chan<- SignalingMessage { return t.signalingIn }

// SignalingOutput mengembalikan channel untuk output signaling message.
func (t *Transport) SignalingOutput() <-chan SignalingMessage { return t.signalingOut }

func (t *Transport) Peers() []core.PeerInfo {
	t.peersMu.RLock()
	defer t.peersMu.RUnlock()
	list := make([]core.PeerInfo, 0, len(t.peers))
	for _, p := range t.peers {
		list = append(list, core.PeerInfo{ID: p.ID, Name: p.Name})
	}
	return list
}

// ConnectToPeer memulai koneksi WebRTC ke peer.
// signalingCh adalah channel untuk mengirim SDP offer.
func (t *Transport) ConnectToPeer(ctx context.Context, peerID, peerName string) error {
	config := webrtc.Configuration{
		ICEServers: []webrtc.ICEServer{
			{URLs: t.stunServers},
		},
	}

	pc, err := webrtc.NewPeerConnection(config)
	if err != nil {
		return fmt.Errorf("buat PeerConnection: %w", err)
	}

	// DataChannel untuk metadata
	metaChan, err := pc.CreateDataChannel(metaChanLabel, nil)
	if err != nil {
		pc.Close()
		return fmt.Errorf("buat meta channel: %w", err)
	}
	metaChan.OnMessage(func(msg webrtc.DataChannelMessage) {
		t.handleDataMessage(msg.Data, peerID)
	})

	// DataChannel untuk file
	fileChan, err := pc.CreateDataChannel(dataChanLabel, &webrtc.DataChannelInit{
		Ordered: boolPtr(true),
	})
	if err != nil {
		pc.Close()
		return fmt.Errorf("buat file channel: %w", err)
	}
	fileChan.OnMessage(func(msg webrtc.DataChannelMessage) {
		t.handleFileMessage(msg.Data, peerID)
	})

	// Buat offer
	offer, err := pc.CreateOffer(nil)
	if err != nil {
		pc.Close()
		return fmt.Errorf("buat offer: %w", err)
	}

	if err := pc.SetLocalDescription(offer); err != nil {
		pc.Close()
		return fmt.Errorf("set local desc: %w", err)
	}

	// Kirim offer ke signaling output
	offerJSON, _ := json.Marshal(offer)
	t.signalingOut <- SignalingMessage{
		Type: "offer",
		Data: fmt.Sprintf("%s|%s|%s", peerID, peerName, string(offerJSON)),
	}

	// Handle ICE candidates
	pc.OnICECandidate(func(candidate *webrtc.ICECandidate) {
		if candidate == nil {
			return
		}
		candJSON, _ := json.Marshal(candidate.ToJSON())
		t.signalingOut <- SignalingMessage{
			Type: "ice",
			Data: fmt.Sprintf("%s|%s", peerID, string(candJSON)),
		}
	})

	// Simpan
	t.peersMu.Lock()
	t.peers[peerID] = &PeerConnection{
		ID:       peerID,
		Name:     peerName,
		PeerConn: pc,
		DataChan: fileChan,
		MetaChan: metaChan,
	}
	t.peersMu.Unlock()

	// Tunggu answer dari signaling
	go func() {
		for msg := range t.signalingIn {
			if strings.HasPrefix(msg.Data, peerID+"|") {
				parts := strings.SplitN(msg.Data, "|", 2)
				if len(parts) != 2 {
					continue
				}

				switch msg.Type {
				case "answer":
					var answer webrtc.SessionDescription
					json.Unmarshal([]byte(parts[1]), &answer)
					pc.SetRemoteDescription(answer)

				case "ice":
					var candidate webrtc.ICECandidateInit
					json.Unmarshal([]byte(parts[1]), &candidate)
					pc.AddICECandidate(candidate)
				}
			}
		}
	}()

	return nil
}

// AcceptConnection menunggu koneksi WebRTC masuk dari signaling.
func (t *Transport) AcceptConnection(ctx context.Context, offer SignalingMessage) error {
	parts := strings.SplitN(offer.Data, "|", 3)
	if len(parts) < 3 {
		return fmt.Errorf("format offer invalid")
	}
	peerID := parts[0]
	peerName := parts[1]

	var sdp webrtc.SessionDescription
	json.Unmarshal([]byte(parts[2]), &sdp)

	config := webrtc.Configuration{
		ICEServers: []webrtc.ICEServer{
			{URLs: t.stunServers},
		},
	}

	pc, err := webrtc.NewPeerConnection(config)
	if err != nil {
		return fmt.Errorf("buat PeerConnection: %w", err)
	}

	// DataChannel untuk metadata
	metaChan, err := pc.CreateDataChannel(metaChanLabel, nil)
	if err != nil {
		pc.Close()
		return err
	}
	metaChan.OnMessage(func(msg webrtc.DataChannelMessage) {
		t.handleDataMessage(msg.Data, peerID)
	})

	// DataChannel untuk file
	fileChan, err := pc.CreateDataChannel(dataChanLabel, &webrtc.DataChannelInit{
		Ordered: boolPtr(true),
	})
	if err != nil {
		pc.Close()
		return err
	}
	fileChan.OnMessage(func(msg webrtc.DataChannelMessage) {
		t.handleFileMessage(msg.Data, peerID)
	})

	if err := pc.SetRemoteDescription(sdp); err != nil {
		pc.Close()
		return fmt.Errorf("set remote desc: %w", err)
	}

	answer, err := pc.CreateAnswer(nil)
	if err != nil {
		pc.Close()
		return fmt.Errorf("buat answer: %w", err)
	}

	if err := pc.SetLocalDescription(answer); err != nil {
		pc.Close()
		return fmt.Errorf("set local desc: %w", err)
	}

	// Kirim answer
	answerJSON, _ := json.Marshal(answer)
	t.signalingOut <- SignalingMessage{
		Type: "answer",
		Data: fmt.Sprintf("%s|%s", peerID, string(answerJSON)),
	}

	// ICE candidates
	pc.OnICECandidate(func(candidate *webrtc.ICECandidate) {
		if candidate == nil {
			return
		}
		candJSON, _ := json.Marshal(candidate.ToJSON())
		t.signalingOut <- SignalingMessage{
			Type: "ice",
			Data: fmt.Sprintf("%s|%s", peerID, string(candJSON)),
		}
	})

	t.peersMu.Lock()
	t.peers[peerID] = &PeerConnection{
		ID:       peerID,
		Name:     peerName,
		PeerConn: pc,
		DataChan: fileChan,
		MetaChan: metaChan,
	}
	t.peersMu.Unlock()

	return nil
}

// StartSignalingConsole memulai signaling interaktif via stdin/stdout.
// User akan diminta menempelkan SDP dari peer lain.
func (t *Transport) StartSignalingConsole(ctx context.Context) {
	go func() {
		scanner := bufio.NewScanner(os.Stdin)
		fmt.Println("\n=== WebRTC Signaling Console ===")
		fmt.Println("Untuk terhubung dengan peer lain:")
		fmt.Println("1. Copy SDP Offer yang muncul di bawah")
		fmt.Println("2. Kirim ke peer lain (email/chat)")
		fmt.Println("3. Paste SDP Answer dari peer")
		fmt.Println("==================================\n")

		for {
			select {
			case <-ctx.Done():
				return
			case msg := <-t.signalingOut:
				switch msg.Type {
				case "offer":
					fmt.Printf("\n>>> SDP OFFER untuk peer:\n%s\n\n", msg.Data)
					fmt.Print("Paste SDP ANSWER dari peer (lalu Ctrl+D):\n> ")
				case "answer":
					fmt.Printf("\n>>> SDP ANSWER untuk peer:\n%s\n\n", msg.Data)
				case "ice":
					fmt.Printf("[ICE] %s\n", msg.Data)
				}
			default:
				// Baca input user
				if scanner.Scan() {
					line := scanner.Text()
					if strings.HasPrefix(line, "{") {
						var sig SignalingMessage
						if err := json.Unmarshal([]byte(line), &sig); err == nil {
							t.signalingIn <- sig
						}
					} else if strings.Contains(line, "v=") {
						// Raw SDP — bungkus dalam signaling message
						t.signalingIn <- SignalingMessage{
							Type: "answer",
							Data: line,
						}
					}
				}
				time.Sleep(100 * time.Millisecond)
			}
		}
	}()
}

func (t *Transport) Start(ctx context.Context) error {
	ctx, cancel := context.WithCancel(ctx)
	t.cancel = cancel

	log.Printf("WebRTC transport siap: %s", t.name)
	log.Printf("STUN servers: %v", t.stunServers)
	log.Println("Gunakan StartSignalingConsole() untuk signaling manual")

	return nil
}

func (t *Transport) Stop() error {
	if t.cancel != nil {
		t.cancel()
	}
	t.peersMu.Lock()
	defer t.peersMu.Unlock()
	for _, pc := range t.peers {
		pc.PeerConn.Close()
	}
	t.peers = make(map[string]*PeerConnection)
	return nil
}

func (t *Transport) handleDataMessage(data []byte, peerID string) {
	var msg struct {
		Type     string                    `json:"type"`
		Meta     *core.FileMeta            `json:"meta,omitempty"`
		Snapshot map[string]core.FileMeta  `json:"snapshot,omitempty"`
	}
	if err := json.Unmarshal(data, &msg); err != nil {
		return
	}

	switch msg.Type {
	case "meta":
		if msg.Meta != nil {
			t.metaCh <- *msg.Meta
		}
	case "snapshot":
		if msg.Snapshot != nil {
			t.snapshotCh <- msg.Snapshot
		}
	}
}

func (t *Transport) handleFileMessage(data []byte, peerID string) {
	// File data: header JSON + binary content
	// Format: [4-byte header length][header JSON][file data]
	if len(data) < 4 {
		return
	}
	headerLen := int32(data[0]) | int32(data[1])<<8 | int32(data[2])<<16 | int32(data[3])<<24
	if int(headerLen)+4 > len(data) {
		return
	}

	var header struct {
		Meta core.FileMeta `json:"meta"`
	}
	json.Unmarshal(data[4:4+headerLen], &header)

	fileData := data[4+headerLen:]

	transfer := core.FileTransfer{
		Meta:   header.Meta,
		Data:   io.NopCloser(newBytesReader(fileData)),
		PeerID: peerID,
	}
	t.fileCh <- transfer
}

// ---- core.Transport interface ----

func (t *Transport) SendMeta(peer core.PeerInfo, meta core.FileMeta) error {
	msg := struct {
		Type string       `json:"type"`
		Meta core.FileMeta `json:"meta"`
	}{"meta", meta}
	data, _ := json.Marshal(msg)

	t.peersMu.RLock()
	pc, ok := t.peers[peer.ID]
	t.peersMu.RUnlock()
	if !ok {
		return fmt.Errorf("peer tidak dikenal: %s", peer.ID)
	}
	if pc.MetaChan == nil {
		return fmt.Errorf("meta channel tidak tersedia")
	}
	return pc.MetaChan.Send(data)
}

func (t *Transport) SendFile(peer core.PeerInfo, meta core.FileMeta, data io.Reader) error {
	t.peersMu.RLock()
	pc, ok := t.peers[peer.ID]
	t.peersMu.RUnlock()
	if !ok {
		return fmt.Errorf("peer tidak dikenal: %s", peer.ID)
	}
	if pc.DataChan == nil {
		return fmt.Errorf("data channel tidak tersedia")
	}

	// Baca semua data file
	fileData, err := io.ReadAll(data)
	if err != nil {
		return err
	}

	// Header
	header := struct {
		Meta core.FileMeta `json:"meta"`
	}{meta}
	headerJSON, _ := json.Marshal(header)

	// Format: 4-byte length + header + file data
	headerLen := int32(len(headerJSON))
	msg := make([]byte, 4+len(headerJSON)+len(fileData))
	msg[0] = byte(headerLen)
	msg[1] = byte(headerLen >> 8)
	msg[2] = byte(headerLen >> 16)
	msg[3] = byte(headerLen >> 24)
	copy(msg[4:], headerJSON)
	copy(msg[4+len(headerJSON):], fileData)

	return pc.DataChan.Send(msg)
}

func (t *Transport) ResolveFile(peer core.PeerInfo, meta core.FileMeta) (io.ReadCloser, error) {
	// Untuk WebRTC, file dikirim via SendFile. Tapi karena kita perlu
	// mekanisme request-response, kita kirim meta via meta channel
	// dan file akan dikirim via file channel.
	//
	// Untuk sederhana: kirim meta dan peer akan mengirim file langsung.
	if err := t.SendMeta(peer, meta); err != nil {
		return nil, err
	}

	// Tunggu file masuk (timeout 30 detik)
	timeout := time.After(30 * time.Second)
	for {
		select {
		case transfer := <-t.fileCh:
			if transfer.Meta.Path == meta.Path {
				return transfer.Data, nil
			}
			// Bukan file yang kita minta, push back
			go func() { t.fileCh <- transfer }()
			return nil, fmt.Errorf("file tidak cocok")
		case <-timeout:
			return nil, fmt.Errorf("timeout menunggu file: %s", meta.Path)
		}
	}
}

func (t *Transport) BroadcastMeta(meta core.FileMeta) error {
	t.peersMu.RLock()
	defer t.peersMu.RUnlock()
	for _, pc := range t.peers {
		if pc.MetaChan == nil {
			continue
		}
		msg := struct {
			Type string       `json:"type"`
			Meta core.FileMeta `json:"meta"`
		}{"meta", meta}
		data, _ := json.Marshal(msg)
		pc.MetaChan.Send(data)
	}
	return nil
}

func (t *Transport) BroadcastSnapshot(snapshot map[string]core.FileMeta) error {
	t.peersMu.RLock()
	defer t.peersMu.RUnlock()
	for _, pc := range t.peers {
		if pc.MetaChan == nil {
			continue
		}
		msg := struct {
			Type     string                    `json:"type"`
			Snapshot map[string]core.FileMeta  `json:"snapshot"`
		}{"snapshot", snapshot}
		data, _ := json.Marshal(msg)
		pc.MetaChan.Send(data)
	}
	return nil
}

// ---- helpers ----

func boolPtr(b bool) *bool { return &b }

// bytesReader mengimplementasikan io.Reader dari []byte.
type bytesReader struct{ data []byte; pos int }

func newBytesReader(data []byte) *bytesReader { return &bytesReader{data: data} }

func (r *bytesReader) Read(p []byte) (int, error) {
	if r.pos >= len(r.data) {
		return 0, io.EOF
	}
	n := copy(p, r.data[r.pos:])
	r.pos += n
	return n, nil
}

func (r *bytesReader) Close() error { return nil }
