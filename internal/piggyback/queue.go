package piggyback

import (
	"context"
	"encoding/base64" // <-- Import necessario
	"encoding/binary"
	"fmt"
	"sync"
	"time"

	"GossipSystemUtilization/internal/logx"
	"GossipSystemUtilization/internal/simclock"

	proto "GossipSystemUtilization/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/metadata"
)

// Torniamo a una chiave normale, perché ora i dati saranno stringhe ASCII valide (Base64)
const mdKey = "x-pb"

// ======= BEGIN PATCH: campi Advert (aggiunti HasGPU e BusyUntilMs) =======
type Advert struct {
	NodeId      string
	Avail       uint8
	CreateMs    int64
	ExpireMs    int64
	HasGPU      bool  // <--- NUOVO: presenza GPU del peer
	BusyUntilMs int64 // <--- NUOVO: cool-off (tempo SIM assoluto), 0 se non busy
}

// ======= END PATCH =======

// ... (Queue, NewQueue, UpsertSelfFromStats, Upsert, TakeForSend, better, encodeAvail255 rimangono identici) ...
type Queue struct {
	log   *logx.Logger
	clock *simclock.Clock

	mu   sync.Mutex
	max  int
	ttl  time.Duration
	data map[string]Advert // per-node: manteniamo il più recente

	selfBusyUntilMs int64 // protetto da mu
}

func NewQueue(log *logx.Logger, clock *simclock.Clock, max int, ttl time.Duration) *Queue {
	if max <= 0 {
		max = 200
	}
	if ttl <= 0 {
		ttl = 110 * time.Second
	}
	return &Queue{
		log:   log,
		clock: clock,
		max:   max,
		ttl:   ttl,
		data:  make(map[string]Advert),
	}
}

// ======= BEGIN PATCH: helper Cool-off & Fresh =======

// SetBusyFor imposta un cool-off (tempo SIM) per il nodo locale.
// Passa una durata in tempo SIM (es. 8s simulati). Usa 0 per cancellarlo.
func (q *Queue) SetBusyFor(d time.Duration) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if d <= 0 {
		q.selfBusyUntilMs = 0
		return
	}
	now := q.clock.NowSim().UnixMilli()
	q.selfBusyUntilMs = now + d.Milliseconds()
}

// getSelfBusyUntilMs: accesso interno con lock già gestito dal chiamante o safe qui.
func (q *Queue) getSelfBusyUntilMs() int64 {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.selfBusyUntilMs
}

// Latest ritorna l'ultimo Advert noto per nodeId.
func (q *Queue) Latest(nodeId string) (Advert, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	a, ok := q.data[nodeId]
	return a, ok
}

// Fresh dice se il dato piggyback per nodeId è fresco (< staleCutoffMs).
// nowMs è il tempo SIM corrente in ms.
func (q *Queue) Fresh(nodeId string, nowMs int64, staleCutoffMs int64) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	a, ok := q.data[nodeId]
	if !ok {
		return false
	}
	if staleCutoffMs <= 0 {
		// fallback: usa TTL residua
		return a.ExpireMs > nowMs
	}
	age := nowMs - a.CreateMs
	return age >= 0 && age <= staleCutoffMs
}

// BusyPenalty restituisce una penalità 0..1 se il peer è in cool-off.
// Se manca l’advert o BusyUntil <= now → 0. Se ancora busy → 1 (semplice).
func (q *Queue) BusyPenalty(nodeId string, nowMs int64) float64 {
	q.mu.Lock()
	defer q.mu.Unlock()
	a, ok := q.data[nodeId]
	if !ok || a.BusyUntilMs <= nowMs {
		return 0.0
	}
	return 1.0
}

// ======= END PATCH =======

func (q *Queue) UpsertSelfFromStats(s *proto.Stats) {
	if s == nil || s.NodeId == "" {
		return
	}
	avail := encodeAvail255(s)
	nowMs := s.TsMs
	if nowMs == 0 {
		nowMs = q.clock.NowSim().UnixMilli()
	}
	// Legge il busy attuale dell'istanza locale
	busy := q.getSelfBusyUntilMs()

	q.Upsert(Advert{
		NodeId:      s.NodeId,
		Avail:       avail,
		CreateMs:    nowMs,
		ExpireMs:    nowMs + int64(q.ttl/time.Millisecond),
		HasGPU:      s.GpuPct >= 0,
		BusyUntilMs: busy,
	})
}

func (q *Queue) Upsert(a Advert) {
	q.mu.Lock()
	defer q.mu.Unlock()

	if prev, ok := q.data[a.NodeId]; ok {
		if a.CreateMs < prev.CreateMs {
			return
		}
	}

	q.log.Infof("PIGGYBACK UPSERT → node=%s avail=%d", a.NodeId, a.Avail)
	q.data[a.NodeId] = a

	if len(q.data) > q.max*2 {
		now := q.clock.NowSim().UnixMilli()
		for k, v := range q.data {
			if v.ExpireMs <= now {
				delete(q.data, k)
			}
		}
		if len(q.data) > q.max {
			i := 0
			for k := range q.data {
				delete(q.data, k)
				i++
				if len(q.data) <= q.max {
					break
				}
			}
			_ = i
		}
	}
}

func (q *Queue) TakeForSend(n int) []Advert {
	if n <= 0 {
		n = 3
	}
	now := q.clock.NowSim().UnixMilli()

	q.mu.Lock()
	defer q.mu.Unlock()

	buf := make([]Advert, 0, len(q.data))
	for _, a := range q.data {
		if a.ExpireMs > now {
			buf = append(buf, a)
		}
	}
	for i := 1; i < len(buf); i++ {
		j := i
		for j > 0 {
			if better(buf[j], buf[j-1]) {
				buf[j], buf[j-1] = buf[j-1], buf[j]
				j--
			} else {
				break
			}
		}
	}
	if len(buf) > n {
		buf = buf[:n]
	}
	return buf
}

func better(a, b Advert) bool {
	if a.Avail != b.Avail {
		return a.Avail > b.Avail
	}
	if a.CreateMs != b.CreateMs {
		return a.CreateMs > b.CreateMs
	}
	return a.ExpireMs > b.ExpireMs
}

func UnaryClientInterceptor(q *Queue) grpc.UnaryClientInterceptor {
	return func(
		ctx context.Context,
		method string,
		req, reply interface{},
		cc *grpc.ClientConn,
		invoker grpc.UnaryInvoker,
		opts ...grpc.CallOption,
	) error {
		if q != nil {
			ads := q.TakeForSend(3)
			if len(ads) > 0 {
				var summary []string
				for _, a := range ads {
					summary = append(summary, fmt.Sprintf("%s(av:%d busy:%d gpu:%t)", a.NodeId, a.Avail, a.BusyUntilMs, a.HasGPU))
				}

				q.log.Infof("PIGGYBACK SEND → attaching %d adverts: %v", len(ads), summary)
				// La chiamata a encodeMD ora restituisce una stringa Base64 sicura
				ctx = metadata.AppendToOutgoingContext(ctx, mdKey, encodeMD(ads))
			}
		}
		err := invoker(ctx, method, req, reply, cc, opts...)
		return err
	}
}

func UnaryServerInterceptor(q *Queue) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,
	) (interface{}, error) {
		if q != nil {
			if md, ok := metadata.FromIncomingContext(ctx); ok {
				vals := md.Get(mdKey)
				for _, v := range vals {
					arr, err := decodeMD(v)
					if err != nil || len(arr) == 0 {
						continue
					}
					var summary []string
					for _, a := range arr {
						summary = append(summary, fmt.Sprintf("%s(av:%d busy:%d gpu:%t)", a.NodeId, a.Avail, a.BusyUntilMs, a.HasGPU))
					}
					q.log.Infof("PIGGYBACK RECV ← received %d adverts: %v", len(arr), summary)
					for _, a := range arr {
						q.Upsert(a)
					}
				}
			}
		}
		resp, err := handler(ctx, req)
		return resp, err
	}
}

func encodeAvail255(s *proto.Stats) uint8 {
	load := s.CpuPct
	if s.MemPct > load {
		load = s.MemPct
	}
	if s.GpuPct >= 0 && s.GpuPct > load {
		load = s.GpuPct
	}
	if load < 0 {
		load = 0
	}
	if load > 100 {
		load = 100
	}
	v := 255 - uint8((load/100.0)*255.0+0.5)
	return v
}

// --- MODIFICA CHIAVE: USA BASE64 PER CODIFICARE E DECODIFICARE I DATI BINARI ---

// --- MODIFICA CHIAVE: USA BASE64 PER CODIFICARE E DECODIFICARE I DATI BINARI ---
func encodeMD(arr []Advert) string {
	var buf []byte
	for _, a := range arr {
		nid := []byte(a.NodeId)
		// layout: | nid_len(2) | nid | avail(1) | create(8) | expire(8) | flags(1) | busy_until(8) |
		tmp := make([]byte, 2+len(nid)+1+8+8+1+8)
		binary.BigEndian.PutUint16(tmp[0:2], uint16(len(nid)))
		copy(tmp[2:2+len(nid)], nid)
		tmp[2+len(nid)] = byte(a.Avail)
		binary.BigEndian.PutUint64(tmp[3+len(nid):11+len(nid)], uint64(a.CreateMs))
		binary.BigEndian.PutUint64(tmp[11+len(nid):19+len(nid)], uint64(a.ExpireMs))
		flags := byte(0)
		if a.HasGPU {
			flags |= 0x01
		}
		tmp[19+len(nid)] = flags
		binary.BigEndian.PutUint64(tmp[20+len(nid):28+len(nid)], uint64(a.BusyUntilMs))
		buf = append(buf, tmp...)
	}
	return base64.RawStdEncoding.EncodeToString(buf)
}

func decodeMD(s string) ([]Advert, error) {
	b, err := base64.RawStdEncoding.DecodeString(s)
	if err != nil {
		return nil, err
	}
	out := make([]Advert, 0, 3)
	for len(b) >= 2 {
		l := int(binary.BigEndian.Uint16(b[0:2]))
		// minimo richiesto: header + nid + avail + 3x uint64 + flags
		if len(b) < 2+l+1+8+8+1+8 {
			break
		}
		nid := string(b[2 : 2+l])
		avail := uint8(b[2+l])
		create := int64(binary.BigEndian.Uint64(b[3+l : 11+l]))
		exp := int64(binary.BigEndian.Uint64(b[11+l : 19+l]))
		flags := b[19+l]
		hasGPU := (flags & 0x01) != 0
		busy := int64(binary.BigEndian.Uint64(b[20+l : 28+l]))

		out = append(out, Advert{
			NodeId:      nid,
			Avail:       avail,
			CreateMs:    create,
			ExpireMs:    exp,
			HasGPU:      hasGPU,
			BusyUntilMs: busy,
		})
		b = b[28+l:]
	}
	return out, nil
}

// Lookup2: ritorna (avail, ok, fresh, busyUntilMs) per un peer.
// fresh = true se l'advert non è troppo vecchio (<= staleCutoffMs) e non è scaduto.
func (q *Queue) Lookup2(nodeID string, nowMs int64, staleCutoffMs int64) (avail uint8, ok bool, fresh bool, busyUntilMs int64) {
	q.mu.Lock()
	defer q.mu.Unlock()

	a, ok := q.data[nodeID]
	if !ok {
		return 0, false, false, 0
	}
	if a.ExpireMs <= nowMs {
		return a.Avail, true, false, a.BusyUntilMs
	}
	if staleCutoffMs > 0 {
		age := nowMs - a.CreateMs
		fresh = age >= 0 && age <= staleCutoffMs
	} else {
		fresh = true
	}
	return a.Avail, true, fresh, a.BusyUntilMs
}
