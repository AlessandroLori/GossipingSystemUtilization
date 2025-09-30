package piggyback

import (
	"context"
	"encoding/base64"
	"encoding/binary"
	"fmt"
	"strings"
	"sync"
	"sync/atomic"
	"time"

	"GossipSystemUtilization/internal/logx"
	"GossipSystemUtilization/internal/simclock"
	proto "GossipSystemUtilization/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/metadata"
	"google.golang.org/grpc/status"
)

const mdKey = "x-pb"

type Advert struct {
	NodeId       string
	Avail        uint8
	CreateMs     int64
	ExpireMs     int64
	HasGPU       bool
	BusyUntilMs  int64
	LeaveUntilMs int64
}

type Queue struct {
	log   *logx.Logger
	clock *simclock.Clock

	mu   sync.Mutex
	max  int
	ttl  time.Duration
	data map[string]Advert

	selfBusyUntilMs  int64
	selfLeaveUntilMs int64
	paused           int32
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

func (q *Queue) SetBusyFor(d time.Duration) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if d <= 0 {
		q.selfBusyUntilMs = 0
		q.log.Infof("ANTI-HERD COOLOFF(local) → cleared")
		return
	}
	now := q.clock.NowSim().UnixMilli()
	q.selfBusyUntilMs = now + d.Milliseconds()
	q.log.Infof("ANTI-HERD COOLOFF(local) → busy for %v (until=%d)", d, q.selfBusyUntilMs)
}

func (q *Queue) SetLeaveFor(d time.Duration) {
	q.mu.Lock()
	defer q.mu.Unlock()
	if d <= 0 {
		q.selfLeaveUntilMs = 0
		q.log.Infof("LEAVE(local) → cleared")
		return
	}
	now := q.clock.NowSim().UnixMilli()
	q.selfLeaveUntilMs = now + d.Milliseconds()
	q.log.Infof("LEAVE(local) → for %v (until=%d)", d, q.selfLeaveUntilMs)
}

func (q *Queue) getSelfBusyUntilMs() int64 {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.selfBusyUntilMs
}
func (q *Queue) getSelfLeaveUntilMs() int64 {
	q.mu.Lock()
	defer q.mu.Unlock()
	return q.selfLeaveUntilMs
}

func (q *Queue) Latest(nodeId string) (Advert, bool) {
	q.mu.Lock()
	defer q.mu.Unlock()
	a, ok := q.data[nodeId]
	return a, ok
}

func (q *Queue) Fresh(nodeId string, nowMs int64, staleCutoffMs int64) bool {
	q.mu.Lock()
	defer q.mu.Unlock()
	a, ok := q.data[nodeId]
	if !ok {
		return false
	}
	if staleCutoffMs <= 0 {
		return a.ExpireMs > nowMs
	}
	age := nowMs - a.CreateMs
	return age >= 0 && age <= staleCutoffMs
}

func (q *Queue) BusyPenalty(nodeId string, nowMs int64) float64 {
	q.mu.Lock()
	defer q.mu.Unlock()
	a, ok := q.data[nodeId]
	if !ok || a.BusyUntilMs <= nowMs {
		return 0.0
	}
	return 1.0
}

func (q *Queue) UpsertSelfFromStats(s *proto.Stats) {
	if s == nil || s.NodeId == "" {
		return
	}

	if q.IsPaused() {
		return // niente rumore mentre siamo in pausa
	}

	avail := encodeAvail255(s)
	nowMs := s.TsMs
	if nowMs == 0 {
		nowMs = q.clock.NowSim().UnixMilli()
	}
	busy := q.getSelfBusyUntilMs()
	leave := q.getSelfLeaveUntilMs()

	q.Upsert(Advert{
		NodeId:       s.NodeId,
		Avail:        avail,
		CreateMs:     nowMs,
		ExpireMs:     nowMs + int64(q.ttl/time.Millisecond),
		HasGPU:       s.GpuPct >= 0,
		BusyUntilMs:  busy,
		LeaveUntilMs: leave,
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
		// Se la coda è in pausa (leave/fault), non inviare proprio l'RPC.
		if q != nil && q.IsPaused() {
			return status.Error(codes.Unavailable, "node temporarily unavailable (leave/fault)")
		}

		// Altrimenti, allega gli adverts (come prima)
		if q != nil {
			ads := q.TakeForSend(3)
			if len(ads) > 0 {
				nowMs := q.clock.NowSim().UnixMilli()
				var summary []string
				for _, a := range ads {
					summary = append(summary, q.fmtAdvert(a, nowMs))
				}
				q.log.Infof("PIGGYBACK SEND → %d adverts:\n  - %s", len(ads), strings.Join(summary, "\n  - "))
				ctx = metadata.AppendToOutgoingContext(ctx, mdKey, encodeMD(ads))
			}

		}
		return invoker(ctx, method, req, reply, cc, opts...)
	}
}

func UnaryServerInterceptor(q *Queue) grpc.UnaryServerInterceptor {
	return func(
		ctx context.Context,
		req interface{},
		info *grpc.UnaryServerInfo,
		handler grpc.UnaryHandler,

	) (interface{}, error) {

		if q != nil && q.IsPaused() {
			return nil, status.Error(codes.Unavailable, "node temporarily unavailable (leave/fault)")
		}
		if q != nil {
			if md, ok := metadata.FromIncomingContext(ctx); ok {
				vals := md.Get(mdKey)
				for _, v := range vals {
					arr, err := decodeMD(v)
					if err != nil || len(arr) == 0 {
						continue
					}
					nowMs := q.clock.NowSim().UnixMilli()
					var summary []string
					for _, a := range arr {
						summary = append(summary, q.fmtAdvert(a, nowMs))
					}
					q.log.Infof("PIGGYBACK RECV ← %d adverts:\n  - %s", len(arr), strings.Join(summary, "\n  - "))
					for _, a := range arr {
						if a.LeaveUntilMs > nowMs {
							rem := time.Duration(a.LeaveUntilMs-nowMs) * time.Millisecond
							q.log.Infof("LEAVE NOTICE ← node=%s for ~%s", a.NodeId, rem.Truncate(100*time.Millisecond))
						}
						q.Upsert(a)
					}
				}
			}

		}
		return handler(ctx, req)
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

// | nid_len(2) | nid | avail(1) | create(8) | expire(8) | flags(1) | busy_until(8) | leave_until(8) |
func encodeMD(arr []Advert) string {
	var buf []byte
	for _, a := range arr {
		nid := []byte(a.NodeId)
		tmp := make([]byte, 2+len(nid)+1+8+8+1+8+8)
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
		binary.BigEndian.PutUint64(tmp[28+len(nid):36+len(nid)], uint64(a.LeaveUntilMs))
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
		if len(b) < 2+l+1+8+8+1+8+8 {
			break
		}
		nid := string(b[2 : 2+l])
		avail := uint8(b[2+l])
		create := int64(binary.BigEndian.Uint64(b[3+l : 11+l]))
		exp := int64(binary.BigEndian.Uint64(b[11+l : 19+l]))
		flags := b[19+l]
		hasGPU := (flags & 0x01) != 0
		busy := int64(binary.BigEndian.Uint64(b[20+l : 28+l]))
		leave := int64(binary.BigEndian.Uint64(b[28+l : 36+l]))

		out = append(out, Advert{
			NodeId:       nid,
			Avail:        avail,
			CreateMs:     create,
			ExpireMs:     exp,
			HasGPU:       hasGPU,
			BusyUntilMs:  busy,
			LeaveUntilMs: leave,
		})
		b = b[36+l:]
	}
	return out, nil
}

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

// Pause/Resume del traffico piggyback (usati da leave/fault)
func (q *Queue) Pause()         { atomic.StoreInt32(&q.paused, 1) }
func (q *Queue) Resume()        { atomic.StoreInt32(&q.paused, 0) }
func (q *Queue) IsPaused() bool { return atomic.LoadInt32(&q.paused) == 1 }

// loadPctFromAvail: converte avail(0..255) in load% (0..100)
func loadPctFromAvail(av uint8) float64 {
	return float64(255-int(av)) * 100.0 / 255.0
}

// fmtAdvert produce una stringa compatta e leggibile per un advert.
func (q *Queue) fmtAdvert(a Advert, nowMs int64) string {
	load := loadPctFromAvail(a.Avail)
	age := time.Duration(nowMs-a.CreateMs) * time.Millisecond
	ttlRem := time.Duration(a.ExpireMs-nowMs) * time.Millisecond
	if ttlRem < 0 {
		ttlRem = 0
	}
	busyRem := time.Duration(a.BusyUntilMs-nowMs) * time.Millisecond
	if busyRem < 0 {
		busyRem = 0
	}
	leaveRem := time.Duration(a.LeaveUntilMs-nowMs) * time.Millisecond
	if leaveRem < 0 {
		leaveRem = 0
	}

	// Tag freschezza (conservativo: se age<0 non lo mostriamo)
	freshTag := "fresh"
	if age > 0 && ttlRem == 0 {
		freshTag = "expired"
	}

	return fmt.Sprintf("%s load=%.1f%% age=%s ttl=%s busy=%s leave=%s gpu=%t %s",
		a.NodeId,
		load,
		age.Truncate(100*time.Millisecond),
		ttlRem.Truncate(100*time.Millisecond),
		busyRem.Truncate(100*time.Millisecond),
		leaveRem.Truncate(100*time.Millisecond),
		a.HasGPU,
		freshTag,
	)
}
