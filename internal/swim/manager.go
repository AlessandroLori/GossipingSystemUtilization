package swim

import (
	"context"
	"math/rand"
	"sort"
	"sync"
	"time"

	"GossipSystemUtilization/internal/logx"
	"GossipSystemUtilization/internal/simclock"
	"GossipSystemUtilization/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Status int

const (
	Alive Status = iota
	Suspect
	Dead
)

func (s Status) String() string {
	switch s {
	case Alive:
		return "ALIVE"
	case Suspect:
		return "SUSPECT"
	default:
		return "DEAD"
	}
}

type peerEntry struct {
	ID     string
	Addr   string
	Status Status

	LastAckSim      time.Time
	SuspectDeadline time.Time
}

type Manager struct {
	selfID   string
	selfAddr string

	log   *logx.Logger
	clock *simclock.Clock
	rnd   *rand.Rand

	mu    sync.RWMutex
	peers map[string]*peerEntry

	// parametri SWIM
	period           time.Duration // es. 1s SIM
	timeout          time.Duration // es. 250ms reale (non sim)
	indirectK        int           // es. 3
	suspicionTimeout time.Duration // es. 6s SIM

	seq uint64

	stopCh chan struct{}
}

type Config struct {
	PeriodSimS        float64
	TimeoutRealMs     int
	IndirectK         int
	SuspicionTimeoutS float64
}

// AlivePeers: snapshot dei peer non DEAD.
type Peer struct{ ID, Addr string }

func (m *Manager) AlivePeers() []Peer {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]Peer, 0, len(m.peers))
	for _, p := range m.peers {
		if p.Status != Dead {
			out = append(out, Peer{ID: p.ID, Addr: p.Addr})
		}
	}
	return out
}

// FanoutHint: suggerisce un fanout minimo (qui costante).
func (m *Manager) FanoutHint(hint int) int {
	if hint <= 0 {
		hint = 2
	}
	return hint
}

func NewManager(selfID, selfAddr string, log *logx.Logger, clock *simclock.Clock, rnd *rand.Rand, cfg Config) *Manager {
	if cfg.PeriodSimS <= 0 {
		cfg.PeriodSimS = 1.0
	}
	if cfg.TimeoutRealMs <= 0 {
		cfg.TimeoutRealMs = 250
	}
	if cfg.IndirectK <= 0 {
		cfg.IndirectK = 3
	}
	if cfg.SuspicionTimeoutS <= 0 {
		cfg.SuspicionTimeoutS = 6.0
	}
	return &Manager{
		selfID:   selfID,
		selfAddr: selfAddr,
		log:      log,
		clock:    clock,
		rnd:      rnd,
		peers:    make(map[string]*peerEntry),

		period:           time.Duration(cfg.PeriodSimS * float64(time.Second)),
		timeout:          time.Duration(cfg.TimeoutRealMs) * time.Millisecond,
		indirectK:        cfg.IndirectK,
		suspicionTimeout: time.Duration(cfg.SuspicionTimeoutS * float64(time.Second)),

		stopCh: make(chan struct{}),
	}
}

func (m *Manager) AddPeer(id, addr string) {
	if id == "" || id == m.selfID || addr == "" {
		return
	}
	m.mu.Lock()
	defer m.mu.Unlock()
	if _, ok := m.peers[id]; ok {
		m.peers[id].Addr = addr // aggiorna eventuale nuovo addr
		return
	}
	m.peers[id] = &peerEntry{ID: id, Addr: addr, Status: Alive, LastAckSim: m.clock.NowSim()}
	m.log.Infof("SWIM ADD-PEER → %s (%s) stato=%s", id, addr, Alive)
}

func (m *Manager) setStatus(p *peerEntry, newSt Status, reason string) {
	if p.Status == newSt {
		return
	}
	old := p.Status
	p.Status = newSt
	switch newSt {
	case Alive:
		p.SuspectDeadline = time.Time{}
	case Suspect:
		p.SuspectDeadline = m.clock.NowSim().Add(m.suspicionTimeout)
	}
	m.log.Warnf("SWIM %s → %s  peer=%s (%s)  reason=%s", old, newSt, p.ID, p.Addr, reason)
}

func (m *Manager) listCandidates() []*peerEntry {
	m.mu.RLock()
	defer m.mu.RUnlock()
	out := make([]*peerEntry, 0, len(m.peers))
	for _, p := range m.peers {
		if p.Status != Dead {
			out = append(out, p)
		}
	}
	return out
}

func (m *Manager) pickTarget() *peerEntry {
	cands := m.listCandidates()
	if len(cands) == 0 {
		return nil
	}
	// mescola e scegli il "più vecchio" in ACK per dare opportunità a tutti
	m.rnd.Shuffle(len(cands), func(i, j int) { cands[i], cands[j] = cands[j], cands[i] })
	sort.SliceStable(cands, func(i, j int) bool { return cands[i].LastAckSim.Before(cands[j].LastAckSim) })
	return cands[0]
}

func (m *Manager) pingOnce(addr string) bool {
	ctx, cancel := context.WithTimeout(context.Background(), m.timeout)
	defer cancel()
	conn, err := grpc.DialContext(ctx, addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		return false
	}
	defer conn.Close()
	cli := proto.NewGossipClient(conn)
	_, err = cli.Ping(ctx, &proto.PingRequest{FromId: m.selfID, Seq: m.nextSeq()})
	return err == nil
}

func (m *Manager) pingReqK(target *peerEntry, helpers []*peerEntry) bool {
	okAny := false
	for i, h := range helpers {
		if i >= m.indirectK {
			break
		}
		ctx, cancel := context.WithTimeout(context.Background(), m.timeout)
		conn, err := grpc.DialContext(ctx, h.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
		cancel()
		if err != nil {
			continue
		}
		cli := proto.NewGossipClient(conn)
		ctx2, cancel2 := context.WithTimeout(context.Background(), m.timeout)
		rep, err := cli.PingReq(ctx2, &proto.PingReqRequest{
			FromId:     m.selfID,
			TargetAddr: target.Addr,
			Seq:        m.nextSeq(),
		})
		cancel2()
		_ = conn.Close()
		if err == nil && rep.Ok {
			okAny = true
			break
		}
	}
	return okAny
}

func (m *Manager) nextSeq() uint64 {
	m.seq++
	return m.seq
}

func (m *Manager) tick() {
	// 1) scadi i SUSPECT → DEAD
	now := m.clock.NowSim()
	m.mu.Lock()
	for _, p := range m.peers {
		if p.Status == Suspect && !p.SuspectDeadline.IsZero() && now.After(p.SuspectDeadline) {
			p.Status = Dead
			m.log.Errorf("SWIM SUSPECT→DEAD  peer=%s (%s)  reason=timeout", p.ID, p.Addr)
		}
	}
	m.mu.Unlock()

	// 2) scegli un target
	target := m.pickTarget()
	if target == nil {
		return
	}

	// 3) ping diretto
	if m.pingOnce(target.Addr) {
		m.mu.Lock()
		target.LastAckSim = now
		m.setStatus(target, Alive, "direct-ack")
		m.mu.Unlock()
		return
	}

	// 4) ping indiretto (k helpers)
	helpers := m.listCandidates()
	// rimuovi target dalla lista
	filter := helpers[:0]
	for _, h := range helpers {
		if h.ID != target.ID {
			filter = append(filter, h)
		}
	}
	helpers = filter
	m.rnd.Shuffle(len(helpers), func(i, j int) { helpers[i], helpers[j] = helpers[j], helpers[i] })

	if m.pingReqK(target, helpers) {
		m.mu.Lock()
		target.LastAckSim = now
		m.setStatus(target, Alive, "indirect-ack")
		m.mu.Unlock()
		return
	}

	// 5) niente ack → SUSPECT (o resta tale)
	m.mu.Lock()
	m.setStatus(target, Suspect, "no-ack direct+indirect")
	m.mu.Unlock()
}

func (m *Manager) Start() {
	go func() {
		for {
			select {
			case <-m.stopCh:
				return
			default:
				m.clock.SleepSim(m.period)
				m.tick()
			}
		}
	}()
}

func (m *Manager) Stop() { close(m.stopCh) }
