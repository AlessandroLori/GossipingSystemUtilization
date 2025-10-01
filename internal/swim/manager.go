package swim

import (
	"context"
	"math/rand"
	"sort"
	"strings"
	"sync"
	"time"

	"GossipSystemUtilization/internal/logx"
	"GossipSystemUtilization/internal/simclock"
	proto "GossipSystemUtilization/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/metadata"
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

const mdKeySwim = "x-swim" // metadata per annunci SWIM: es. "dead:<nodeID>"

// Callback opzionale per notifiche membership (ALIVE/SUSPECT/DEAD)
type StatusChangeHandler func(id, addr string, old, new Status, reason string)

type Manager struct {
	selfID   string
	selfAddr string

	log   *logx.Logger
	clock *simclock.Clock
	rnd   *rand.Rand

	mu            sync.RWMutex
	peers         map[string]*peerEntry
	onChange      StatusChangeHandler
	deathAnnounce map[string]time.Time

	// parametri SWIM
	period           time.Duration // es. 1s SIM
	timeout          time.Duration // es. 250ms reale (non sim)
	indirectK        int           // es. 3
	suspicionTimeout time.Duration // es. 6s SIM

	seq uint64

	stopCh chan struct{}
	gate   func() bool
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

func NewManager(selfID, selfAddr string, log *logx.Logger, clock *simclock.Clock, rnd *rand.Rand, cfg Config, gate func() bool) *Manager {
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
		selfID:        selfID,
		selfAddr:      selfAddr,
		log:           log,
		clock:         clock,
		rnd:           rnd,
		peers:         make(map[string]*peerEntry),
		deathAnnounce: make(map[string]time.Time),

		period:           time.Duration(cfg.PeriodSimS * float64(time.Second)),
		timeout:          time.Duration(cfg.TimeoutRealMs) * time.Millisecond,
		indirectK:        cfg.IndirectK,
		suspicionTimeout: time.Duration(cfg.SuspicionTimeoutS * float64(time.Second)),

		stopCh: make(chan struct{}),
		gate:   gate,
	}
}

func (m *Manager) SetStatusChangeHandler(h StatusChangeHandler) {
	m.onChange = h
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
	case Dead:
		m.markDeath(p.ID) // annunceremo questa morte per qualche tick
	}
	m.log.Warnf("SWIM %s → %s  peer=%s (%s)  reason=%s", old, newSt, p.ID, p.Addr, reason)

	if m.onChange != nil {
		m.onChange(p.ID, p.Addr, old, newSt, reason)
	}
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
	// gate immediato: se il nodo è "down", niente rete
	if m.gate != nil && !m.gate() {
		return false
	}

	deaths := m.collectDeathAnnounces()
	ctx, cancel := context.WithTimeout(context.Background(), m.timeout)
	defer cancel()

	if len(deaths) > 0 {
		kv := make([]string, 0, len(deaths)*2)
		for _, id := range deaths {
			kv = append(kv, mdKeySwim, "dead:"+id)
		}
		ctx = metadata.AppendToOutgoingContext(ctx, kv...)
		m.log.Warnf("SWIM UPDATE SEND → %d death(s) to=%s  ids=%s", len(deaths), addr, strings.Join(deaths, ","))
	}

	// ricontrolla gate prima del dial
	if m.gate != nil && !m.gate() {
		return false
	}

	conn, err := grpc.DialContext(ctx, addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		m.log.Warnf("SWIM PING → to=%s dial fail", addr)
		return false
	}
	defer conn.Close()

	// ricontrolla gate anche dopo il dial (se è sceso nel frattempo)
	if m.gate != nil && !m.gate() {
		return false
	}

	cli := proto.NewGossipClient(conn)
	m.log.Infof("SWIM PING → to=%s", addr)
	_, err = cli.Ping(ctx, &proto.PingRequest{FromId: m.selfID, Seq: m.nextSeq()})
	if err != nil {
		m.log.Warnf("SWIM PING ← to=%s ok=false (%v)", addr, err)
		return false
	}
	m.log.Infof("SWIM PING ← to=%s ok=true", addr)
	return true
}

func (m *Manager) pingReqK(target *peerEntry, helpers []*peerEntry) bool {
	// gate immediato
	if m.gate != nil && !m.gate() {
		return false
	}

	deaths := m.collectDeathAnnounces()
	okAny := false

	for i, h := range helpers {
		if i >= m.indirectK {
			break
		}

		// ricontrollo gate ad ogni iterazione
		if m.gate != nil && !m.gate() {
			return false
		}

		ctx, cancel := context.WithTimeout(context.Background(), m.timeout)
		if len(deaths) > 0 {
			kv := make([]string, 0, len(deaths)*2)
			for _, id := range deaths {
				kv = append(kv, mdKeySwim, "dead:"+id)
			}
			ctx = metadata.AppendToOutgoingContext(ctx, kv...)
			m.log.Warnf("SWIM UPDATE SEND → %d death(s) via helper=%s  ids=%s", len(deaths), h.Addr, strings.Join(deaths, ","))
		}

		// ricontrolla gate prima del dial
		if m.gate != nil && !m.gate() {
			cancel()
			return false
		}

		conn, err := grpc.DialContext(ctx, h.Addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
		if err != nil {
			cancel()
			m.log.Warnf("SWIM PINGREQ → helper=%s dial fail", h.Addr)
			continue
		}
		cli := proto.NewGossipClient(conn)

		m.log.Infof("SWIM PINGREQ → helper=%s target=%s", h.Addr, target.Addr)
		rep, err := cli.PingReq(ctx, &proto.PingReqRequest{
			FromId:     m.selfID,
			TargetAddr: target.Addr,
			Seq:        m.nextSeq(),
		})
		_ = conn.Close()
		cancel()

		if err == nil && rep.Ok {
			m.log.Infof("SWIM PINGREQ ← helper=%s ok=true", h.Addr)
			okAny = true
			break
		}
		if err != nil {
			m.log.Warnf("SWIM PINGREQ ← helper=%s ok=false (%v)", h.Addr, err)
		} else {
			m.log.Warnf("SWIM PINGREQ ← helper=%s ok=false", h.Addr)
		}
	}
	return okAny
}

func (m *Manager) nextSeq() uint64 {
	m.seq++
	return m.seq
}

func (m *Manager) markDeath(id string) {
	m.mu.Lock()
	// Paracadute nel caso qualcuno in futuro cambi il costruttore
	if m.deathAnnounce == nil {
		m.deathAnnounce = make(map[string]time.Time)
	}
	m.deathAnnounce[id] = m.clock.NowSim().Add(5 * m.period) // annuncia ~5 tick
	m.mu.Unlock()
}

func (m *Manager) collectDeathAnnounces() (ids []string) {
	now := m.clock.NowSim()
	m.mu.Lock()
	for id, until := range m.deathAnnounce {
		if now.Before(until) {
			ids = append(ids, id)
		} else {
			delete(m.deathAnnounce, id)
		}
	}
	m.mu.Unlock()
	return
}

func (m *Manager) tick() {
	// se il nodo è "down", nessuna attività SWIM
	if m.gate != nil && !m.gate() {
		return
	}

	// 1) scadi i SUSPECT → DEAD SENZA tenere il lock durante setStatus
	now := m.clock.NowSim()

	var toDead []*peerEntry
	m.mu.Lock()
	for _, p := range m.peers {
		if p.Status == Suspect && !p.SuspectDeadline.IsZero() && now.After(p.SuspectDeadline) {
			toDead = append(toDead, p)
		}
	}
	m.mu.Unlock()

	for _, p := range toDead {
		m.setStatus(p, Dead, "timeout")
	}

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

	// 4) ping indiretto
	helpers := m.listCandidates()
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

	// 5) nessun ack
	m.maybeSetSuspect(target, "no-ack direct+indirect")
}

// maybeSetSuspect porta a SUSPECT solo se era ALIVE; se è già SUSPECT non tocca la deadline.
func (m *Manager) maybeSetSuspect(p *peerEntry, reason string) {
	m.mu.Lock()
	defer m.mu.Unlock()
	switch p.Status {
	case Alive:
		m.setStatus(p, Suspect, reason) // qui la deadline viene impostata una sola volta
	case Suspect:
		// non rinfrescare la deadline: lasciamo che il timeout faccia SUSPECT→DEAD
		m.log.Warnf("SWIM still SUSPECT  peer=%s (%s) reason=%s", p.ID, p.Addr, reason)
	default:
		// DEAD: nulla
	}
}

func (m *Manager) Start() {
	go func() {
		for {
			select {
			case <-m.stopCh:
				return
			default:
			}
			// gate: se il nodo è in leave, SWIM resta silenzioso
			if m.gate != nil && !m.gate() {
				m.clock.SleepSim(m.period)
				continue
			}
			m.clock.SleepSim(m.period)
			m.tick()
		}
	}()
}

func (m *Manager) Stop() { close(m.stopCh) }
