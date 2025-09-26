package affinity

import (
	"context"
	"fmt"
	"math/rand"
	"sort"
	"strings"
	"time"

	"GossipSystemUtilization/internal/simclock"
)

type Manager struct {
	cfg Config
	rng *rand.Rand
	clk *simclock.Clock

	rep *ReputationTable
	fr  *Friends
}

func NewManager(cfg Config, rng *rand.Rand, clk *simclock.Clock) *Manager {
	cfg.Sanitize()
	if rng == nil {
		rng = rand.New(rand.NewSource(time.Now().UnixNano()))
	}
	m := &Manager{
		cfg: cfg,
		rng: rng,
		clk: clk,
		rep: NewReputationTable(clk, cfg.HalfLife.Seconds(), cfg.DecayEvery.Milliseconds(), cfg.MinScore, cfg.MaxScore, cfg.Verbose),
		fr:  NewFriends(cfg.MaxFriendsPerClass, cfg.Verbose),
	}
	return m
}

// Avvia il decadimento periodico della reputation.
func (m *Manager) StartDecayLoop(ctx context.Context) {
	stop := make(chan struct{})
	m.rep.StartDecayLoop(stop)
	go func() {
		<-ctx.Done()
		close(stop)
	}()
}

// Log periodico (telemetria) — invia DumpString() al sink
func (m *Manager) StartLogLoop(ctx context.Context, interval time.Duration, sink func(string)) {
	if interval <= 0 {
		interval = 10 * time.Second
	}
	if sink == nil {
		sink = func(string) {}
	}
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				s := m.DumpString(8)
				sink(s)
				if m.clk != nil {
					m.clk.SleepSim(interval)
				} else {
					time.Sleep(interval)
				}
			}
		}
	}()
}

// ===== Hooks =====

func (m *Manager) UpdateOnProbe(peer string, class JobClass, accepted bool) {
	if accepted {
		m.rep.OnProbeAccept(class, peer)
		m.touchFriend(class, peer)
	} else {
		m.rep.OnProbeRefuse(class, peer)
	}
}

func (m *Manager) UpdateOnCommit(peer string, class JobClass, outcome Outcome) {
	switch outcome {
	case OutcomeCompleted:
		m.rep.OnCommitOk(class, peer)
		m.touchFriend(class, peer)
	case OutcomeRefused:
		m.rep.OnCommitRefused(class, peer)
	case OutcomeTimeout:
		m.rep.OnTimeout(class, peer)
	case OutcomeCancelled:
		m.rep.OnCancelled(class, peer)
	}
}

func (m *Manager) touchFriend(class JobClass, peer string) {
	now := nowSimMs(m.clk)
	m.fr.Touch(class, peer, now)
}

// ===== Ranking =====

// Rank calcola lo score composito e restituisce i migliori topK (ordinati desc).
// Stampa anche un riepilogo dettagliato dei componenti di score.
func (m *Manager) Rank(class JobClass, in []Candidate, topK int) []ScoredCandidate {
	if topK <= 0 {
		topK = 8
	}
	scored := make([]ScoredCandidate, 0, len(in))

	for _, c := range in {
		// filtro compatibilità GPU per classi GPU-heavy
		if class == ClassGPUHeavy && !c.HasGPU {
			continue
		}
		// componi score
		p, a, l, pen, s := m.scoreOne(class, c)
		sc := ScoredCandidate{Candidate: c, Score: s}
		scored = append(scored, sc)

		// stampa breakdown per ciascun candidato
		fmt.Printf("[Affinity/Rank] class=%v peer=%s  P=%.3f A=%.3f L=%.3f Pen=%.2f  | wP=%.2f wA=%.2f wL=%.2f wX=%.2f  => SCORE=%.4f fresh=%v gpu=%v\n",
			class, c.PeerID, p, a, l, pen, m.cfg.WReputation, m.cfg.WPiggyback, m.cfg.WLeastLoad, m.cfg.WPenalty, s, c.Fresh, c.HasGPU)
	}

	// ordina desc per score
	sort.Slice(scored, func(i, j int) bool {
		if scored[i].Score == scored[j].Score {
			// tie-break stabile: per id
			return scored[i].PeerID < scored[j].PeerID
		}
		return scored[i].Score > scored[j].Score
	})

	if len(scored) > topK {
		scored = scored[:topK]
	}

	// stampa topK riassunto
	var b strings.Builder
	fmt.Fprintf(&b, "[Affinity/Rank] TOPK class=%v (k=%d)\n", class, len(scored))
	for i, sc := range scored {
		fmt.Fprintf(&b, "  #%d %-24s score=%.4f\n", i+1, sc.PeerID, sc.Score)
	}
	fmt.Print(b.String())

	// aggiorna Friends con i topK (recency)
	now := nowSimMs(m.clk)
	for _, sc := range scored {
		m.fr.Touch(class, sc.PeerID, now)
	}

	return scored
}

func (m *Manager) scoreOne(class JobClass, c Candidate) (P, A, L, Penalty, Score float64) {
	// Pheromone (reputation) normalizzato [0..1]
	P = m.rep.GetNorm(class, c.PeerID)

	// Piggyback avail in [0..1] se presente, altrimenti neutro 0.5
	A = c.AdvertAvail
	if A < 0 {
		A = 0.5
	}

	// LeastLoad: 1 - projectedLoad se noto, altrimenti 0.5
	if c.ProjectedLoad >= 0 {
		L = 1.0 - clamp01(c.ProjectedLoad)
	} else {
		L = 0.5
	}

	// Penalty (cooldown) [0..1]
	Penalty = clamp01(c.CooldownPenalty)

	// Penalizza dati stantii: attenua contributi A e L
	if !c.Fresh {
		A *= 0.5
		L *= 0.7
		// piccola penalità extra
		Penalty = clamp01(Penalty + 0.1)
	}

	Score = m.cfg.WReputation*P + m.cfg.WPiggyback*A + m.cfg.WLeastLoad*L - m.cfg.WPenalty*Penalty
	return
}

// ===== Telemetria =====

func (m *Manager) DumpString(maxPerClass int) string {
	if maxPerClass <= 0 {
		maxPerClass = 8
	}
	classes := []JobClass{ClassGeneral, ClassCPUOnly, ClassMemHeavy, ClassGPUHeavy}

	var b strings.Builder
	fmt.Fprintf(&b, "Friends (max=%d) & Reputation (%.0fs HL)\n",
		m.cfg.MaxFriendsPerClass, m.cfg.HalfLife.Seconds())

	for _, c := range classes {
		fmt.Fprintf(&b, "- Class=%v\n", c)

		// Friends snapshot
		fs := m.fr.Snapshot(c)
		if len(fs) > maxPerClass {
			fs = fs[:maxPerClass]
		}
		fmt.Fprintf(&b, "  Friends: %v\n", fs)

		// Reputation top
		snap := m.rep.SnapshotClass(c)
		type kv struct {
			id  string
			raw float64
		}
		top := make([]kv, 0, len(snap))
		for id, v := range snap {
			top = append(top, kv{id: id, raw: v})
		}
		sort.Slice(top, func(i, j int) bool { return top[i].raw > top[j].raw })
		if len(top) > maxPerClass {
			top = top[:maxPerClass]
		}
		fmt.Fprintf(&b, "  RepTop: [")
		for i, t := range top {
			if i > 0 {
				b.WriteString("  ")
			}
			fmt.Fprintf(&b, "%s: raw=%.2f norm=%.2f", t.id, t.raw, (t.raw-m.cfg.MinScore)/(m.cfg.MaxScore-m.cfg.MinScore))
		}
		b.WriteString("]\n")
	}
	return b.String()
}

// ===== Helpers =====

func clamp01(x float64) float64 {
	if x < 0 {
		return 0
	}
	if x > 1 {
		return 1
	}
	return x
}
