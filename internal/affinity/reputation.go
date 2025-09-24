package affinity

import (
	"math"
	"sync"

	"GossipSystemUtilization/internal/simclock"
)

type repEntry struct {
	Score      float64
	UpdatedSim int64 // ms SIM
}

type ReputationTable struct {
	mu    sync.RWMutex
	table map[JobClass]map[string]*repEntry // class -> peerID -> entry
	cfg   *Config
	clk   *simclock.Clock
}

func newReputationTable(cfg *Config, clk *simclock.Clock) *ReputationTable {
	return &ReputationTable{
		table: map[JobClass]map[string]*repEntry{
			ClassCPUOnly:  {},
			ClassGPUHeavy: {},
			ClassMemHeavy: {},
			ClassGeneral:  {},
		},
		cfg: cfg,
		clk: clk,
	}
}

func (t *ReputationTable) get(class JobClass, peer string) float64 {
	t.mu.RLock()
	defer t.mu.RUnlock()
	if m := t.table[class]; m != nil {
		if e := m[peer]; e != nil {
			return e.Score
		}
	}
	return 0
}

func (t *ReputationTable) bump(class JobClass, peer string, delta float64) {
	now := t.clk.NowSimMs()
	t.mu.Lock()
	defer t.mu.Unlock()
	m := t.table[class]
	if m == nil {
		m = make(map[string]*repEntry)
		t.table[class] = m
	}
	e := m[peer]
	if e == nil {
		e = &repEntry{}
		m[peer] = e
	}
	e.Score += delta
	if e.Score < t.cfg.MinScore {
		e.Score = t.cfg.MinScore
	}
	if e.Score > t.cfg.MaxScore {
		e.Score = t.cfg.MaxScore
	}
	e.UpdatedSim = now
}

func (t *ReputationTable) DecayAll() {
	hl := t.cfg.HalfLife
	if hl <= 0 {
		return
	}
	// fattore di decadimento per "DecayEvery"
	f := math.Exp(-float64(t.cfg.DecayEvery) / float64(hl))

	t.mu.Lock()
	defer t.mu.Unlock()
	for _, m := range t.table {
		for _, e := range m {
			e.Score *= f
			if e.Score < t.cfg.MinScore {
				e.Score = t.cfg.MinScore
			}
			if e.Score > t.cfg.MaxScore {
				e.Score = t.cfg.MaxScore
			}
		}
	}
}

func (t *ReputationTable) UpdateOnProbe(class JobClass, peer string, accepted bool) {
	if accepted {
		t.bump(class, peer, +0.5)
	} else {
		t.bump(class, peer, -0.5)
	}
}

func (t *ReputationTable) UpdateOnCommit(class JobClass, peer string, outcome Outcome) {
	switch outcome {
	case OutcomeCompleted:
		t.bump(class, peer, +2.0)
	case OutcomeRefused:
		t.bump(class, peer, -1.0)
	case OutcomeTimeout:
		t.bump(class, peer, -3.0)
	case OutcomeCancelled:
		t.bump(class, peer, -1.5)
	}
}
