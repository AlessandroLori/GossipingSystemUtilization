package affinity

import (
	"context"
	"fmt"
	"math"
	"math/rand"
	"sort"
	"strings"
	"time"

	"GossipSystemUtilization/internal/simclock"
)

type Manager struct {
	cfg  Config
	rep  *ReputationTable
	fr   *Friends
	rand *rand.Rand
	clk  *simclock.Clock
}

func NewManager(cfg Config, r *rand.Rand, clk *simclock.Clock) *Manager {
	if r == nil {
		r = rand.New(rand.NewSource(time.Now().UnixNano()))
	}
	if clk == nil {
		clk = simclock.New(1.0) // fallback: no scaling
	}
	return &Manager{
		cfg:  cfg,
		rep:  newReputationTable(&cfg, clk),
		fr:   newFriends(cfg.MaxFriendsPerClass),
		rand: r,
		clk:  clk,
	}
}

// Decadimento legato al tempo SIM (usa SleepSim)
func (m *Manager) StartDecayLoop(ctx context.Context) {
	go func() {
		for {
			select {
			case <-ctx.Done():
				return
			default:
				m.rep.DecayAll()
				m.clk.SleepSim(m.cfg.DecayEvery)
			}
		}
	}()
}

// ===== Hooks =====

func (m *Manager) UpdateOnProbe(peer string, class JobClass, accepted bool) {
	m.rep.UpdateOnProbe(class, peer, accepted)
}
func (m *Manager) UpdateOnCommit(peer string, class JobClass, outcome Outcome) {
	m.rep.UpdateOnCommit(class, peer, outcome)
}
func (m *Manager) TouchFriend(peer string, class JobClass) {
	m.fr.Touch(class, peer, m.clk.NowSimMs())
}

// ===== Scoring & Ranking =====

func (m *Manager) scoreOne(class JobClass, c Candidate) float64 {
	// Incompatibilità hard
	if class == ClassGPUHeavy && !c.HasGPU {
		return -1e9
	}
	// P: normalizza score in [0..1]
	P := (m.rep.get(class, c.PeerID) - m.cfg.MinScore) / (m.cfg.MaxScore - m.cfg.MinScore)
	if P < 0 {
		P = 0
	}
	if P > 1 {
		P = 1
	}

	// A: availability da piggyback [0..1], neutro se ignoto
	A := c.AdvertAvail
	if A < 0 {
		A = 0.5
	}
	if A > 1 {
		A = 1
	}
	if A < 0 {
		A = 0
	}

	// L: least-load (1-libero), neutro se ignoto
	L := 0.5
	if c.ProjectedLoad >= 0 {
		L = 1.0 - c.ProjectedLoad // 1=libero
		if L < 0 {
			L = 0
		}
		if L > 1 {
			L = 1
		}
	}

	pen := c.CooldownPenalty
	if pen < 0 {
		pen = 0
	}
	if pen > 1 {
		pen = 1
	}

	score := m.cfg.WReputation*P + m.cfg.WPiggyback*A + m.cfg.WLeastLoad*L - m.cfg.WPenalty*pen
	// penalità leggera se non Fresh
	if !c.Fresh {
		score -= 0.15
	}
	return score
}

func softmaxPick(r *rand.Rand, xs []float64, temp float64) int {
	if temp <= 0 {
		temp = 0.2
	}
	sum := 0.0
	ws := make([]float64, len(xs))
	for i, v := range xs {
		w := math.Exp(v / temp)
		ws[i] = w
		sum += w
	}
	u := r.Float64() * sum
	acc := 0.0
	for i, w := range ws {
		acc += w
		if u <= acc {
			return i
		}
	}
	return len(xs) - 1
}

// Rank + selezione con ε-greedy + softmax
func (m *Manager) Rank(class JobClass, in []Candidate, topN int) []ScoredCandidate {
	// Filtri rapidi: compatibilità
	cands := make([]Candidate, 0, len(in))
	for _, c := range in {
		if class == ClassGPUHeavy && !c.HasGPU {
			continue
		}
		cands = append(cands, c)
	}
	if len(cands) == 0 {
		return nil
	}

	// Compute base scores
	scores := make([]ScoredCandidate, 0, len(cands))
	for _, c := range cands {
		s := m.scoreOne(class, c)
		scores = append(scores, ScoredCandidate{Candidate: c, Score: s})
	}
	// Ordina per score desc
	sort.Slice(scores, func(i, j int) bool { return scores[i].Score > scores[j].Score })

	// Esplorazione: prendi i topN poi softmax
	N := topN
	if N > len(scores) {
		N = len(scores)
	}
	top := scores[:N]

	res := make([]ScoredCandidate, 0, N)
	used := make(map[string]struct{})

	pool := make([]float64, N)
	for i := 0; i < N; i++ {
		pool[i] = top[i].Score
	}
	for len(res) < N && len(res) < len(top) {
		idx := softmaxPick(m.rand, pool, m.cfg.SoftmaxTemp)
		c := top[idx]
		if _, ok := used[c.PeerID]; !ok {
			res = append(res, c)
			used[c.PeerID] = struct{}{}
		} else {
			break
		}
	}

	// ε-greedy: forse aggiungi 1 jolly fuori topN
	if m.rand.Float64() < m.cfg.Epsilon && len(scores) > N {
		j := N + m.rand.Intn(len(scores)-N)
		res = append(res, scores[j])
	}

	// Touch friends
	for _, sc := range res {
		m.TouchFriend(sc.PeerID, class)
	}
	return res
}

// ProjectedLoadByClass: 0=libero .. 1=pieno
func ProjectedLoadByClass(cpuPct, memPct, gpuPct float64, class JobClass, hasGPU bool) float64 {
	switch class {
	case ClassCPUOnly:
		return cpuPct / 100.0
	case ClassMemHeavy:
		return memPct / 100.0
	case ClassGPUHeavy:
		if !hasGPU {
			return 1.0
		}
		return gpuPct / 100.0
	case ClassGeneral:
		m := cpuPct
		if memPct > m {
			m = memPct
		}
		if gpuPct > m {
			m = gpuPct
		}
		return m / 100.0
	default:
		return 1.0
	}
}

// ===== Pretty logging (dump) =====

func className(c JobClass) string {
	switch c {
	case ClassCPUOnly:
		return "CPU_ONLY"
	case ClassGPUHeavy:
		return "GPU_HEAVY"
	case ClassMemHeavy:
		return "MEM_HEAVY"
	case ClassGeneral:
		return "GENERAL"
	default:
		return fmt.Sprintf("CLASS_%d", c)
	}
}

// DumpString restituisce una stringa leggibile con:
//   - Top-N peer per reputazione (score e "age" dall'ultimo update)
//   - Friends (lista abbreviata con età ultima vista)
func (m *Manager) DumpString(maxPerClass int) string {
	if maxPerClass <= 0 {
		maxPerClass = 8
	}
	nowMs := m.clk.NowSimMs()

	var b strings.Builder
	fmt.Fprintf(&b, "Friends & Reputation @ %s (SIM)\n", m.clk.NowSim().Format("15:04:05.000"))

	classes := []JobClass{ClassCPUOnly, ClassMemHeavy, ClassGPUHeavy, ClassGeneral}
	for _, cls := range classes {
		// --- snapshot reputazione (copy sotto lock) ---
		type pair struct {
			peer  string
			score float64
			ageMs int64
		}
		reps := make([]pair, 0, 32)
		func() {
			m.rep.mu.RLock()
			defer m.rep.mu.RUnlock()
			if mp := m.rep.table[cls]; mp != nil {
				for p, e := range mp {
					age := nowMs - e.UpdatedSim
					reps = append(reps, pair{peer: p, score: e.Score, ageMs: age})
				}
			}
		}()
		sort.Slice(reps, func(i, j int) bool { return reps[i].score > reps[j].score })
		if len(reps) > maxPerClass {
			reps = reps[:maxPerClass]
		}

		// --- snapshot friends (copy sotto lock) ---
		type fpair struct {
			peer  string
			ageMs int64
		}
		friends := make([]fpair, 0, 32)
		func() {
			m.fr.mu.Lock()
			defer m.fr.mu.Unlock()
			if mp := m.fr.table[cls]; mp != nil {
				for p, ts := range mp {
					age := nowMs - ts
					friends = append(friends, fpair{peer: p, ageMs: age})
				}
			}
		}()
		sort.Slice(friends, func(i, j int) bool { return friends[i].ageMs < friends[j].ageMs })
		if len(friends) > maxPerClass {
			friends = friends[:maxPerClass]
		}

		// --- stampa sezione classe ---
		fmt.Fprintf(&b, "· %s\n", className(cls))

		if len(reps) == 0 {
			fmt.Fprintf(&b, "   TopRep: (vuoto)\n")
		} else {
			fmt.Fprintf(&b, "   TopRep: ")
			for i, r := range reps {
				if i > 0 {
					b.WriteString(" | ")
				}
				fmt.Fprintf(&b, "%s s=%.2f age=%.1fs", r.peer, r.score, float64(r.ageMs)/1000.0)
			}
			b.WriteByte('\n')
		}

		if len(friends) == 0 {
			fmt.Fprintf(&b, "   Friends: (vuoto)\n")
		} else {
			fmt.Fprintf(&b, "   Friends: ")
			for i, f := range friends {
				if i > 0 {
					b.WriteString(" | ")
				}
				fmt.Fprintf(&b, "%s age=%.1fs", f.peer, float64(f.ageMs)/1000.0)
			}
			b.WriteByte('\n')
		}
	}
	return b.String()
}

// StartLogLoop stampa periodicamente il dump leggibile usando la sink passata.
// Esempio di sink: func(s string){ log.Infof("AFFINITY\n%s", s) }
func (m *Manager) StartLogLoop(ctx context.Context, interval time.Duration, sink func(string)) {
	if interval <= 0 {
		interval = 12 * time.Second
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
				m.clk.SleepSim(interval)
			}
		}
	}()
}
