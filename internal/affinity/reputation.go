package affinity

import (
	"fmt"
	"math"
	"sync"
	"time"

	simclock "GossipSystemUtilization/internal/simclock"
)

// ReputationTable mantiene score per (classe-di-job, peer) con decadimento temporale.
// Gli score sono clampati in [minScore, maxScore] e normalizzabili in [0,1] per lo scoring composito.
type ReputationTable struct {
	clk *simclock.Clock

	mu        sync.RWMutex
	score     map[JobClass]map[string]float64 // class -> peer -> raw score
	lastDecay int64                           // last decay timestamp (SIM, ms)

	halfLifeSec   float64 // half-life in secondi di TEMPO SIMULATO
	decayEveryMs  int64   // passo di decadimento (SIM, ms)
	minScore      float64 // clamp minimo
	maxScore      float64 // clamp massimo
	verbosePrints bool
}

// NewReputationTable crea una nuova tabella reputazione.
func NewReputationTable(clk *simclock.Clock, halfLifeSec float64, decayEveryMs int64, minScore, maxScore float64, verbose bool) *ReputationTable {
	if halfLifeSec <= 0 {
		halfLifeSec = 90
	}
	if decayEveryMs <= 0 {
		decayEveryMs = 5000
	}
	if minScore >= maxScore {
		minScore, maxScore = -5, 10
	}

	now := nowSimMs(clk)

	rt := &ReputationTable{
		clk:           clk,
		score:         make(map[JobClass]map[string]float64, 8),
		lastDecay:     now,
		halfLifeSec:   halfLifeSec,
		decayEveryMs:  decayEveryMs,
		minScore:      minScore,
		maxScore:      maxScore,
		verbosePrints: verbose,
	}
	return rt
}

// ---------- Lettura ----------

// GetRaw ritorna lo score grezzo (clampato) per (classe, peer).
func (r *ReputationTable) GetRaw(class JobClass, peer string) float64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	m := r.ensureClassLocked(class)
	return clamp(m[peer], r.minScore, r.maxScore)
}

// GetNorm ritorna lo score normalizzato in [0,1].
func (r *ReputationTable) GetNorm(class JobClass, peer string) float64 {
	raw := r.GetRaw(class, peer)
	// Mappa [min..max] -> [0..1].
	return (raw - r.minScore) / (r.maxScore - r.minScore)
}

// SnapshotClass torna la mappa (copia) score della classe (solo per debug/telemetria).
func (r *ReputationTable) SnapshotClass(class JobClass) map[string]float64 {
	r.mu.RLock()
	defer r.mu.RUnlock()
	orig := r.ensureClassLocked(class)
	out := make(map[string]float64, len(orig))
	for k, v := range orig {
		out[k] = v
	}
	return out
}

// ---------- Aggiornamenti (rinforzi/penalità) ----------

// Bump applica un delta allo score (con clamp).
func (r *ReputationTable) Bump(class JobClass, peer string, delta float64, reason string) {
	r.mu.Lock()
	defer r.mu.Unlock()

	m := r.ensureClassLocked(class)
	old := clamp(m[peer], r.minScore, r.maxScore)
	newv := clamp(old+delta, r.minScore, r.maxScore)
	m[peer] = newv

	if r.verbosePrints {
		fmt.Printf("[Affinity/Rep] class=%v peer=%s delta=%+.2f reason=%s  |  raw: %.3f -> %.3f (norm=%.3f)\n",
			class, peer, delta, reason, old, newv, (newv-r.minScore)/(r.maxScore-r.minScore))
	}
}

// Eventi
func (r *ReputationTable) OnProbeAccept(class JobClass, peer string) {
	r.Bump(class, peer, +0.5, "probe_accept")
}
func (r *ReputationTable) OnProbeRefuse(class JobClass, peer string) {
	r.Bump(class, peer, -0.5, "probe_refuse")
}
func (r *ReputationTable) OnCommitOk(class JobClass, peer string) {
	r.Bump(class, peer, +2.0, "commit_ok")
}
func (r *ReputationTable) OnCommitRefused(class JobClass, peer string) {
	r.Bump(class, peer, -1.0, "commit_refused")
}
func (r *ReputationTable) OnTimeout(class JobClass, peer string) {
	r.Bump(class, peer, -3.0, "timeout")
}
func (r *ReputationTable) OnCancelled(class JobClass, peer string) {
	r.Bump(class, peer, -1.5, "cancelled")
}

// ---------- Decadimento ----------

// DecayAll applica il decadimento esponenziale su tutti gli score in base al
// tempo SIM trascorso dall’ultima applicazione (half-life).
func (r *ReputationTable) DecayAll() {
	r.mu.Lock()
	defer r.mu.Unlock()

	now := nowSimMs(r.clk)
	dtMs := now - r.lastDecay
	if dtMs <= 0 {
		return
	}

	// Fattore di decadimento: score *= exp(-Δt / halfLife).
	// halfLife in secondi; dt in ms -> converto.
	halfLifeMs := r.halfLifeSec * 1000.0
	f := math.Exp(-float64(dtMs) / halfLifeMs)
	if f >= 0.999999 { // delta troppo piccolo, salta
		return
	}

	for class, mm := range r.score {
		for peer, v := range mm {
			nv := clamp(v*f, r.minScore, r.maxScore)
			if r.verbosePrints && nv != v {
				fmt.Printf("[Affinity/Rep] decay class=%v peer=%s  %.3f -> %.3f (factor=%.6f)\n", class, peer, v, nv, f)
			}
			mm[peer] = nv
		}
	}
	r.lastDecay = now
}

// StartDecayLoop lancia una goroutine che applica il decadimento ogni decayEveryMs (tempo SIM).
func (r *ReputationTable) StartDecayLoop(stopCh <-chan struct{}) {
	interval := r.decayEveryMs
	if interval <= 0 {
		interval = 5000
	}
	go func() {
		for {
			select {
			case <-stopCh:
				if r.verbosePrints {
					fmt.Printf("[Affinity/Rep] decay loop: STOP\n")
				}
				return
			default:
				r.DecayAll()
				// Usa il clock simulato per dormire in millisecondi.
				if r.clk != nil {
					r.clk.SleepSim(time.Duration(interval) * time.Millisecond)
				} else {
					time.Sleep(time.Duration(interval) * time.Millisecond)
				}
			}
		}
	}()
}

// ---------- Helpers ----------

func (r *ReputationTable) ensureClassLocked(class JobClass) map[string]float64 {
	m, ok := r.score[class]
	if !ok {
		m = make(map[JobClass]map[string]float64, 64)[class] // placeholder per evitare nil
		m = make(map[string]float64, 64)
		r.score[class] = m
	}
	return m
}

func clamp(x, lo, hi float64) float64 {
	if x < lo {
		return lo
	}
	if x > hi {
		return hi
	}
	return x
}
