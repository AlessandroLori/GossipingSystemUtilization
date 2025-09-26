package scheduler

import (
	"fmt"
	"math"
	"math/rand"
	"sort"
	"time"

	"GossipSystemUtilization/internal/jobs"
	"GossipSystemUtilization/internal/logx"
	"GossipSystemUtilization/internal/simclock"
	"GossipSystemUtilization/proto"
)

// Forniremo qui engine.LocalSample dal main.
type CandidateSampler func(max int) []*proto.Stats

// Applica il job localmente (es. node.StartJobLoad). true se accettato.
type LocalCommitFn func(job jobs.Spec) bool

// (Opzionale) Hook per leggere (dal Piggyback) l'ultimo advert noto per un peer.
// Se non lo imposti, lo scoring usa solo AE/Stats.
// - avail: 0..255 (stesso significato di Advert.Avail). Verrà normalizzato a 0..1.
// - ok:    true se l’advert è noto.
// - fresh: true se l’advert è fresco (non scaduto, secondo il tuo piggyback).
type PBLookup func(nodeID string) (avail uint8, ok bool, fresh bool)

// Parametri dello scheduler (blocco "scheduler" nel config.json).
type Params struct {
	Mode             string  // "local" (applica qui) oppure "advisory" (solo log)
	TopK             int     // quanti candidati considerare (post-ranking)
	OverprovisionPct float64 // margine di sicurezza (es 5 = 5%)
	PrintDecisions   bool

	// === Opzioni di esplorazione ===
	// Se SoftmaxTau > 0 usa softmax sampling sui punteggi; altrimenti ε-greedy.
	ExploreEpsilon float64 // prob. di esplorare (ε-greedy). Es: 0.10
	SoftmaxTau     float64 // temperatura softmax. Es: 0.20 (più bassa = più "argmax").

	// Staleness: quanto vecchie possono essere le Stats per considerarle "fresh".
	// Usiamo il timestamp TS delle Stats (tempo SIM). Default: 15s.
	FreshCutoffMs int64

	// Pesi dello score composito (tutti opzionali; se 0 usiamo defaults interni).
	Weights Weights
}

// Pesi per score = wAvail*Avail + wFresh*Fresh + wGPU*HasGpuOK - wProjected*Projected
type Weights struct {
	WAvail     float64
	WFresh     float64
	WGPU       float64
	WProjected float64
}

type Coordinator struct {
	log     *logx.Logger
	clock   *simclock.Clock
	par     Params
	sampler CandidateSampler
	local   LocalCommitFn

	// (Opzionale) Sorgente piggyback per leggere avail/fresh dell’advert.
	pbLookup PBLookup
}

func NewCoordinator(
	log *logx.Logger, clock *simclock.Clock, par Params,
	sampler CandidateSampler, local LocalCommitFn,
) *Coordinator {
	if par.TopK <= 0 {
		par.TopK = 3
	}
	if par.FreshCutoffMs <= 0 {
		par.FreshCutoffMs = 15000 // 15s SIM
	}
	// Default pesi se non impostati
	if par.Weights == (Weights{}) {
		par.Weights = Weights{
			WAvail:     0.50,
			WFresh:     0.20,
			WGPU:       0.15,
			WProjected: 0.40,
		}
	}
	return &Coordinator{log: log, clock: clock, par: par, sampler: sampler, local: local}
}

// Con questa puoi iniettare la lettura dal Piggyback senza cambiare la firma del costruttore.
func (c *Coordinator) WithPBLookup(fn PBLookup) *Coordinator {
	c.pbLookup = fn
	return c
}

func (c *Coordinator) OnJob(j jobs.Spec) {
	stats := c.sampler(16)
	if len(stats) == 0 {
		c.log.Warnf("COORD: nessun candidato; drop %s", j.Pretty())
		return
	}

	ordered := c.rankCandidatesForJob(j, stats, c.par.TopK)

	// Scegli l’indice secondo softmax/epsilon-greedy, poi prendi il candidato.
	idx := c.pickIndex(ordered)
	best := ordered[idx].s
	accept := c.checkHeadroom(best, j)

	switch c.par.Mode {
	case "local":
		if c.local == nil {
			c.log.Warnf("COORD(local): LocalCommitFn assente; drop %s", j.Pretty())
			return
		}
		ok := false
		if accept {
			ok = c.local(j)
		}
		if c.par.PrintDecisions {
			if ok {
				c.log.Infof("COORD(local) DECISION → run %s HERE (picked=%s cpu=%.1f%% score=%.4f)", j.Pretty(), best.NodeId, best.CpuPct, ordered[idx].score)
			} else {
				c.log.Warnf("COORD(local) REJECTED → headroom insufficiente per %s (picked=%s)", j.Pretty(), best.NodeId)
			}
		}
	default: // "advisory"
		if c.par.PrintDecisions {
			if accept {
				c.log.Infof("COORD(advisory) SUGGEST → run %s on %s (cpu=%.1f%% score=%.4f)", j.Pretty(), best.NodeId, best.CpuPct, ordered[idx].score)
			} else {
				c.log.Warnf("COORD(advisory) NO-FIT → picked=%s non ha headroom per %s", best.NodeId, j.Pretty())
			}
		}
	}
}

func (c *Coordinator) checkHeadroom(s *proto.Stats, j jobs.Spec) bool {
	safety := c.par.OverprovisionPct / 100.0
	headCPU := 100 - s.CpuPct
	headMEM := 100 - s.MemPct
	headGPU := 100.0
	if s.GpuPct < 0 {
		if j.GPU > 0 {
			return false
		}
		headGPU = -1
	} else {
		headGPU = 100 - s.GpuPct
	}

	if j.CPU > 0 && j.CPU > headCPU*(1.0-safety) {
		return false
	}
	if j.MEM > 0 && j.MEM > headMEM*(1.0-safety) {
		return false
	}
	if j.GPU > 0 && headGPU >= 0 && j.GPU > headGPU*(1.0-safety) {
		return false
	}
	return true
}

// utility locale (se mai servisse)
func d(sec float64) time.Duration { return time.Duration(sec * float64(time.Second)) }

// ===============================
// ===== RANKING & PICKING =======
// ===============================

type cand struct {
	s          *proto.Stats
	score      float64
	availAE    float64 // 0..1 stimata da Stats (AE)
	availPB    float64 // 0..1 stimata da Piggyback (se presente)
	availComb  float64 // combinazione AE/PB
	freshV     float64 // 0..1 (1 = freschissimo)
	hasGpuOK   float64 // 1 se (job.GPU==0) o (s.GpuPct>=0), altrimenti 0
	projectedV float64 // 0..1 carico post-job (più alto = peggio)
}

func (c *Coordinator) rankCandidatesForJob(j jobs.Spec, stats []*proto.Stats, topK int) []cand {
	if topK <= 0 {
		topK = 3
	}
	nowMs := c.clock.NowSim().UnixMilli()

	out := make([]cand, 0, len(stats))
	for _, s := range stats {
		if s == nil || s.NodeId == "" {
			continue
		}

		// AE-based availability (da Stats live)
		availAE := availabilityFromStats(s)

		// Piggyback (opzionale)
		availPB := -1.0
		freshPB := false
		if c.pbLookup != nil {
			if av255, ok, fresh := c.pbLookup(s.NodeId); ok {
				availPB = float64(av255) / 255.0
				freshPB = fresh
			}
		}

		// Freshness: se ho piggyback fresh uso quello; altrimenti età delle Stats.
		freshV := 0.0
		if freshPB {
			freshV = 1.0
		} else {
			age := nowMs - s.TsMs
			if age < 0 {
				age = 0
			}
			cut := c.par.FreshCutoffMs
			if cut <= 0 {
				cut = 15000
			}
			if age >= cut {
				freshV = 0.0
			} else {
				freshV = 1.0 - float64(age)/float64(cut)
			}
		}

		// HasGPU soddisfatto?
		hasGpuOK := 1.0
		if j.GPU > 0 && s.GpuPct < 0 {
			hasGpuOK = 0.0
		}

		// Carico proiettato post-job (0..1)
		projectedV := projectedLoadAfterJob(s, j)

		// Combina AE/PB: se PB disponibile, media semplice; altrimenti usa AE.
		availComb := availAE
		if availPB >= 0 {
			availComb = 0.5*availAE + 0.5*availPB
		}

		score := c.par.Weights.WAvail*availComb +
			c.par.Weights.WFresh*freshV +
			c.par.Weights.WGPU*hasGpuOK -
			c.par.Weights.WProjected*projectedV

		out = append(out, cand{
			s:          s,
			score:      score,
			availAE:    availAE,
			availPB:    availPB,
			availComb:  availComb,
			freshV:     freshV,
			hasGpuOK:   hasGpuOK,
			projectedV: projectedV,
		})
	}

	// Ordina per score desc
	sort.Slice(out, func(i, k int) bool { return out[i].score > out[k].score })

	// Troncatura TopK
	if len(out) > topK {
		out = out[:topK]
	}

	if c.par.PrintDecisions && len(out) > 0 {
		c.log.Infof("COORD rank (Top%d):", len(out))
		for i := range out {
			s := out[i]
			pbStr := "-"
			if s.availPB >= 0 {
				pbStr = fmt.Sprintf("%.3f", s.availPB)
			}
			c.log.Infof("  #%d %s  score=%.4f  availAE=%.3f availPB=%s avail=%.3f fresh=%.2f gpuOK=%.0f proj=%.3f cpu=%.1f mem=%.1f gpu=%.1f",
				i+1, s.s.NodeId, s.score, s.availAE, pbStr, s.availComb, s.freshV, s.hasGpuOK, s.projectedV, s.s.CpuPct, s.s.MemPct, s.s.GpuPct)
		}
	}

	return out
}

// pickIndex sceglie l’indice nel vettore ordinato:
// - Se SoftmaxTau > 0: softmax sampling sui punteggi.
// - Altrimenti se ExploreEpsilon > 0: ε-greedy (ε = uniforme su TopK; 1-ε = best).
// - Fallback: 0 (best).
func (c *Coordinator) pickIndex(ordered []cand) int {
	n := len(ordered)
	if n == 0 {
		return 0
	}

	// Softmax
	if c.par.SoftmaxTau > 0 {
		tau := c.par.SoftmaxTau
		// per stabilità numerica: sottraggo max
		maxSc := ordered[0].score
		for i := 1; i < n; i++ {
			if ordered[i].score > maxSc {
				maxSc = ordered[i].score
			}
		}
		sum := 0.0
		ws := make([]float64, n)
		for i := 0; i < n; i++ {
			ws[i] = math.Exp((ordered[i].score - maxSc) / tau)
			sum += ws[i]
		}
		if sum <= 0 {
			return 0
		}
		r := rand.Float64() * sum
		acc := 0.0
		for i := 0; i < n; i++ {
			acc += ws[i]
			if r <= acc {
				return i
			}
		}
		return n - 1
	}

	// ε-greedy
	if c.par.ExploreEpsilon > 0 {
		if rand.Float64() < c.par.ExploreEpsilon {
			return rand.Intn(n)
		}
		return 0
	}

	// greedy
	return 0
}

// === Helpers scoring ===

// availabilityFromStats: 1 - max(util)/100; se GPU assente, ignora GPU.
func availabilityFromStats(s *proto.Stats) float64 {
	maxu := s.CpuPct
	if s.MemPct > maxu {
		maxu = s.MemPct
	}
	if s.GpuPct >= 0 && s.GpuPct > maxu {
		maxu = s.GpuPct
	}
	if maxu < 0 {
		maxu = 0
	}
	if maxu > 100 {
		maxu = 100
	}
	return 1.0 - (maxu / 100.0)
}

// projectedLoadAfterJob: carico previsto dopo il job, normalizzato 0..1 (più alto = peggio).
func projectedLoadAfterJob(s *proto.Stats, j jobs.Spec) float64 {
	cpu := clamp01((s.CpuPct + j.CPU) / 100.0)
	mem := clamp01((s.MemPct + j.MEM) / 100.0)
	gpu := 0.0
	if s.GpuPct >= 0 {
		gpu = clamp01((s.GpuPct + j.GPU) / 100.0)
	}
	// worst dimension
	w := cpu
	if mem > w {
		w = mem
	}
	if s.GpuPct >= 0 && gpu > w {
		w = gpu
	}
	return w
}

func clamp01(x float64) float64 {
	if x < 0 {
		return 0
	}
	if x > 1 {
		return 1
	}
	return x
}
