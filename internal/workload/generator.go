package workload

import (
	crand "crypto/rand"
	"encoding/hex"
	"math"
	"math/rand"
	"time"

	"GossipSystemUtilization/internal/jobs"
	"GossipSystemUtilization/internal/logx"
	"GossipSystemUtilization/internal/simclock"
)

// Parametri del generatore (mappano il blocco "workload" del config.json).
type Params struct {
	Enabled              bool
	MeanInterarrivalSimS float64 // inter-arrivo esponenziale (in secondi di TEMPO SIM)

	// Bucket Durata (secondi SIM)
	DurationSmallSimS float64
	DurationMedSimS   float64
	DurationLargeSimS float64
	PDurationSmall    float64 // p(small)
	PDurationMed      float64 // p(medium)
	PDurationLarge    float64 // p(large)

	// Bucket CPU (%)
	CPUSmallPct float64
	CPUMedPct   float64
	CPULargePct float64
	PCPUSmall   float64
	PCPUMed     float64
	PCPULarge   float64

	// Bucket MEM (%)
	MEMSmallPct float64
	MEMMedPct   float64
	MEMLargePct float64
	PMEMSmall   float64
	PMEMMed     float64
	PMEMLarge   float64

	// Bucket GPU (%) — se tutte le prob sono ~0 ⇒ nessuna richiesta GPU (GPU = -1)
	GPUSmallPct float64
	GPUMedPct   float64
	GPULargePct float64
	PGPUSmall   float64
	PGPUMed     float64
	PGPULarge   float64
}

// Job sintetici sul canale Out() secondo i parametri e l'orologio simulato.
type Generator struct {
	log   *logx.Logger
	clock *simclock.Clock
	rnd   *rand.Rand
	par   Params

	outCh chan jobs.Spec
	stop  chan struct{}
}

func NewGenerator(log *logx.Logger, clock *simclock.Clock, r *rand.Rand, par Params) *Generator {
	if r == nil {
		r = rand.New(rand.NewSource(time.Now().UnixNano()))
	}
	return &Generator{
		log:   log,
		clock: clock,
		rnd:   r,
		par:   par,
		outCh: make(chan jobs.Spec, 64),
		stop:  make(chan struct{}),
	}
}

func (g *Generator) Out() <-chan jobs.Spec { return g.outCh }
func (g *Generator) Stop()                 { close(g.stop) }

func (g *Generator) Start() {
	if !g.par.Enabled {
		g.log.Warnf("WORKLOAD: generator disabilitato da config")
		return
	}
	go g.loop()
}

func (g *Generator) loop() {
	g.log.Infof("WORKLOAD: start — mean interarrival = %.2fs(sim)", g.par.MeanInterarrivalSimS)
	for {
		// sleep esponenziale (tempo SIM)
		lam := 1.0 / math.Max(g.par.MeanInterarrivalSimS, 0.001)
		waitSimS := randExp(g.rnd, lam)
		select {
		case <-g.stop:
			return
		default:
			g.clock.SleepSim(d(waitSimS))
		}

		j := g.nextSpec()
		select {
		case g.outCh <- j:
			g.log.Infof("WORKLOAD NEW → %s", j.Pretty())
		default:
			g.log.Warnf("WORKLOAD: canale pieno, drop del job")
		}
	}
}

func (g *Generator) nextSpec() jobs.Spec {
	id := newID()
	cpu := pickBucket(g.rnd,
		[]float64{g.par.PCPUSmall, g.par.PCPUMed, g.par.PCPULarge},
		[]float64{g.par.CPUSmallPct, g.par.CPUMedPct, g.par.CPULargePct},
	)
	mem := pickBucket(g.rnd,
		[]float64{g.par.PMEMSmall, g.par.PMEMMed, g.par.PMEMLarge},
		[]float64{g.par.MEMSmallPct, g.par.MEMMedPct, g.par.MEMLargePct},
	)

	// GPU opzionale
	gpu := -1.0
	if g.par.PGPUSmall+g.par.PGPUMed+g.par.PGPULarge > 1e-6 {
		gpu = pickBucket(g.rnd,
			[]float64{g.par.PGPUSmall, g.par.PGPUMed, g.par.PGPULarge},
			[]float64{g.par.GPUSmallPct, g.par.GPUMedPct, g.par.GPULargePct},
		)
	}

	dur := pickBucket(g.rnd,
		[]float64{g.par.PDurationSmall, g.par.PDurationMed, g.par.PDurationLarge},
		[]float64{g.par.DurationSmallSimS, g.par.DurationMedSimS, g.par.DurationLargeSimS},
	)

	return jobs.Spec{
		ID:       id,
		CPU:      cpu,
		MEM:      mem,
		GPU:      gpu,
		Duration: d(dur),
	}
}

func randExp(r *rand.Rand, lambda float64) float64 {
	u := r.Float64()
	if u <= 0 {
		u = 1e-9
	}
	return -math.Log(u) / lambda
}

func pickBucket(r *rand.Rand, probs, values []float64) float64 {
	sum := 0.0
	for _, p := range probs {
		if p > 0 {
			sum += p
		}
	}
	if sum <= 0 {
		return 0
	}
	x := r.Float64() * sum
	for i, p := range probs {
		if p <= 0 {
			continue
		}
		if x < p {
			return values[i]
		}
		x -= p
	}
	return values[len(values)-1]
}

func newID() string {
	var b [6]byte
	_, _ = crand.Read(b[:])
	return "job-" + hex.EncodeToString(b[:])
}

func d(sec float64) time.Duration { return time.Duration(sec * float64(time.Second)) }
