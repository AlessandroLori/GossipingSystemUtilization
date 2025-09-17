package node

import (
	"math/rand"
	"time"

	"GossipSystemUtilization/internal/logx"
	"GossipSystemUtilization/internal/model"
	"GossipSystemUtilization/internal/simclock"
)

type Node struct {
	ID          string
	Addr        string
	Incarnation uint64

	Power  model.PowerClass
	Cap    model.Capacity   // H/s
	Bg     model.Background // H/s
	Used   model.Rates      // H/s (job in corso)
	HasGPU bool

	Clock *simclock.Clock
	Log   *logx.Logger
	rnd   *rand.Rand
}

func pct(used, cap float64) float64 {
	if cap <= 0 {
		return -1
	}
	v := 100.0 * used / cap
	if v < 0 {
		v = 0
	}
	if v > 100 {
		v = 100
	}
	return v
}

func (n *Node) PublishedPercentages() model.Percentages {
	cpu := pct(n.Bg.CPU+n.Used.CPU, n.Cap.CPU)
	mem := pct(n.Bg.MEM+n.Used.MEM, n.Cap.MEM)
	gpu := -1.0
	if n.Cap.GPU > 0 {
		gpu = pct(n.Bg.GPU+n.Used.GPU, n.Cap.GPU)
	}
	return model.Percentages{CPU: cpu, MEM: mem, GPU: gpu}
}

// New costruisce un Node e imposta l'RNG interno (non esportato).
func New(id, addr string, clock *simclock.Clock, log *logx.Logger, r *rand.Rand) *Node {
	if r == nil {
		// fallback, ma normalmente passa sempre un *rand.Rand dal main
		r = rand.New(rand.NewSource(time.Now().UnixNano()))
	}
	return &Node{
		ID:    id,
		Addr:  addr,
		Clock: clock,
		Log:   log,
		rnd:   r,
	}
}

type PowerCaps struct {
	WeakCap, MediumCap, StrongCap             model.Capacity
	GPUProbWeak, GPUProbMedium, GPUProbStrong float64
	CapJitterFraction                         float64
}

type BgBaselines struct {
	CPUmean float64 // punti percentuali (0..100)
	MEMmean float64
	GPUmean float64
}

// Init: assegna capacità (con jitter), presenza GPU e background iniziale; stampa tutto.
func (n *Node) Init(power model.PowerClass, caps PowerCaps, bgWeak, bgMed, bgStrong BgBaselines) {
	n.Power = power

	var base model.Capacity
	var gpuProb float64
	switch power {
	case model.PowerWeak:
		base = caps.WeakCap
		gpuProb = caps.GPUProbWeak
	case model.PowerMedium:
		base = caps.MediumCap
		gpuProb = caps.GPUProbMedium
	case model.PowerStrong:
		base = caps.StrongCap
		gpuProb = caps.GPUProbStrong
	}

	// jitter ±CapJitterFraction
	jitter := func(v float64) float64 {
		f := (n.rnd.Float64()*2 - 1) * caps.CapJitterFraction
		return v * (1 + f)
	}
	n.Cap.CPU = jitter(base.CPU)
	n.Cap.MEM = jitter(base.MEM)

	n.HasGPU = n.rnd.Float64() < gpuProb
	if n.HasGPU {
		n.Cap.GPU = jitter(base.GPU)
	} else {
		n.Cap.GPU = 0
	}

	// Baseline background in % (per semplicità: solo mean in questo step)
	var cpuPct, memPct, gpuPct float64
	switch power {
	case model.PowerWeak:
		cpuPct, memPct, gpuPct = bgWeak.CPUmean, bgWeak.MEMmean, bgWeak.GPUmean
	case model.PowerMedium:
		cpuPct, memPct, gpuPct = bgMed.CPUmean, bgMed.MEMmean, bgMed.GPUmean
	case model.PowerStrong:
		cpuPct, memPct, gpuPct = bgStrong.CPUmean, bgStrong.MEMmean, bgStrong.GPUmean
	}

	// % → H/s
	n.Bg.CPU = n.Cap.CPU * cpuPct / 100.0
	n.Bg.MEM = n.Cap.MEM * memPct / 100.0
	if n.HasGPU {
		n.Bg.GPU = n.Cap.GPU * gpuPct / 100.0
	} else {
		n.Bg.GPU = 0
	}

	// Stampe
	gpuStr := "assente"
	if n.HasGPU {
		gpuStr = "presente"
	}
	n.Log.Infof("BOOT → classe=%s  capacita(H/s){cpu=%.0f mem=%.0f gpu=%.0f}  GPU=%s",
		n.Power, n.Cap.CPU, n.Cap.MEM, n.Cap.GPU, gpuStr)

	pcts := n.PublishedPercentages()
	bgCPU := 0.0
	bgMEM := 0.0
	bgGPU := -1.0
	if n.Cap.CPU > 0 {
		bgCPU = (n.Bg.CPU / n.Cap.CPU) * 100
	}
	if n.Cap.MEM > 0 {
		bgMEM = (n.Bg.MEM / n.Cap.MEM) * 100
	}
	if n.HasGPU && n.Cap.GPU > 0 {
		bgGPU = (n.Bg.GPU / n.Cap.GPU) * 100
	}

	n.Log.Infof("BACKGROUND iniziale → cpu≈%.1f%% mem≈%.1f%% gpu≈%.1f%%  ⇒ published{cpu=%.1f%% mem=%.1f%% gpu=%.1f%%}",
		bgCPU, bgMEM, bgGPU, pcts.CPU, pcts.MEM, pcts.GPU)
}
