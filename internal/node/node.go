package node

import (
	"math/rand"
	"sync"
	"time"

	"GossipSystemUtilization/internal/logx"
	"GossipSystemUtilization/internal/model"
	"GossipSystemUtilization/internal/simclock"
	"GossipSystemUtilization/proto"
)

type Node struct {
	ID          string
	Addr        string
	Incarnation uint64

	Power  model.PowerClass
	Cap    model.Capacity   // H/s
	Bg     model.Background // H/s (carico di base “di sistema”)
	Used   model.Rates      // H/s (se in futuro userai carichi in H/s)
	HasGPU bool

	// --- Stato carichi extra in percentuale (per i job simulati) ---
	mu       sync.Mutex
	extraCPU float64
	extraMEM float64
	extraGPU float64
	jobs     map[string]struct{ cpu, mem, gpu float64 }

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

func (n *Node) SelfStatsSim() *proto.Stats {
	p := n.PublishedPercentages()
	return &proto.Stats{
		NodeId: n.ID,
		CpuPct: p.CPU,
		MemPct: p.MEM,
		GpuPct: p.GPU,
		TsMs:   n.Clock.NowSimMs(),
	}
}

// PublishedPercentages calcola:
//   - baseline% da Bg/Cap (cpu, mem, gpu)  (+ eventuale Used se lo userai)
//   - somma i carichi extra (extraCPU/MEM/GPU) dei job attivi
//
// Nota: GPU=-1 “assente” e ignora extraGPU.
func (n *Node) PublishedPercentages() model.Percentages {
	// baseline dalle H/s di background (e Used se vuoi in futuro)
	baseCPU := pct(n.Bg.CPU+n.Used.CPU, n.Cap.CPU)
	baseMEM := pct(n.Bg.MEM+n.Used.MEM, n.Cap.MEM)

	var baseGPU float64
	if !n.HasGPU || n.Cap.GPU <= 0 {
		baseGPU = -1
	} else {
		baseGPU = pct(n.Bg.GPU+n.Used.GPU, n.Cap.GPU)
	}

	// aggiungi carichi extra in percentuale
	n.mu.Lock()
	exCPU := n.extraCPU
	exMEM := n.extraMEM
	exGPU := n.extraGPU
	n.mu.Unlock()

	clamp := func(x float64) float64 {
		if x < 0 {
			return 0
		}
		if x > 100 {
			return 100
		}
		return x
	}

	cpu := clamp(baseCPU + exCPU)
	mem := clamp(baseMEM + exMEM)

	gpu := baseGPU
	if gpu >= 0 {
		gpu = clamp(baseGPU + exGPU)
	} // se -1 (assente), resta -1

	return model.Percentages{CPU: cpu, MEM: mem, GPU: gpu}
}

// New costruisce un Node e imposta l'RNG interno (non esportato).
func New(id, addr string, clock *simclock.Clock, log *logx.Logger, r *rand.Rand) *Node {
	if r == nil {
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
	CPUmean float64 // punti percentuali
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

	// Baseline background in % (solo mean)
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

// Restituisce le percentuali correnti in proto.Stats.
func (n *Node) CurrentStatsProto() *proto.Stats {
	p := n.PublishedPercentages()
	return &proto.Stats{
		NodeId: n.ID,
		CpuPct: p.CPU,
		MemPct: p.MEM,
		GpuPct: p.GPU,
		TsMs:   n.Clock.NowSimMs(),
	}
}

// TryReserve controlla l’headroom: true se si possono aggiungere quelle % senza saturare.
func (n *Node) TryReserve(cpuPct, memPct, gpuPct float64) (ok bool, reason string) {
	const safety = 0.05 // 5% margine
	p := n.PublishedPercentages()

	headCPU := 100 - p.CPU
	headMEM := 100 - p.MEM
	var headGPU float64
	if p.GPU < 0 {
		// GPU assente
		if gpuPct > 0 {
			return false, "gpu_absent"
		}
		headGPU = -1
	} else {
		headGPU = 100 - p.GPU
	}

	if cpuPct > 0 && cpuPct > headCPU*(1.0-safety) {
		return false, "cpu_insufficient"
	}
	if memPct > 0 && memPct > headMEM*(1.0-safety) {
		return false, "mem_insufficient"
	}
	if gpuPct > 0 && headGPU >= 0 && gpuPct > headGPU*(1.0-safety) {
		return false, "gpu_insufficient"
	}
	return true, ""
}

// StartJobLoad applica i carichi extra e pianifica la rimozione dopo 'dur' (tempo SIM).
func (n *Node) StartJobLoad(jobID string, cpuPct, memPct, gpuPct float64, dur time.Duration) bool {
	ok, _ := n.TryReserve(cpuPct, memPct, gpuPct)
	if !ok {
		return false
	}

	n.mu.Lock()
	if n.jobs == nil {
		n.jobs = make(map[string]struct{ cpu, mem, gpu float64 })
	}
	n.extraCPU += cpuPct
	n.extraMEM += memPct
	if gpuPct > 0 && n.HasGPU {
		n.extraGPU += gpuPct
	}
	n.jobs[jobID] = struct{ cpu, mem, gpu float64 }{cpuPct, memPct, gpuPct}
	n.mu.Unlock()

	n.Log.Infof("JOB START → job=%s cpu=%.1f%% mem=%.1f%% gpu=%.1f%% dur=%s",
		jobID, cpuPct, memPct, gpuPct, dur)

	go func() {
		n.Clock.SleepSim(dur)
		n.CancelJob(jobID)
	}()

	return true
}

// CancelJob sottrae il carico extra associato al job e lo rimuove dalla mappa.
func (n *Node) CancelJob(jobID string) bool {
	n.mu.Lock()
	j, ok := n.jobs[jobID]
	if ok {
		n.extraCPU -= j.cpu
		n.extraMEM -= j.mem
		if j.gpu > 0 && n.HasGPU {
			n.extraGPU -= j.gpu
		}
		delete(n.jobs, jobID)
	}
	n.mu.Unlock()

	if ok {
		n.Log.Infof("JOB END   → job=%s", jobID)
	}
	return ok
}

// Applica un rallentamento come se fosse un "job speciale" (riusa StartJobLoad).
// jobID: usa prefisso "fault:" per distinguerli nei log.
func (n *Node) ApplySlowdownFault(jobID string, cpuPct, memPct, gpuPct float64, dur time.Duration) bool {
	if jobID == "" {
		jobID = "fault:slowdown"
	} else {
		jobID = "fault:" + jobID
	}
	n.Log.Warnf("FAULT SLOWDOWN → +cpu=%.1f%% +mem=%.1f%% +gpu=%.1f%% dur=%s", cpuPct, memPct, gpuPct, dur)
	return n.StartJobLoad(jobID, cpuPct, memPct, gpuPct, dur)
}

// Rimuove (se presente) il rallentamento simulato creato sopra.
func (n *Node) ClearSlowdownFault(jobID string) bool {
	if jobID == "" {
		jobID = "fault:slowdown"
	} else {
		jobID = "fault:" + jobID
	}
	ok := n.CancelJob(jobID)
	if ok {
		n.Log.Warnf("FAULT CLEAR   → %s", jobID)
	}
	return ok
}
