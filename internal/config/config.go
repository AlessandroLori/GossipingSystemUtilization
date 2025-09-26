package config

import (
	"encoding/json"
	"os"

	"GossipSystemUtilization/internal/faults"
)

// ===== Top-level =====

type Config struct {
	Simulation       Simulation       `json:"simulation"`
	NodePowerClasses NodePowerClasses `json:"node_power_classes"`
	BackgroundLoad   BackgroundLoad   `json:"background_load"`

	Workload  Workload  `json:"workload"`
	Scheduler Scheduler `json:"scheduler"`
	Faults    Faults    `json:"faults"`

	// ===== NUOVO =====
	Leaves          Leaves          `json:"leaves"`
	FriendsAffinity FriendsAffinity `json:"friends_affinity"`
}

// ===== Simulation =====

type Simulation struct {
	TimeScale float64 `json:"time_scale"`
}

// ===== Node power classes =====

type NodePowerClasses struct {
	PeerPowerDistribution  map[string]float64     `json:"peer_power_distribution"`
	GPUPresenceProbability map[string]float64     `json:"gpu_presence_probability"`
	CapacityHps            map[string]CapacityHps `json:"capacity_Hps"`
	CapacityJitterFraction float64                `json:"capacity_jitter_fraction"`
}

type CapacityHps struct {
	CPU float64 `json:"cpu"`
	MEM float64 `json:"mem"`
	GPU float64 `json:"gpu"`
}

// ===== Background load =====

type BackgroundLoad struct {
	PercentBaselineByPower map[string]BgParams `json:"percent_baseline_by_power"`
}

type BgParams struct {
	CPUMeanPct  float64 `json:"cpu_mean_pct"`
	CPUStddevPP float64 `json:"cpu_stddev_pp"`
	MEMMeanPct  float64 `json:"mem_mean_pct"`
	MEMStddevPP float64 `json:"mem_stddev_pp"`
	GPUMeanPct  float64 `json:"gpu_mean_pct"`
	GPUStddevPP float64 `json:"gpu_stddev_pp"`
}

// ======== NUOVO: bucket + probabilità ========

// Percentuali (0..100) con triple Small/Medium/Large + Probabilità
type ProbBucketsPct struct {
	SmallPct   float64 `json:"small_pct"`
	MediumPct  float64 `json:"medium_pct"`
	LargePct   float64 `json:"large_pct"`
	ProbSmall  float64 `json:"prob_small"`
	ProbMedium float64 `json:"prob_medium"`
	ProbLarge  float64 `json:"prob_large"`
}

// Durate in secondi simulati con triple Small/Medium/Large + Probabilità
type ProbBucketsDur struct {
	SmallSimS  float64 `json:"small_sim_s"`
	MediumSimS float64 `json:"medium_sim_s"`
	LargeSimS  float64 `json:"large_sim_s"`
	ProbSmall  float64 `json:"prob_small"`
	ProbMedium float64 `json:"prob_medium"`
	ProbLarge  float64 `json:"prob_large"`
}

// ===== Workload (iperparametri job) =====

type Workload struct {
	Enabled              bool          `json:"enabled"`
	GenerateOnPeers      bool          `json:"generate_on_peers"`
	MeanInterarrivalSimS float64       `json:"mean_interarrival_sim_s"`
	JobCPU               RangePct      `json:"job_cpu"`
	JobMEM               RangePct      `json:"job_mem"`
	JobGPU               RangePct      `json:"job_gpu"`
	JobDurationSimS      DurationRange `json:"job_duration_sim_s"`

	// (NUOVO) Bucket + probabilità; se TUTTI presenti, sovrascrivono i range legacy
	CPUBuckets      *ProbBucketsPct `json:"cpu_buckets,omitempty"`
	MEMBuckets      *ProbBucketsPct `json:"mem_buckets,omitempty"`
	GPUBuckets      *ProbBucketsPct `json:"gpu_buckets,omitempty"`
	DurationBuckets *ProbBucketsDur `json:"duration_buckets,omitempty"`
}

type RangePct struct {
	MinPct float64 `json:"min_pct"`
	MaxPct float64 `json:"max_pct"`
}

type DurationRange struct {
	MinS float64 `json:"min_s"`
	MaxS float64 `json:"max_s"`
}

// ===== Friends & Affinity (pesi/soglie) =====
type FriendsAffinity struct {
	// Pesi composizione score (0..1 ciascuno; verranno normalizzati alla somma)
	WeightPerf   float64 `json:"weight_perf"`   // (1 - projectedLoad)
	WeightAdvert float64 `json:"weight_advert"` // avail da piggyback 0..1
	WeightLoad   float64 `json:"weight_load"`   // (1 - currentLoad)

	// Esplorazione
	Epsilon     float64 `json:"epsilon"`      // ε-greedy (0..1); se >0 ha precedenza
	SoftmaxTemp float64 `json:"softmax_temp"` // temperatura softmax (>0 per usare softmax)

	// Freshness e penalità
	StaleCutoffMs int64   `json:"stale_cutoff_ms"` // quanto vecchio può essere un advert per dirsi "fresh"
	BusyPenalty   float64 `json:"busy_penalty"`    // 0..1: penalità se peer in cool-off

	// Cool-off da applicare ai commit (tempo SIM)
	CooldownSimMs int64 `json:"cooldown_sim_ms"`

	// TopK candidato per la fase di pick (se 0 → usa Scheduler.ProbeFanout o default locale)
	TopK int `json:"topk"`
}

// Usa i bucket (invece dei range) solo se sono TUTTI presenti
func (w *Workload) UseBuckets() bool {
	return w != nil && w.CPUBuckets != nil && w.MEMBuckets != nil && w.GPUBuckets != nil && w.DurationBuckets != nil
}

// Normalizza le terne di probabilità dei bucket, se presenti
func (w *Workload) normalize() {
	if w == nil {
		return
	}
	if w.CPUBuckets != nil {
		norm3(&w.CPUBuckets.ProbSmall, &w.CPUBuckets.ProbMedium, &w.CPUBuckets.ProbLarge)
	}
	if w.MEMBuckets != nil {
		norm3(&w.MEMBuckets.ProbSmall, &w.MEMBuckets.ProbMedium, &w.MEMBuckets.ProbLarge)
	}
	if w.GPUBuckets != nil {
		norm3(&w.GPUBuckets.ProbSmall, &w.GPUBuckets.ProbMedium, &w.GPUBuckets.ProbLarge)
	}
	if w.DurationBuckets != nil {
		norm3(&w.DurationBuckets.ProbSmall, &w.DurationBuckets.ProbMedium, &w.DurationBuckets.ProbLarge)
	}
}

func norm3(a, b, c *float64) {
	sum := *a + *b + *c
	if sum <= 0 {
		return
	}
	*a /= sum
	*b /= sum
	*c /= sum
}

// Soglie heavy effettive: default (70/70/50), ma se ci sono i bucket usiamo i "large"
func (c *Config) EffectiveThresholds() (cpuHeavy, memHeavy, gpuHeavy float64) {
	cpuHeavy, memHeavy, gpuHeavy = 70.0, 70.0, 50.0
	if c.Workload.CPUBuckets != nil && c.Workload.CPUBuckets.LargePct > 0 {
		cpuHeavy = c.Workload.CPUBuckets.LargePct
	}
	if c.Workload.MEMBuckets != nil && c.Workload.MEMBuckets.LargePct > 0 {
		memHeavy = c.Workload.MEMBuckets.LargePct
	}
	if c.Workload.GPUBuckets != nil && c.Workload.GPUBuckets.LargePct > 0 {
		gpuHeavy = c.Workload.GPUBuckets.LargePct
	}
	return
}

// ===== Scheduler (Probe/Commit) =====

type Scheduler struct {
	ProbeFanout        int     `json:"probe_fanout"`
	ProbeTimeoutRealMs int     `json:"probe_timeout_real_ms"`
	JollyPct           float64 `json:"jolly_pct"`
}

type Leaves struct {
	Enabled               *bool              `json:"enabled,omitempty"`
	PrintTransitions      *bool              `json:"print_transitions,omitempty"`
	FrequencyClassWeights map[string]float64 `json:"frequency_class_weights,omitempty"` // es: none/low/medium/high
	FrequencyPerMinSim    map[string]float64 `json:"frequency_per_min_sim,omitempty"`   // rate per classe
	DurationClassWeights  map[string]float64 `json:"duration_class_weights,omitempty"`  // es: short/medium/long
	DurationMeanSimS      map[string]float64 `json:"duration_mean_sim_s,omitempty"`     // media durata per classe
}

// ===== Faults =====

// Supporto NUOVO + LEGACY
type Faults struct {
	Enabled          *bool `json:"enabled,omitempty"`
	PrintTransitions *bool `json:"print_transitions,omitempty"`

	// --- NUOVO formato "a buckets" ---
	FrequencyClassWeights map[string]float64 `json:"frequency_class_weights,omitempty"`
	FrequencyPerMinSim    map[string]float64 `json:"frequency_per_min_sim,omitempty"`
	DurationClassWeights  map[string]float64 `json:"duration_class_weights,omitempty"`
	DurationMeanSimS      map[string]float64 `json:"duration_mean_sim_s,omitempty"`

	// --- LEGACY ---
	// Esempio: {"rangepct": {"none":0.1,"low":0.4,"medium":0.35,"high":0.15}}
	RangePct map[string]float64 `json:"rangepct,omitempty"`
	// Esempio: {"durationrange": {"small":5,"medium":20,"grave":60}}
	DurationRange map[string]float64 `json:"durationrange,omitempty"`
	// opzionale legacy: singolo λ globale
	FreqPerMinSim *float64 `json:"freq_per_min_sim,omitempty"`
}

// ===== Loader =====

func Load(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}

	var c Config
	if err := json.Unmarshal(b, &c); err != nil {
		return nil, err
	}

	// (NUOVO) normalizza le probabilità dei bucket, se presenti
	c.Workload.normalize()

	c.applyDefaults() // defaults non-faults

	return &c, nil
}

func (c *Config) applyDefaults() {
	// Simulation
	if c.Simulation.TimeScale <= 0 {
		c.Simulation.TimeScale = 60
	}

	// Workload defaults
	if c.Workload.MeanInterarrivalSimS <= 0 {
		c.Workload.MeanInterarrivalSimS = 15 // sec simulati
	}
	// Range di default “leggero”
	if c.Workload.JobCPU.MaxPct == 0 && c.Workload.JobCPU.MinPct == 0 {
		c.Workload.JobCPU = RangePct{MinPct: 3, MaxPct: 12}
	}
	if c.Workload.JobMEM.MaxPct == 0 && c.Workload.JobMEM.MinPct == 0 {
		c.Workload.JobMEM = RangePct{MinPct: 3, MaxPct: 12}
	}
	// GPU opzionale
	// Durata default 10-30s simulati
	if c.Workload.JobDurationSimS.MinS == 0 && c.Workload.JobDurationSimS.MaxS == 0 {
		c.Workload.JobDurationSimS = DurationRange{MinS: 10, MaxS: 30}
	}

	// Scheduler defaults
	if c.Scheduler.ProbeFanout <= 0 {
		c.Scheduler.ProbeFanout = 3
	}
	if c.Scheduler.ProbeTimeoutRealMs <= 0 {
		c.Scheduler.ProbeTimeoutRealMs = 300
	}
	if c.Scheduler.JollyPct < 0 {
		c.Scheduler.JollyPct = 0
	}

	// Friends & Affinity defaults
	if c.FriendsAffinity.WeightPerf == 0 && c.FriendsAffinity.WeightAdvert == 0 && c.FriendsAffinity.WeightLoad == 0 {
		c.FriendsAffinity.WeightPerf = 0.50
		c.FriendsAffinity.WeightAdvert = 0.30
		c.FriendsAffinity.WeightLoad = 0.20
	}
	if c.FriendsAffinity.Epsilon < 0 {
		c.FriendsAffinity.Epsilon = 0
	}
	if c.FriendsAffinity.SoftmaxTemp < 0 {
		c.FriendsAffinity.SoftmaxTemp = 0
	}
	if c.FriendsAffinity.StaleCutoffMs <= 0 {
		c.FriendsAffinity.StaleCutoffMs = 12_000 // 12s SIM
	}
	if c.FriendsAffinity.BusyPenalty <= 0 {
		c.FriendsAffinity.BusyPenalty = 1.0
	}
	if c.FriendsAffinity.CooldownSimMs <= 0 {
		c.FriendsAffinity.CooldownSimMs = 8_000 // 8s SIM di cool-off dopo commit
	}
	if c.FriendsAffinity.TopK <= 0 {
		// fallback: usa probe_fanout se presente, altrimenti 3
		if c.Scheduler.ProbeFanout > 0 {
			c.FriendsAffinity.TopK = c.Scheduler.ProbeFanout
		} else {
			c.FriendsAffinity.TopK = 3
		}
	}

}

// ===== Traduzione Faults → faults.AutoProfileDef =====

func (c *Config) ToFaultsAutoProfileDef() faults.AutoProfileDef {
	def := faults.DefaultAutoProfile(false)

	// flags base
	if c.Faults.PrintTransitions != nil {
		def.PrintTransitions = *c.Faults.PrintTransitions
	}
	if c.Faults.Enabled != nil {
		def.Enabled = *c.Faults.Enabled
	}

	// Caso 1: NUOVO formato → copia diretto se presente
	if len(c.Faults.FrequencyClassWeights) > 0 {
		def.FrequencyClassWeights = copyMap(c.Faults.FrequencyClassWeights, def.FrequencyClassWeights)
	}
	if len(c.Faults.FrequencyPerMinSim) > 0 {
		def.FrequencyPerMinSim = copyMap(c.Faults.FrequencyPerMinSim, def.FrequencyPerMinSim)
	}
	if len(c.Faults.DurationClassWeights) > 0 {
		def.DurationClassWeights = copyMap(c.Faults.DurationClassWeights, def.DurationClassWeights)
	}
	if len(c.Faults.DurationMeanSimS) > 0 {
		def.DurationMeanSimS = copyMap(c.Faults.DurationMeanSimS, def.DurationMeanSimS)
	}

	// Caso 2: LEGACY → mappe su buckets
	if len(c.Faults.RangePct) > 0 {
		for _, k := range []string{"none", "low", "medium", "high"} {
			if v, ok := c.Faults.RangePct[k]; ok {
				def.FrequencyClassWeights[k] = v
			}
		}
		normWeights(def.FrequencyClassWeights)
	}
	if len(c.Faults.DurationRange) > 0 {
		for _, k := range []string{"small", "medium", "grave"} {
			if v, ok := c.Faults.DurationRange[k]; ok && v > 0 {
				def.DurationMeanSimS[k] = v
			}
		}
		// se vuoi derivare anche pesi durata da qualcos'altro, lasciare i default è ok
	}
	if c.Faults.FreqPerMinSim != nil && len(c.Faults.FrequencyPerMinSim) == 0 {
		lambda := *c.Faults.FreqPerMinSim
		def.FrequencyPerMinSim["none"] = 0.0
		def.FrequencyPerMinSim["low"] = lambda * 0.25
		def.FrequencyPerMinSim["medium"] = lambda * 1.0
		def.FrequencyPerMinSim["high"] = lambda * 3.0
	}

	return def
}

// --- helpers locali ---

func copyMap(src, dst map[string]float64) map[string]float64 {
	if src == nil {
		return dst
	}
	if dst == nil {
		dst = make(map[string]float64, len(src))
	}
	for k, v := range src {
		dst[k] = v
	}
	return dst
}

func normWeights(m map[string]float64) {
	var sum float64
	for _, v := range m {
		sum += v
	}
	if sum <= 0 {
		return
	}
	for k, v := range m {
		m[k] = v / sum
	}
}
