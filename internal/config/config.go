package config

import (
	"encoding/json"
	"os"
)

// ===== Top-level =====

type Config struct {
	Simulation       Simulation       `json:"simulation"`
	NodePowerClasses NodePowerClasses `json:"node_power_classes"`
	BackgroundLoad   BackgroundLoad   `json:"background_load"`

	// Nuove sezioni guidate da config.json
	Workload  Workload  `json:"workload"`
	Scheduler Scheduler `json:"scheduler"`
	// Faults li lasciamo già definiti per eventuali step futuri; non sono obbligatori nel JSON
	Faults Faults `json:"faults"`
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

// ===== Workload (iperparametri job) =====

type Workload struct {
	Enabled              bool          `json:"enabled"`
	MeanInterarrivalSimS float64       `json:"mean_interarrival_sim_s"`
	JobCPU               RangePct      `json:"job_cpu"`
	JobMEM               RangePct      `json:"job_mem"`
	JobGPU               RangePct      `json:"job_gpu"`
	JobDurationSimS      DurationRange `json:"job_duration_sim_s"`
}

type RangePct struct {
	MinPct float64 `json:"min_pct"`
	MaxPct float64 `json:"max_pct"`
}

type DurationRange struct {
	MinS float64 `json:"min_s"`
	MaxS float64 `json:"max_s"`
}

// ===== Scheduler (Probe/Commit) =====

type Scheduler struct {
	ProbeFanout        int `json:"probe_fanout"`
	ProbeTimeoutRealMs int `json:"probe_timeout_real_ms"`
}

// ===== Faults (placeholder per step futuri) =====

type Faults struct {
	Enabled            bool    `json:"enabled"`
	CrashProbPerMinSim float64 `json:"crash_prob_per_min_sim"`
	DropCommitProb     float64 `json:"drop_commit_prob"`
}

type FaultsConfig struct {
	Enabled            bool    `json:"enabled"`
	UptimeMeanSimSec   float64 `json:"uptime_mean_sim_s"`   // media uptime (secondi simulati)
	DowntimeMeanSimSec float64 `json:"downtime_mean_sim_s"` // media downtime (secondi simulati)
	JitterPct          float64 `json:"jitter_pct"`          // 0.0 .. 0.5 tipicamente (10% = 0.10)
}

// ===== Loader + defaults =====

func Load(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	var c Config
	if err := json.Unmarshal(b, &c); err != nil {
		return nil, err
	}
	c.applyDefaults()
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
	// GPU opzionale: se non presente resta 0-0 (nessun carico GPU)
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
}
