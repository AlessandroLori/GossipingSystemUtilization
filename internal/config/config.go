package config

import (
	"encoding/json"
	"fmt"
	"os"
)

type Capacity struct {
	CPU float64 `json:"cpu"`
	MEM float64 `json:"mem"`
	GPU float64 `json:"gpu"`
}

type Simulation struct {
	TimeScale float64 `json:"time_scale"` // es. 60 ⇒ 1 min SIM = 1 s reale
}

type NodePowerClasses struct {
	PeerPowerDistribution  map[string]float64  `json:"peer_power_distribution"`  // "weak"/"medium"/"powerful" → peso
	GpuPresenceProbability map[string]float64  `json:"gpu_presence_probability"` // per classe
	CapacityHps            map[string]Capacity `json:"capacity_Hps"`             // per classe
	CapacityJitterFraction float64             `json:"capacity_jitter_fraction"` // es. 0.10
}

type PercentBaseline struct {
	CPUMeanPct float64 `json:"cpu_mean_pct"`
	CPUStdPP   float64 `json:"cpu_stddev_pp"`
	MEMMeanPct float64 `json:"mem_mean_pct"`
	MEMStdPP   float64 `json:"mem_stddev_pp"`
	GPUMeanPct float64 `json:"gpu_mean_pct"`
	GPUStdPP   float64 `json:"gpu_stddev_pp"`
}

type BackgroundLoad struct {
	PercentBaselineByPower map[string]PercentBaseline `json:"percent_baseline_by_power"` // per classe
}

type Config struct {
	Simulation       Simulation       `json:"simulation"`
	NodePowerClasses NodePowerClasses `json:"node_power_classes"`
	BackgroundLoad   BackgroundLoad   `json:"background_load"`
}

func Load(path string) (*Config, error) {
	b, err := os.ReadFile(path)
	if err != nil {
		return nil, fmt.Errorf("read %s: %w", path, err)
	}
	var cfg Config
	if err := json.Unmarshal(b, &cfg); err != nil {
		return nil, fmt.Errorf("parse %s: %w", path, err)
	}
	if cfg.Simulation.TimeScale <= 0 {
		cfg.Simulation.TimeScale = 60
	}
	return &cfg, nil
}
