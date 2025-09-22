package main

import (
	crand "crypto/rand"
	"encoding/hex"
	"os"
	"strconv"
	"strings"
	"time"

	"GossipSystemUtilization/internal/app"

	"GossipSystemUtilization/internal/antientropy"
	"GossipSystemUtilization/internal/config"
	"GossipSystemUtilization/internal/logx"
	"GossipSystemUtilization/internal/model"
	"GossipSystemUtilization/internal/node"
	"GossipSystemUtilization/internal/simclock"

	mrand "math/rand"
)

func newNodeID() string {
	b := make([]byte, 6)
	_, _ = crand.Read(b)
	return "node-" + hex.EncodeToString(b)
}

func getenv(key, def string) string {
	v := os.Getenv(key)
	if v == "" {
		return def
	}
	return v
}
func getenvBool(key string) bool {
	v := strings.ToLower(os.Getenv(key))
	return v == "1" || v == "true" || v == "yes"
}
func getenvFloat(key string, def float64) float64 {
	s := os.Getenv(key)
	if s == "" {
		return def
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return def
	}
	return f
}

// pick pesata: accetta mappa "label"->peso, restituisce una key
func weightedPick(m map[string]float64, r *mrand.Rand) string {
	var sum float64
	for _, w := range m {
		if w > 0 {
			sum += w
		}
	}
	if sum <= 0 {
		return ""
	}
	x := r.Float64() * sum
	for k, w := range m {
		if w <= 0 {
			continue
		}
		if x < w {
			return k
		}
		x -= w
	}
	for k := range m {
		return k
	}
	return ""
}

func main() {
	// === Config & clock ===
	cfgPath := getenv("CONFIG_PATH", "config.json")
	cfg, err := config.Load(cfgPath)
	if err != nil {
		panic(err)
	}
	clock := simclock.New(cfg.Simulation.TimeScale)

	// === RNG & log ===
	r := mrand.New(mrand.NewSource(time.Now().UnixNano()))
	id := newNodeID()
	log := logx.New(id, clock)
	log.Infof("Config caricata da %s — time_scale=%.1f", cfgPath, cfg.Simulation.TimeScale)

	// === Classe di potenza & capacità ===
	var pc model.PowerClass
	switch weightedPick(cfg.NodePowerClasses.PeerPowerDistribution, r) {
	case "weak":
		pc = model.PowerWeak
	case "powerful":
		pc = model.PowerStrong
	default:
		pc = model.PowerMedium
	}
	getCap := func(label string) model.Capacity {
		c := cfg.NodePowerClasses.CapacityHps[label]
		return model.Capacity{CPU: c.CPU, MEM: c.MEM, GPU: c.GPU}
	}
	caps := node.PowerCaps{
		WeakCap:           getCap("weak"),
		MediumCap:         getCap("medium"),
		StrongCap:         getCap("powerful"),
		GPUProbWeak:       cfg.NodePowerClasses.GPUPresenceProbability["weak"],
		GPUProbMedium:     cfg.NodePowerClasses.GPUPresenceProbability["medium"],
		GPUProbStrong:     cfg.NodePowerClasses.GPUPresenceProbability["powerful"],
		CapJitterFraction: cfg.NodePowerClasses.CapacityJitterFraction,
	}
	getBG := func(label string) node.BgBaselines {
		p := cfg.BackgroundLoad.PercentBaselineByPower[label]
		return node.BgBaselines{CPUmean: p.CPUMeanPct, MEMmean: p.MEMMeanPct, GPUmean: p.GPUMeanPct}
	}
	bgWeak := getBG("weak")
	bgMed := getBG("medium")
	bgStr := getBG("powerful")

	// === Node & store ===
	n := node.New(id, "0.0.0.0:0", clock, log, r)
	n.Init(pc, caps, bgWeak, bgMed, bgStr)
	store := antientropy.NewStore(log, clock)

	// === Parametri rete via ENV ===
	grpcAddr := getenv("GRPC_ADDR", "127.0.0.1:9001")
	isSeed := getenvBool("IS_SEED")
	seedsCSV := getenv("SEEDS", "")
	bootDelaySim := getenvFloat("BOOT_DELAY_SIM_S", 0)

	// === Runtime (SWIM + AE + Reporter + gRPC) ===
	rt := app.NewRuntime(id, grpcAddr, isSeed, seedsCSV, log, clock, r, n, store)
	if err := rt.StartAll(); err != nil {
		log.Errorf("start runtime: %v", err)
		return
	}

	// === Coordinator (solo seed) — già in app/seed_coordinator.go ===
	if isSeed && cfg.Workload.Enabled {
		app.StartSeedCoordinator(log, clock, r, cfg, rt.Registry, id)
	}

	// === Fault-sim (Crash & Recovery) — hook interni al package app ===
	app.StartFaultRecoveryWithRuntime(
		log, clock, r,
		app.FaultProfileInput{
			Enabled:               cfg.Faults.Enabled,
			PrintTransitions:      cfg.Faults.PrintTransitions,
			FrequencyClassWeights: cfg.Faults.FrequencyClassWeights,
			FrequencyPerMinSim:    cfg.Faults.FrequencyPerMinSim,
			DurationClassWeights:  cfg.Faults.DurationClassWeights,
			DurationMeanSimS:      cfg.Faults.DurationMeanSimS,
		},
		rt,
	)

	// === Delay di ingresso (ondate) + Join iniziale (solo peer non-seed) ===
	if bootDelaySim > 0 {
		log.Infof("Attendo BOOT_DELAY_SIM_S=%.1f (tempo SIM) prima del Join…", bootDelaySim)
		clock.SleepSim(time.Duration(bootDelaySim * float64(time.Second)))
	}
	rt.TryJoinIfNeeded()

	select {}
}
