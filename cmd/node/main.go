package main

//TODO reporter dettagliati per nodo , un nodo per container , semplificazion id nodi e stampe più chiare.
// #CONFIG_PATH=./config.json IS_SEED=false GRPC_ADDR=127.0.0.1:9020 SEEDS=127.0.0.1:9004 go run ./cmd/node ---> configurazioni del nodo, peggior caso unico seed

import (
	crand "crypto/rand"
	"encoding/hex"
	"fmt"
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
	"GossipSystemUtilization/internal/piggyback"
	"GossipSystemUtilization/internal/simclock"

	proto "GossipSystemUtilization/proto"

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
func getenvInt(key string, def int) int {
	s := os.Getenv(key)
	if s == "" {
		return def
	}
	i, err := strconv.Atoi(s)
	if err != nil {
		return def
	}
	return i
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

func powerClassName(pc model.PowerClass) string {
	switch pc {
	case model.PowerWeak:
		return "weak"
	case model.PowerStrong:
		return "powerful"
	default:
		return "medium"
	}
}

func className(i int) string {
	switch i {
	case 1:
		return "CPU_ONLY"
	case 2:
		return "MEM_HEAVY"
	case 3:
		return "GPU_HEAVY"
	default:
		return "GENERAL"
	}
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

	// === LOG: profilo nodo (suitability) + mix generazione + soglie heavy ===
	var capSel model.Capacity
	var gpuProb float64
	switch pc {
	case model.PowerWeak:
		capSel = caps.WeakCap
		gpuProb = caps.GPUProbWeak
	case model.PowerStrong:
		capSel = caps.StrongCap
		gpuProb = caps.GPUProbStrong
	default:
		capSel = caps.MediumCap
		gpuProb = caps.GPUProbMedium
	}
	// Heuristica presenza GPU: dallo snapshot delle stats (GpuPct<0 → no GPU)
	snap := n.CurrentStatsProto()
	hasGPU := snap.GpuPct >= 0
	tot := capSel.CPU + capSel.MEM
	if hasGPU {
		tot += capSel.GPU
	}
	var pCPU, pMEM, pGPU float64
	if tot > 0 {
		pCPU = capSel.CPU / tot
		pMEM = capSel.MEM / tot
		if hasGPU {
			pGPU = capSel.GPU / tot
		} else {
			pGPU = 0
		}
	}
	log.Infof("NODE PROFILE → power=%s hasGPU=%v gpu_prob=%.2f cap[CPU=%.1f MEM=%.1f GPU=%.1f] suitability≈{CPU=%.2f MEM=%.2f GPU=%.2f}",
		powerClassName(pc), hasGPU, gpuProb, capSel.CPU, capSel.MEM, capSel.GPU, pCPU, pMEM, pGPU)

	// Mix generazione job + soglie heavy effettive (valide per tutti i nodi)
	cTh, mTh, gTh := cfg.EffectiveThresholds()
	if cfg.Workload.UseBuckets() {
		cb, mb, gb, db := cfg.Workload.CPUBuckets, cfg.Workload.MEMBuckets, cfg.Workload.GPUBuckets, cfg.Workload.DurationBuckets
		log.Infof("JOB GEN MIX (bucket/prob): CPU[s=%.0f,m=%.0f,l=%.0f | p=%.2f/%.2f/%.2f] MEM[s=%.0f,m=%.0f,l=%.0f | p=%.2f/%.2f/%.2f] GPU[s=%.0f,m=%.0f,l=%.0f | p=%.2f/%.2f/%.2f] DUR[s=%.0fs,m=%.0fs,l=%.0fs | p=%.2f/%.2f/%.2f]",
			cb.SmallPct, cb.MediumPct, cb.LargePct, cb.ProbSmall, cb.ProbMedium, cb.ProbLarge,
			mb.SmallPct, mb.MediumPct, mb.LargePct, mb.ProbSmall, mb.ProbMedium, mb.ProbLarge,
			gb.SmallPct, gb.MediumPct, gb.LargePct, gb.ProbSmall, gb.ProbMedium, gb.ProbLarge,
			db.SmallSimS, db.MediumSimS, db.LargeSimS, db.ProbSmall, db.ProbMedium, db.ProbLarge,
		)
	} else {
		log.Infof("JOB GEN MIX (legacy ranges): CPU[%.0f..%.0f] MEM[%.0f..%.0f] GPU[%.0f..%.0f] DUR[%.0fs..%.0fs]",
			cfg.Workload.JobCPU.MinPct, cfg.Workload.JobCPU.MaxPct,
			cfg.Workload.JobMEM.MinPct, cfg.Workload.JobMEM.MaxPct,
			cfg.Workload.JobGPU.MinPct, cfg.Workload.JobGPU.MaxPct,
			cfg.Workload.JobDurationSimS.MinS, cfg.Workload.JobDurationSimS.MaxS,
		)
	}
	log.Infof("AFFINITY HEAVY THRESHOLDS → CPU≥%.0f%% MEM≥%.0f%% GPU≥%.0f%%", cTh, mTh, gTh)

	// === Parametri rete via ENV ===
	grpcAddr := getenv("GRPC_ADDR", "127.0.0.1:9001")
	isSeed := getenvBool("IS_SEED")
	seedsCSV := getenv("SEEDS", "")
	bootDelaySim := getenvFloat("BOOT_DELAY_SIM_S", 0)

	// === Piggyback queue (micro-annunci) + self-advert periodico ===
	pbq := piggyback.NewQueue(log, clock, 200, 110*time.Second)
	go func() {
		for {
			s := n.CurrentStatsProto()
			s.TsMs = clock.NowSimMs()
			pbq.UpsertSelfFromStats(s)
			clock.SleepSim(2 * time.Second)
		}
	}()

	// === Runtime (SWIM + AE + Reporter + gRPC) ===
	rt := app.NewRuntime(id, grpcAddr, isSeed, seedsCSV, log, clock, r, n, store, pbq)
	if err := rt.StartAll(); err != nil {
		log.Errorf("start runtime: %v", err)
		return
	}

	// === Coordinator (seed e/o peer) ===
	if cfg.Workload.Enabled && (isSeed || cfg.Workload.GenerateOnPeers) {
		// Persona per-nodo (come già fai per il seed)
		u := r.Float64()
		primary := 0 // GENERAL
		switch {
		case u < 0.25:
			primary = 0
		case u < 0.50:
			primary = 1
		case u < 0.75:
			primary = 2
		default:
			primary = 3
		}
		dominance := getenvFloat("NODE_PRIMARY_DOMINANCE", 0.70)
		if dominance < 0.50 {
			dominance = 0.50
		}
		if dominance > 0.90 {
			dominance = 0.90
		}
		mix := [4]float64{}
		rest := (1.0 - dominance) / 3.0
		for i := 0; i < 4; i++ {
			mix[i] = rest
		}
		mix[primary] = dominance

		// rate label
		u2 := r.Float64()
		rateLabel := "normal"
		rateFactor := 1.00
		switch {
		case u2 < 0.25:
			rateLabel, rateFactor = "slow", 1.50
		case u2 < 0.75:
			rateLabel, rateFactor = "normal", 1.00
		default:
			rateLabel, rateFactor = "fast", 0.70
		}
		meanBase := cfg.Workload.MeanInterarrivalSimS
		if meanBase <= 0 {
			meanBase = 10
		}
		meanNode := meanBase * rateFactor

		persona := app.NodePersona{
			Primary:              primary,
			Dominance:            dominance,
			Mix:                  mix,
			RateLabel:            rateLabel,
			RateFactor:           rateFactor,
			MeanInterarrivalSimS: meanNode,
		}

		// Lo stesso coordinator gira sia su seed che su peer
		app.StartSeedCoordinator(log, clock, r, cfg, rt.Registry, rt.Mgr, id, rt.PBQueue, persona)

		// === Leave-sim (Graceful leave & recovery) ===
		app.StartLeaveRecoveryWithRuntime(
			log, clock, r,
			app.LeaveProfileInput{
				Enabled:               cfg.Leaves.Enabled,
				PrintTransitions:      cfg.Leaves.PrintTransitions,
				FrequencyClassWeights: cfg.Leaves.FrequencyClassWeights,
				FrequencyPerMinSim:    cfg.Leaves.FrequencyPerMinSim,
				DurationClassWeights:  cfg.Leaves.DurationClassWeights,
				DurationMeanSimS:      cfg.Leaves.DurationMeanSimS,
			},
			rt,
			id,
		)

	}

	// === Fault-sim (Crash & Recovery) ===
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

	// === Tracker 1: discovery via Stats/AE (sempre attivo) ===
	{
		expected := getenvInt("EXPECTED_NODES", 20)
		sampler := func(max int) []*proto.Stats { return rt.Store.SnapshotSample(max, nil) }
		if os.Getenv("TTFD_CSV") == "" {
			os.Setenv("TTFD_CSV", fmt.Sprintf("./out/ttfd-%s.csv", id))
		}
		if os.Getenv("TTFD_PERIOD_MS") == "" {
			os.Setenv("TTFD_PERIOD_MS", "200")
		}
		app.StartTTFDTracker(log, clock, sampler, id, expected)
	}

	// === Tracker 2: PRIMO CONTATTO (SWIM ∪ Stats) — sempre attivo, ma NIL-SAFE ===
	{
		expected := getenvInt("EXPECTED_NODES", 20)

		// Se SWIM non è ancora pronto o è stato stoppato (fault/leave), restituisce lista vuota.
		listIDs := func() []string {
			if rt.Mgr == nil {
				return nil
			}
			peers := rt.Mgr.AlivePeers()
			out := make([]string, 0, len(peers))
			for _, p := range peers {
				if p.ID != "" {
					out = append(out, p.ID)
				}
			}
			return out
		}
		sampler := func(max int) []*proto.Stats { return rt.Store.SnapshotSample(max, nil) }

		if os.Getenv("FC_CSV") == "" {
			os.Setenv("FC_CSV", fmt.Sprintf("./out/fc-ttfd-%s.csv", id))
		}
		if os.Getenv("FC_PERIOD_MS") == "" {
			os.Setenv("FC_PERIOD_MS", "200")
		}
		app.StartFirstContactDiscoveryTracker(log, clock, listIDs, sampler, id, expected)
	}

	select {}
}
