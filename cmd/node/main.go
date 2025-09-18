package main

import (
	crand "crypto/rand"
	"encoding/hex"
	"net"
	"os"
	"strconv"
	"strings"
	"time"

	"GossipSystemUtilization/internal/antientropy"
	"GossipSystemUtilization/internal/config"
	"GossipSystemUtilization/internal/logx"
	"GossipSystemUtilization/internal/model"
	"GossipSystemUtilization/internal/node"
	"GossipSystemUtilization/internal/seed"
	"GossipSystemUtilization/internal/simclock"
	"GossipSystemUtilization/internal/swim"
	"GossipSystemUtilization/proto"

	mrand "math/rand"

	"google.golang.org/grpc"
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
	} // fallback
	return ""
}

func main() {
	// === Carica config ===
	cfgPath := getenv("CONFIG_PATH", "config.json")
	cfg, err := config.Load(cfgPath)
	if err != nil {
		panic(err)
	}

	// Clock simulato secondo config
	clock := simclock.New(cfg.Simulation.TimeScale)

	// RNG e logger
	r := mrand.New(mrand.NewSource(time.Now().UnixNano()))
	id := newNodeID()
	log := logx.New(id, clock)
	log.Infof("Config caricata da %s — time_scale=%.1f", cfgPath, cfg.Simulation.TimeScale)

	// Classe potenza dal peso in config
	var pc model.PowerClass
	switch weightedPick(cfg.NodePowerClasses.PeerPowerDistribution, r) {
	case "weak":
		pc = model.PowerWeak
	case "powerful":
		pc = model.PowerStrong
	default:
		pc = model.PowerMedium
	}

	// Costruisci le PowerCaps dai valori in config
	getCap := func(label string) model.Capacity {
		c := cfg.NodePowerClasses.CapacityHps[label]
		return model.Capacity{CPU: c.CPU, MEM: c.MEM, GPU: c.GPU}
	}
	caps := node.PowerCaps{
		WeakCap:           getCap("weak"),
		MediumCap:         getCap("medium"),
		StrongCap:         getCap("powerful"),
		GPUProbWeak:       cfg.NodePowerClasses.GpuPresenceProbability["weak"],
		GPUProbMedium:     cfg.NodePowerClasses.GpuPresenceProbability["medium"],
		GPUProbStrong:     cfg.NodePowerClasses.GpuPresenceProbability["powerful"],
		CapJitterFraction: cfg.NodePowerClasses.CapacityJitterFraction,
	}

	// Baseline background per classe (medie)
	getBG := func(label string) node.BgBaselines {
		p := cfg.BackgroundLoad.PercentBaselineByPower[label]
		return node.BgBaselines{CPUmean: p.CPUMeanPct, MEMmean: p.MEMMeanPct, GPUmean: p.GPUMeanPct}
	}
	bgWeak := getBG("weak")
	bgMed := getBG("medium")
	bgStr := getBG("powerful")

	// Crea e inizializza il nodo (stampe Step 4)
	n := node.New(id, "0.0.0.0:0", clock, log, r)
	n.Init(pc, caps, bgWeak, bgMed, bgStr)

	// === Parametri rete via ENV ===
	grpcAddr := getenv("GRPC_ADDR", "127.0.0.1:9001")
	isSeed := getenvBool("IS_SEED")
	seedsCSV := getenv("SEEDS", "")
	bootDelaySim := getenvFloat("BOOT_DELAY_SIM_S", 0)

	// === SWIM manager (periodo 1s SIM; timeout 250ms REAL; k=3; suspicion=6s SIM) ===
	swimCfg := swim.Config{
		PeriodSimS:        1.0,
		TimeoutRealMs:     250,
		IndirectK:         3,
		SuspicionTimeoutS: 6.0,
	}
	mgr := swim.NewManager(id, grpcAddr, log, clock, r, swimCfg)
	mgr.Start()

	// === Anti-entropy (avail gossip) ===
	store := antientropy.NewStore(log, clock)
	// Usa tempo SIM per il timestamp, così TTL funziona correttamente
	selfSampler := func() *proto.Stats {
		s := n.CurrentStatsProto()
		s.TsMs = clock.NowSimMs()
		return s
	}

	aeCfg := antientropy.Config{
		PeriodSimS: 3.0,
		Fanout:     2,
		SampleSize: 8,
		TtlSimS:    12.0,
	}
	engine := antientropy.NewEngine(log, clock, r, store, mgr, selfSampler, aeCfg)
	engine.Start()

	// Semina lo store con le stats locali per evitare age-out dello self
	//store.UpsertBatch([]*proto.Stats{n.CurrentStatsProto()})

	// === gRPC server (Join solo sui seed; Ping/PingReq/ExchangeAvail su tutti) ===
	lis, err := net.Listen("tcp", grpcAddr)
	if err != nil {
		log.Errorf("listen %s: %v", grpcAddr, err)
		return
	}
	s := grpc.NewServer()

	if isSeed {
		reg := seed.NewRegistry(r)
		// registra il seed nel registry (così può essere restituito nei campioni)
		reg.UpsertPeer(&proto.PeerInfo{NodeId: id, Addr: grpcAddr, IsSeed: true})

		srv := seed.NewServer(true, reg, log, clock, mgr, id,
			func(max int) []*proto.Stats { return engine.LocalSample(max) })
		proto.RegisterGossipServer(s, srv)
		log.Infof("SEED attivo su %s (Join/Ping/PingReq/ExchangeAvail pronti)", grpcAddr)
	} else {
		// server che espone Ping/PingReq/ExchangeAvail; Join restituisce Unimplemented
		srv := seed.NewServer(false, nil, log, clock, mgr, id,
			func(max int) []*proto.Stats { return engine.LocalSample(max) })
		proto.RegisterGossipServer(s, srv)
		log.Infof("Peer non-seed su %s (Ping/PingReq/ExchangeAvail pronti)", grpcAddr)
	}

	go func() {
		if err := s.Serve(lis); err != nil {
			log.Errorf("gRPC Serve: %v", err)
		}
	}()

	// === Delay di ingresso (ondate) ===
	if bootDelaySim > 0 {
		log.Infof("Attendo BOOT_DELAY_SIM_S=%.1f (tempo SIM) prima del Join…", bootDelaySim)
		clock.SleepSim(time.Duration(bootDelaySim * float64(time.Second)))
	}

	// === Join (solo peer non-seed con SEEDS configurato) ===
	if !isSeed && seedsCSV != "" {
		jc := seed.NewJoinClient(log, clock)
		pcts := n.PublishedPercentages()
		req := &proto.JoinRequest{
			NodeId:      n.ID,
			Addr:        grpcAddr,
			Incarnation: uint64(time.Now().UnixMilli()),
			MyStats: &proto.Stats{
				NodeId: n.ID,
				CpuPct: pcts.CPU,
				MemPct: pcts.MEM,
				GpuPct: pcts.GPU,
				TsMs:   clock.NowSimMs(),
			},
		}
		rep, seedAddr, err := jc.TryJoin(seedsCSV, req)
		if err != nil {
			log.Warnf("JOIN non riuscito: %v (senza view iniziale)")
		} else {
			log.Infof("JOIN riuscito via %s — peers iniziali=%d:", seedAddr, len(rep.Peers))
			// Inserisci il seed nella membership (ID fittizio se non lo conosci)
			mgr.AddPeer("seed@"+seedAddr, seedAddr)
			// Inserisci i peer ricevuti
			for _, p := range rep.Peers {
				mgr.AddPeer(p.NodeId, p.Addr)
				log.Infof("  peer: node_id=%s addr=%s seed=%v", p.NodeId, p.Addr, p.IsSeed)
			}
			// Semina lo store con lo snapshot ricevuto
			if len(rep.StatsSnapshot) > 0 {
				store.UpsertBatch(rep.StatsSnapshot)
			}
		}
	} else if !isSeed {
		log.Warnf("SEEDS non configurato: salto Join (peer non-seed)")
	}

	select {}
}
