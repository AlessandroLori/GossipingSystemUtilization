package main

import (
	"context"
	crand "crypto/rand"
	"encoding/hex"
	"fmt"
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
	proto "GossipSystemUtilization/proto"

	mrand "math/rand"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
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
		GPUProbWeak:       cfg.NodePowerClasses.GPUPresenceProbability["weak"],
		GPUProbMedium:     cfg.NodePowerClasses.GPUPresenceProbability["medium"],
		GPUProbStrong:     cfg.NodePowerClasses.GPUPresenceProbability["powerful"],
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

	// === Reporter periodico (riepilogo cluster) ===
	repCfg := antientropy.ReporterConfig{
		PeriodSimS: 10.0, // ogni 10s di tempo simulato
		TopK:       3,
	}
	reporter := antientropy.NewReporter(log, clock, store, selfSampler, repCfg)
	reporter.Start()

	// Semina lo store con le stats locali per evitare age-out dello self
	store.UpsertBatch([]*proto.Stats{selfSampler()})

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

		srv := seed.NewServer(
			true, reg, log, clock, mgr, id,
			func(max int) []*proto.Stats { return engine.LocalSample(max) }, // sampler anti-entropy
			func() *proto.Stats { // selfStatsFn per Probe
				s := n.CurrentStatsProto()
				s.TsMs = clock.NowSim().UnixMilli()
				return s
			},
			func(jobID string, cpu, mem, gpu float64, durMs int64) bool { // applyCommitFn
				return n.StartJobLoad(jobID, cpu, mem, gpu, time.Duration(durMs)*time.Millisecond)
			},
			func(jobID string) bool { // cancelFn
				return n.CancelJob(jobID)
			},
		)

		proto.RegisterGossipServer(s, srv)

		if cfg.Workload.Enabled {
			startSeedCoordinator(log, clock, r, cfg, reg, id)
		}

		log.Infof("SEED attivo su %s (Join/Ping/PingReq/ExchangeAvail pronti)", grpcAddr)
	} else {
		// server che espone Ping/PingReq/ExchangeAvail; Join restituisce Unimplemented
		srv := seed.NewServer(
			false, nil, log, clock, mgr, id,
			func(max int) []*proto.Stats { return engine.LocalSample(max) }, // sampler anti-entropy
			func() *proto.Stats {
				s := n.CurrentStatsProto()
				s.TsMs = clock.NowSim().UnixMilli()
				return s
			},
			func(jobID string, cpu, mem, gpu float64, durMs int64) bool {
				return n.StartJobLoad(jobID, cpu, mem, gpu, time.Duration(durMs)*time.Millisecond)
			},
			func(jobID string) bool {
				return n.CancelJob(jobID)
			},
		)

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

// --- Scheduler seed-only: generazione job + Probe/Commit ---

type schedJob struct {
	id       string
	cpu, mem float64
	gpu      float64
	duration time.Duration
}

func startSeedCoordinator(
	log *logx.Logger,
	clock *simclock.Clock,
	r *mrand.Rand,
	cfg *config.Config,
	reg *seed.Registry,
	selfID string,
) {
	go func() {
		// opzionale: un piccolo delay iniziale per dare tempo all'anti-entropy
		clock.SleepSim(2 * time.Second)

		for {
			// 1) Attendi il prossimo job (inter-arrivo esponenziale in tempo SIM)
			mean := cfg.Workload.MeanInterarrivalSimS
			if mean <= 0 {
				mean = 10 // fallback robusto
			}
			waitS := mean * r.ExpFloat64() // Exp(lambda=1) → mean scaling
			clock.SleepSim(time.Duration(waitS * float64(time.Second)))

			// 2) Disegna un job dalle distribuzioni del config
			job := drawJob(r, cfg)

			// 3) Scegli i candidati dal registry del seed (escludendo me stesso)
			//    campioniamo un po' di peer e poi da quelli prendiamo k per la Probe
			k := cfg.Scheduler.ProbeFanout
			if k <= 0 {
				k = 3
			}
			candidates := 2 * k
			peers := []*proto.PeerInfo(nil)
			if reg != nil {
				peers = reg.SamplePeers(selfID, candidates)
			}
			if len(peers) == 0 {
				log.Warnf("COORD: nessun peer disponibile per job=%s; ritento più tardi", job.id)
				continue
			}
			peers = samplePeers(peers, k, r)

			// 4) Probe dei candidati e scelta del migliore
			bestAddr := ""
			bestID := ""
			bestScore := -1.0
			for _, p := range peers {
				ok, score, reason := probeNode(clock, p.Addr, selfID, job, cfg.Scheduler.ProbeTimeoutRealMs)
				if ok {
					if score > bestScore {
						bestScore = score
						bestAddr = p.Addr
						bestID = p.NodeId
					}
				} else {
					log.Infof("PROBE → %s refuse (reason=%s) job=%s", p.NodeId, reason, job.id)
				}
			}
			if bestAddr == "" {
				log.Infof("COORD: nessun peer ha accettato il job=%s", job.id)
				continue
			}

			// 5) Commit sul vincitore
			if commitJob(clock, bestAddr, job, cfg.Scheduler.ProbeTimeoutRealMs) {
				log.Infof("COORD COMMIT ✓ target=%s job=%s cpu=%.1f%% mem=%.1f%% gpu=%.1f%% dur=%s",
					bestID, job.id, job.cpu, job.mem, job.gpu, job.duration)
			} else {
				log.Warnf("COORD COMMIT ✖ target=%s job=%s", bestID, job.id)
			}
		}
	}()
}

func drawJob(r *mrand.Rand, cfg *config.Config) schedJob {
	id := fmt.Sprintf("job-%06x", r.Uint32())

	// helper uniformi
	unif := func(min, max float64) float64 {
		if max <= min {
			return min
		}
		return min + r.Float64()*(max-min)
	}

	// percentuali
	cpu := unif(cfg.Workload.JobCPU.MinPct, cfg.Workload.JobCPU.MaxPct)
	mem := unif(cfg.Workload.JobMEM.MinPct, cfg.Workload.JobMEM.MaxPct)

	// GPU: se range > 0 la usiamo, altrimenti restiamo a 0 (anche se il target potrà rifiutare se non ha GPU)
	gpu := 0.0
	if cfg.Workload.JobGPU.MaxPct > 0 {
		gpu = unif(cfg.Workload.JobGPU.MinPct, cfg.Workload.JobGPU.MaxPct)
	}

	// durata (tempo SIM)
	dmin := cfg.Workload.JobDurationSimS.MinS
	dmax := cfg.Workload.JobDurationSimS.MaxS
	if dmax <= dmin {
		dmax = dmin
	}
	dS := dmin + r.Float64()*(dmax-dmin)
	dur := time.Duration(dS * float64(time.Second))

	return schedJob{
		id:       id,
		cpu:      cpu,
		mem:      mem,
		gpu:      gpu,
		duration: dur,
	}
}

func samplePeers(in []*proto.PeerInfo, k int, r *mrand.Rand) []*proto.PeerInfo {
	if k <= 0 || k >= len(in) {
		return in
	}
	out := make([]*proto.PeerInfo, 0, k)
	seen := make(map[int]struct{})
	for len(out) < k && len(out) < len(in) {
		i := r.Intn(len(in))
		if _, ok := seen[i]; ok {
			continue
		}
		seen[i] = struct{}{}
		out = append(out, in[i])
	}
	return out
}

func probeNode(clock *simclock.Clock, addr, requesterID string, job schedJob, timeoutMs int) (accept bool, score float64, reason string) {
	dialCtx, cancel := context.WithTimeout(context.Background(), time.Duration(timeoutMs)*time.Millisecond)
	defer cancel()

	conn, err := grpc.DialContext(dialCtx, addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		return false, 0, "dial_error"
	}
	defer conn.Close()

	cli := proto.NewGossipClient(conn)
	ctx2, cancel2 := context.WithTimeout(context.Background(), time.Duration(timeoutMs)*time.Millisecond)
	defer cancel2()

	js := &proto.JobSpec{
		CpuPct:     job.cpu,
		MemPct:     job.mem,
		GpuPct:     job.gpu,
		DurationMs: int64(job.duration / time.Millisecond),
	}
	rep, err := cli.Probe(ctx2, &proto.ProbeRequest{
		//RequesterId: requesterID,
		Job: js,
		//TsMs:        clock.NowSim().UnixMilli(),
	})
	if err != nil {
		return false, 0, "rpc_error"
	}
	return rep.WillAccept, rep.Score, rep.Reason
}

func commitJob(clock *simclock.Clock, addr string, job schedJob, timeoutMs int) bool {
	dialCtx, cancel := context.WithTimeout(context.Background(), time.Duration(timeoutMs)*time.Millisecond)
	defer cancel()

	conn, err := grpc.DialContext(dialCtx, addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		return false
	}
	defer conn.Close()

	cli := proto.NewGossipClient(conn)
	ctx2, cancel2 := context.WithTimeout(context.Background(), time.Duration(timeoutMs)*time.Millisecond)
	defer cancel2()

	_, err = cli.Commit(ctx2, &proto.CommitRequest{
		JobId:      job.id,
		CpuPct:     job.cpu,
		MemPct:     job.mem,
		GpuPct:     job.gpu,
		DurationMs: int64(job.duration / time.Millisecond),
		//TsMs:       clock.NowSim().UnixMilli(),
	})
	return err == nil
}
