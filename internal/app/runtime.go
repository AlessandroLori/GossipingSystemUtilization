package app

import (
	"net"
	"time"

	mrand "math/rand"

	"google.golang.org/grpc"

	"GossipSystemUtilization/internal/antientropy"
	"GossipSystemUtilization/internal/grpcserver"
	"GossipSystemUtilization/internal/logx"
	"GossipSystemUtilization/internal/node"
	"GossipSystemUtilization/internal/piggyback"
	"GossipSystemUtilization/internal/seed"
	"GossipSystemUtilization/internal/simclock"
	"GossipSystemUtilization/internal/swim"

	proto "GossipSystemUtilization/proto"
)

type Runtime struct {
	// static
	ID       string
	GRPCAddr string
	IsSeed   bool
	SeedsCSV string

	Log   *logx.Logger
	Clock *simclock.Clock
	Rng   *mrand.Rand

	// building blocks
	Node    *node.Node
	Store   *antientropy.Store
	PBQueue *piggyback.Queue // Ora è un building block

	// live components (managed)
	Mgr      *swim.Manager
	Engine   *antientropy.Engine
	Reporter *antientropy.Reporter
	GRPC     *grpc.Server
	Listener net.Listener
	Registry *seed.Registry

	selfStats func() *proto.Stats
}

// factory
// --- MODIFICA 1: Aggiungi pbq come parametro ---
func NewRuntime(
	id, grpcAddr string,
	isSeed bool,
	seedsCSV string,
	log *logx.Logger,
	clock *simclock.Clock,
	r *mrand.Rand,
	n *node.Node,
	store *antientropy.Store,
	pbq *piggyback.Queue, // <-- NUOVO PARAMETRO
) *Runtime {
	rt := &Runtime{
		ID:       id,
		GRPCAddr: grpcAddr,
		IsSeed:   isSeed,
		SeedsCSV: seedsCSV,
		Log:      log,
		Clock:    clock,
		Rng:      r,
		Node:     n,
		Store:    store,
		PBQueue:  pbq, // <-- ASSEGNA LA CODA PASSATA DALL'ESTERNO
	}
	rt.selfStats = func() *proto.Stats {
		s := rt.Node.CurrentStatsProto()
		s.TsMs = rt.Clock.NowSimMs()
		return s
	}
	return rt
}

// StartAll avvia SWIM, Anti-Entropy, Reporter e gRPC server (e semina lo store con self).
func (rt *Runtime) StartAll() error {
	// SWIM
	swimCfg := swim.Config{
		PeriodSimS:        1.0,
		TimeoutRealMs:     250,
		IndirectK:         3,
		SuspicionTimeoutS: 6.0,
	}
	rt.Mgr = swim.NewManager(rt.ID, rt.GRPCAddr, rt.Log, rt.Clock, rt.Rng, swimCfg)
	rt.Mgr.Start()

	// Anti-Entropy
	aeCfg := antientropy.Config{
		PeriodSimS: 3.0,
		Fanout:     2,
		SampleSize: 8,
		TtlSimS:    12.0,
	}
	rt.Engine = antientropy.NewEngine(rt.Log, rt.Clock, rt.Rng, rt.Store, rt.Mgr, rt.PBQueue, rt.selfStats, aeCfg)
	rt.Engine.Start()

	// Reporter
	repCfg := antientropy.ReporterConfig{PeriodSimS: 10.0, TopK: 3}
	rt.Reporter = antientropy.NewReporter(rt.Log, rt.Clock, rt.Store, rt.selfStats, repCfg)
	rt.Reporter.Start()

	// Semina lo store con le stats locali
	rt.Store.UpsertBatch([]*proto.Stats{rt.selfStats()})

	// --- MODIFICA 2: RIMUOVI LA CREAZIONE DELLA CODA DA QUI ---
	// La coda viene ora creata in main.go e passata al Runtime.

	// gRPC server
	var err error
	rt.GRPC, rt.Listener, rt.Registry, err = grpcserver.Start(
		rt.IsSeed,
		rt.GRPCAddr,
		rt.Log,
		rt.Clock,
		rt.Mgr,
		rt.ID,
		seed.Sampler(func(max int) []*proto.Stats { return rt.Engine.LocalSample(max) }),
		func() *proto.Stats {
			s := rt.Node.CurrentStatsProto()
			s.TsMs = rt.Clock.NowSim().UnixMilli()
			return s
		},
		func(jobID string, cpu, mem, gpu float64, durMs int64) bool {
			return rt.Node.StartJobLoad(jobID, cpu, mem, gpu, time.Duration(durMs)*time.Millisecond)
		},
		func(jobID string) bool {
			return rt.Node.CancelJob(jobID)
		},
		rt.Rng,
		rt.PBQueue,
	)
	return err
}

// StopAll ferma gRPC, Reporter, Anti-Entropy e SWIM (in quest’ordine).
func (rt *Runtime) StopAll() {
	grpcserver.Stop(rt.GRPC, rt.Listener, rt.Log)
	if rt.Reporter != nil {
		rt.Reporter.Stop()
	}
	if rt.Engine != nil {
		rt.Engine.Stop()
	}
	if rt.Mgr != nil {
		rt.Mgr.Stop()
	}
}

// RecoverAll ricrea SWIM, AE, Reporter, gRPC server e gestisce il re-join se necessario.
func (rt *Runtime) RecoverAll() {
	// SWIM
	swimCfg := swim.Config{PeriodSimS: 1.0, TimeoutRealMs: 250, IndirectK: 3, SuspicionTimeoutS: 6.0}
	rt.Mgr = swim.NewManager(rt.ID, rt.GRPCAddr, rt.Log, rt.Clock, rt.Rng, swimCfg)
	rt.Mgr.Start()

	// AE
	aeCfg := antientropy.Config{PeriodSimS: 3.0, Fanout: 2, SampleSize: 8, TtlSimS: 12.0}
	rt.Engine = antientropy.NewEngine(rt.Log, rt.Clock, rt.Rng, rt.Store, rt.Mgr, rt.PBQueue, rt.selfStats, aeCfg)
	rt.Engine.Start()

	// Reporter
	repCfg := antientropy.ReporterConfig{PeriodSimS: 10.0, TopK: 3}
	rt.Reporter = antientropy.NewReporter(rt.Log, rt.Clock, rt.Store, rt.selfStats, repCfg)
	rt.Reporter.Start()

	// --- CORREZIONE: RIMUOVIAMO LA LOGICA CONDIZIONALE PER LA PBQueue ---
	// La PBQueue è gestita esternamente e persiste tra i recovery. Non va toccata.

	// gRPC
	var err error
	rt.GRPC, rt.Listener, rt.Registry, err = grpcserver.Start(
		rt.IsSeed, rt.GRPCAddr, rt.Log, rt.Clock, rt.Mgr, rt.ID,
		seed.Sampler(func(max int) []*proto.Stats { return rt.Engine.LocalSample(max) }),
		func() *proto.Stats {
			s := rt.Node.CurrentStatsProto()
			s.TsMs = rt.Clock.NowSim().UnixMilli()
			return s
		},
		func(jobID string, cpu, mem, gpu float64, durMs int64) bool {
			return rt.Node.StartJobLoad(jobID, cpu, mem, gpu, time.Duration(durMs)*time.Millisecond)
		},
		func(jobID string) bool {
			return rt.Node.CancelJob(jobID)
		},
		rt.Rng,
		rt.PBQueue,
	)
	if err != nil {
		rt.Log.Errorf("startGRPCServer (recovery) failed: %v", err)
	}

	// Re-join se necessario
	rt.tryJoinPostRecovery()
}

// ... il resto del file (TryJoinIfNeeded, etc.) resta invariato ...
func (rt *Runtime) TryJoinIfNeeded() {
	if rt.IsSeed || rt.SeedsCSV == "" {
		return
	}
	rt.doJoin("JOIN")
}

func (rt *Runtime) tryJoinPostRecovery() {
	if rt.IsSeed {
		if rt.Registry != nil {
			rt.Registry.UpsertPeer(&proto.PeerInfo{NodeId: rt.ID, Addr: rt.GRPCAddr, IsSeed: true})
		}
		return
	}
	if rt.SeedsCSV == "" {
		return
	}
	rt.doJoin("JOIN post-recovery")
}

func (rt *Runtime) doJoin(prefix string) {
	jc := seed.NewJoinClient(rt.Log, rt.Clock)
	pcts := rt.Node.PublishedPercentages()
	req := &proto.JoinRequest{
		NodeId:      rt.Node.ID,
		Addr:        rt.GRPCAddr,
		Incarnation: uint64(time.Now().UnixMilli()),
		MyStats: &proto.Stats{
			NodeId: rt.Node.ID,
			CpuPct: pcts.CPU,
			MemPct: pcts.MEM,
			GpuPct: pcts.GPU,
			TsMs:   rt.Clock.NowSimMs(),
		},
	}
	rep, seedAddr, err := jc.TryJoin(rt.SeedsCSV, req)
	if err != nil {
		rt.Log.Warnf("%s non riuscito: %v", prefix, err)
		return
	}
	rt.Log.Infof("%s via %s — peers=%d", prefix, seedAddr, len(rep.Peers))

	rt.Mgr.AddPeer("seed@"+seedAddr, seedAddr)
	for _, p := range rep.Peers {
		rt.Mgr.AddPeer(p.NodeId, p.Addr)
		rt.Log.Infof("  peer: node_id=%s addr=%s seed=%v", p.NodeId, p.Addr, p.IsSeed)
	}
	if len(rep.StatsSnapshot) > 0 {
		rt.Store.UpsertBatch(rep.StatsSnapshot)
	}
}
