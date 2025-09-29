package app

import (
	"net"
	"sync/atomic"
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
	ID       string
	GRPCAddr string
	IsSeed   bool
	SeedsCSV string

	Log   *logx.Logger
	Clock *simclock.Clock
	Rng   *mrand.Rand

	Node    *node.Node
	Store   *antientropy.Store
	PBQueue *piggyback.Queue

	Mgr      *swim.Manager
	Engine   *antientropy.Engine
	Reporter *antientropy.Reporter
	GRPC     *grpc.Server
	Listener net.Listener
	Registry *seed.Registry

	selfStats func() *proto.Stats
}

// Stato process-level del nodo: true = servizi attivi; false = in leave/fault (servizi fermi).
var nodeUp atomic.Bool

func IsNodeUp() bool { return nodeUp.Load() }

func NewRuntime(
	id, grpcAddr string,
	isSeed bool,
	seedsCSV string,
	log *logx.Logger,
	clock *simclock.Clock,
	r *mrand.Rand,
	n *node.Node,
	store *antientropy.Store,
	pbq *piggyback.Queue,
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
		PBQueue:  pbq,
	}
	rt.selfStats = func() *proto.Stats {
		s := rt.Node.CurrentStatsProto()
		s.TsMs = rt.Clock.NowSimMs()
		return s
	}
	return rt
}

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
	aeCfg := antientropy.Config{PeriodSimS: 3.0, Fanout: 2, SampleSize: 8, TtlSimS: 12.0}
	rt.Engine = antientropy.NewEngine(rt.Log, rt.Clock, rt.Rng, rt.Store, rt.Mgr, rt.PBQueue, rt.selfStats, aeCfg)
	rt.Engine.Start()

	// Reporter
	repCfg := antientropy.ReporterConfig{PeriodSimS: 10.0, TopK: 3}
	rt.Reporter = antientropy.NewReporter(rt.Log, rt.Clock, rt.Store, rt.selfStats, repCfg)
	rt.Reporter.Start()

	// Seed dello store con self
	rt.Store.UpsertBatch([]*proto.Stats{rt.selfStats()})

	// gRPC server (+ Registry locale SEMPRE)
	var err error
	rt.GRPC, rt.Listener, rt.Registry, err = grpcserver.Start(
		rt.IsSeed,
		rt.GRPCAddr,
		rt.Log,
		rt.Clock,
		rt.Mgr,
		rt.ID,
		seed.Sampler(rt.safeLocalSample),
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
		return err
	}

	// ➜ SYNC PERIODICO: porta i peer ALIVE di SWIM dentro la registry locale (anche sui peer).
	//    Questo dà sempre candidati al coordinator, anche se la registry del seed non è raggiungibile.
	go func() {
		for {
			if !IsNodeUp() {
				rt.Clock.SleepSim(500 * time.Millisecond)
				continue
			}
			if rt.Registry != nil && rt.Mgr != nil {
				ap := rt.Mgr.AlivePeers()
				for _, p := range ap {
					if p.ID == "" || p.ID == rt.ID || p.Addr == "" {
						continue
					}
					rt.Registry.UpsertPeer(&proto.PeerInfo{
						NodeId: p.ID,
						Addr:   p.Addr,
						IsSeed: false, // da SWIM non lo sappiamo; non serve ai fini del coordinamento
					})
				}
			}
			rt.Clock.SleepSim(2 * time.Second)
		}
	}()

	nodeUp.Store(true)
	return nil
}

func (rt *Runtime) StopAll() {
	nodeUp.Store(false)
	if rt.PBQueue != nil {
		rt.PBQueue.Pause()
	}

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
	// azzera i riferimenti per evitare doppi Stop su stessa istanza
	rt.GRPC = nil
	rt.Listener = nil
	rt.Reporter = nil
	rt.Engine = nil
	rt.Mgr = nil
	// PBQueue, Store restano (persistono tra recovery)
}

func (rt *Runtime) RecoverAll() {
	if rt.PBQueue != nil {
		rt.PBQueue.Resume()
	}
	nodeUp.Store(true)

	swimCfg := swim.Config{PeriodSimS: 1.0, TimeoutRealMs: 250, IndirectK: 3, SuspicionTimeoutS: 6.0}
	rt.Mgr = swim.NewManager(rt.ID, rt.GRPCAddr, rt.Log, rt.Clock, rt.Rng, swimCfg)
	rt.Mgr.Start()

	aeCfg := antientropy.Config{PeriodSimS: 3.0, Fanout: 2, SampleSize: 8, TtlSimS: 12.0}
	rt.Engine = antientropy.NewEngine(rt.Log, rt.Clock, rt.Rng, rt.Store, rt.Mgr, rt.PBQueue, rt.selfStats, aeCfg)
	rt.Engine.Start()

	repCfg := antientropy.ReporterConfig{PeriodSimS: 10.0, TopK: 3}
	rt.Reporter = antientropy.NewReporter(rt.Log, rt.Clock, rt.Store, rt.selfStats, repCfg)
	rt.Reporter.Start()

	nodeUp.Store(true)

	var err error
	rt.GRPC, rt.Listener, rt.Registry, err = grpcserver.Start(
		rt.IsSeed, rt.GRPCAddr, rt.Log, rt.Clock, rt.Mgr, rt.ID,
		//seed.Sampler(func(max int) []*proto.Stats { return rt.Engine.LocalSample(max) }),
		seed.Sampler(rt.safeLocalSample),
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

	rt.tryJoinPostRecovery()
}

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

// dentro internal/app/runtime.go
func (rt *Runtime) safeLocalSample(max int) []*proto.Stats {
	// Durante recovery Engine può essere momentaneamente nil.
	if rt.Engine != nil {
		return rt.Engine.LocalSample(max)
	}
	// Fallback: prendiamo uno snapshot direttamente dallo Store.
	if rt.Store != nil {
		var self *proto.Stats
		if rt.selfStats != nil {
			self = rt.selfStats()
		}
		return rt.Store.SnapshotSample(max, self)
	}
	return nil
}
