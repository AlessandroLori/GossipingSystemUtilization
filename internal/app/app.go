package app

import (
	"math/rand"
	"time"

	"google.golang.org/grpc"

	"GossipSystemUtilization/internal/antientropy"
	"GossipSystemUtilization/internal/config"
	"GossipSystemUtilization/internal/grpcserver"
	"GossipSystemUtilization/internal/logx"
	"GossipSystemUtilization/internal/node"
	"GossipSystemUtilization/internal/seed"
	"GossipSystemUtilization/internal/simclock"
	"GossipSystemUtilization/internal/swim"

	proto "GossipSystemUtilization/proto"
)

// App incapsula i componenti principali.
type App struct {
	ID     string
	Addr   string
	IsSeed bool

	Clock *simclock.Clock
	Log   *logx.Logger
	Rng   *rand.Rand

	Node     *node.Node
	SwimMgr  *swim.Manager
	AEStore  *antientropy.Store
	AEEng    *antientropy.Engine
	Reporter *antientropy.Reporter

	// dal grpcserver.Start
	grpcSrv *grpc.Server
	reg     *seed.Registry

	Cfg *config.Config
}

// Init prepara tutti i componenti (non blocca).
func (a *App) Init() error {
	// Nodo
	a.Node = node.New(a.ID, a.Addr, a.Clock, a.Log, a.Rng)

	// SWIM
	swimCfg := swim.Config{
		PeriodSimS:        1.0,
		TimeoutRealMs:     250,
		IndirectK:         3,
		SuspicionTimeoutS: 6.0,
	}
	a.SwimMgr = swim.NewManager(a.ID, a.Addr, a.Log, a.Clock, a.Rng, swimCfg)
	a.SwimMgr.Start()

	// Anti-entropy
	a.AEStore = antientropy.NewStore(a.Log, a.Clock)
	selfSampler := func() *proto.Stats {
		s := a.Node.CurrentStatsProto()
		s.TsMs = a.Clock.NowSimMs()
		return s
	}

	aeCfg := antientropy.Config{
		PeriodSimS: 3.0,
		Fanout:     2,
		SampleSize: 8,
		TtlSimS:    12.0,
	}
	a.AEEng = antientropy.NewEngine(a.Log, a.Clock, a.Rng, a.AEStore, a.SwimMgr, selfSampler, aeCfg)
	a.AEEng.Start()

	// Reporter
	repCfg := antientropy.ReporterConfig{PeriodSimS: 10.0, TopK: 3}
	a.Reporter = antientropy.NewReporter(a.Log, a.Clock, a.AEStore, selfSampler, repCfg)
	a.Reporter.Start()

	// Semina lo store con le stats locali
	a.AEStore.UpsertBatch([]*proto.Stats{selfSampler()})

	// gRPC server
	var err error
	_, _, a.reg, err = grpcserver.Start(
		a.IsSeed,
		a.Addr,
		a.Log,
		a.Clock,
		a.SwimMgr,
		a.ID,
		func(max int) []*proto.Stats { return a.AEEng.LocalSample(max) },
		func() *proto.Stats {
			s := a.Node.CurrentStatsProto()
			s.TsMs = a.Clock.NowSimMs()
			return s
		},
		func(jobID string, cpu, mem, gpu float64, durMs int64) bool {
			return a.Node.StartJobLoad(jobID, cpu, mem, gpu, time.Duration(durMs)*time.Millisecond)
		},
		func(jobID string) bool {
			return a.Node.CancelJob(jobID)
		},
		a.Rng,
	)
	if err != nil {
		return err
	}

	/*
		// Coordinator (solo seed)
		if a.IsSeed && a.Cfg != nil && a.Cfg.Workload.Enabled {
			StartSeedCoordinator(a.Log, a.Clock, a.Rng, a.Cfg, a.reg, a.ID)
		}
	*/
	return nil

}
