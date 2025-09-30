// internal/app/app.go

package app

import (
	"fmt"
	"math/rand"
	"time"

	"google.golang.org/grpc"

	"GossipSystemUtilization/internal/antientropy"
	"GossipSystemUtilization/internal/config"
	"GossipSystemUtilization/internal/grpcserver"
	"GossipSystemUtilization/internal/logx"
	"GossipSystemUtilization/internal/node"
	"GossipSystemUtilization/internal/piggyback"
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

	// piggyback queue locale
	PBQ *piggyback.Queue

	Cfg *config.Config
}

// Init prepara tutti i componenti (non blocca).
func (a *App) Init() error {
	// Nodo
	a.Node = node.New(a.ID, a.Addr, a.Clock, a.Log, a.Rng)

	// SWIM (con gate IsNodeUp)
	swimCfg := swim.Config{
		PeriodSimS:        1.0,
		TimeoutRealMs:     250,
		IndirectK:         3,
		SuspicionTimeoutS: 6.0,
	}
	a.SwimMgr = swim.NewManager(a.ID, a.Addr, a.Log, a.Clock, a.Rng, swimCfg, IsNodeUp)
	a.SwimMgr.Start()

	// Anti-entropy
	a.AEStore = antientropy.NewStore(a.Log, a.Clock)
	selfSampler := func() *proto.Stats {
		s := a.Node.CurrentStatsProto()
		s.TsMs = a.Clock.NowSimMs()
		return s
	}

	// Piggyback queue (TTL ~110s, cap 200) — inizializzata PRIMA dell'engine
	a.PBQ = piggyback.NewQueue(a.Log, a.Clock, 200, 110*time.Second)

	aeCfg := antientropy.Config{
		PeriodSimS: 3.0,
		Fanout:     2,
		SampleSize: 8,
		TtlSimS:    12.0,
	}
	a.AEEng = antientropy.NewEngine(a.Log, a.Clock, a.Rng, a.AEStore, a.SwimMgr, a.PBQ, selfSampler, aeCfg)
	a.AEEng.Start()

	// Reporter
	repCfg := antientropy.ReporterConfig{PeriodSimS: 10.0, TopK: 3}
	a.Reporter = antientropy.NewReporter(a.Log, a.Clock, a.AEStore, selfSampler, repCfg)
	a.Reporter.Start()

	// Semina lo store con le stats locali
	a.AEStore.UpsertBatch([]*proto.Stats{selfSampler()})

	// Aggiornamento periodico dello "self advert" in PBQ
	go func() {
		for {
			s := a.Node.CurrentStatsProto()
			s.TsMs = a.Clock.NowSimMs()
			a.PBQ.UpsertSelfFromStats(s)
			a.Clock.SleepSim(2 * time.Second)
		}
	}()

	// gRPC server
	var err error
	statsSampler := func(max int) []*proto.Stats {
		return a.AEEng.LocalSample(max)
	}

	_, _, a.reg, err = grpcserver.Start(
		a.IsSeed,
		a.Addr,
		a.Log,
		a.Clock,
		a.SwimMgr,
		a.ID,
		seed.Sampler(statsSampler), // <-- tipo corretto
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
		a.PBQ,
		IsNodeUp, // <-- gate per rifiutare RPC durante la leave
	)
	if err != nil {
		return fmt.Errorf("start gRPC server: %w", err)
	}

	return nil
}
