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

// QuiesceForLeave: ferma tutto ciò che genera traffico o riceve RPC,
// ma lascia attiva la PBQueue per attaccare i metadati agli ultimi avvisi.
func (rt *Runtime) QuiesceForLeave() {
	nodeUp.Store(false)

	// Ferma subito i loop che generano I/O in uscita
	if rt.Reporter != nil {
		rt.Reporter.Stop()
	}
	if rt.Engine != nil {
		rt.Engine.Stop()
	}

	// Blocca subito le RPC IN INGRESSO (non vogliamo più ricevere nulla)
	grpcserver.Stop(rt.GRPC, rt.Listener, rt.Log)
	rt.GRPC = nil
	rt.Listener = nil

	// Ferma SWIM (niente ping/pingreq ulteriori)
	if rt.Mgr != nil {
		rt.Mgr.Stop()
		rt.Mgr = nil
	}

	// La PBQueue resta attiva per l'annuncio leave.
	// Il gating della RECV lo fai in leave_sim.go (SetRecvEnabled(false) prima dell'annuncio).
}

// FinalizeLeaveStop: dopo gli avvisi, freddiamo anche la PBQueue.
func (rt *Runtime) FinalizeLeaveStop() {
	if rt.PBQueue != nil {
		// Dopo l'annuncio: freddiamo completamente la PBQueue
		rt.PBQueue.SetSendEnabled(false)
		rt.PBQueue.SetRecvEnabled(false)
		rt.PBQueue.Pause()
	}
}

func (rt *Runtime) StartAll() error {
	// SWIM
	swimCfg := swim.Config{
		PeriodSimS:        1.0,
		TimeoutRealMs:     250,
		IndirectK:         3,
		SuspicionTimeoutS: 6.0,
	}
	rt.Mgr = swim.NewManager(rt.ID, rt.GRPCAddr, rt.Log, rt.Clock, rt.Rng, swimCfg, IsNodeUp)
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

	// gRPC server
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
		IsNodeUp,
	)

	// Riapri completamente la PBQueue
	if rt.PBQueue != nil {
		rt.PBQueue.Resume()
		rt.PBQueue.SetSendEnabled(true)
		rt.PBQueue.SetRecvEnabled(true)
	}

	nodeUp.Store(true)
	return err
}

func (rt *Runtime) StopAll() {
	nodeUp.Store(false)

	// Congela piggyback
	if rt.PBQueue != nil {
		rt.PBQueue.SetSendEnabled(false)
		rt.PBQueue.SetRecvEnabled(false)
		rt.PBQueue.Pause()
	}

	// Ferma subito i loop che generano I/O in uscita
	if rt.Reporter != nil {
		rt.Reporter.Stop()
	}
	if rt.Engine != nil {
		rt.Engine.Stop()
	}

	// Chiudi l'esposizione di RPC in ingresso
	grpcserver.Stop(rt.GRPC, rt.Listener, rt.Log)

	// Ferma SWIM
	if rt.Mgr != nil {
		rt.Mgr.Stop()
	}

	// azzera i riferimenti
	rt.GRPC = nil
	rt.Listener = nil
	rt.Reporter = nil
	rt.Engine = nil
	rt.Mgr = nil
}

func (rt *Runtime) RecoverAll() {
	// PBQueue di nuovo attiva
	if rt.PBQueue != nil {
		rt.PBQueue.Resume()
		rt.PBQueue.SetSendEnabled(true)
		rt.PBQueue.SetRecvEnabled(true)
	}

	nodeUp.Store(true)

	swimCfg := swim.Config{PeriodSimS: 1.0, TimeoutRealMs: 250, IndirectK: 3, SuspicionTimeoutS: 6.0}
	rt.Mgr = swim.NewManager(rt.ID, rt.GRPCAddr, rt.Log, rt.Clock, rt.Rng, swimCfg, IsNodeUp)
	rt.Mgr.Start()

	aeCfg := antientropy.Config{PeriodSimS: 3.0, Fanout: 2, SampleSize: 8, TtlSimS: 12.0}
	rt.Engine = antientropy.NewEngine(rt.Log, rt.Clock, rt.Rng, rt.Store, rt.Mgr, rt.PBQueue, rt.selfStats, aeCfg)
	rt.Engine.Start()

	repCfg := antientropy.ReporterConfig{PeriodSimS: 10.0, TopK: 3}
	rt.Reporter = antientropy.NewReporter(rt.Log, rt.Clock, rt.Store, rt.selfStats, repCfg)
	rt.Reporter.Start()

	var err error
	rt.GRPC, rt.Listener, rt.Registry, err = grpcserver.Start(
		rt.IsSeed, rt.GRPCAddr, rt.Log, rt.Clock, rt.Mgr, rt.ID,
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
		IsNodeUp,
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

// safeAddPeer aggiunge un peer alla membership SWIM solo se il Manager è attivo.
// Evita panic se rt.Mgr è nil (es. durante leave/fault o durante il riavvio dei servizi).
func (rt *Runtime) safeAddPeer(id, addr string) {
	if id == "" || addr == "" {
		return
	}
	if rt.Mgr == nil {
		rt.Log.Warnf("JOIN: SWIM Manager nil — skip AddPeer(%s,%s). Probabile fase di fault/leave.", id, addr)
		return
	}
	rt.Mgr.AddPeer(id, addr)
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

	// Accetta 1 o 2 seed (TryJoinTwo prova a contattarli fino a due distinti se disponibili).
	rep, seedAddrs, err := jc.TryJoinTwo(rt.SeedsCSV, req)
	if err != nil {
		rt.Log.Warnf("%s non riuscito: %v (contattati=%v)", prefix, err, seedAddrs)
		return
	}
	if rep == nil {
		rt.Log.Warnf("%s: join reply nil (contattati=%v)", prefix, seedAddrs)
		return
	}
	rt.Log.Infof("%s via %v — peers=%d", prefix, seedAddrs, len(rep.Peers))

	// registra i seed contattati (0..2) in modo sicuro (no panic se SWIM è down)
	if len(seedAddrs) >= 1 && seedAddrs[0] != "" {
		rt.safeAddPeer("seed@"+seedAddrs[0], seedAddrs[0])
	}
	if len(seedAddrs) >= 2 && seedAddrs[1] != "" {
		rt.safeAddPeer("seed@"+seedAddrs[1], seedAddrs[1])
	}

	// registra eventuali peer restituiti dal seed (ignora entry vuote)
	for _, p := range rep.Peers {
		if p == nil || p.NodeId == "" || p.Addr == "" {
			continue
		}
		rt.safeAddPeer(p.NodeId, p.Addr)
		rt.Log.Infof("  peer: node_id=%s addr=%s seed=%v", p.NodeId, p.Addr, p.IsSeed)
	}

	// semina/aggiorna lo store con lo snapshot di Stats (se presente)
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
