package seed

import (
	"context"
	"time"

	"GossipSystemUtilization/internal/logx"
	"GossipSystemUtilization/internal/simclock"
	"GossipSystemUtilization/internal/swim"
	"GossipSystemUtilization/proto"

	"google.golang.org/grpc"
	//"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	//"google.golang.org/grpc/status"
)

type Sampler func(max int) []*proto.Stats

type Server struct {
	proto.UnimplementedGossipServer
	isSeed  bool
	reg     *Registry // solo sui seed
	log     *logx.Logger
	clock   *simclock.Clock
	mgr     *swim.Manager
	myID    string
	sampler Sampler // come riempire l'AvailBatch di risposta
	// callback per job
	selfStatsFn   func() *proto.Stats
	applyCommitFn func(jobID string, cpu, mem, gpu float64, durMs int64) bool
	cancelFn      func(jobID string) bool
}

func NewServer(
	isSeed bool,
	reg *Registry,
	log *logx.Logger,
	clock *simclock.Clock,
	mgr *swim.Manager,
	myID string,
	sampler Sampler,
	selfStatsFn func() *proto.Stats,
	applyCommitFn func(jobID string, cpu, mem, gpu float64, durMs int64) bool,
	cancelFn func(jobID string) bool,
) *Server {
	return &Server{
		isSeed:        isSeed,
		reg:           reg,
		log:           log,
		clock:         clock,
		mgr:           mgr,
		myID:          myID,
		sampler:       sampler,
		selfStatsFn:   selfStatsFn,
		applyCommitFn: applyCommitFn,
		cancelFn:      cancelFn,
	}
}

// === JOIN: solo i seed rispondono ===
func (s *Server) Join(ctx context.Context, req *proto.JoinRequest) (*proto.JoinReply, error) {
	if req == nil {
		return &proto.JoinReply{}, nil
	}

	// Aggiorna membership SWIM (così comparirà tra gli ALIVE)
	if s.mgr != nil {
		s.mgr.AddPeer(req.NodeId, req.Addr)
	}

	// ➜ Registry locale: traccia sempre il peer e il suo "ultimo-visto" di Stats
	if s.reg != nil {
		s.reg.UpsertPeer(&proto.PeerInfo{
			NodeId: req.NodeId,
			Addr:   req.Addr,
			// IsSeed non lo sappiamo dal JoinRequest; non è necessario qui
		})
		if req.MyStats != nil {
			s.reg.UpsertStats(req.MyStats)
		}
	}

	// Prepariamo una reply sensata:
	// - peers: un piccolo campione dalla nostra registry (esclude l'ID del joiner)
	// - stats_snapshot: qualche Stat recente dal nostro store via sampler
	var peers []*proto.PeerInfo
	if s.reg != nil {
		// sovracampiona un po' e poi lascia che il client tagli se vuole
		peers = s.reg.SamplePeers(req.NodeId, 32)
	}
	var snapshot []*proto.Stats
	if s.sampler != nil {
		snapshot = s.sampler(64)
	}

	return &proto.JoinReply{
		Peers:         peers,
		StatsSnapshot: snapshot,
	}, nil
}

// === PING: tutti i nodi rispondono ===
func (s *Server) Ping(ctx context.Context, req *proto.PingRequest) (*proto.PingReply, error) {
	return &proto.PingReply{Ok: true, TsMs: s.clock.NowSimMs()}, nil
}

// === PINGREQ: tutti i nodi aiutano a fare un ping indiretto ===
func (s *Server) PingReq(ctx context.Context, req *proto.PingReqRequest) (*proto.PingReqReply, error) {
	dialCtx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	conn, err := grpc.DialContext(dialCtx, req.TargetAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock())
	if err != nil {
		return &proto.PingReqReply{Ok: false, TsMs: s.clock.NowSimMs()}, nil
	}
	defer conn.Close()

	cli := proto.NewGossipClient(conn)
	ctx2, cancel2 := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel2()
	_, err = cli.Ping(ctx2, &proto.PingRequest{FromId: s.myID, Seq: req.Seq})
	if err != nil {
		return &proto.PingReqReply{Ok: false, TsMs: s.clock.NowSimMs()}, nil
	}
	return &proto.PingReqReply{Ok: true, TsMs: s.clock.NowSimMs()}, nil
}

// === EXCHANGE AVAIL: push-pull semplice ===
func (s *Server) ExchangeAvail(ctx context.Context, in *proto.AvailBatch) (*proto.AvailBatch, error) {
	// ➜ Registry locale: aggiorna "ultimo-visto" delle stats per ogni nodo
	if s.reg != nil && in != nil {
		for _, st := range in.Stats {
			if st != nil {
				s.reg.UpsertStats(st)
			}
		}
	}

	// Rispondi con un piccolo snapshot dal nostro store (via sampler),
	// così l'anti-entropy continua a diffondere informazioni aggiornate.
	var out []*proto.Stats
	if s.sampler != nil {
		out = s.sampler(64)
	}
	return &proto.AvailBatch{Stats: out}, nil
}

func (s *Server) Probe(ctx context.Context, req *proto.ProbeRequest) (*proto.ProbeReply, error) {
	js := req.Job
	var p *proto.Stats
	if s.selfStatsFn != nil {
		p = s.selfStatsFn()
	}
	if p == nil {
		return &proto.ProbeReply{
			NodeId: s.myID, WillAccept: false, Reason: "no_self_stats",
			TsMs: s.clock.NowSimMs(),
		}, nil
	}

	// headroom stimato
	headCPU := 100 - p.CpuPct
	headMEM := 100 - p.MemPct
	var headGPU float64
	if p.GpuPct < 0 {
		headGPU = -1
	} else {
		headGPU = 100 - p.GpuPct
	}

	const safety = 0.05 // 5%
	accept := true
	reason := ""
	if js.CpuPct > 0 && js.CpuPct > headCPU*(1.0-safety) {
		accept, reason = false, "cpu_insufficient"
	}
	if accept && js.MemPct > 0 && js.MemPct > headMEM*(1.0-safety) {
		accept, reason = false, "mem_insufficient"
	}
	if accept && js.GpuPct > 0 {
		if headGPU < 0 {
			accept, reason = false, "gpu_absent"
		} else if js.GpuPct > headGPU*(1.0-safety) {
			accept, reason = false, "gpu_insufficient"
		}
	}

	score := 0.0
	if accept {
		// più headroom residuo = punteggio migliore
		remCPU := headCPU - js.CpuPct
		remMEM := headMEM - js.MemPct
		remGPU := 0.0
		if headGPU >= 0 {
			remGPU = headGPU - js.GpuPct
		}
		// somma normalizzata
		score = remCPU + remMEM + remGPU
	}

	return &proto.ProbeReply{
		NodeId:      s.myID,
		WillAccept:  accept,
		Score:       score,
		HeadroomCpu: headCPU,
		HeadroomMem: headMEM,
		HeadroomGpu: headGPU,
		Reason:      reason,
		TsMs:        s.clock.NowSim().UnixMilli(),
	}, nil
}

func (s *Server) Commit(ctx context.Context, req *proto.CommitRequest) (*proto.CommitReply, error) {
	// Log di ingresso
	s.log.Infof("COMMIT ← job=%s cpu=%.1f%% mem=%.1f%% gpu=%.1f%% dur=%s",
		req.JobId, req.CpuPct, req.MemPct, req.GpuPct, time.Duration(req.DurationMs)*time.Millisecond)

	if s.applyCommitFn == nil {
		s.log.Warnf("COMMIT ✖ job=%s reason=no_apply_commit", req.JobId)
		return &proto.CommitReply{Ok: false, Reason: "no_apply_commit"}, nil
	}

	ok := s.applyCommitFn(req.JobId, req.CpuPct, req.MemPct, req.GpuPct, req.DurationMs)
	if !ok {
		s.log.Warnf("COMMIT ✖ job=%s reason=insufficient_headroom", req.JobId)
		return &proto.CommitReply{Ok: false, Reason: "insufficient_headroom"}, nil
	}

	s.log.Infof("COMMIT ✓ job=%s", req.JobId)
	return &proto.CommitReply{Ok: true}, nil
}

func (s *Server) Cancel(ctx context.Context, req *proto.CancelRequest) (*proto.CancelReply, error) {
	// Log di ingresso
	s.log.Infof("CANCEL ← job=%s", req.JobId)

	if s.cancelFn == nil {
		s.log.Warnf("CANCEL ✖ job=%s reason=no_cancel_fn", req.JobId)
		return &proto.CancelReply{Ok: false, Reason: "no_cancel_fn"}, nil
	}

	ok := s.cancelFn(req.JobId)
	if !ok {
		s.log.Warnf("CANCEL ✖ job=%s reason=unknown_job", req.JobId)
		return &proto.CancelReply{Ok: false, Reason: "unknown_job"}, nil
	}

	s.log.Infof("CANCEL ✓ job=%s", req.JobId)
	return &proto.CancelReply{Ok: true}, nil
}
