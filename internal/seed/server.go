package seed

import (
	"context"
	"time"

	"GossipSystemUtilization/internal/logx"
	"GossipSystemUtilization/internal/simclock"
	"GossipSystemUtilization/internal/swim"
	"GossipSystemUtilization/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/codes"
	"google.golang.org/grpc/credentials/insecure"
	"google.golang.org/grpc/status"
)

type Server struct {
	proto.UnimplementedGossipServer
	isSeed bool
	reg    *Registry // solo usato se isSeed==true
	log    *logx.Logger
	clock  *simclock.Clock
	mgr    *swim.Manager // per aggiungere peer alla membership al Join
	myID   string
}

func NewServer(isSeed bool, reg *Registry, log *logx.Logger, clock *simclock.Clock, mgr *swim.Manager, myID string) *Server {
	return &Server{
		isSeed: isSeed,
		reg:    reg,
		log:    log,
		clock:  clock,
		mgr:    mgr,
		myID:   myID,
	}
}

// === JOIN: solo i seed rispondono ===
func (s *Server) Join(ctx context.Context, req *proto.JoinRequest) (*proto.JoinReply, error) {
	if !s.isSeed {
		return nil, status.Error(codes.Unimplemented, "Join non disponibile su peer non-seed")
	}
	ms := req.MyStats
	s.log.Infof("JOIN ← node_id=%s addr=%s inc=%d cpu=%.1f%% mem=%.1f%% gpu=%.1f%%",
		req.NodeId, req.Addr, req.Incarnation, ms.CpuPct, ms.MemPct, ms.GpuPct)

	// registra nel registry del seed (per rispondere con un campione)
	if s.reg != nil {
		s.reg.UpsertPeer(&proto.PeerInfo{NodeId: req.NodeId, Addr: req.Addr, IsSeed: false})
		s.reg.UpsertStats(req.MyStats)
	}

	// registra anche nella membership locale (così il seed lo pinga)
	if s.mgr != nil {
		s.mgr.AddPeer(req.NodeId, req.Addr)
	}

	peers := []*proto.PeerInfo{}
	stats := []*proto.Stats{}
	if s.reg != nil {
		peers = s.reg.SamplePeers(req.NodeId, 10)
		stats = s.reg.SnapshotStats(20)
	}

	s.log.Infof("JOIN → a %s: peers=%d, stats=%d", req.NodeId, len(peers), len(stats))
	return &proto.JoinReply{Peers: peers, StatsSnapshot: stats}, nil
}

// === PING: tutti i nodi rispondono ===
func (s *Server) Ping(ctx context.Context, req *proto.PingRequest) (*proto.PingReply, error) {
	// è un “echo” per failure detection
	return &proto.PingReply{Ok: true, TsMs: time.Now().UnixMilli()}, nil
}

// === PINGREQ: tutti i nodi aiutano a fare un ping indiretto ===
func (s *Server) PingReq(ctx context.Context, req *proto.PingReqRequest) (*proto.PingReqReply, error) {
	// Prova a pingare il target_addr e riferisci l’esito
	dialCtx, cancel := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel()

	conn, err := grpc.DialContext(dialCtx, req.TargetAddr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock())
	if err != nil {
		return &proto.PingReqReply{Ok: false, TsMs: time.Now().UnixMilli()}, nil
	}
	defer conn.Close()

	cli := proto.NewGossipClient(conn)
	ctx2, cancel2 := context.WithTimeout(context.Background(), 250*time.Millisecond)
	defer cancel2()
	_, err = cli.Ping(ctx2, &proto.PingRequest{FromId: s.myID, Seq: req.Seq})
	if err != nil {
		return &proto.PingReqReply{Ok: false, TsMs: time.Now().UnixMilli()}, nil
	}
	return &proto.PingReqReply{Ok: true, TsMs: time.Now().UnixMilli()}, nil
}
