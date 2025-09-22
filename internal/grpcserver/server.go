package grpcserver

import (
	"fmt"
	"math/rand"
	"net"

	"GossipSystemUtilization/internal/logx"
	"GossipSystemUtilization/internal/seed"
	"GossipSystemUtilization/internal/simclock"
	"GossipSystemUtilization/internal/swim"
	"GossipSystemUtilization/proto"

	"google.golang.org/grpc"
)

// Start avvia il server gRPC e (se seed) crea il Registry locale.
// È un estratto 1:1 di startGRPCServer dal main, così non cambi le call-site.
func Start(
	isSeed bool,
	grpcAddr string,
	log *logx.Logger,
	clock *simclock.Clock,
	mgr *swim.Manager,
	myID string,
	sampler seed.Sampler,
	selfStatsFn func() *proto.Stats,
	applyCommitFn func(string, float64, float64, float64, int64) bool,
	cancelFn func(string) bool,
	r *rand.Rand,
) (s *grpc.Server, lis net.Listener, reg *seed.Registry, err error) {
	lis, err = net.Listen("tcp", grpcAddr)
	if err != nil {
		return nil, nil, nil, fmt.Errorf("listen %s: %w", grpcAddr, err)
	}
	s = grpc.NewServer()

	if isSeed {
		reg = seed.NewRegistry(r)
		reg.UpsertPeer(&proto.PeerInfo{NodeId: myID, Addr: grpcAddr, IsSeed: true})
	}

	srv := seed.NewServer(
		isSeed,
		reg,
		log,
		clock,
		mgr,
		myID,
		sampler,
		selfStatsFn,
		applyCommitFn,
		cancelFn,
	)

	proto.RegisterGossipServer(s, srv)

	go func() {
		if err := s.Serve(lis); err != nil {
			log.Errorf("gRPC Serve: %v", err)
		}
	}()

	if isSeed {
		log.Infof("SEED attivo su %s (Join/Ping/PingReq/ExchangeAvail/Probe/Commit/Cancel)", grpcAddr)
	} else {
		log.Infof("Peer non-seed su %s (Ping/PingReq/ExchangeAvail/Probe/Commit/Cancel)", grpcAddr)
	}

	return s, lis, reg, nil
}

// Stop chiude server e listener (idempotente).
func Stop(s *grpc.Server, lis net.Listener, log *logx.Logger) {
	if s != nil {
		s.Stop()
	}
	if lis != nil {
		_ = lis.Close()
	}
	log.Warnf("gRPC fermato (server e listener chiusi)")
}
