package scheduler

import (
	"context"
	"time"

	proto "GossipSystemUtilization/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type GRPCRPC struct{}

func NewGRPCRPC() *GRPCRPC { return &GRPCRPC{} }

func (r *GRPCRPC) Probe(ctx context.Context, addr string, req *proto.ProbeRequest, timeout time.Duration) (*proto.ProbeReply, error) {
	ctx2, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	conn, err := grpc.DialContext(ctx2, addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	cli := proto.NewGossipClient(conn)
	return cli.Probe(ctx2, req)
}

func (r *GRPCRPC) Commit(ctx context.Context, addr string, req *proto.CommitRequest, timeout time.Duration) (*proto.CommitReply, error) {
	ctx2, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	conn, err := grpc.DialContext(ctx2, addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	cli := proto.NewGossipClient(conn)
	return cli.Commit(ctx2, req)
}

func (r *GRPCRPC) Cancel(ctx context.Context, addr string, req *proto.CancelRequest, timeout time.Duration) (*proto.CancelReply, error) {
	ctx2, cancel := context.WithTimeout(ctx, timeout)
	defer cancel()
	conn, err := grpc.DialContext(ctx2, addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
	if err != nil {
		return nil, err
	}
	defer conn.Close()
	cli := proto.NewGossipClient(conn)
	return cli.Cancel(ctx2, req)
}
