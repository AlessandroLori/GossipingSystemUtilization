package grpcserver

import (
	"context"

	"GossipSystemUtilization/internal/piggyback"

	"google.golang.org/grpc"
)

// DialWithPiggyback aggiunge l'unary client interceptor di piggybacking.
func DialWithPiggyback(ctx context.Context, addr string, q *piggyback.Queue, opts ...grpc.DialOption) (*grpc.ClientConn, error) {
	if q != nil {
		opts = append(opts, grpc.WithUnaryInterceptor(piggyback.UnaryClientInterceptor(q)))
	}
	return grpc.DialContext(ctx, addr, opts...)
}
