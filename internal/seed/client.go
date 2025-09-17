package seed

import (
	"context"
	"fmt"
	"strings"
	"time"

	"GossipSystemUtilization/internal/logx"
	"GossipSystemUtilization/internal/simclock"
	"GossipSystemUtilization/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type JoinClient struct {
	log   *logx.Logger
	clock *simclock.Clock
}

func NewJoinClient(log *logx.Logger, clock *simclock.Clock) *JoinClient {
	return &JoinClient{log: log, clock: clock}
}

func (c *JoinClient) TryJoin(seedsCSV string, req *proto.JoinRequest) (*proto.JoinReply, string, error) {
	seeds := strings.Split(seedsCSV, ",")
	delaySim := 0.5 // secondi SIM
	for attempt := 1; attempt <= 6; attempt++ {
		for _, addr := range seeds {
			addr = strings.TrimSpace(addr)
			if addr == "" {
				continue
			}
			c.log.Infof("JOIN/TRY #%d → seed=%s", attempt, addr)

			ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
			conn, err := grpc.DialContext(ctx, addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
			cancel()
			if err != nil {
				c.log.Warnf("connessione a %s fallita: %v", addr, err)
				continue
			}

			cli := proto.NewGossipClient(conn)
			ctx2, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
			rep, err := cli.Join(ctx2, req)
			cancel2()
			_ = conn.Close()

			if err != nil {
				c.log.Warnf("Join su %s fallito: %v", addr, err)
				continue
			}
			c.log.Infof("JOIN/OK via %s → peers=%d, stats=%d", addr, len(rep.Peers), len(rep.StatsSnapshot))
			return rep, addr, nil
		}
		c.log.Warnf("JOIN retry tra %.1fs (tempo SIM)", delaySim)
		c.clock.SleepSim(time.Duration(delaySim * float64(time.Second)))
		delaySim *= 2 // 0.5 → 1 → 2 → 4 → 8 …
	}
	return nil, "", fmt.Errorf("join non riuscito dopo vari tentativi")
}
