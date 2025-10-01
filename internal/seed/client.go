package seed

import (
	"context"
	"fmt"
	"math/rand"
	"slices"
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

// === JOIN singolo (come prima) ===
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

// === NEW: JOIN con almeno 2 seed distinti; merge di peers/stats ===
func (c *JoinClient) TryJoinTwo(seedsCSV string, req *proto.JoinRequest) (*proto.JoinReply, []string, error) {
	all := strings.Split(seedsCSV, ",")
	seeds := make([]string, 0, len(all))
	for _, s := range all {
		s = strings.TrimSpace(s)
		if s != "" {
			seeds = append(seeds, s)
		}
	}
	if len(seeds) < 2 {
		return nil, nil, fmt.Errorf("TryJoinTwo richiede almeno 2 seed in SEEDS")
	}

	// proveremo più volte a raccogliere 2 successi da seed distinti
	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	delaySim := 0.5
	successAddrs := []string{}
	peersByID := map[string]*proto.PeerInfo{}
	statsByID := map[string]*proto.Stats{}

	tryOne := func(addr string) bool {
		ctx, cancel := context.WithTimeout(context.Background(), 2*time.Second)
		conn, err := grpc.DialContext(ctx, addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
		cancel()
		if err != nil {
			c.log.Warnf("JOIN(2) dial %s fallito: %v", addr, err)
			return false
		}
		defer conn.Close()

		cli := proto.NewGossipClient(conn)
		ctx2, cancel2 := context.WithTimeout(context.Background(), 2*time.Second)
		rep, err := cli.Join(ctx2, req)
		cancel2()
		if err != nil {
			c.log.Warnf("JOIN(2) su %s fallito: %v", addr, err)
			return false
		}
		if !slices.Contains(successAddrs, addr) {
			successAddrs = append(successAddrs, addr)
		}
		for _, p := range rep.Peers {
			peersByID[p.NodeId] = p
		}
		for _, s := range rep.StatsSnapshot {
			statsByID[s.NodeId] = s
		}
		c.log.Infof("JOIN(2)/OK via %s → peers+=%d, stats+=%d  (tot so far: peers=%d stats=%d)",
			addr, len(rep.Peers), len(rep.StatsSnapshot), len(peersByID), len(statsByID))
		return true
	}

	for attempt := 1; attempt <= 6 && len(successAddrs) < 2; attempt++ {
		// randomizza l'ordine ad ogni tentativo, ma salta seed già riusciti
		rnd.Shuffle(len(seeds), func(i, j int) { seeds[i], seeds[j] = seeds[j], seeds[i] })
		for _, addr := range seeds {
			if len(successAddrs) >= 2 {
				break
			}
			if slices.Contains(successAddrs, addr) {
				continue
			}
			c.log.Infof("JOIN(2)/TRY #%d → seed=%s", attempt, addr)
			_ = tryOne(addr)
		}
		if len(successAddrs) >= 2 {
			break
		}
		c.log.Warnf("JOIN(2) retry tra %.1fs (tempo SIM) — successi finora=%d", delaySim, len(successAddrs))
		c.clock.SleepSim(time.Duration(delaySim * float64(time.Second)))
		delaySim *= 2
	}

	if len(successAddrs) < 2 {
		return nil, successAddrs, fmt.Errorf("TryJoinTwo: successi=%d, servono 2", len(successAddrs))
	}

	// costruisci la reply aggregata
	out := &proto.JoinReply{
		Peers:         make([]*proto.PeerInfo, 0, len(peersByID)),
		StatsSnapshot: make([]*proto.Stats, 0, len(statsByID)),
	}
	for _, p := range peersByID {
		out.Peers = append(out.Peers, p)
	}
	for _, s := range statsByID {
		out.StatsSnapshot = append(out.StatsSnapshot, s)
	}
	return out, successAddrs, nil
}
