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

// === JOIN singolo ===
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

// === JOIN con seed distinti; merge di peers/stats ===
func (c *JoinClient) TryJoinTwo(seedsCSV string, req *proto.JoinRequest) (*proto.JoinReply, []string, error) {
	// parse & normalizza
	all := strings.Split(seedsCSV, ",")
	seeds := make([]string, 0, len(all))
	seen := make(map[string]struct{}, len(all))
	for _, s := range all {
		s = strings.TrimSpace(s)
		if s == "" {
			continue
		}
		if _, ok := seen[s]; ok {
			continue
		}
		seen[s] = struct{}{}
		seeds = append(seeds, s)
	}
	if len(seeds) < 1 {
		return nil, nil, fmt.Errorf("TryJoinTwo: nessun seed valido in SEEDS (serve almeno 1)")
	}

	// obiettivo: più successi se disponibili, altrimenti 1 se c'è un solo seed
	target := 2
	if len(seeds) == 1 {
		target = 1
	}

	rnd := rand.New(rand.NewSource(time.Now().UnixNano()))
	delaySim := 0.5 // secondi SIM
	successAddrs := []string{}
	peersByID := map[string]*proto.PeerInfo{}
	statsByID := map[string]*proto.Stats{}

	tryOne := func(addr string) bool {
		// salta se già riuscito con questo seed
		if slices.Contains(successAddrs, addr) {
			return false
		}

		// connessione gRPC al seed
		ctxDial, cancelDial := context.WithTimeout(context.Background(), 2*time.Second)
		conn, err := grpc.DialContext(ctxDial, addr, grpc.WithTransportCredentials(insecure.NewCredentials()), grpc.WithBlock())
		cancelDial()
		if err != nil {
			c.log.Warnf("JOIN dial %s fallito: %v", addr, err)
			return false
		}
		defer conn.Close()

		cli := proto.NewGossipClient(conn)
		ctxRPC, cancelRPC := context.WithTimeout(context.Background(), 2*time.Second)
		rep, err := cli.Join(ctxRPC, req)
		cancelRPC()
		if err != nil {
			c.log.Warnf("JOIN RPC su %s fallito: %v", addr, err)
			return false
		}

		// successo: registra seed e unisci le risposte
		if !slices.Contains(successAddrs, addr) {
			successAddrs = append(successAddrs, addr)
		}
		for _, p := range rep.GetPeers() {
			if p != nil && p.NodeId != "" {
				peersByID[p.NodeId] = p
			}
		}
		for _, s := range rep.GetStatsSnapshot() {
			if s != nil && s.NodeId != "" {
				statsByID[s.NodeId] = s
			}
		}
		return true
	}

	// tenta più giri finché non raggiungi target o esaurisci i tentativi
	for attempt := 1; attempt <= 6 && len(successAddrs) < target; attempt++ {
		// ordine random ad ogni giro
		rnd.Shuffle(len(seeds), func(i, j int) { seeds[i], seeds[j] = seeds[j], seeds[i] })
		for _, addr := range seeds {
			if len(successAddrs) >= target {
				break
			}
			c.log.Infof("JOIN/TRY #%d → seed=%s", attempt, addr)
			_ = tryOne(addr)
		}
		if len(successAddrs) >= target {
			break
		}
		// backoff
		if c.clock != nil {
			c.clock.SleepSim(time.Duration(delaySim * float64(time.Second)))
		} else {
			time.Sleep(200 * time.Millisecond)
		}
	}

	// se non ne è andato a buon fine nemmeno uno
	if len(successAddrs) == 0 {
		return nil, nil, fmt.Errorf("JOIN fallito: tutti i seed irraggiungibili (%d candidati)", len(seeds))
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
