package app

import (
	"context"
	"math/rand"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	//"GossipSystemUtilization/internal/grpcserver"
	"GossipSystemUtilization/internal/logx"
	"GossipSystemUtilization/internal/piggyback"
	"GossipSystemUtilization/internal/simclock"
	proto "GossipSystemUtilization/proto"
)

type LeaveProfileInput struct {
	Enabled               *bool
	PrintTransitions      *bool
	FrequencyClassWeights map[string]float64
	FrequencyPerMinSim    map[string]float64
	DurationClassWeights  map[string]float64
	DurationMeanSimS      map[string]float64
}

func StartLeaveRecoveryWithRuntime(
	log *logx.Logger,
	clock *simclock.Clock,
	r *rand.Rand,
	in LeaveProfileInput,
	rt *Runtime,
	selfID string,
) {
	if rt == nil || clock == nil || log == nil {
		return
	}
	if in.Enabled == nil || !*in.Enabled {
		return
	}

	printTrans := (in.PrintTransitions != nil && *in.PrintTransitions)

	log.Infof("LEAVE PROFILE → freq_weights=%v freq_rate_per_min=%v dur_weights=%v dur_mean_s=%v",
		in.FrequencyClassWeights, in.FrequencyPerMinSim, in.DurationClassWeights, in.DurationMeanSimS)

	go func() {
		pickKey := func(m map[string]float64) string {
			sum := 0.0
			for _, v := range m {
				if v > 0 {
					sum += v
				}
			}
			if sum <= 0 {
				return ""
			}
			x := r.Float64() * sum
			for k, v := range m {
				if v <= 0 {
					continue
				}
				if x < v {
					return k
				}
				x -= v
			}
			for k := range m {
				return k
			}
			return ""
		}

		for {
			// 1) attesa simulata prima della leave
			freqClass := pickKey(in.FrequencyClassWeights)
			ratePerMin := in.FrequencyPerMinSim[freqClass]
			waitSim := 999999.0
			if ratePerMin > 0 {
				waitSim = (r.ExpFloat64() / ratePerMin) * 60.0
			}
			if waitSim > 1e8 {
				waitSim = 60.0
			}
			clock.SleepSim(time.Duration(waitSim * float64(time.Second)))

			// 2) durata leave
			durClass := pickKey(in.DurationClassWeights)
			meanS := in.DurationMeanSimS[durClass]
			if meanS <= 0 {
				meanS = 10
			}
			durS := meanS * (0.7 + 0.6*r.Float64())
			dur := time.Duration(durS * float64(time.Second))

			// === Sequenza hard-stop ===
			// A) mettiamo subito il semaforo rosso per chi prova a generare/mandare job
			nodeUp.Store(false)

			// B) aggiorniamo i flag di busy/leave e pubblichiamo immediatamente lo self-advert
			if rt.PBQueue != nil {
				rt.PBQueue.SetBusyFor(dur)
				rt.PBQueue.SetLeaveFor(dur)
				forceSelfAdvertLeave(rt, clock, selfID, dur)
			}

			// C) QUIET MODE: fermiamo Reporter, Engine, SWIM e chiudiamo il server gRPC
			//    (nessun inbound né traffic di background). Lasciamo attiva solo la PBQueue.
			rt.QuiesceForLeave()

			// D) inviamo SINCRONO 2–3 avvisi (client-side) che veicolano il piggyback di leave
			announceLeaveToNeighborsSync(log, clock, r, rt, selfID, dur)

			if printTrans {
				log.Warnf("LEAVE → DOWN (graceful) — duration=%s", dur.Truncate(100*time.Millisecond))
			}

			// E) freddiamo anche la PBQueue: da qui in avanti nessun attach/recv piggyback
			rt.FinalizeLeaveStop()

			// 3) attendi la durata simulata
			clock.SleepSim(dur)

			// 4) restart + pulizia flag + re-join
			if printTrans {
				log.Warnf("LEAVE ↑ RECOVERY — restart SWIM + AntiEntropy + Reporter + gRPC")
			}
			if err := rt.StartAll(); err != nil {
				log.Errorf("LEAVE restart error: %v", err)
			}
			if rt.PBQueue != nil {
				rt.PBQueue.SetLeaveFor(0)
				rt.PBQueue.SetBusyFor(0)
				forceSelfAdvertLeave(rt, clock, selfID, 0)
				// piccolo “touch” di rientro
				announceLeaveToNeighborsSync(log, clock, r, rt, selfID, 250*time.Millisecond)
			}
			rt.TryJoinIfNeeded()
		}
	}()
}

// forceSelfAdvertLeave: duplica l’ultimo self-advert, aggiorna busy/leave e timestamp, e lo rimette in coda
func forceSelfAdvertLeave(rt *Runtime, clock *simclock.Clock, selfID string, d time.Duration) {
	if rt == nil || rt.PBQueue == nil || clock == nil || selfID == "" {
		return
	}
	nowMs := clock.NowSim().UnixMilli()

	// base: prova a prendere l’ultimo advert locale
	a, ok := rt.PBQueue.Latest(selfID)
	if !ok {
		// fallback minimale
		a = piggyback.Advert{
			NodeId: selfID,
			Avail:  224, // “alta” disponibilità di default
		}
	}
	ttlMs := int64(110000) // allinea al TTL di default della queue (110s)
	if a.ExpireMs > a.CreateMs {
		ttlMs = a.ExpireMs - a.CreateMs
	}
	a.CreateMs = nowMs
	a.ExpireMs = nowMs + ttlMs
	if d > 0 {
		a.BusyUntilMs = nowMs + d.Milliseconds()
		a.LeaveUntilMs = a.BusyUntilMs
	} else {
		a.BusyUntilMs = 0
		a.LeaveUntilMs = 0
	}
	rt.PBQueue.Upsert(a)
}

// announceLeaveToNeighborsSync: seleziona pochi vicini e invia SINCRONO una Probe "vuota"
// per veicolare il piggyback con BusyUntil/LeaveUntil aggiornati. Timeout stretto, nessuna goroutine.
func announceLeaveToNeighborsSync(
	log *logx.Logger,
	clock *simclock.Clock,
	r *rand.Rand,
	rt *Runtime,
	selfID string,
	dur time.Duration,
) {
	if rt == nil || rt.Registry == nil {
		return
	}
	peers := rt.Registry.SamplePeers(selfID, 6)
	if len(peers) == 0 {
		return
	}
	n := 3
	if len(peers) < n {
		n = len(peers)
	}
	log.Infof("LEAVE announce → touching %d neighbors to spread BusyUntil+Leave=%s",
		n, dur.Truncate(100*time.Millisecond))

	// mescola e prendi i primi n
	r.Shuffle(len(peers), func(i, j int) { peers[i], peers[j] = peers[j], peers[i] })
	peers = peers[:n]

	for _, p := range peers {
		addr := p.Addr
		// timeout piccolo per non restare attivi a lungo
		ctx, cancel := context.WithTimeout(context.Background(), 250*time.Millisecond)
		conn, err := grpc.DialContext(ctx, addr,
			grpc.WithTransportCredentials(insecure.NewCredentials()),
			grpc.WithBlock(),
			grpc.WithUnaryInterceptor(piggyback.UnaryClientInterceptor(rt.PBQueue)),
		)
		cancel()
		if err != nil {
			continue
		}
		func() {
			defer conn.Close()
			cli := proto.NewGossipClient(conn)
			ctx2, cancel2 := context.WithTimeout(context.Background(), 200*time.Millisecond)
			defer cancel2()
			// Job minimale (0%) solo per generare traffico e portare l’advert
			js := &proto.JobSpec{CpuPct: 0, MemPct: 0, GpuPct: 0, DurationMs: 1}
			_, _ = cli.Probe(ctx2, &proto.ProbeRequest{Job: js})
			// il server remoto stamperà “LEAVE UPDATE RECV …”
		}()
	}
}
