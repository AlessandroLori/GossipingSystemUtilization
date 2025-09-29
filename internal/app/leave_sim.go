package app

import (
	"context"
	"math/rand"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

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

	// Profilo iniziale (come per i fault)
	log.Infof("LEAVE PROFILE → freq_weights=%v freq_rate_per_min=%v dur_weights=%v dur_mean_s=%v",
		in.FrequencyClassWeights, in.FrequencyPerMinSim, in.DurationClassWeights, in.DurationMeanSimS)

	go func() {
		// helper per pick pesato
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
			// 1) Quando scatta la leave (tempo simulato)
			freqClass := pickKey(in.FrequencyClassWeights) // none/low/medium/high
			ratePerMin := in.FrequencyPerMinSim[freqClass]
			waitSim := 999999.0
			if ratePerMin > 0 {
				minutes := r.ExpFloat64() / ratePerMin
				waitSim = minutes * 60.0
			}
			if waitSim > 1e8 { // “none”
				waitSim = 60.0 // riprova tra un minuto sim per sicurezza
			}
			clock.SleepSim(time.Duration(waitSim * float64(time.Second)))

			// 2) Quanto dura
			durClass := pickKey(in.DurationClassWeights) // short/medium/long
			meanS := in.DurationMeanSimS[durClass]
			if meanS <= 0 {
				meanS = 10
			}
			durS := meanS * (0.7 + 0.6*r.Float64()) // 0.7–1.3 × mean
			dur := time.Duration(durS * float64(time.Second))

			// 3) Imposta Busy+Leave e FORZA subito un self-advert aggiornato
			if rt.PBQueue != nil {
				rt.PBQueue.SetBusyFor(dur)
				rt.PBQueue.SetLeaveFor(dur)
				rt.PBQueue.Pause()
				forceSelfAdvertLeave(rt, clock, selfID, dur)
			}
			announceLeaveToNeighbors(log, clock, r, rt, selfID, dur)

			if printTrans {
				log.Warnf("LEAVE → DOWN (graceful) — duration=%s", dur.Truncate(100*time.Millisecond))
			}

			// 4) Stop servizi (come i fault)
			rt.StopAll()

			// 5) Attendi durata simulata
			clock.SleepSim(dur)

			// 6) Restart + re-join
			if printTrans {
				log.Warnf("LEAVE ↑ RECOVERY — restart SWIM + AntiEntropy + Reporter + gRPC")
			}
			if err := rt.StartAll(); err != nil {
				log.Errorf("LEAVE restart error: %v", err)
			}
			// cancella i flag di leave/busy per i nuovi piggyback e pubblica stato “clean”
			if rt.PBQueue != nil {
				rt.PBQueue.SetLeaveFor(0)
				rt.PBQueue.SetBusyFor(0)
				forceSelfAdvertLeave(rt, clock, selfID, 0)
				rt.PBQueue.Resume()
				// ping soft di rientro (riusa announceLeave ma con dur=0 o fai una back-announce simile)
				announceLeaveToNeighbors(log, clock, r, rt, selfID, 250*time.Millisecond)
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

func announceLeaveToNeighbors(log *logx.Logger, clock *simclock.Clock, r *rand.Rand, rt *Runtime, selfID string, dur time.Duration) {
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

	for i := 0; i < n; i++ {
		p := peers[r.Intn(len(peers))]
		go func(addr string) {
			ctx, cancel := context.WithTimeout(context.Background(), 200*time.Millisecond)
			defer cancel()
			conn, err := grpc.DialContext(ctx, addr,
				grpc.WithTransportCredentials(insecure.NewCredentials()),
				grpc.WithBlock(),
				grpc.WithUnaryInterceptor(piggyback.UnaryClientInterceptor(rt.PBQueue)),
			)
			if err != nil {
				return
			}
			defer conn.Close()
			cli := proto.NewGossipClient(conn)
			// Job minimale (0%) solo per generare traffico e portare l’advert
			js := &proto.JobSpec{CpuPct: 0, MemPct: 0, GpuPct: 0, DurationMs: 1}
			_, _ = cli.Probe(ctx, &proto.ProbeRequest{Job: js})
		}(p.Addr)
	}
}
