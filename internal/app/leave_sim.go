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
			// fallback: un qualunque k
			for k := range m {
				return k
			}
			return ""
		}

		for {
			// 1) Quanto spesso (sim time): scegli frequenza
			freqClass := pickKey(in.FrequencyClassWeights) // none/low/medium/high
			ratePerMin := in.FrequencyPerMinSim[freqClass]
			waitSim := 999999.0
			if ratePerMin > 0 {
				minutes := r.ExpFloat64() / ratePerMin // Exp(rate)
				waitSim = minutes * 60.0
			}
			if waitSim > 1e8 { // “none”
				waitSim = 60.0 // riprova tra un minuto sim per sicurezza
			}
			clock.SleepSim(time.Duration(waitSim * float64(time.Second)))

			// 2) Quanto dura la leave
			durClass := pickKey(in.DurationClassWeights) // short/medium/long
			meanS := in.DurationMeanSimS[durClass]
			if meanS <= 0 {
				meanS = 10
			}
			durS := meanS * (0.7 + 0.6*r.Float64()) // 0.7–1.3 × mean
			dur := time.Duration(durS * float64(time.Second))

			// 3) ANNUNCIO: imposta BusyUntil & LeaveUntil nel piggyback e “tocca” alcuni vicini
			if rt.PBQueue != nil {
				rt.PBQueue.SetBusyFor(dur)  // farà comparire busy nel prossimo piggyback
				rt.PBQueue.SetLeaveFor(dur) // segnale esplicito di leave
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
			// cancella il flag di leave per i nuovi piggyback
			if rt.PBQueue != nil {
				rt.PBQueue.SetLeaveFor(0)
			}
			rt.TryJoinIfNeeded()
		}
	}()
}

func announceLeaveToNeighbors(log *logx.Logger, clock *simclock.Clock, r *rand.Rand, rt *Runtime, selfID string, dur time.Duration) {
	if rt == nil || rt.Registry == nil {
		return
	}
	// prova a “pizzicare” fino a 3-6 vicini per veicolare il piggyback (Busy/Leave)
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
		// usiamo un Probe “light” per attaccare piggyback via interceptor
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
