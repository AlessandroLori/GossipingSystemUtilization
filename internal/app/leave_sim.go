package app

import (
	"context"
	"math"
	"math/rand"
	"time"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"GossipSystemUtilization/internal/logx"
	"GossipSystemUtilization/internal/piggyback"
	"GossipSystemUtilization/internal/simclock"

	//"GossipSystemUtilization/internal/swim"
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

// === helper: pick su mappa pesata ===
func pickKeyWeighted(r *rand.Rand, m map[string]float64) string {
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

// === helper: snapshot degli indirizzi vicini PRIMA di spegnere la rete ===
func snapshotNeighborAddrs(rt *Runtime, selfID string, r *rand.Rand, maxTouch int) []string {
	addrs := make([]string, 0, maxTouch)

	// 1) seed registry disponibile → usa quello
	if rt != nil && rt.Registry != nil {
		peers := rt.Registry.SamplePeers(selfID, 16) // prendiamo più di quanto toccheremo
		if len(peers) > 0 {
			// mescola
			r.Shuffle(len(peers), func(i, j int) { peers[i], peers[j] = peers[j], peers[i] })
			limit := maxTouch
			if len(peers) < limit {
				limit = len(peers)
			}
			for i := 0; i < limit; i++ {
				addrs = append(addrs, peers[i].Addr)
			}
			return addrs
		}
	}

	// 2) fallback: SWIM manager (peer non-seed)
	if rt != nil && rt.Mgr != nil {
		ap := rt.Mgr.AlivePeers()
		if len(ap) > 0 {
			r.Shuffle(len(ap), func(i, j int) { ap[i], ap[j] = ap[j], ap[i] })
			limit := maxTouch
			if len(ap) < limit {
				limit = len(ap)
			}
			for i := 0; i < limit; i++ {
				addrs = append(addrs, ap[i].Addr)
			}
			return addrs
		}
	}

	return addrs // eventualmente vuoto
}

// === annuncio leave verso una lista di indirizzi (sincrono, nessuna goroutine) ===
func announceLeaveToAddressesSync(
	log *logx.Logger,
	clock *simclock.Clock,
	r *rand.Rand,
	rt *Runtime,
	addrs []string,
	dur time.Duration,
) {
	if rt == nil || len(addrs) == 0 {
		return
	}
	n := 3
	if len(addrs) < n {
		n = len(addrs)
	}
	log.Infof("LEAVE announce → touching %d neighbors to spread BusyUntil+Leave=%s",
		n, dur.Truncate(100*time.Millisecond))

	for i := 0; i < n; i++ {
		addr := addrs[i]
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
			// con RecvEnabled=false non applicheremo eventuali adverts in reply
		}()
	}
}

// === duplica e aggiorna self-advert con flag busy/leave ===
func forceSelfAdvertLeave(rt *Runtime, clock *simclock.Clock, selfID string, d time.Duration) {
	if rt == nil || rt.PBQueue == nil || clock == nil || selfID == "" {
		return
	}
	nowMs := clock.NowSim().UnixMilli()

	a, ok := rt.PBQueue.Latest(selfID)
	if !ok {
		a = piggyback.Advert{NodeId: selfID, Avail: 224}
	}
	ttlMs := int64(110000) // allineato al TTL default della queue
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

	// Log "a mappe" (config effettivo)
	log.Infof("LEAVE PROFILE → freq_weights=%v freq_rate_per_min=%v dur_weights=%v dur_mean_s=%v",
		in.FrequencyClassWeights, in.FrequencyPerMinSim, in.DurationClassWeights, in.DurationMeanSimS)

	// --- PROFILO PER-NODO (one-shot) ---
	if r == nil {
		r = rand.New(rand.NewSource(time.Now().UnixNano()))
	}
	freqClass := pickKeyWeighted(r, in.FrequencyClassWeights)
	ratePerMin := in.FrequencyPerMinSim[freqClass]

	durClass := pickKeyWeighted(r, in.DurationClassWeights)
	meanAway := in.DurationMeanSimS[durClass]

	meanUp := math.Inf(1) // 60/λ, se λ=0 mostriamo +Inf
	if ratePerMin > 0 {
		meanUp = 60.0 / ratePerMin
	}
	log.Infof(
		"LEAVE PROFILE → freqClass=%s (λ≈%.3f leave/min), durClass=%s (meanAway=%.1fs), meanUp=%.1fs",
		freqClass, ratePerMin, durClass, meanAway, meanUp,
	)
	// --- fine profilo per-nodo ---

	go func() {
		for {
			// 1) attesa simulata prima della leave (comportamento invariato)
			freqClass := pickKeyWeighted(r, in.FrequencyClassWeights)
			ratePerMin := in.FrequencyPerMinSim[freqClass]
			waitSim := 999999.0
			if ratePerMin > 0 {
				waitSim = (r.ExpFloat64() / ratePerMin) * 60.0
			}
			if waitSim > 1e8 {
				waitSim = 60.0
			}
			clock.SleepSim(time.Duration(waitSim * float64(time.Second)))

			// 2) durata leave (comportamento invariato)
			durClass := pickKeyWeighted(r, in.DurationClassWeights)
			meanS := in.DurationMeanSimS[durClass]
			if meanS <= 0 {
				meanS = 10
			}
			durS := meanS * (0.7 + 0.6*r.Float64())
			dur := time.Duration(durS * float64(time.Second))

			// === Sequenza leave ===

			// A) blocca subito la generazione/accettazione job
			nodeUp.Store(false)

			// B) marca busy/leave nello self-advert locale e DISABILITA RECV piggyback
			if rt.PBQueue != nil {
				rt.PBQueue.SetBusyFor(dur)
				rt.PBQueue.SetLeaveFor(dur)
				rt.PBQueue.SetRecvEnabled(false) // fondamentale: non applicare reply
				forceSelfAdvertLeave(rt, clock, selfID, dur)
			}

			// B2) snapshot dei vicini PRIMA di spegnere la rete
			addrs := snapshotNeighborAddrs(rt, selfID, r, 6)

			// C) Quiesce: ferma Reporter, AE, gRPC e SWIM
			rt.QuiesceForLeave()

			// D) avvisa SINCRONO i vicini raccolti (solo attach, nessuna recv)
			announceLeaveToAddressesSync(log, clock, r, rt, addrs, dur)

			// E) congela completamente la PBQueue (niente attach/recv dopo questo punto)
			rt.FinalizeLeaveStop()

			if printTrans {
				log.Warnf("LEAVE → DOWN (graceful) — duration=%s", dur.Truncate(100*time.Millisecond))
			}

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
				rt.PBQueue.SetRecvEnabled(true) // riabilita RECV
				forceSelfAdvertLeave(rt, clock, selfID, 0)
			}
			rt.TryJoinIfNeeded()
		}
	}()
}
