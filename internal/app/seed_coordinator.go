// app/seed_coordinator.go
package app

import (
	"context"
	"fmt"
	"time"

	mrand "math/rand"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"GossipSystemUtilization/internal/config"
	//"GossipSystemUtilization/internal/grpcserver"
	"GossipSystemUtilization/internal/logx"
	"GossipSystemUtilization/internal/piggyback"
	"GossipSystemUtilization/internal/seed"
	"GossipSystemUtilization/internal/simclock"

	proto "GossipSystemUtilization/proto"
)

// --- Scheduler seed-only: generazione job + Probe/Commit ---

type schedJob struct {
	id       string
	cpu, mem float64
	gpu      float64
	duration time.Duration
}

func StartSeedCoordinator(
	log *logx.Logger,
	clock *simclock.Clock,
	r *mrand.Rand,
	cfg *config.Config,
	reg *seed.Registry,
	selfID string,
	pbq *piggyback.Queue,
) {
	go func() {
		// opzionale: un piccolo delay iniziale per dare tempo all'anti-entropy
		clock.SleepSim(2 * time.Second)

		for {
			// 1) Attendi il prossimo job (inter-arrivo esponenziale in tempo SIM)
			mean := cfg.Workload.MeanInterarrivalSimS
			if mean <= 0 {
				mean = 10 // fallback robusto
			}
			waitS := mean * r.ExpFloat64() // Exp(lambda=1) → mean scaling
			clock.SleepSim(time.Duration(waitS * float64(time.Second)))

			// 2) Disegna un job dalle distribuzioni del config
			job := drawJob(r, cfg)

			// 3) Scegli i candidati dal registry del seed (escludendo me stesso)
			//    campioniamo un po' di peer e poi da quelli prendiamo k per la Probe
			k := cfg.Scheduler.ProbeFanout
			if k <= 0 {
				k = 3
			}
			candidates := 2 * k
			peers := []*proto.PeerInfo(nil)
			if reg != nil {
				peers = reg.SamplePeers(selfID, candidates)
			}
			if len(peers) == 0 {
				log.Warnf("COORD: nessun peer disponibile per job=%s; ritento più tardi", job.id)
				continue
			}
			peers = samplePeers(peers, k, r)

			// 4) Probe dei candidati e scelta del migliore
			bestAddr := ""
			bestID := ""
			bestScore := -1.0
			for _, p := range peers {
				ok, score, reason := probeNode(clock, p.Addr, selfID, job, cfg.Scheduler.ProbeTimeoutRealMs, pbq)
				if ok {
					if score > bestScore {
						bestScore = score
						bestAddr = p.Addr
						bestID = p.NodeId
					}
				} else {
					log.Infof("PROBE → %s refuse (reason=%s) job=%s", p.NodeId, reason, job.id)
				}
			}
			if bestAddr == "" {
				log.Infof("COORD: nessun peer ha accettato il job=%s", job.id)
				continue
			}

			// 5) Commit sul vincitore
			if commitJob(clock, bestAddr, job, cfg.Scheduler.ProbeTimeoutRealMs, pbq) {
				log.Infof("COORD COMMIT ✓ target=%s job=%s cpu=%.1f%% mem=%.1f%% gpu=%.1f%% dur=%s",
					bestID, job.id, job.cpu, job.mem, job.gpu, job.duration)
			} else {
				log.Warnf("COORD COMMIT ✖ target=%s job=%s", bestID, job.id)
			}
		}
	}()
}

func drawJob(r *mrand.Rand, cfg *config.Config) schedJob {
	id := fmt.Sprintf("job-%06x", r.Uint32())

	// helper uniformi
	unif := func(min, max float64) float64 {
		if max <= min {
			return min
		}
		return min + r.Float64()*(max-min)
	}

	// percentuali
	cpu := unif(cfg.Workload.JobCPU.MinPct, cfg.Workload.JobCPU.MaxPct)
	mem := unif(cfg.Workload.JobMEM.MinPct, cfg.Workload.JobMEM.MaxPct)

	// GPU: se range > 0 la usiamo, altrimenti restiamo a 0
	gpu := 0.0
	if cfg.Workload.JobGPU.MaxPct > 0 {
		gpu = unif(cfg.Workload.JobGPU.MinPct, cfg.Workload.JobGPU.MaxPct)
	}

	// durata (tempo SIM)
	dmin := cfg.Workload.JobDurationSimS.MinS
	dmax := cfg.Workload.JobDurationSimS.MaxS
	if dmax <= dmin {
		dmax = dmin
	}
	dS := dmin + r.Float64()*(dmax-dmin)
	dur := time.Duration(dS * float64(time.Second))

	return schedJob{
		id:       id,
		cpu:      cpu,
		mem:      mem,
		gpu:      gpu,
		duration: dur,
	}
}

func samplePeers(in []*proto.PeerInfo, k int, r *mrand.Rand) []*proto.PeerInfo {
	if k <= 0 || k >= len(in) {
		return in
	}
	out := make([]*proto.PeerInfo, 0, k)
	seen := make(map[int]struct{})
	for len(out) < k && len(out) < len(in) {
		i := r.Intn(len(in))
		if _, ok := seen[i]; ok {
			continue
		}
		seen[i] = struct{}{}
		out = append(out, in[i])
	}
	return out
}

func probeNode(clock *simclock.Clock, addr, requesterID string, job schedJob, timeoutMs int, pbq *piggyback.Queue) (accept bool, score float64, reason string) {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeoutMs)*time.Millisecond)
	defer cancel()

	// --- CORREZIONE: Utilizziamo un dialer gRPC standard con l'interceptor,
	// invece della funzione custom che creava problemi. ---
	conn, err := grpc.DialContext(ctx, addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
		grpc.WithUnaryInterceptor(piggyback.UnaryClientInterceptor(pbq)), // Aggiungiamo l'interceptor qui
	)

	if err != nil {
		return false, 0, "dial_error"
	}
	defer conn.Close()

	cli := proto.NewGossipClient(conn)

	js := &proto.JobSpec{
		CpuPct:     job.cpu,
		MemPct:     job.mem,
		GpuPct:     job.gpu,
		DurationMs: int64(job.duration / time.Millisecond),
	}
	rep, err := cli.Probe(ctx, &proto.ProbeRequest{
		Job: js,
	})
	if err != nil {
		return false, 0, "rpc_error"
	}
	return rep.WillAccept, rep.Score, rep.Reason
}

func commitJob(clock *simclock.Clock, addr string, job schedJob, timeoutMs int, pbq *piggyback.Queue) bool {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeoutMs)*time.Millisecond)
	defer cancel()

	// --- CORREZIONE: Stessa modifica anche qui ---
	conn, err := grpc.DialContext(ctx, addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
		grpc.WithUnaryInterceptor(piggyback.UnaryClientInterceptor(pbq)), // Aggiungiamo l'interceptor qui
	)
	if err != nil {
		return false
	}
	defer conn.Close()

	cli := proto.NewGossipClient(conn)

	_, err = cli.Commit(ctx, &proto.CommitRequest{
		JobId:      job.id,
		CpuPct:     job.cpu,
		MemPct:     job.mem,
		GpuPct:     job.gpu,
		DurationMs: int64(job.duration / time.Millisecond),
	})
	return err == nil
}
