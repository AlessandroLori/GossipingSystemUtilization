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
	"GossipSystemUtilization/internal/affinity"
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

// StartSeedCoordinator: versione con Friends & Reputation e decay legato al clock simulato
func StartSeedCoordinator(
	log *logx.Logger,
	clock *simclock.Clock,
	r *mrand.Rand,
	cfg *config.Config,
	reg *seed.Registry,
	selfID string,
	pbq *piggyback.Queue,
) {
	// === Friends & Reputation: init manager + decay loop ===
	affCfg := affinity.DefaultConfig()
	affCfg.HalfLife = 60 * time.Second
	affCfg.DecayEvery = 5 * time.Second
	affCfg.WReputation, affCfg.WPiggyback, affCfg.WLeastLoad, affCfg.WPenalty = 0.5, 0.3, 0.2, 0.4
	affCfg.Epsilon, affCfg.SoftmaxTemp = 0.10, 0.20
	affCfg.StaleCutoff = 5 * time.Second

	aff := affinity.NewManager(affCfg, r, clock)
	ctxDecay, cancelDecay := context.WithCancel(context.Background())
	go aff.StartDecayLoop(ctxDecay)

	go func() {
		defer cancelDecay()

		// piccolo delay iniziale per dare tempo ad AE e SWIM
		clock.SleepSim(2 * time.Second)

		for {
			// 1) Inter-arrivo jobs (SIM)
			mean := cfg.Workload.MeanInterarrivalSimS
			if mean <= 0 {
				mean = 10
			}
			waitS := mean * r.ExpFloat64()
			clock.SleepSim(time.Duration(waitS * float64(time.Second)))

			// 2) Disegna job
			job := drawJob(r, cfg)

			// 3) Candidati dal registry (escludi self se necessario nel registry)
			k := cfg.Scheduler.ProbeFanout
			if k <= 0 {
				k = 3
			}
			candidates := 2 * k
			var peers []*proto.PeerInfo
			if reg != nil {
				peers = reg.SamplePeers(selfID, candidates)
			}
			if len(peers) == 0 {
				log.Warnf("COORD: nessun peer disponibile per job=%s; ritento più tardi", job.id)
				continue
			}

			// 3bis) Ranking (Friends & Reputation). Se vuoto, fallback random.
			ordered := rankCandidatesForJob(clock, job, peers, k, aff /*, pbq*/)
			if len(ordered) == 0 {
				// Fallback conservativo: prendi k random unici
				ordered = samplePeers(peers, k, r)
				if len(ordered) == 0 {
					log.Warnf("COORD: nessun candidato per job=%s; ritento più tardi", job.id)
					continue
				}
			}

			// 4) Probe nell'ordine suggerito
			class := affinity.GuessClass(job.cpu, job.mem, job.gpu)
			bestAddr := ""
			bestID := ""
			bestScore := -1.0

			for _, p := range ordered {
				ok, score, reason := probeNode(clock, p.Addr, selfID, job, cfg.Scheduler.ProbeTimeoutRealMs, pbq)
				aff.UpdateOnProbe(p.NodeId, class, ok) // hook reputazione

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
				aff.UpdateOnCommit(bestID, class, affinity.OutcomeCompleted)
				log.Infof("COORD COMMIT ✓ target=%s job=%s cpu=%.1f%% mem=%.1f%% gpu=%.1f%% dur=%s",
					bestID, job.id, job.cpu, job.mem, job.gpu, job.duration)
			} else {
				aff.UpdateOnCommit(bestID, class, affinity.OutcomeRefused)
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

// rankCandidatesForJob ordina i candidati usando Friends & Reputation.
// I segnali Piggyback/AE/Cool-off sono lasciati neutri (stub) per compilare subito.
// topK: quanti candidati vuoi davvero sondare con Probe.
// rankCandidatesForJob: usa Friends & Reputation; segnali piggyback/AE/cool-off per ora neutri
func rankCandidatesForJob(
	clock *simclock.Clock,
	job schedJob,
	sampled []*proto.PeerInfo,
	topK int,
	aff *affinity.Manager,
	// pbq *piggyback.Queue,
) []*proto.PeerInfo {

	class := affinity.GuessClass(job.cpu, job.mem, job.gpu)

	id2peer := make(map[string]*proto.PeerInfo, len(sampled))
	cands := make([]affinity.Candidate, 0, len(sampled))

	// 🔧 FIX: usare il metodo del clock, non funzioni globali inesistenti
	nowMs := clock.NowSimMs()
	_ = nowMs // (lo useremo quando aggiungiamo il cool-off)

	for _, p := range sampled {
		if p == nil {
			continue
		}
		id2peer[p.NodeId] = p

		// ==== STUB segnali (neutri finché non leghiamo piggyback/AE/cool-off) ====
		hasGPU := true   // TODO: derivare da AE quando disponibile
		advAvail := -1.0 // -1 = ignoto (neutro)
		fresh := true    // nessuna penalità staleness
		penalty := 0.0   // cool-off non ancora propagato via piggyback

		// Carico previsto: ignoto per ora → neutro (-1).
		projected := -1.0
		// Quando collegherai AE:
		// projected = affinity.ProjectedLoadByClass(cpuPct, memPct, gpuPct, class, hasGPU)

		cands = append(cands, affinity.Candidate{
			PeerID:          p.NodeId,
			HasGPU:          hasGPU,
			AdvertAvail:     advAvail,
			ProjectedLoad:   projected,
			CooldownPenalty: penalty,
			Fresh:           fresh,
		})
	}

	scored := aff.Rank(class, cands, topK)
	if len(scored) == 0 {
		return nil
	}

	out := make([]*proto.PeerInfo, 0, len(scored))
	seen := make(map[string]struct{}, len(scored))
	for _, sc := range scored {
		if p := id2peer[sc.PeerID]; p != nil {
			if _, dup := seen[p.NodeId]; !dup {
				out = append(out, p)
				seen[p.NodeId] = struct{}{}
			}
		}
	}
	return out
}
