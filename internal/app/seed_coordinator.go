package app

import (
	"context"
	"fmt"
	"math"
	"sync"
	"time"

	"math/rand"
	mrand "math/rand"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"

	"GossipSystemUtilization/internal/affinity"
	"GossipSystemUtilization/internal/config"
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

// ====== PERSONA PER-NODO ======
// NOTE override veloci via ENV (opzionali, vedi main.go):
//
//	NODE_PRIMARY_DOMINANCE (0.50..0.90) default 0.70
//	NODE_RATE_DIST (slow|normal|fast|mix) default mix
type NodePersona struct {
	Primary              int        // 0=GENERAL, 1=CPU_ONLY, 2=MEM_HEAVY, 3=GPU_HEAVY
	Dominance            float64    // tipicamente 0.6..0.85
	Mix                  [4]float64 // prob. con cui si sceglie la classe ad ogni job (somma=1)
	RateLabel            string     // "slow"|"normal"|"fast"
	RateFactor           float64    // 1.50 (slow), 1.00 (normal), 0.70 (fast)
	MeanInterarrivalSimS float64    // mean base (da cfg) moltiplicato per RateFactor
}

// ====== GENERATORE PER-NODO ======

type nodeJobGenerator struct {
	r      *mrand.Rand
	cfg    *config.Config
	classP [4]float64 // order: 0=GENERAL, 1=CPU_ONLY, 2=MEM_HEAVY, 3=GPU_HEAVY
	meanS  float64    // mean interarrival (SIM) del nodo

	// soglie "heavy" effettive dal config (aligned ai bucket large)
	cpuTh, memTh, gpuTh float64
}

var (
	affOnce      sync.Once
	affMgr       *affinity.Manager
	affCtx       context.Context
	affCtxCancel context.CancelFunc
)

func newNodeJobGeneratorFromPersona(r *mrand.Rand, cfg *config.Config, persona NodePersona) *nodeJobGenerator {
	if r == nil {
		r = mrand.New(mrand.NewSource(time.Now().UnixNano()))
	}
	cTh, mTh, gTh := cfg.EffectiveThresholds()
	return &nodeJobGenerator{
		r:      r,
		cfg:    cfg,
		classP: persona.Mix,
		meanS:  persona.MeanInterarrivalSimS,
		cpuTh:  cTh, memTh: mTh, gpuTh: gTh,
	}
}

// pick classe secondo le probabilità del nodo
func (g *nodeJobGenerator) pickClass() int {
	u := g.r.Float64()
	acc := 0.0
	for i, p := range g.classP {
		acc += p
		if u <= acc {
			return i // 0=GENERAL 1=CPU_ONLY 2=MEM_HEAVY 3=GPU_HEAVY
		}
	}
	return 0
}

// sampling valori coerenti con la classe scelta
func (g *nodeJobGenerator) drawJob() schedJob {
	id := fmt.Sprintf("job-%06x", g.r.Uint32())
	cls := g.pickClass()

	// Helpers
	unif := func(min, max float64) float64 {
		if max <= min {
			return min
		}
		return min + g.r.Float64()*(max-min)
	}
	// Campiona "sotto" una soglia; se il range bucket è già sopra la soglia, rientra verso la soglia.
	sampleBelow := func(min, max, upper float64) float64 {
		if upper <= 0 {
			return 0
		}
		hi := math.Min(max, upper)
		if hi <= min {
			// Il bucket è troppo alto rispetto alla soglia: spingi verso valori davvero "low".
			lo := upper * 0.35
			hi2 := upper * 0.95
			if hi2 <= lo {
				return math.Max(0, upper*0.9)
			}
			return unif(lo, hi2)
		}
		return unif(min, hi)
	}
	// ⇒ garantisce value ≥ lower (se il range è troppo basso, allarghiamo la coda alta)
	sampleAtLeast := func(min, max, lower float64) float64 {
		lo := min
		if lower > lo {
			lo = lower
		}
		hi := max
		if hi < lo {
			hi = 100.0
		} // estendi fino a 100 se serve
		if hi > 100 {
			hi = 100
		}
		if lo < 0 {
			lo = 0
		}
		// bias verso la coda alta
		u := 0.7 + 0.3*g.r.Float64()
		return lo + u*(hi-lo)
	}
	capIfNoExplicit := func(curMax float64) float64 {
		// Se assente o "aperto" (0 o >=100), usa 50 come cap di default
		if curMax <= 0 || curMax >= 100 {
			return 50
		}
		return curMax
	}

	// Leggi intervalli base
	cpuMin, cpuMax := g.cfg.Workload.JobCPU.MinPct, g.cfg.Workload.JobCPU.MaxPct
	memMin, memMax := g.cfg.Workload.JobMEM.MinPct, g.cfg.Workload.JobMEM.MaxPct
	gpuMin, gpuMax := g.cfg.Workload.JobGPU.MinPct, g.cfg.Workload.JobGPU.MaxPct

	// Se buckets attivi, usa gli anchor pesati: "Large" SOLO sulla risorsa heavy della classe.
	if g.cfg.Workload.UseBuckets() {
		cb, mb, gb := g.cfg.Workload.CPUBuckets, g.cfg.Workload.MEMBuckets, g.cfg.Workload.GPUBuckets
		expand := func(anchor float64) (float64, float64) {
			lo := anchor * 0.8
			hi := anchor * 1.2
			if lo < 0 {
				lo = 0
			}
			if hi > 100 {
				hi = 100
			}
			if hi <= lo {
				hi = lo + 1
			}
			return lo, hi
		}
		cpuMin, cpuMax = expand(weightedAnchor(cls, "cpu", cb, g))
		memMin, memMax = expand(weightedAnchor(cls, "mem", mb, g))
		gpuMin, gpuMax = expand(weightedAnchor(cls, "gpu", gb, g))
	}

	// Per GENERAL, se non c'è cap esplicito mettiamo 50 max (1–50).
	if cls == 0 { // GENERAL
		// Non toccare cap espliciti <50 (li rispettiamo), ma se "aperti" li portiamo a 50.
		if !g.cfg.Workload.UseBuckets() {
			cpuMax = capIfNoExplicit(cpuMax)
			memMax = capIfNoExplicit(memMax)
			gpuMax = capIfNoExplicit(gpuMax)
		}
		// In ogni caso, impedisci che i bucket spingano sopra 50 per GENERAL.
		cpuMax = math.Min(cpuMax, 50)
		memMax = math.Min(memMax, 50)
		gpuMax = math.Min(gpuMax, 50)

		// assicura min ragionevoli
		if cpuMin < 1 {
			cpuMin = 1
		}
		if memMin < 1 {
			memMin = 1
		}
		if gpuMin < 1 {
			gpuMin = 1
		}
		if cpuMin >= cpuMax {
			cpuMin = math.Max(0, cpuMax-1)
		}
		if memMin >= memMax {
			memMin = math.Max(0, memMax-1)
		}
		if gpuMin >= gpuMax {
			gpuMin = math.Max(0, gpuMax-1)
		}
	}

	// Campionamento coerente con la classe
	var cpu, mem, gpu float64
	switch cls {
	case 1: // CPU_ONLY
		cpu = sampleAtLeast(cpuMin, cpuMax, g.cpuTh) // ≥ soglia (es. 70)
		mem = sampleBelow(memMin, memMax, g.memTh)   // sotto soglia
		gpu = sampleBelow(gpuMin, gpuMax, g.gpuTh*0.5)
	case 2: // MEM_HEAVY
		mem = sampleAtLeast(memMin, memMax, g.memTh) // ≥ soglia (es. 65)
		cpu = sampleBelow(cpuMin, cpuMax, g.cpuTh)
		gpu = sampleBelow(gpuMin, gpuMax, g.gpuTh*0.5)
	case 3: // GPU_HEAVY
		gpu = sampleAtLeast(gpuMin, gpuMax, g.gpuTh) // ≥ soglia (es. 60)
		cpu = sampleBelow(cpuMin, cpuMax, g.cpuTh)
		mem = sampleBelow(memMin, memMax, g.memTh)
	default: // 0: GENERAL
		cpu = sampleBelow(cpuMin, cpuMax, 50)
		mem = sampleBelow(memMin, memMax, 50)
		gpu = sampleBelow(gpuMin, gpuMax, 50)
	}

	// Durata
	var dur time.Duration
	if g.cfg.Workload.UseBuckets() && g.cfg.Workload.DurationBuckets != nil {
		db := g.cfg.Workload.DurationBuckets
		anchor := pickByProb(g.r,
			[]float64{db.ProbSmall, db.ProbMedium, db.ProbLarge},
			[]float64{db.SmallSimS, db.MediumSimS, db.LargeSimS},
		)
		lo := anchor * 0.8
		hi := anchor * 1.2
		if hi <= lo {
			hi = lo + 1
		}
		sec := lo + g.r.Float64()*(hi-lo)
		dur = time.Duration(sec * float64(time.Second))
	} else {
		dmin := g.cfg.Workload.JobDurationSimS.MinS
		dmax := g.cfg.Workload.JobDurationSimS.MaxS
		if dmax <= dmin {
			dmax = dmin
		}
		dS := dmin + g.r.Float64()*(dmax-dmin)
		dur = time.Duration(dS * float64(time.Second))
	}

	return schedJob{id: id, cpu: cpu, mem: mem, gpu: gpu, duration: dur}
}

// weightedAnchor: applica Large SOLO sulla risorsa heavy della classe; altrimenti usa le probabilità.
func weightedAnchor(cls int, kind string, b *config.ProbBucketsPct, g *nodeJobGenerator) float64 {
	if b == nil {
		return 10
	}
	isHeavy := false
	switch cls {
	case 1: // CPU_ONLY
		isHeavy = (kind == "cpu")
	case 2: // MEM_HEAVY
		isHeavy = (kind == "mem")
	case 3: // GPU_HEAVY
		isHeavy = (kind == "gpu")
	}
	if isHeavy {
		return b.LargePct
	}
	// GENERALE o risorse "non heavy" → pesato Small/Medium/Large secondo le prob.
	anchors := []float64{b.SmallPct, b.MediumPct, b.LargePct}
	probs := []float64{b.ProbSmall, b.ProbMedium, b.ProbLarge}
	return pickByProb(g.r, probs, anchors)
}

func pickByProb(r *mrand.Rand, probs []float64, values []float64) float64 {
	if len(probs) != len(values) || len(probs) == 0 {
		return 0
	}
	sum := 0.0
	for _, p := range probs {
		sum += p
	}
	u := r.Float64() * sum
	acc := 0.0
	for i, p := range probs {
		acc += p
		if u <= acc {
			return values[i]
		}
	}
	return values[len(values)-1]
}

// ===== StartSeedCoordinator: persona per-nodo + Friends/Reputation =====

func StartSeedCoordinator(
	log *logx.Logger,
	clock *simclock.Clock,
	r *mrand.Rand,
	cfg *config.Config,
	reg *seed.Registry,
	selfID string,
	pbq *piggyback.Queue,
	persona NodePersona,
) {
	// 1) Allinea soglie "heavy" ai bucket (se presenti)
	cTh, mTh, gTh := cfg.EffectiveThresholds()
	affinity.SetThresholds(cTh, mTh, gTh)

	// 2) Friends & Reputation: init + decay + log periodico
	affCfg := affinity.DefaultConfig()
	affCfg.HalfLife = 60 * time.Second
	affCfg.DecayEvery = 5 * time.Second
	affCfg.WReputation, affCfg.WPiggyback, affCfg.WLeastLoad, affCfg.WPenalty = 0.5, 0.3, 0.2, 0.4
	affCfg.Epsilon, affCfg.SoftmaxTemp = 0.10, 0.20
	affCfg.StaleCutoff = 5 * time.Second

	aff := affinity.NewManager(affCfg, r, clock)
	ctxDecay, cancelDecay := context.WithCancel(context.Background())
	go aff.StartDecayLoop(ctxDecay)
	aff.StartLogLoop(ctxDecay, 12*time.Second, func(s string) { log.Infof("AFFINITY\n%s", s) })

	// 3) Stampa (una volta) la mix di generazione e le soglie
	//logWorkloadMixOnce(log, cfg, cTh, mTh, gTh)

	// 4) Generatore per-nodo e LOG della persona
	gen := newNodeJobGeneratorFromPersona(r, cfg, persona)
	log.Infof("NODE JOB GEN PERSONA → primary=%s dominance=%.2f mix={GEN=%.2f CPU=%.2f MEM=%.2f GPU=%.2f} rate=%s(×%.2f) mean_interarrival_sim_s=%.2f",
		className(persona.Primary), persona.Dominance,
		gen.classP[0], gen.classP[1], gen.classP[2], gen.classP[3],
		persona.RateLabel, persona.RateFactor, gen.meanS,
	)

	go func() {
		defer cancelDecay()
		clock.SleepSim(2 * time.Second) // lascia stabilizzare AE/SWIM

		for {
			// 1) Inter-arrivo secondo il mean del nodo (SIM)
			waitS := gen.meanS * r.ExpFloat64()
			clock.SleepSim(time.Duration(waitS * float64(time.Second)))

			// 2) Disegna job secondo classe del nodo
			job := gen.drawJob()

			// 3) Candidati
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

			ordered := rankCandidatesForJob(clock, job, peers, k, aff)
			if len(ordered) == 0 {
				ordered = samplePeers(peers, k, r)
				if len(ordered) == 0 {
					log.Warnf("COORD: nessun candidato per job=%s; ritento più tardi", job.id)
					continue
				}
			}

			// 4) Probe/Commit (+ update reputazione)
			classesForUpdate := []affinity.JobClass{affinity.PrimaryClass(job.cpu, job.mem, job.gpu)}
			bestAddr, bestID := "", ""
			bestScore := -1.0

			for _, p := range ordered {
				ok, score, reason := probeNode(clock, p.Addr, selfID, job, cfg.Scheduler.ProbeTimeoutRealMs, pbq)
				for _, cls := range classesForUpdate {
					aff.UpdateOnProbe(p.NodeId, cls, ok)
				}
				if ok && score > bestScore {
					bestScore, bestAddr, bestID = score, p.Addr, p.NodeId
				}
				if !ok {
					log.Infof("PROBE → %s refuse (reason=%s) job=%s", p.NodeId, reason, job.id)
				}
			}
			if bestAddr == "" {
				log.Infof("COORD: nessun peer ha accettato il job=%s", job.id)
				continue
			}

			if commitJob(clock, bestAddr, job, cfg.Scheduler.ProbeTimeoutRealMs, pbq) {
				for _, cls := range classesForUpdate {
					aff.UpdateOnCommit(bestID, cls, affinity.OutcomeCompleted)
				}
				log.Infof("COORD COMMIT ✓ target=%s job=%s cpu=%.1f%% mem=%.1f%% gpu=%.1f%% dur=%s",
					bestID, job.id, job.cpu, job.mem, job.gpu, job.duration)
			} else {
				for _, cls := range classesForUpdate {
					aff.UpdateOnCommit(bestID, cls, affinity.OutcomeRefused)
				}
				log.Warnf("COORD COMMIT ✖ target=%s job=%s", bestID, job.id)
			}
		}
	}()
}

func className(i int) string {
	switch i {
	case 1:
		return "CPU_ONLY"
	case 2:
		return "MEM_HEAVY"
	case 3:
		return "GPU_HEAVY"
	default:
		return "GENERAL"
	}
}

// stampa una volta la mix di generazione e soglie heavy effettive
func logWorkloadMixOnce(log *logx.Logger, cfg *config.Config, cTh, mTh, gTh float64) {
	if cfg.Workload.UseBuckets() {
		cb, mb, gb, db := cfg.Workload.CPUBuckets, cfg.Workload.MEMBuckets, cfg.Workload.GPUBuckets, cfg.Workload.DurationBuckets
		log.Infof("JOB GEN MIX (bucket/prob): CPU[s=%.0f,m=%.0f,l=%.0f | p=%.2f/%.2f/%.2f]  MEM[s=%.0f,m=%.0f,l=%.0f | p=%.2f/%.2f/%.2f]  GPU[s=%.0f,m=%.0f,l=%.0f | p=%.2f/%.2f/%.2f]  DUR[s=%.0fs,m=%.0fs,l=%.0fs | p=%.2f/%.2f/%.2f]",
			cb.SmallPct, cb.MediumPct, cb.LargePct, cb.ProbSmall, cb.ProbMedium, cb.ProbLarge,
			mb.SmallPct, mb.MediumPct, mb.LargePct, mb.ProbSmall, mb.ProbMedium, mb.ProbLarge,
			gb.SmallPct, gb.MediumPct, gb.LargePct, gb.ProbSmall, gb.ProbMedium, gb.ProbLarge,
			db.SmallSimS, db.MediumSimS, db.LargeSimS, db.ProbSmall, db.ProbMedium, db.ProbLarge,
		)
	} else {
		log.Infof("JOB GEN MIX (legacy ranges): CPU[%.0f..%.0f] MEM[%.0f..%.0f] GPU[%.0f..%.0f] DUR[%.0fs..%.0fs]",
			cfg.Workload.JobCPU.MinPct, cfg.Workload.JobCPU.MaxPct,
			cfg.Workload.JobMEM.MinPct, cfg.Workload.JobMEM.MaxPct,
			cfg.Workload.JobGPU.MinPct, cfg.Workload.JobGPU.MaxPct,
			cfg.Workload.JobDurationSimS.MinS, cfg.Workload.JobDurationSimS.MaxS,
		)
	}
	log.Infof("AFFINITY HEAVY THRESHOLDS → CPU≥%.0f%%  MEM≥%.0f%%  GPU≥%.0f%%", cTh, mTh, gTh)
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

	conn, err := grpc.DialContext(ctx, addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
		grpc.WithUnaryInterceptor(piggyback.UnaryClientInterceptor(pbq)),
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
	rep, err := cli.Probe(ctx, &proto.ProbeRequest{Job: js})
	if err != nil {
		return false, 0, "rpc_error"
	}
	return rep.WillAccept, rep.Score, rep.Reason
}

func commitJob(clock *simclock.Clock, addr string, job schedJob, timeoutMs int, pbq *piggyback.Queue) bool {
	ctx, cancel := context.WithTimeout(context.Background(), time.Duration(timeoutMs)*time.Millisecond)
	defer cancel()

	conn, err := grpc.DialContext(ctx, addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
		grpc.WithUnaryInterceptor(piggyback.UnaryClientInterceptor(pbq)),
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
func rankCandidatesForJob(
	clock *simclock.Clock,
	job schedJob,
	sampled []*proto.PeerInfo,
	topK int,
	aff *affinity.Manager,
) []*proto.PeerInfo {

	// 1) Classe primaria del job (usa le soglie già impostate all'avvio)
	class := affinity.GuessClass(job.cpu, job.mem, job.gpu)

	// 2) Log header (usa schedJob.id, NON proto.JobSpec)
	fmt.Printf("[Affinity] ranking job id=%s class=%v among %d candidates\n", job.id, class, len(sampled))

	// 3) Prepara mapping id -> PeerInfo e costruisci i Candidate per lo scoring
	id2peer := make(map[string]*proto.PeerInfo, len(sampled))
	cands := make([]affinity.Candidate, 0, len(sampled))

	for _, p := range sampled {
		if p == nil {
			continue
		}
		id2peer[p.NodeId] = p

		// Dati minimi per ora (vendor-agnostic): se non hai integrazione AE/PB qui, usa default neutri
		hasGPU := true    // se vuoi, qui puoi filtrare davvero (da AE) per ClassGPUHeavy
		advAvail := -1.0  // -1 = "non disponibile" (piggyback non consultato qui)
		fresh := true     // se usi uno staleness cutoff, metti false quando vecchio
		projected := -1.0 // -1 = ignoto (AE non consultato qui)
		penalty := 0.0    // penalità da cool-off (0..1)

		// Se esiste un cool-off globale, trasformalo in una penalità [0..1] (scala 5s → 1.0)
		if globalCoolOff != nil {
			rem := globalCoolOff.Remaining(p.Addr)
			if rem > 0 {
				sec := rem.Seconds()
				if sec < 0 {
					sec = 0
				}
				penalty = math.Min(sec/5.0, 1.0)
			}
		}

		cands = append(cands, affinity.Candidate{
			PeerID:          p.NodeId,
			HasGPU:          hasGPU,
			AdvertAvail:     advAvail,
			ProjectedLoad:   projected,
			CooldownPenalty: penalty,
			Fresh:           fresh,
		})
	}

	// 4) Ranking con Friends & Reputation (stampa breakdown P/A/L/Penalty dentro aff.Rank)
	scored := aff.Rank(class, cands, topK)

	// 5) Ricostruisci la lista di PeerInfo ordinata
	out := make([]*proto.PeerInfo, 0, len(scored))
	for _, sc := range scored {
		if pi, ok := id2peer[sc.PeerID]; ok {
			out = append(out, pi)
		}
	}
	return out
}

// AffinityInit va chiamata una sola volta all'avvio del coordinator (seed).
// Esempio: all'interno della tua Start() del coordinator, dopo aver creato il clock.
func AffinityInit(clk *simclock.Clock) {
	affOnce.Do(func() {
		cfg := affinity.DefaultConfig()
		cfg.Verbose = true // stampe dettagliate (Rep, Friends, Rank breakdown)

		affMgr = affinity.NewManager(cfg, rand.New(rand.NewSource(time.Now().UnixNano())), clk)
		affCtx, affCtxCancel = context.WithCancel(context.Background())
		affMgr.StartDecayLoop(affCtx)
		affMgr.StartLogLoop(affCtx, 15*time.Second, func(s string) {
			fmt.Printf("[Affinity/Telem]\n%s", s)
		})

		fmt.Printf("[Affinity] init done (HalfLife=%v DecayEvery=%v MaxFriends=%d)\n",
			cfg.HalfLife, cfg.DecayEvery, cfg.MaxFriendsPerClass)
	})
}

/*
// AffinityShutdown ferma i loop di decay/log quando chiudi il coordinator (opzionale).
func AffinityShutdown() {
	if affCtxCancel != nil {
		affCtxCancel()
		fmt.Printf("[Affinity] shutdown requested\n")
	}
}

// AffinityPickProbeOrder calcola l'ordine dei peer da probare per il job.
// Input: lista di candidati già arricchiti (HasGPU, Fresh, AdvertAvail, ProjectedLoad, CooldownPenalty).
// Ritorna: lista di peerID in ordine decrescente di score. Stampa automaticamente il breakdown per peer.
func AffinityPickProbeOrder(job *proto.JobSpec, cands []affinity.Candidate, topK int) []string {
	if affMgr == nil {
		fmt.Printf("[Affinity] WARN: manager nil; ritorno i candidati così come sono (%d)\n", len(cands))
		out := make([]string, 0, len(cands))
		for _, c := range cands {
			out = append(out, c.PeerID)
		}
		return out
	}

	class := affinity.PrimaryClass(float64(job.CpuPct), float64(job.MemPct), float64(job.GpuPct))
	fmt.Printf("[Affinity] ranking job id=%s class=%v among %d candidates\n", job.Id, class, len(cands))

	top := affMgr.Rank(class, cands, topK)

	// Converte in sola lista di PeerID per il probe loop.
	order := make([]string, 0, len(top))
	for _, sc := range top {
		order = append(order, sc.PeerID)
	}
	return order
}

// AffinityOnProbe va chiamata dopo la risposta di Probe (accepted/refused).
func AffinityOnProbe(peer string, job *proto.JobSpec, accepted bool) {
	if affMgr == nil || job == nil {
		return
	}
	class := affinity.PrimaryClass(float64(job.CpuPct), float64(job.MemPct), float64(job.GpuPct))
	affMgr.UpdateOnProbe(peer, class, accepted)
}

// AffinityOnCommit va chiamata all'esito del commit (completed/refused/timeout/cancelled).
func AffinityOnCommit(peer string, job *proto.JobSpec, outcome affinity.Outcome) {
	if affMgr == nil || job == nil {
		return
	}
	class := affinity.PrimaryClass(float64(job.CpuPct), float64(job.MemPct), float64(job.GpuPct))
	affMgr.UpdateOnCommit(peer, class, outcome)
}
*/

// ======== COOL-OFF REGISTRY ========
// Mappa per indirizzo -> scadenza cool-off

type coolOffRegistry struct {
	mu            sync.Mutex
	byAddr        map[string]time.Time
	afterCommit   time.Duration // quando un COMMIT va a buon fine
	afterRefusal  time.Duration // quando un probe/commit viene rifiutato
	afterRPCError time.Duration // quando fallisce dial/RPC/timeout
}

var globalCoolOff *coolOffRegistry

func newCoolOffRegistry(afterCommit, afterRefusal, afterRPCError time.Duration) *coolOffRegistry {
	return &coolOffRegistry{
		byAddr:        make(map[string]time.Time),
		afterCommit:   afterCommit,
		afterRefusal:  afterRefusal,
		afterRPCError: afterRPCError,
	}
}

func (cr *coolOffRegistry) shouldSkip(addr string, now time.Time) (skip bool, remain time.Duration) {
	cr.mu.Lock()
	defer cr.mu.Unlock()
	until, ok := cr.byAddr[addr]
	if !ok {
		return false, 0
	}
	if now.Before(until) {
		return true, until.Sub(now)
	}
	// scaduto → rimuovo
	delete(cr.byAddr, addr)
	return false, 0
}

func (cr *coolOffRegistry) markFor(addr string, dur time.Duration) {
	if dur <= 0 {
		return
	}
	cr.mu.Lock()
	cr.byAddr[addr] = time.Now().Add(dur)
	cr.mu.Unlock()
}

func (cr *coolOffRegistry) OnCommit(addr string)   { cr.markFor(addr, cr.afterCommit) }
func (cr *coolOffRegistry) OnRefusal(addr string)  { cr.markFor(addr, cr.afterRefusal) }
func (cr *coolOffRegistry) OnRPCError(addr string) { cr.markFor(addr, cr.afterRPCError) }

func (cr *coolOffRegistry) Remaining(addr string) time.Duration {
	cr.mu.Lock()
	defer cr.mu.Unlock()
	if t, ok := cr.byAddr[addr]; ok {
		if d := time.Until(t); d > 0 {
			return d
		}
		delete(cr.byAddr, addr)
	}
	return 0
}

// ======== COOL-OFF HELPERS ========
// Helper senza toccare le firme delle funzioni esistenti.

func initCoolOffIfNeeded() {
	if globalCoolOff == nil {
		// Valori di default "sicuri". Se vuoi, possiamo scalarli con il time_scale.
		globalCoolOff = newCoolOffRegistry(
			1500*time.Millisecond, // dopo COMMIT
			800*time.Millisecond,  // dopo REFUSAL
			1200*time.Millisecond, // dopo RPC ERROR / TIMEOUT
		)
	}
}

func CoolOffShouldSkip(addr string, log *logx.Logger) bool {
	initCoolOffIfNeeded()
	if skip, remain := globalCoolOff.shouldSkip(addr, time.Now()); skip {
		log.Infof("COORD COOL-OFF → skip addr=%s remain=%s", addr, remain.Truncate(100*time.Millisecond))
		return true
	}
	return false
}

func CoolOffOnCommit(addr string, log *logx.Logger) {
	initCoolOffIfNeeded()
	globalCoolOff.OnCommit(addr)
	rem := globalCoolOff.Remaining(addr)
	log.Infof("COORD COOL-OFF ← set after COMMIT addr=%s cooloff=%s", addr, rem.Truncate(100*time.Millisecond))
}

func CoolOffOnRefusal(addr string, log *logx.Logger) {
	initCoolOffIfNeeded()
	globalCoolOff.OnRefusal(addr)
	rem := globalCoolOff.Remaining(addr)
	log.Infof("COORD COOL-OFF ← set after REFUSAL addr=%s cooloff=%s", addr, rem.Truncate(100*time.Millisecond))
}

func CoolOffOnRPCError(addr string, log *logx.Logger) {
	initCoolOffIfNeeded()
	globalCoolOff.OnRPCError(addr)
	rem := globalCoolOff.Remaining(addr)
	log.Infof("COORD COOL-OFF ← set after RPC-ERROR addr=%s cooloff=%s", addr, rem.Truncate(100*time.Millisecond))
}
