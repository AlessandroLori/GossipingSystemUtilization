// internal/app/app.go

package app

import (
	"fmt"
	"os"
	"path/filepath"
	"time"

	"math/rand"

	"google.golang.org/grpc"

	"GossipSystemUtilization/internal/antientropy"
	"GossipSystemUtilization/internal/config"
	"GossipSystemUtilization/internal/grpcserver"
	"GossipSystemUtilization/internal/logx"
	"GossipSystemUtilization/internal/node"
	"GossipSystemUtilization/internal/piggyback"
	"GossipSystemUtilization/internal/seed"
	"GossipSystemUtilization/internal/simclock"
	"GossipSystemUtilization/internal/swim"

	proto "GossipSystemUtilization/proto"
)

// App incapsula i componenti principali.
type App struct {
	ID     string
	Addr   string
	IsSeed bool

	Clock *simclock.Clock
	Log   *logx.Logger
	Rng   *rand.Rand

	Node     *node.Node
	SwimMgr  *swim.Manager
	AEStore  *antientropy.Store
	AEEng    *antientropy.Engine
	Reporter *antientropy.Reporter

	// dal grpcserver.Start
	grpcSrv *grpc.Server
	reg     *seed.Registry

	// piggyback queue locale
	PBQ *piggyback.Queue

	Cfg *config.Config
}

// Init prepara tutti i componenti (non blocca).
func (a *App) Init() error {
	// Nodo
	a.Node = node.New(a.ID, a.Addr, a.Clock, a.Log, a.Rng)

	// SWIM (con gate IsNodeUp)
	swimCfg := swim.Config{
		PeriodSimS:        1.0,
		TimeoutRealMs:     250,
		IndirectK:         3,
		SuspicionTimeoutS: 6.0,
	}
	a.SwimMgr = swim.NewManager(a.ID, a.Addr, a.Log, a.Clock, a.Rng, swimCfg, IsNodeUp)
	a.SwimMgr.Start()

	// Anti-entropy
	a.AEStore = antientropy.NewStore(a.Log, a.Clock)
	selfSampler := func() *proto.Stats {
		s := a.Node.CurrentStatsProto()
		s.TsMs = a.Clock.NowSimMs()
		return s
	}

	// Piggyback queue (TTL ~110s, cap 200) — inizializzata PRIMA dell'engine
	a.PBQ = piggyback.NewQueue(a.Log, a.Clock, 200, 110*time.Second)

	aeCfg := antientropy.Config{
		PeriodSimS: 3.0,
		Fanout:     2,
		SampleSize: 8,
		TtlSimS:    12.0,
	}
	a.AEEng = antientropy.NewEngine(a.Log, a.Clock, a.Rng, a.AEStore, a.SwimMgr, a.PBQ, selfSampler, aeCfg)
	a.AEEng.Start()

	// Reporter
	repCfg := antientropy.ReporterConfig{PeriodSimS: 10.0, TopK: 3}
	a.Reporter = antientropy.NewReporter(a.Log, a.Clock, a.AEStore, selfSampler, repCfg)
	a.Reporter.Start()

	// Semina lo store con le stats locali
	a.AEStore.UpsertBatch([]*proto.Stats{selfSampler()})

	// Aggiornamento periodico dello "self advert" in PBQ
	go func() {
		for {
			s := a.Node.CurrentStatsProto()
			s.TsMs = a.Clock.NowSimMs()
			a.PBQ.UpsertSelfFromStats(s)
			a.Clock.SleepSim(2 * time.Second)
		}
	}()

	// gRPC server
	var err error
	statsSampler := func(max int) []*proto.Stats {
		return a.AEEng.LocalSample(max)
	}

	_, _, a.reg, err = grpcserver.Start(
		a.IsSeed,
		a.Addr,
		a.Log,
		a.Clock,
		a.SwimMgr,
		a.ID,
		seed.Sampler(statsSampler), // <-- tipo corretto
		func() *proto.Stats {
			s := a.Node.CurrentStatsProto()
			s.TsMs = a.Clock.NowSimMs()
			return s
		},
		func(jobID string, cpu, mem, gpu float64, durMs int64) bool {
			return a.Node.StartJobLoad(jobID, cpu, mem, gpu, time.Duration(durMs)*time.Millisecond)
		},
		func(jobID string) bool {
			return a.Node.CancelJob(jobID)
		},
		a.Rng,
		a.PBQ,
		IsNodeUp, // <-- gate per rifiutare RPC durante la leave
	)
	if err != nil {
		return fmt.Errorf("start gRPC server: %w", err)
	}

	return nil
}

// StartTTFDTracker: scoperta basata su Stats/AE (quando arriva una Stats con NodeId nuovo)
func StartTTFDTracker(
	log *logx.Logger,
	clock *simclock.Clock,
	sampler func(max int) []*proto.Stats,
	selfID string,
	expected int,
) {
	periodMs := 200
	if s := os.Getenv("TTFD_PERIOD_MS"); s != "" {
		if v, err := strconvAtoiSafe(s); err == nil && v > 0 {
			periodMs = v
		}
	}
	csvPath := os.Getenv("TTFD_CSV")
	if csvPath == "" {
		csvPath = fmt.Sprintf("./out/ttfd-%s.csv", selfID)
	}

	go func() {
		// protezione da panics: non uccidere il processo
		defer func() {
			if r := recover(); r != nil {
				log.Warnf("TTFD: recovered from panic: %v", r)
			}
		}()

		t0ms := clock.NowSimMs()

		// assicura la cartella
		dir := filepath.Dir(csvPath)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			log.Warnf("TTFD: mkdir %s fallita: %v", dir, err)
		}

		// CSV
		var f *os.File
		if ff, err := os.Create(csvPath); err == nil {
			f = ff
			fmt.Fprintln(f, "ms,discovered,discovered_others")
			defer f.Close()
		} else {
			log.Warnf("TTFD: impossibile creare CSV %s: %v", csvPath, err)
		}

		seen := make(map[string]struct{})
		seen[selfID] = struct{}{} // includi self
		targetOthers := expected - 1
		if targetOthers < 0 {
			targetOthers = 0
		}

		ticker := time.NewTicker(time.Duration(periodMs) * time.Millisecond)
		defer ticker.Stop()

		for {
			<-ticker.C

			// Stats / Anti-Entropy
			if sampler != nil {
				for _, s := range sampler(512) {
					if id := s.GetNodeId(); id != "" {
						seen[id] = struct{}{}
					}
				}
			}

			n := len(seen)  // include self
			others := n - 1 // esclude self
			if others < 0 {
				others = 0
			}
			elapsed := clock.NowSimMs() - t0ms

			if f != nil {
				fmt.Fprintf(f, "%d,%d,%d\n", elapsed, n, others)
			}
			log.Infof("TTFD/PROGRESS discovered=%d/%d elapsed_ms=%d", others, targetOthers, elapsed)

			if n >= expected {
				log.Infof("TTFD/DONE expected=%d elapsed_ms=%d path=%s", expected, elapsed, csvPath)
				return
			}
		}
	}()
}

// helper atoi "tollerante"
func strconvAtoiSafe(s string) (int, error) {
	var n int
	sign := 1
	for i, r := range s {
		if i == 0 && (r == '-' || r == '+') {
			if r == '-' {
				sign = -1
			}
			continue
		}
		if r < '0' || r > '9' {
			return 0, fmt.Errorf("not a number")
		}
		n = n*10 + int(r-'0')
	}
	return sign * n, nil
}

// StartFirstContactDiscoveryTracker misura la scoperta al PRIMO CONTATTO:
// un nodo è "scoperto" se compare in SWIM (membership) OPPURE arriva una Stats per lui.
// Scrive CSV: ms,discovered,discovered_others
func StartFirstContactDiscoveryTracker(
	log *logx.Logger,
	clock *simclock.Clock,
	listIDs func() []string, // membership SWIM (AlivePeers -> IDs)
	sampler func(max int) []*proto.Stats, // campione Stats/AE
	selfID string,
	expected int,
) {
	periodMs := 200
	if s := os.Getenv("FC_PERIOD_MS"); s != "" {
		if v, err := strconvAtoiSafe(s); err == nil && v > 0 {
			periodMs = v
		}
	}
	csvPath := os.Getenv("FC_CSV")
	if csvPath == "" {
		csvPath = fmt.Sprintf("./out/fc-ttfd-%s.csv", selfID)
	}

	go func() {
		// protezione da panics: non uccidere il processo
		defer func() {
			if r := recover(); r != nil {
				log.Warnf("FC-TTFD: recovered from panic: %v", r)
			}
		}()

		t0ms := clock.NowSimMs()

		// assicura la cartella
		dir := filepath.Dir(csvPath)
		if err := os.MkdirAll(dir, 0o755); err != nil {
			log.Warnf("FC-TTFD: mkdir %s fallita: %v", dir, err)
		}

		// CSV
		var f *os.File
		if ff, err := os.Create(csvPath); err == nil {
			f = ff
			fmt.Fprintln(f, "ms,discovered,discovered_others")
			defer f.Close()
		} else {
			log.Warnf("FC-TTFD: impossibile creare CSV %s: %v", csvPath, err)
		}

		seen := make(map[string]struct{})
		seen[selfID] = struct{}{}
		targetOthers := expected - 1
		if targetOthers < 0 {
			targetOthers = 0
		}

		ticker := time.NewTicker(time.Duration(periodMs) * time.Millisecond)
		defer ticker.Stop()

		for {
			<-ticker.C

			// 1) SWIM membership (nil-safe)
			if listIDs != nil {
				for _, id := range listIDs() {
					if id != "" {
						seen[id] = struct{}{}
					}
				}
			}

			// 2) Stats / Anti-Entropy (nil-safe)
			if sampler != nil {
				for _, s := range sampler(512) {
					if id := s.GetNodeId(); id != "" {
						seen[id] = struct{}{}
					}
				}
			}

			n := len(seen)
			others := n - 1
			if others < 0 {
				others = 0
			}
			elapsed := clock.NowSimMs() - t0ms

			if f != nil {
				// ms, discovered (incl. self), discovered_others
				fmt.Fprintf(f, "%d,%d,%d\n", elapsed, n, others)
			}
			log.Infof("FC-TTFD/PROGRESS discovered=%d/%d elapsed_ms=%d", others, targetOthers, elapsed)

			if n >= expected {
				log.Infof("FC-TTFD/DONE expected=%d elapsed_ms=%d path=%s", expected, elapsed, csvPath)
				return
			}
		}
	}()
}
