package antientropy

import (
	"fmt"
	"sort"
	"strings"
	"sync"
	"time"

	"GossipSystemUtilization/internal/logx"
	"GossipSystemUtilization/internal/simclock"
	proto "GossipSystemUtilization/proto"
)

type ReporterConfig struct {
	PeriodSimS float64
	TopK       int
}

type Reporter struct {
	log         *logx.Logger
	clk         *simclock.Clock
	store       *Store
	selfSampler func() *proto.Stats

	period time.Duration
	topK   int

	stopCh   chan struct{}
	stopOnce sync.Once
}

func NewReporter(log *logx.Logger, clk *simclock.Clock, store *Store, selfSampler func() *proto.Stats, cfg ReporterConfig) *Reporter {
	period := time.Duration(cfg.PeriodSimS * float64(time.Second))
	if period <= 0 {
		period = 10 * time.Second
	}
	topK := cfg.TopK
	if topK <= 0 {
		topK = 3
	}
	return &Reporter{
		log:         log,
		clk:         clk,
		store:       store,
		selfSampler: selfSampler,
		period:      period,
		topK:        topK,
		stopCh:      make(chan struct{}),
	}
}

func (r *Reporter) Start() {
	// re-init per riavvii post leave/recovery
	r.stopOnce = sync.Once{}
	r.stopCh = make(chan struct{})
	go r.loop()
}

func (r *Reporter) Stop() {
	r.stopOnce.Do(func() {
		if r.stopCh != nil {
			close(r.stopCh)
		}
		if r.log != nil {
			r.log.Warnf("Reporter stopped")
		}
	})
}

func (r *Reporter) loop() {
	for {
		// Uscita immediata se stoppato
		select {
		case <-r.stopCh:
			return
		default:
		}

		// Attendi il periodo simulato
		r.clk.SleepSim(r.period)

		// Ricontrolla stop prima di stampare
		select {
		case <-r.stopCh:
			return
		default:
			r.printSummary()
		}
	}
}

func (r *Reporter) printSummary() {
	var self *proto.Stats
	if r.selfSampler != nil {
		self = r.selfSampler()
	}
	stats := r.store.SnapshotSample(10000, self)
	if len(stats) == 0 {
		return
	}

	var (
		sumCPU, sumMEM, sumGPU          float64
		nCPU, nMEM, nGPU                int
		minCPUPct, minMEMPct, minGPUPct = 1e9, 1e9, 1e9
		//minCPUID, minMEMID, minGPUID    string
	)

	for _, s := range stats {
		sumCPU += s.CpuPct
		sumMEM += s.MemPct
		nCPU++
		nMEM++
		if s.CpuPct < minCPUPct {
			minCPUPct = s.CpuPct
		}
		if s.MemPct < minMEMPct {
			minMEMPct = s.MemPct
		}
		if s.GpuPct >= 0 {
			sumGPU += s.GpuPct
			nGPU++
			if s.GpuPct < minGPUPct {
				minGPUPct = s.GpuPct
			}
		}
	}
	/*

		CLUSTER

		avgCPU := sumCPU / float64(nCPU)
		avgMEM := sumMEM / float64(nMEM)


		if nGPU > 0 {
			avgGPU := sumGPU / float64(nGPU)
			r.log.Infof("CLUSTER SUMMARY → nodes=%d cpu_avg=%.1f%% mem_avg=%.1f%% gpu_avg=%.1f%% (gpu_nodes=%d) minCPU=%s@%.1f%% minMEM=%s@%.1f%% minGPU=%s@%.1f%%",
				len(stats), avgCPU, avgMEM, avgGPU, nGPU,
				minCPUID, minCPUPct, minMEMID, minMEMPct, minGPUID, minGPUPct)
		} else {
			r.log.Infof("CLUSTER SUMMARY → nodes=%d cpu_avg=%.1f%% mem_avg=%.1f%% (no GPU nodes) minCPU=%s@%.1f%% minMEM=%s@%.1f%%",
				len(stats), avgCPU, avgMEM, minCPUID, minCPUPct, minMEMID, minMEMPct)
		}
	*/

	type kv struct {
		id string
		v  float64
	}
	var all []kv
	for _, s := range stats {
		all = append(all, kv{id: s.NodeId, v: s.CpuPct})
	}
	sort.Slice(all, func(i, j int) bool { return all[i].v < all[j].v })
	k := r.topK
	if k > len(all) {
		k = len(all)
	}
	top := make([]string, 0, k)
	for i := 0; i < k; i++ {
		top = append(top, fmt.Sprintf("%s (%.1f%% CPU)", all[i].id, all[i].v))
	}
	if len(top) > 0 {
		r.log.Infof("TOP%d least CPU → %s", k, strings.Join(top, ", "))
	}
}

func sprintf(format string, a ...any) string { return fmt.Sprintf(format, a...) }
