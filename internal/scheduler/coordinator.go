package scheduler

import (
	"sort"
	"time"

	"GossipSystemUtilization/internal/jobs"
	"GossipSystemUtilization/internal/logx"
	"GossipSystemUtilization/internal/simclock"
	"GossipSystemUtilization/proto"
)

// Forniremo qui engine.LocalSample dal main.
type CandidateSampler func(max int) []*proto.Stats

// Applica il job localmente (es. node.StartJobLoad). true se accettato.
type LocalCommitFn func(job jobs.Spec) bool

// Parametri dello scheduler (blocco "scheduler" nel config.json).
type Params struct {
	Mode             string  // "local" (applica qui) oppure "advisory" (solo log)
	TopK             int     // quanti candidati a minor CPU considerare
	OverprovisionPct float64 // margine di sicurezza (es 5 = 5%)
	PrintDecisions   bool
}

type Coordinator struct {
	log     *logx.Logger
	clock   *simclock.Clock
	par     Params
	sampler CandidateSampler
	local   LocalCommitFn
}

func NewCoordinator(
	log *logx.Logger, clock *simclock.Clock, par Params,
	sampler CandidateSampler, local LocalCommitFn,
) *Coordinator {
	if par.TopK <= 0 {
		par.TopK = 3
	}
	return &Coordinator{log: log, clock: clock, par: par, sampler: sampler, local: local}
}

func (c *Coordinator) OnJob(j jobs.Spec) {
	stats := c.sampler(16)
	if len(stats) == 0 {
		c.log.Warnf("COORD: nessun candidato; drop %s", j.Pretty())
		return
	}
	sort.Slice(stats, func(i, k int) bool { return stats[i].CpuPct < stats[k].CpuPct })
	best := stats[0]
	accept := c.checkHeadroom(best, j)

	switch c.par.Mode {
	case "local":
		if c.local == nil {
			c.log.Warnf("COORD(local): LocalCommitFn assente; drop %s", j.Pretty())
			return
		}
		ok := false
		if accept {
			ok = c.local(j)
		}
		if c.par.PrintDecisions {
			if ok {
				c.log.Infof("COORD(local) DECISION → run %s HERE (best=%s cpu=%.1f%%)", j.Pretty(), best.NodeId, best.CpuPct)
			} else {
				c.log.Warnf("COORD(local) REJECTED → headroom insufficiente per %s", j.Pretty())
			}
		}
	default: // "advisory"
		if c.par.PrintDecisions {
			if accept {
				c.log.Infof("COORD(advisory) SUGGEST → run %s on %s (cpu=%.1f%%)", j.Pretty(), best.NodeId, best.CpuPct)
			} else {
				c.log.Warnf("COORD(advisory) NO-FIT → best=%s non ha headroom per %s", best.NodeId, j.Pretty())
			}
		}
	}
}

func (c *Coordinator) checkHeadroom(s *proto.Stats, j jobs.Spec) bool {
	safety := c.par.OverprovisionPct / 100.0
	headCPU := 100 - s.CpuPct
	headMEM := 100 - s.MemPct
	headGPU := 100.0
	if s.GpuPct < 0 {
		if j.GPU > 0 {
			return false
		}
		headGPU = -1
	} else {
		headGPU = 100 - s.GpuPct
	}

	if j.CPU > 0 && j.CPU > headCPU*(1.0-safety) {
		return false
	}
	if j.MEM > 0 && j.MEM > headMEM*(1.0-safety) {
		return false
	}
	if j.GPU > 0 && headGPU >= 0 && j.GPU > headGPU*(1.0-safety) {
		return false
	}
	return true
}

// utility locale (se mai servisse)
func d(sec float64) time.Duration { return time.Duration(sec * float64(time.Second)) }
