package faults

import (
	"math"
	"math/rand"
	"time"

	"GossipSystemUtilization/internal/logx"
	"GossipSystemUtilization/internal/simclock"
)

// Parametri per la simulazione fault (blocco "faults" nel config.json).
type Params struct {
	Enabled          bool
	FailureProb      float64 // prob. di essere DOWN allo start
	MeanUpSimS       float64 // uptime medio (sec SIM)
	MeanDownSimS     float64 // downtime medio (sec SIM)
	FlapProb         float64 // prob. di flip immediato (flapping)
	PrintTransitions bool
}

// Hook opzionali da collegare dal main.
type Hooks struct {
	OnDown func() // es. sospendere risposte RPC
	OnUp   func() // es. riprendere risposte RPC
}

type Sim struct {
	log   *logx.Logger
	clock *simclock.Clock
	par   Params
	hooks Hooks
	rnd   *rand.Rand

	stop chan struct{}
}

func NewSim(log *logx.Logger, clock *simclock.Clock, r *rand.Rand, par Params, hooks Hooks) *Sim {
	if r == nil {
		r = rand.New(rand.NewSource(time.Now().UnixNano()))
	}
	return &Sim{log: log, clock: clock, par: par, hooks: hooks, rnd: r, stop: make(chan struct{})}
}

func (s *Sim) Start() {
	if !s.par.Enabled {
		s.log.Warnf("FAULTS: disabilitato da config")
		return
	}
	go s.loop()
}

func (s *Sim) Stop() { close(s.stop) }

func (s *Sim) loop() {
	down := s.rnd.Float64() < s.par.FailureProb
	s.apply(down)

	for {
		mean := s.par.MeanUpSimS
		if down {
			mean = s.par.MeanDownSimS
		}
		wait := exp(s.rnd, 1.0/math.Max(mean, 0.001))
		select {
		case <-s.stop:
			return
		default:
			s.clock.SleepSim(time.Duration(wait * float64(time.Second)))
		}
		down = !down
		if s.rnd.Float64() < s.par.FlapProb {
			down = !down
		}
		s.apply(down)
	}
}

func (s *Sim) apply(down bool) {
	if down {
		if s.par.PrintTransitions {
			s.log.Warnf("FAULTS → DOWN")
		}
		if s.hooks.OnDown != nil {
			s.hooks.OnDown()
		}
	} else {
		if s.par.PrintTransitions {
			s.log.Infof("FAULTS → UP")
		}
		if s.hooks.OnUp != nil {
			s.hooks.OnUp()
		}
	}
}

func exp(r *rand.Rand, lambda float64) float64 {
	u := r.Float64()
	if u <= 0 {
		u = 1e-9
	}
	return -math.Log(u) / lambda
}
