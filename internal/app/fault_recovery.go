package app

import (
	mrand "math/rand"

	"GossipSystemUtilization/internal/faults"
	"GossipSystemUtilization/internal/logx"
	"GossipSystemUtilization/internal/simclock"
)

// Parametri della fault-sim come da config (*bool per rispettare i default)
type FaultProfileInput struct {
	Enabled               *bool
	PrintTransitions      *bool
	FrequencyClassWeights map[string]float64
	FrequencyPerMinSim    map[string]float64
	DurationClassWeights  map[string]float64
	DurationMeanSimS      map[string]float64
}

// Variante “low-ceremony”: prende direttamente la Runtime e crea gli hook.
func StartFaultRecoveryWithRuntime(
	log *logx.Logger,
	clock *simclock.Clock,
	r *mrand.Rand,
	in FaultProfileInput,
	rt *Runtime,
) {
	hooks := faults.Hooks{
		OnDown: func() {
			log.Warnf("FAULT ↓ CRASH — stop gRPC + SWIM + AntiEntropy + Reporter")
			rt.StopAll()
		},
		OnUp: func() {
			log.Warnf("FAULT ↑ RECOVERY — restart SWIM + AntiEntropy + Reporter + gRPC")
			rt.RecoverAll()
		},
	}
	StartFaultRecovery(log, clock, r, in, hooks)
}

// Versione “generica” usata internamente, ma disponibile se servisse.
func StartFaultRecovery(
	log *logx.Logger,
	clock *simclock.Clock,
	r *mrand.Rand,
	in FaultProfileInput,
	hooks faults.Hooks,
) {
	boolv := func(p *bool, def bool) bool {
		if p != nil {
			return *p
		}
		return def
	}

	fdef := struct {
		Enabled               bool
		PrintTransitions      bool
		FrequencyClassWeights map[string]float64
		FrequencyPerMinSim    map[string]float64
		DurationClassWeights  map[string]float64
		DurationMeanSimS      map[string]float64
	}{
		Enabled:               boolv(in.Enabled, false),
		PrintTransitions:      boolv(in.PrintTransitions, false),
		FrequencyClassWeights: in.FrequencyClassWeights,
		FrequencyPerMinSim:    in.FrequencyPerMinSim,
		DurationClassWeights:  in.DurationClassWeights,
		DurationMeanSimS:      in.DurationMeanSimS,
	}

	if !fdef.Enabled {
		return
	}

	_, fsim := faults.InitSimWithProfile(log, clock, r, fdef, hooks)
	fsim.Start()
}
