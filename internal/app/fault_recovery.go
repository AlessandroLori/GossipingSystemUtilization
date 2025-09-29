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

// Avvio fault-sim con accesso al Runtime (stop/recover servizi)
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
			// nodeUp=false viene impostato da StopAll()
			rt.StopAll()
		},
		OnUp: func() {
			log.Warnf("FAULT ↑ RECOVERY — restart SWIM + AntiEntropy + Reporter + gRPC")
			// nodeUp=true viene impostato da RecoverAll()
			rt.RecoverAll()

			// Annuncio immediato di recovery: pulisci leave e forza self-advert 'clean'
			if rt != nil && rt.PBQueue != nil {
				rt.PBQueue.SetLeaveFor(0)
				// Usa l'helper già definito in leave_sim.go
				forceSelfAdvertLeave(rt, rt.Clock, rt.ID, 0)
			}

			// Riallinea membership se necessario (utile sui peer non-seed)
			rt.TryJoinIfNeeded()
		},
	}
	StartFaultRecovery(log, clock, r, in, hooks)
}

// Versione generica riutilizzabile con Hooks esterni
func StartFaultRecovery(
	log *logx.Logger,
	clock *simclock.Clock,
	r *mrand.Rand,
	in FaultProfileInput,
	hooks faults.Hooks,
) {
	// utility per i default dei puntatori bool
	boolv := func(p *bool, def bool) bool {
		if p != nil {
			return *p
		}
		return def
	}

	// Mappa input → profilo automatico del package faults
	fdef := faults.AutoProfileDef{
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
