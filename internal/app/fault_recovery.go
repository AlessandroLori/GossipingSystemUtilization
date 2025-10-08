package app

import (
	"math/rand"
	mrand "math/rand"
	"time"

	"GossipSystemUtilization/internal/faults"
	"GossipSystemUtilization/internal/logx"
	"GossipSystemUtilization/internal/simclock"
)

// Parametri della fault-sim come da config
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
	r *rand.Rand,
	in FaultProfileInput,
	rt *Runtime,
) {
	if rt == nil || clock == nil || log == nil {
		return
	}
	if in.Enabled == nil || !*in.Enabled {
		return
	}
	printTrans := (in.PrintTransitions != nil && *in.PrintTransitions)

	log.Infof("FAULT PROFILE → freq_weights=%v freq_rate_per_min=%v dur_weights=%v dur_mean_s=%v",
		in.FrequencyClassWeights, in.FrequencyPerMinSim, in.DurationClassWeights, in.DurationMeanSimS)

	go func() {
		for {
			// 1) attesa simulata prima del fault
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

			// 2) durata fault
			durClass := pickKeyWeighted(r, in.DurationClassWeights)
			meanS := in.DurationMeanSimS[durClass]
			if meanS <= 0 {
				meanS = 8
			}
			durS := meanS * (0.7 + 0.6*r.Float64())
			dur := time.Duration(durS * float64(time.Second))

			// === CRASH (stop immediato rete) ===
			nodeUp.Store(false)
			if printTrans {
				log.Warnf("FAULTS → DOWN")
				log.Warnf("FAULT ↓ CRASH — stop gRPC + SWIM + AntiEntropy + Reporter")
			}

			// blocco piggyback e servizi
			if rt.PBQueue != nil {
				rt.PBQueue.SetSendEnabled(false)
				rt.PBQueue.SetRecvEnabled(false)
				rt.PBQueue.Pause()
			}
			rt.StopAll() // spegne SWIM/AE/Reporter/gRPC

			// attesa crash
			clock.SleepSim(dur)

			// === Recovery ===
			if printTrans {
				log.Warnf("FAULTS → UP")
				log.Warnf("FAULT ↑ RECOVERY — restart SWIM + AntiEntropy + Reporter + gRPC")
			}
			// riattiva piggyback prima di far ripartire i servizi
			if rt.PBQueue != nil {
				rt.PBQueue.Resume()
				rt.PBQueue.SetSendEnabled(true)
				rt.PBQueue.SetRecvEnabled(true)
			}
			rt.RecoverAll()
		}
	}()
}

// Versione generica per Hooks esterni
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
