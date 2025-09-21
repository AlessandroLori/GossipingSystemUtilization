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
	// Estrai stato iniziale
	down := s.rnd.Float64() < s.par.FailureProb

	// Non generare un "UP" spurio alla partenza.
	if down {
		s.apply(true)
	}
	prev := down

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

		// toggle + eventuale flapping
		down = !down
		if s.rnd.Float64() < s.par.FlapProb {
			down = !down
		}

		// notifica solo su transizione reale
		if down != prev {
			s.apply(down)
			prev = down
		}
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
	if lambda <= 1e-9 {
		lambda = 1e-9
	}
	return -math.Log(u) / lambda
}

// ==== PROFILI FAULT PER-NODO (estrazione automatica classe freq/dur) ====

type NodeFaultProfile struct {
	// etichette estratte (utili per log/debug)
	FreqClass string // "high" | "medium" | "low" | "none"
	DurClass  string // "grave" | "medium" | "small"

	// parametri numerici risultanti
	CrashProbPerMinSim float64 // frequenza guasti del nodo (per minuto simulato)
	DownTimeMeanSimS   float64 // durata media di ciascun guasto (secondi simulati)
}

// [NUOVO] AutoProfileDef: profilo “a buckets” (nuovo JSON o compat legacy)
type AutoProfileDef struct {
	Enabled               bool
	PrintTransitions      bool
	FrequencyClassWeights map[string]float64 // none/low/medium/high → peso
	FrequencyPerMinSim    map[string]float64 // none/low/medium/high → λ crash/min
	DurationClassWeights  map[string]float64 // small/medium/grave → peso
	DurationMeanSimS      map[string]float64 // small/medium/grave → mean down (s)
}

// [NUOVO] default robusti
func DefaultAutoProfile(printTransitions bool) AutoProfileDef {
	return AutoProfileDef{
		Enabled:          true,
		PrintTransitions: printTransitions,
		FrequencyClassWeights: map[string]float64{
			"high":   0.15,
			"medium": 0.35,
			"low":    0.40,
			"none":   0.10,
		},
		FrequencyPerMinSim: map[string]float64{
			"high":   1.2,
			"medium": 0.4,
			"low":    0.1,
			"none":   0.0,
		},
		DurationClassWeights: map[string]float64{
			"grave":  0.15,
			"medium": 0.50,
			"small":  0.35,
		},
		DurationMeanSimS: map[string]float64{
			"grave":  60.0,
			"medium": 20.0,
			"small":  5.0,
		},
	}
}

// pickWeighted: estrae una chiave da una mappa pesata {classe: peso}
func pickWeighted(m map[string]float64, r *rand.Rand) string {
	total := 0.0
	for _, w := range m {
		if w > 0 {
			total += w
		}
	}
	if total <= 0 {
		// fallback deterministico
		for k := range m {
			return k
		}
		return ""
	}
	x := r.Float64() * total
	acc := 0.0
	for k, w := range m {
		if w <= 0 {
			continue
		}
		acc += w
		if x <= acc {
			return k
		}
	}
	// fallback: restituisci la prima chiave
	for k := range m {
		return k
	}
	return ""
}

func safeGetFloat(m map[string]float64, key string) float64 {
	if m == nil {
		return 0
	}
	if v, ok := m[key]; ok {
		return v
	}
	return 0
}

// DrawNodeFaultProfile: dato l'AutoProfileDef, estrae classi freq/dur e produce il profilo numerico del nodo
func DrawNodeFaultProfile(def AutoProfileDef, r *rand.Rand) NodeFaultProfile {
	if r == nil {
		r = rand.New(rand.NewSource(time.Now().UnixNano()))
	}
	freqClass := pickWeighted(def.FrequencyClassWeights, r) // "high" | "medium" | "low" | "none"
	durClass := pickWeighted(def.DurationClassWeights, r)   // "grave" | "medium" | "small"

	return NodeFaultProfile{
		FreqClass:          freqClass,
		DurClass:           durClass,
		CrashProbPerMinSim: safeGetFloat(def.FrequencyPerMinSim, freqClass),
		DownTimeMeanSimS:   safeGetFloat(def.DurationMeanSimS, durClass),
	}
}

// BuildParamsFromProfile: traduce il profilo in Params per la Sim
func BuildParamsFromProfile(profile NodeFaultProfile, printTransitions bool) Params {
	// modello alternanza UP/DOWN a esponenziale:
	// MeanUpSimS = 60 / p  (se p>0), altrimenti molto grande
	meanUp := 1.0e9
	if profile.CrashProbPerMinSim > 0 {
		meanUp = 60.0 / profile.CrashProbPerMinSim
	}
	downMean := profile.DownTimeMeanSimS
	if downMean <= 0 {
		downMean = 1 // evita 0 per tempi di attesa
	}
	return Params{
		Enabled:          true,
		FailureProb:      0.0, // non partiamo già down (puoi cambiarlo se vuoi)
		MeanUpSimS:       meanUp,
		MeanDownSimS:     downMean,
		FlapProb:         0.0,
		PrintTransitions: printTransitions,
	}
}

// InitSimWithProfile: funzione “one-shot” per disegnare il profilo e creare la Sim pronta a partire
func InitSimWithProfile(
	log *logx.Logger,
	clock *simclock.Clock,
	r *rand.Rand,
	def AutoProfileDef,
	hooks Hooks,
) (NodeFaultProfile, *Sim) {
	if r == nil {
		r = rand.New(rand.NewSource(time.Now().UnixNano()))
	}

	profile := DrawNodeFaultProfile(def, r)
	par := BuildParamsFromProfile(profile, def.PrintTransitions)
	par.Enabled = def.Enabled

	// Log del profilo completo all’istanziazione del nodo
	meanUp := par.MeanUpSimS
	log.Infof("FAULT PROFILE → freqClass=%s (λ≈%.3f crash/min), durClass=%s (meanDown=%.1fs), meanUp=%.1fs",
		profile.FreqClass,
		profile.CrashProbPerMinSim,
		profile.DurClass,
		profile.DownTimeMeanSimS,
		meanUp,
	)

	sim := NewSim(log, clock, r, par, hooks)
	return profile, sim
}

// InitSimAuto: wrapper con default sensati per classi/pesi/valori.
func InitSimAuto(
	log *logx.Logger,
	clock *simclock.Clock,
	r *rand.Rand,
	printTransitions bool,
	hooks Hooks,
) (NodeFaultProfile, *Sim) {
	def := DefaultAutoProfile(printTransitions)
	return InitSimWithProfile(log, clock, r, def, hooks)
}
