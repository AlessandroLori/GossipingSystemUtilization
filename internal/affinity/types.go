package affinity

type Outcome uint8

const (
	OutcomeCompleted Outcome = iota
	OutcomeRefused
	OutcomeTimeout
	OutcomeCancelled
)

// Dati per il ranking pre-Probe (li componi in seed_coordinator)
type Candidate struct {
	PeerID          string  // obbligatorio
	HasGPU          bool    // compatibilità per ClassGPUHeavy
	AdvertAvail     float64 // [0..1] da piggyback (se non noto, usa -1 per "assente")
	ProjectedLoad   float64 // [0..1] da AE (0=libero,1=pieno); se ignoto usa -1
	CooldownPenalty float64 // [0..1] 0 nessuna penalità
	Fresh           bool    // true se stats/advert recenti (<= StaleCutoff)
}
type ScoredCandidate struct {
	Candidate
	Score float64
}
