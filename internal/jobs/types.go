package jobs

import (
	"fmt"
	"time"
)

// Spec descrive un job sintetico espresso in PERCENTUALI della capacità del nodo.
// GPU < 0  ⇒ "non applicabile" (il job non richiede GPU).
type Spec struct {
	ID       string        // id univoco del job
	CPU      float64       // percentuale CPU richiesta
	MEM      float64       // percentuale MEM richiesta
	GPU      float64       // percentuale GPU richiesta; 0 se non usata; <0 = non applicabile
	Duration time.Duration // durata della prenotazione (tempo SIM)
}

// Rappresentazione compatta per i log.
func (s Spec) Pretty() string {
	g := "n/a"
	if s.GPU >= 0 {
		g = fmt.Sprintf("%.1f%%", s.GPU)
	}
	return fmt.Sprintf("job=%s cpu=%.1f%% mem=%.1f%% gpu=%s dur=%s",
		s.ID, s.CPU, s.MEM, g, s.Duration)
}
