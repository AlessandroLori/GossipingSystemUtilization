package scheduler

import (
	"math"
	"math/rand"
	"time"

	"GossipSystemUtilization/internal/affinity"
)

func pickEpsilonGreedy(scored []affinity.ScoredCandidate, epsilon float64) string {
	if len(scored) == 0 {
		return ""
	}
	if epsilon < 0 {
		epsilon = 0
	}
	if epsilon > 1 {
		epsilon = 1
	}
	r := rand.Float64()
	if r < epsilon {
		// esplora: scegli random uniforme tra i topK passati
		rand.Seed(time.Now().UnixNano())
		return scored[rand.Intn(len(scored))].PeerID
	}
	// sfrutta: top1
	return scored[0].PeerID
}

func pickSoftmax(scored []affinity.ScoredCandidate, tau float64) string {
	if len(scored) == 0 {
		return ""
	}
	if tau <= 0 {
		// temperatura non valida -> top1
		return scored[0].PeerID
	}
	// Boltzmann over SCORE
	maxScore := scored[0].Score
	for _, s := range scored {
		if s.Score > maxScore {
			maxScore = s.Score
		}
	}

	// stabilità numerica
	var sum float64
	probs := make([]float64, len(scored))
	for i, s := range scored {
		z := (s.Score - maxScore) / tau
		probs[i] = math.Exp(z)
		sum += probs[i]
	}
	if sum == 0 {
		return scored[0].PeerID
	}
	// campionamento
	r := rand.Float64()
	cum := 0.0
	for i, p := range probs {
		cum += p / sum
		if r <= cum {
			return scored[i].PeerID
		}
	}
	return scored[len(scored)-1].PeerID
}
