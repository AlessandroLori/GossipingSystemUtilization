package affinity

import "time"

type Config struct {
	// Reputation decay
	HalfLife   time.Duration // es. 60s (SIM)
	DecayEvery time.Duration // es. 5s (SIM)
	MinScore   float64       // clamp min
	MaxScore   float64       // clamp max

	// Friends shortlist
	MaxFriendsPerClass int // es. 32

	// Ranking weights
	WReputation float64 // P
	WPiggyback  float64 // A
	WLeastLoad  float64 // L
	WPenalty    float64 // X (cooldown)

	// Exploration
	Epsilon     float64       // ε-greedy
	SoftmaxTemp float64       // τ
	StaleCutoff time.Duration // scarta stats > 5s SIM dai candidati
}

func DefaultConfig() Config {
	return Config{
		HalfLife:           60 * time.Second,
		DecayEvery:         5 * time.Second,
		MinScore:           -5.0,
		MaxScore:           10.0,
		MaxFriendsPerClass: 32,
		WReputation:        0.5,
		WPiggyback:         0.3,
		WLeastLoad:         0.2,
		WPenalty:           0.4,
		Epsilon:            0.10,
		SoftmaxTemp:        0.20,
		StaleCutoff:        5 * time.Second,
	}
}
