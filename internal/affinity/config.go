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
	WPenalty    float64 // penalità (sottratta)

	// Esplorazione (oggi non usata in Rank(), utile se vorrai una Pick())
	Epsilon     float64 // ε-greedy
	SoftmaxTemp float64 // τ per softmax

	// Freschezza
	StaleCutoff time.Duration // es. 5s SIM

	// Verbose prints
	Verbose bool
}

func DefaultConfig() Config {
	return Config{
		HalfLife:           60 * time.Second,
		DecayEvery:         5 * time.Second,
		MinScore:           -5.0,
		MaxScore:           10.0,
		MaxFriendsPerClass: 4,
		WReputation:        0.5,
		WPiggyback:         0.3,
		WLeastLoad:         0.2,
		WPenalty:           0.4,
		Epsilon:            0.10,
		SoftmaxTemp:        0.20,
		StaleCutoff:        5 * time.Second,
		Verbose:            true,
	}
}

// Sanitize applica default di sicurezza e clamp semplici.
func (c *Config) Sanitize() {
	if c.HalfLife <= 0 {
		c.HalfLife = 60 * time.Second
	}
	if c.DecayEvery <= 0 {
		c.DecayEvery = 5 * time.Second
	}
	if c.MinScore >= c.MaxScore {
		c.MinScore, c.MaxScore = -5, 10
	}
	if c.MaxFriendsPerClass <= 0 {
		c.MaxFriendsPerClass = 32
	}
	if c.WReputation < 0 {
		c.WReputation = 0.5
	}
	if c.WPiggyback < 0 {
		c.WPiggyback = 0.3
	}
	if c.WLeastLoad < 0 {
		c.WLeastLoad = 0.2
	}
	if c.WPenalty < 0 {
		c.WPenalty = 0.4
	}
	if c.Epsilon < 0 || c.Epsilon > 1 {
		c.Epsilon = 0.1
	}
	if c.SoftmaxTemp <= 0 {
		c.SoftmaxTemp = 0.2
	}
	if c.StaleCutoff <= 0 {
		c.StaleCutoff = 5 * time.Second
	}
}
