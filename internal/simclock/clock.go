package simclock

import (
	"time"
)

type Clock struct {
	scale     float64 // es. 60 => 1 min SIM = 1 s reale
	startReal time.Time
	startSim  time.Time
}

func New(scale float64) *Clock {
	if scale <= 0 {
		scale = 1
	}
	return &Clock{
		scale:     scale,
		startReal: time.Now(),
		startSim:  time.Unix(0, 0), // t=0 SIM
	}
}

func (c *Clock) NowSim() time.Time {
	elapsedReal := time.Since(c.startReal)
	simNs := float64(elapsedReal.Nanoseconds()) * c.scale
	return c.startSim.Add(time.Duration(simNs))
}

// Dorme "durata SIM" convertita in reale
func (c *Clock) SleepSim(d time.Duration) {
	if c.scale <= 0 {
		time.Sleep(d)
		return
	}
	realD := time.Duration(float64(d) / c.scale)
	if realD < time.Millisecond {
		realD = time.Millisecond
	}
	time.Sleep(realD)
}

// SimToReal converte una durata SIM nella corrispondente durata reale.
func (c *Clock) SimToReal(d time.Duration) time.Duration {
	if c.scale <= 0 {
		return d
	}
	realD := time.Duration(float64(d) / c.scale)
	if realD < time.Millisecond {
		return time.Millisecond
	}
	return realD
}

// Utility per formattare timestamp SIM breve
func (c *Clock) Stamp() string {
	t := c.NowSim()
	return t.Format("15:04:05.000") // hh:mm:ss.mmm (SIM)
}
