package logx

import (
	"fmt"
	"log"
	"sync"
	"sync/atomic"

	"GossipSystemUtilization/internal/simclock"
)

type Logger struct {
	mu     sync.Mutex
	id     string
	clock  *simclock.Clock
	prefix string
}

func New(id string, clock *simclock.Clock) *Logger {
	return &Logger{id: id, clock: clock}
}

func (l *Logger) with(level string, msg string) string {
	ts := l.clock.Stamp()
	return fmt.Sprintf("[%s] [%s] [%s] %s", ts, l.id, level, msg)
}

// ------- NEW: mute INFO globally when node is down -------
var infoMuted atomic.Bool

func MuteInfo()   { infoMuted.Store(true) }
func UnmuteInfo() { infoMuted.Store(false) }

// ---------------------------------------------------------

func (l *Logger) Infof(f string, a ...any) {
	// Mute INFO while node is down (leave/fault)
	if infoMuted.Load() {
		return
	}
	log.Println(l.with("INFO", fmt.Sprintf(f, a...)))
}
func (l *Logger) Warnf(f string, a ...any)  { log.Println(l.with("WARN", fmt.Sprintf(f, a...))) }
func (l *Logger) Errorf(f string, a ...any) { log.Println(l.with("ERROR", fmt.Sprintf(f, a...))) }
