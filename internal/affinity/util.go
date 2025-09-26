package affinity

import (
	"time"

	simclock "GossipSystemUtilization/internal/simclock"
)

// nowSimMs ritorna il tempo SIM
func nowSimMs(clk *simclock.Clock) int64 {
	if clk == nil {
		return time.Now().UnixMilli()
	}
	return clk.NowSimMs()
}
