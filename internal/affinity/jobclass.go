package affinity

// Classi
type JobClass uint8

const (
	ClassCPUOnly JobClass = iota
	ClassGPUHeavy
	ClassMemHeavy
	ClassGeneral
)

// soglie "heavy" configurabili a runtime
var cpuHeavyTh = 70.0
var memHeavyTh = 70.0
var gpuHeavyTh = 50.0

// SetThresholds consente di allineare le soglie alle config (es. bucket.large_pct)
func SetThresholds(cpu, mem, gpu float64) {
	if cpu > 0 {
		cpuHeavyTh = cpu
	}
	if mem > 0 {
		memHeavyTh = mem
	}
	if gpu > 0 {
		gpuHeavyTh = gpu
	}
}

// PrimaryClass: usa le soglie correnti
func PrimaryClass(cpuPct, memPct, gpuPct float64) JobClass {
	switch {
	case gpuPct >= gpuHeavyTh:
		return ClassGPUHeavy
	case memPct >= memHeavyTh:
		return ClassMemHeavy
	case cpuPct >= cpuHeavyTh:
		return ClassCPUOnly
	default:
		return ClassGeneral
	}
}

// Alias per retrocompat
func GuessClass(cpuPct, memPct, gpuPct float64) JobClass {
	return PrimaryClass(cpuPct, memPct, gpuPct)
}
