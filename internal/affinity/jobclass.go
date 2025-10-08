package affinity

// Classi
type JobClass uint8

const (
	ClassCPUOnly JobClass = iota
	ClassGPUHeavy
	ClassMemHeavy
	ClassGeneral
)

// soglie "heavy"
var cpuHeavyTh = 70.0
var memHeavyTh = 70.0
var gpuHeavyTh = 50.0

// SetThresholds consente di calibrare le soglie da config/persona.
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

// PrimaryClass determina la classe dominante del job dato il profilo %.
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
