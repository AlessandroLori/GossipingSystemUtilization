package affinity

type JobClass uint8

const (
	ClassCPUOnly JobClass = iota
	ClassGPUHeavy
	ClassMemHeavy
	ClassGeneral // NEW: job non massicci su tutte le risorse
)

// Soglie semplici in percentuale (0..100)
const (
	cpuHeavy = 70.0
	memHeavy = 70.0
	gpuHeavy = 50.0 // "richiede GPU in modo significativo"
)

// Deriva la classe dal JobSpec (percentuali richieste)
func GuessClass(cpuPct, memPct, gpuPct float64) JobClass {
	switch {
	case gpuPct >= gpuHeavy:
		return ClassGPUHeavy
	case memPct >= memHeavy:
		return ClassMemHeavy
	case cpuPct >= cpuHeavy:
		return ClassCPUOnly
	default:
		return ClassGeneral
	}
}
