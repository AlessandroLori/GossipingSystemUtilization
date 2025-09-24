package affinity

import "sync"

type Friends struct {
	mu    sync.Mutex
	maxN  int
	table map[JobClass]map[string]int64 // class -> peerID -> lastSeenSimMs
}

func newFriends(maxN int) *Friends {
	f := &Friends{
		maxN:  maxN,
		table: make(map[JobClass]map[string]int64, 4),
	}
	// Inizializza TUTTE le classi, inclusa ClassGeneral
	f.table[ClassCPUOnly] = make(map[string]int64)
	f.table[ClassGPUHeavy] = make(map[string]int64)
	f.table[ClassMemHeavy] = make(map[string]int64)
	f.table[ClassGeneral] = make(map[string]int64)
	return f
}

// Touch inserisce/aggiorna il peer e fa evict dell'elemento più vecchio se si supera maxN.
func (f *Friends) Touch(class JobClass, peer string, nowMs int64) {
	f.mu.Lock()
	defer f.mu.Unlock()

	m, ok := f.table[class]
	if !ok || m == nil {
		m = make(map[string]int64) // difensiva: crea la mappa se assente
		f.table[class] = m
	}
	m[peer] = nowMs

	// Evict se eccediamo la cap (se maxN <= 0, nessun limite)
	if f.maxN > 0 && len(m) > f.maxN {
		var oldestPeer string
		var oldestTs int64 = 1 << 62
		for p, t := range m {
			if t < oldestTs {
				oldestTs = t
				oldestPeer = p
			}
		}
		if oldestPeer != "" {
			delete(m, oldestPeer)
		}
	}
}

func (f *Friends) Snapshot(class JobClass) []string {
	f.mu.Lock()
	defer f.mu.Unlock()
	m := f.table[class]
	out := make([]string, 0, len(m))
	for p := range m {
		out = append(out, p)
	}
	return out
}
