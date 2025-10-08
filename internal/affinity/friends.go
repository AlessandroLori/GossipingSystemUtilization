package affinity

import (
	"fmt"
	"sort"
	"sync"
)

// Friends mantiene, per ciascuna JobClass, una shortlist LRU (bounded) di peer candidati.
// Salviamo solo lastSeenMs (tempo SIM) per semplicità; l’ordinamento discendente per recency.
type Friends struct {
	maxPerClass   int
	verbosePrints bool

	mu    sync.RWMutex
	table map[JobClass]map[string]int64 // class -> (peer -> lastSeenMs)
}

// NewFriends crea una tabella friends bounded (per classe).
func NewFriends(maxPerClass int, verbose bool) *Friends {
	if maxPerClass <= 0 {
		maxPerClass = 4
	}
	return &Friends{
		maxPerClass:   maxPerClass,
		verbosePrints: verbose,
		table:         make(map[JobClass]map[string]int64, 8),
	}
}

// Touch inserisce/aggiorna un peer come "visto di recente" per la classe.
// Esegue evict LRU se si supera maxPerClass.
func (f *Friends) Touch(class JobClass, peer string, nowSimMs int64) {
	f.mu.Lock()
	defer f.mu.Unlock()

	m := f.ensureClassLocked(class)
	old, existed := m[peer]
	m[peer] = nowSimMs

	if f.verbosePrints {
		if existed {
			fmt.Printf("[Affinity/Friends] touch class=%v peer=%s  lastSeen: %d -> %d\n", class, peer, old, nowSimMs)
		} else {
			fmt.Printf("[Affinity/Friends] add   class=%v peer=%s  lastSeen: %d\n", class, peer, nowSimMs)
		}
	}

	// Evict se sforo il cap.
	if len(m) > f.maxPerClass {
		evicted := evictOldestLocked(m, len(m)-f.maxPerClass)
		if f.verbosePrints && len(evicted) > 0 {
			fmt.Printf("[Affinity/Friends] evict class=%v peers=%v (len=%d cap=%d)\n",
				class, evicted, len(m), f.maxPerClass)
		}
	}
}

// Merge esegue Touch per un batch di peers (utile quando ricevi snapshot/piggyback).
func (f *Friends) Merge(class JobClass, peers []string, nowSimMs int64) {
	for _, p := range peers {
		f.Touch(class, p, nowSimMs)
	}
}

// Snapshot ritorna la lista dei peer (ordinati per recency desc).
func (f *Friends) Snapshot(class JobClass) []string {
	f.mu.RLock()
	defer f.mu.RUnlock()

	m, ok := f.table[class]
	if !ok || len(m) == 0 {
		return nil
	}
	type pair struct {
		peer string
		ts   int64
	}
	a := make([]pair, 0, len(m))
	for p, ts := range m {
		a = append(a, pair{peer: p, ts: ts})
	}
	sort.Slice(a, func(i, j int) bool { return a[i].ts > a[j].ts }) // più recente prima
	out := make([]string, 0, len(a))
	for _, x := range a {
		out = append(out, x.peer)
	}
	return out
}

// Remove elimina (se presente) un peer dalla classe (es. ban temporaneo).
func (f *Friends) Remove(class JobClass, peer string) {
	f.mu.Lock()
	defer f.mu.Unlock()
	if m, ok := f.table[class]; ok {
		delete(m, peer)
		if f.verbosePrints {
			fmt.Printf("[Affinity/Friends] remove class=%v peer=%s\n", class, peer)
		}
	}
}

// ----------- helpers (lock held) -----------

func (f *Friends) ensureClassLocked(class JobClass) map[string]int64 {
	m, ok := f.table[class]
	if !ok {
		m = make(map[string]int64, f.maxPerClass+4)
		f.table[class] = m
	}
	return m
}

func evictOldestLocked(m map[string]int64, n int) []string {
	if n <= 0 || len(m) == 0 {
		return nil
	}
	type kv struct {
		k  string
		ts int64
	}
	all := make([]kv, 0, len(m))
	for k, ts := range m {
		all = append(all, kv{k: k, ts: ts})
	}
	// Ordina crescente per timestamp (più vecchi primi), poi prendi i primi n.
	sort.Slice(all, func(i, j int) bool { return all[i].ts < all[j].ts })
	if n > len(all) {
		n = len(all)
	}
	evicted := make([]string, 0, n)
	for i := 0; i < n; i++ {
		delete(m, all[i].k)
		evicted = append(evicted, all[i].k)
	}
	return evicted
}
