package antientropy

import (
	"sync"
	"time"

	"GossipSystemUtilization/internal/logx"
	"GossipSystemUtilization/internal/simclock"
	"GossipSystemUtilization/proto"
)

type Store struct {
	mu   sync.RWMutex
	log  *logx.Logger
	clk  *simclock.Clock
	data map[string]*proto.Stats // ultima stats per node_id
}

func NewStore(log *logx.Logger, clk *simclock.Clock) *Store {
	return &Store{
		log:  log,
		clk:  clk,
		data: make(map[string]*proto.Stats),
	}
}

// Inserisce/aggiorna batch di stats (solo se ts è più recente). Restituisce quante righe nuove/aggiornate.
func (s *Store) UpsertBatch(batch []*proto.Stats) (updated int) {
	if len(batch) == 0 {
		return 0
	}
	s.mu.Lock()
	defer s.mu.Unlock()
	for _, st := range batch {
		if st == nil || st.NodeId == "" {
			continue
		}
		if old, ok := s.data[st.NodeId]; ok && st.TsMs <= old.TsMs {
			continue
		}
		s.data[st.NodeId] = st
		updated++
		s.log.Infof("AVAIL UPDATE → %s cpu=%.1f%% mem=%.1f%% gpu=%.1f%% ts=%d",
			st.NodeId, st.CpuPct, st.MemPct, st.GpuPct, st.TsMs)
	}
	return
}

// Ritorna un campione fino a max record, includendo opzionalmente le stats locali self.
func (s *Store) SnapshotSample(max int, self *proto.Stats) []*proto.Stats {
	s.mu.RLock()
	defer s.mu.RUnlock()
	out := make([]*proto.Stats, 0, max)
	if self != nil {
		out = append(out, self)
	}
	for _, st := range s.data {
		if self != nil && st.NodeId == self.NodeId {
			continue
		}
		out = append(out, st)
		if len(out) >= max {
			break
		}
	}
	return out
}

// Rimuove entry più vecchie del TTL (in tempo SIM). Restituisce quante righe rimosse.
func (s *Store) AgeOut(ttlSim time.Duration) (removed int) {
	ttlReal := s.clk.SimToReal(ttlSim)
	cut := time.Now().Add(-ttlReal).UnixMilli()

	s.mu.Lock()
	defer s.mu.Unlock()
	for id, st := range s.data {
		if st == nil || st.TsMs < cut {
			delete(s.data, id)
			removed++
			s.log.Warnf("AVAIL AGED-OUT → %s", id)
		}
	}
	return
}
