package seed

import (
	"math/rand"
	"sync"
	"time"

	"GossipSystemUtilization/proto"
)

type Registry struct {
	mu    sync.RWMutex
	peers map[string]*proto.PeerInfo
	stats map[string]*proto.Stats
	rnd   *rand.Rand
}

func NewRegistry(r *rand.Rand) *Registry {
	if r == nil {
		r = rand.New(rand.NewSource(time.Now().UnixNano()))
	}
	return &Registry{
		peers: make(map[string]*proto.PeerInfo),
		stats: make(map[string]*proto.Stats),
		rnd:   r,
	}
}

func (r *Registry) UpsertPeer(p *proto.PeerInfo) {
	r.mu.Lock()
	r.peers[p.NodeId] = p
	r.mu.Unlock()
}

func (r *Registry) UpsertStats(s *proto.Stats) {
	if s == nil {
		return
	}
	r.mu.Lock()
	r.stats[s.NodeId] = s
	r.mu.Unlock()
}

func (r *Registry) SamplePeers(exclude string, n int) []*proto.PeerInfo {
	r.mu.RLock()
	defer r.mu.RUnlock()
	all := make([]*proto.PeerInfo, 0, len(r.peers))
	for _, p := range r.peers {
		if p.NodeId == exclude {
			continue
		}
		all = append(all, p)
	}
	// shuffle
	r.rnd.Shuffle(len(all), func(i, j int) { all[i], all[j] = all[j], all[i] })
	if n > len(all) {
		n = len(all)
	}
	return all[:n]
}

func (r *Registry) SnapshotStats(max int) []*proto.Stats {
	r.mu.RLock()
	defer r.mu.RUnlock()
	out := make([]*proto.Stats, 0, max)
	for _, s := range r.stats {
		out = append(out, s)
		if len(out) >= max {
			break
		}
	}
	return out
}
