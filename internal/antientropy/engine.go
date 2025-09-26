package antientropy

import (
	"context"
	"math/rand"
	"sync"
	"time"

	"GossipSystemUtilization/internal/logx"
	"GossipSystemUtilization/internal/piggyback"
	"GossipSystemUtilization/internal/simclock"
	"GossipSystemUtilization/internal/swim"
	proto "GossipSystemUtilization/proto"

	"google.golang.org/grpc"
	"google.golang.org/grpc/credentials/insecure"
)

type Config struct {
	PeriodSimS float64
	Fanout     int
	SampleSize int
	TtlSimS    float64
}

type Engine struct {
	log *logx.Logger
	clk *simclock.Clock
	rnd *rand.Rand

	store *Store
	swim  *swim.Manager
	pbq   *piggyback.Queue

	selfSampler func() *proto.Stats

	period     time.Duration
	ttlSim     time.Duration
	fanout     int
	sampleSize int

	stopCh   chan struct{}
	stopOnce sync.Once
}

func NewEngine(log *logx.Logger, clk *simclock.Clock, rnd *rand.Rand,
	store *Store, swimMgr *swim.Manager, pbq *piggyback.Queue,
	selfSampler func() *proto.Stats, cfg Config) *Engine {

	if cfg.PeriodSimS <= 0 {
		cfg.PeriodSimS = 3.0
	}
	if cfg.Fanout <= 0 {
		cfg.Fanout = 2
	}
	if cfg.SampleSize <= 0 {
		cfg.SampleSize = 8
	}
	if cfg.TtlSimS <= 0 {
		cfg.TtlSimS = 12.0
	}

	return &Engine{
		log:         log,
		clk:         clk,
		rnd:         rnd,
		store:       store,
		swim:        swimMgr,
		pbq:         pbq,
		selfSampler: selfSampler,
		period:      time.Duration(cfg.PeriodSimS * float64(time.Second)),
		ttlSim:      time.Duration(cfg.TtlSimS * float64(time.Second)),
		fanout:      cfg.Fanout,
		sampleSize:  cfg.SampleSize,
		stopCh:      make(chan struct{}),
	}
}

func (e *Engine) Start() {
	// re-init per riavvii post leave/recovery
	e.stopOnce = sync.Once{}
	e.stopCh = make(chan struct{})
	go e.loopExchange()
	go e.loopAging()
}

func (e *Engine) Stop() {
	e.stopOnce.Do(func() {
		if e.stopCh != nil {
			close(e.stopCh)
		}
		if e.log != nil {
			e.log.Warnf("Anti-Entropy engine stopped")
		}
	})
}

func (e *Engine) LocalSample(max int) []*proto.Stats {
	return e.store.SnapshotSample(max, e.selfSampler())
}

func (e *Engine) loopExchange() {
	for {
		select {
		case <-e.stopCh:
			return
		default:
		}
		e.clk.SleepSim(e.period)
		peers := e.swim.AlivePeers()
		if len(peers) == 0 {
			continue
		}
		e.rnd.Shuffle(len(peers), func(i, j int) { peers[i], peers[j] = peers[j], peers[i] })
		fan := e.fanout
		if fan <= 0 || fan > len(peers) {
			fan = len(peers)
		}
		targets := peers[:fan]
		req := &proto.AvailBatch{
			Stats: e.store.SnapshotSample(e.sampleSize, e.selfSampler()),
		}
		for _, t := range targets {
			e.exchangeWith(t.Addr, req)
		}
	}
}

func (e *Engine) exchangeWith(addr string, req *proto.AvailBatch) {
	ctx, cancel := context.WithTimeout(context.Background(), 350*time.Millisecond)
	defer cancel()

	conn, err := grpc.DialContext(ctx, addr,
		grpc.WithTransportCredentials(insecure.NewCredentials()),
		grpc.WithBlock(),
		grpc.WithUnaryInterceptor(piggyback.UnaryClientInterceptor(e.pbq)),
	)
	if err != nil {
		return
	}
	defer conn.Close()

	cli := proto.NewGossipClient(conn)
	ctx2, cancel2 := context.WithTimeout(context.Background(), 300*time.Millisecond)
	defer cancel2()

	rep, err := cli.ExchangeAvail(ctx2, req)
	if err != nil || rep == nil {
		return
	}
	if up := e.store.UpsertBatch(rep.Stats); up > 0 {
		e.log.Infof("GOSSIP EXCHANGE ← %s  updated=%d", addr, up)
	}
}

func (e *Engine) loopAging() {
	period := e.period
	if period < 500*time.Millisecond {
		period = 500 * time.Millisecond
	}
	for {
		select {
		case <-e.stopCh:
			return
		default:
		}
		e.clk.SleepSim(period)
		removed := e.store.AgeOut(e.ttlSim)
		if removed > 0 {
			e.log.Warnf("GOSSIP AGE-OUT removed=%d", removed)
		}
	}
}
