package engine

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/lisama569/order-matching-engine-go/internal/domain"
	"github.com/lisama569/order-matching-engine-go/internal/orderbook"
	"github.com/lisama569/order-matching-engine-go/internal/risk"
)

// depthFromRegistryLocked 在测试内持锁从注册表逐单重算各价位汇总（快照的对照组）。
func depthFromRegistryLocked(e *Engine, symbol string) (bidQty, askQty map[int64]int64, bidCnt, askCnt map[int64]int) {
	bidQty, askQty = map[int64]int64{}, map[int64]int64{}
	bidCnt, askCnt = map[int64]int{}, map[int64]int{}
	e.mu.Lock()
	defer e.mu.Unlock()
	for _, o := range e.registry {
		if o.Symbol != symbol || o.Type != domain.OrderLimit || o.Remaining() <= 0 {
			continue
		}
		if o.Status != domain.StatusNew && o.Status != domain.StatusPartiallyFilled {
			continue
		}
		if o.Side == domain.SideBuy {
			bidQty[o.Price] += o.Remaining()
			bidCnt[o.Price]++
		} else {
			askQty[o.Price] += o.Remaining()
			askCnt[o.Price]++
		}
	}
	return
}

func checkDepthAgainstRegistry(t *testing.T, e *Engine, symbol string, snap DepthSnapshot) {
	t.Helper()
	bidQty, askQty, bidCnt, askCnt := depthFromRegistryLocked(e, symbol)
	bv, ok := snap.Books[symbol]
	if !ok {
		t.Fatalf("snapshot missing symbol %s", symbol)
	}
	if len(bv.Bids) != len(bidQty) || len(bv.Asks) != len(askQty) {
		t.Fatalf("level count mismatch: snap bids=%d asks=%d, registry bids=%d asks=%d",
			len(bv.Bids), len(bv.Asks), len(bidQty), len(askQty))
	}
	for _, lv := range bv.Bids {
		if bidQty[lv.Price] != lv.Quantity || bidCnt[lv.Price] != lv.Orders {
			t.Fatalf("bid level %d: snap=%d量/%d笔, 明细重算=%d量/%d笔",
				lv.Price, lv.Quantity, lv.Orders, bidQty[lv.Price], bidCnt[lv.Price])
		}
	}
	for _, lv := range bv.Asks {
		if askQty[lv.Price] != lv.Quantity || askCnt[lv.Price] != lv.Orders {
			t.Fatalf("ask level %d: snap=%d量/%d笔, 明细重算=%d量/%d笔",
				lv.Price, lv.Quantity, lv.Orders, askQty[lv.Price], askCnt[lv.Price])
		}
	}
}

// validateDepthInvariants 校验快照内部不变量（无需锁）：买盘降序、卖盘升序、量为正。
func validateDepthInvariants(t *testing.T, snap DepthSnapshot) {
	t.Helper()
	for sym, bv := range snap.Books {
		for i := 1; i < len(bv.Bids); i++ {
			if bv.Bids[i-1].Price <= bv.Bids[i].Price {
				t.Fatalf("symbol %s bids not descending at %d: %+v", sym, i, bv.Bids)
			}
		}
		for i := 1; i < len(bv.Asks); i++ {
			if bv.Asks[i-1].Price >= bv.Asks[i].Price {
				t.Fatalf("symbol %s asks not ascending at %d: %+v", sym, i, bv.Asks)
			}
		}
		for _, lv := range append(append([]struct {
			P, Q int64
			N    int
		}{}, tup(bv.Bids)...), tup(bv.Asks)...) {
			if lv.Q <= 0 || lv.N <= 0 {
				t.Fatalf("symbol %s degenerate level %+v", sym, lv)
			}
		}
	}
}

func tup(lv []orderbook.PriceLevel) []struct {
	P, Q int64
	N    int
} {
	out := make([]struct {
		P, Q int64
		N    int
	}, len(lv))
	for i, l := range lv {
		out[i] = struct {
			P, Q int64
			N    int
		}{l.Price, l.Quantity, l.Orders}
	}
	return out
}

// TestDepthSnapshotMatchesRestingDetail：快照每档总量/笔数与挂单明细严格对得上。
func TestDepthSnapshotMatchesRestingDetail(t *testing.T) {
	e := buildScenario(t)
	snap := e.Depth()
	if snap.Seq != e.Events().LastSeq() {
		t.Fatalf("snapshot seq=%d, store LastSeq=%d", snap.Seq, e.Events().LastSeq())
	}
	for _, sym := range []string{"BTCUSD", "ETHUSD"} {
		checkDepthAgainstRegistry(t, e, sym, snap)
	}
	t.Logf("快照 seq=%d: BTCUSD 买%d档/卖%d档, ETHUSD 买%d档/卖%d档, 每档总量=该价位挂单剩余量之和 ✔",
		snap.Seq, len(snap.Books["BTCUSD"].Bids), len(snap.Books["BTCUSD"].Asks),
		len(snap.Books["ETHUSD"].Bids), len(snap.Books["ETHUSD"].Asks))
}

// TestDepthSnapshotUnderConcurrency：并发下单/撤单/成交期间取快照——
// 不阻塞撮合、不丢事件，且任一快照都是某个序号上的完整一致视图。
func TestDepthSnapshotUnderConcurrency(t *testing.T) {
	cfg := risk.Config{MaxPrice: 1000, Default: risk.Limits{
		MaxPosition: 1_000_000, MaxOpenOrders: 1000, MaxExposure: 1 << 50, MaxSingleOrderNotional: 1 << 50}}
	e := New([]string{"BTCUSD"}, cfg, map[string]int64{"BTCUSD": 100},
		map[string]bool{"maker": true, "taker": true})

	// 铺底：20 档卖单
	for i := 0; i < 20; i++ {
		r := e.Submit(sell(fmt.Sprintf("seed-%02d", i), "maker", "BTCUSD", domain.OrderLimit, int64(100+i), 5))
		if !r.Accepted {
			t.Fatalf("seed rejected: %s", r.Reject)
		}
	}

	const submitters = 8
	const perWorker = 30
	var sampledMu sync.Mutex
	var sampled []DepthSnapshot

	stop := make(chan struct{})
	var readers sync.WaitGroup
	// 4 个读者疯狂取快照：若取快照会被撮合卡住，这里将无法在限时内完成。
	for r := 0; r < 4; r++ {
		readers.Add(1)
		go func(id int) {
			defer readers.Done()
			n := 0
			for {
				select {
				case <-stop:
					return
				default:
				}
				snap := e.Depth()
				validateDepthInvariants(t, snap)
				if id == 0 && n%50 == 0 {
					sampledMu.Lock()
					sampled = append(sampled, snap)
					sampledMu.Unlock()
				}
				n++
			}
		}(r)
	}

	// 并发下单（限价挂单 + 市价吃单）与撤单。
	var writers sync.WaitGroup
	for w := 0; w < submitters; w++ {
		writers.Add(1)
		go func(w int) {
			defer writers.Done()
			for i := 0; i < perWorker; i++ {
				oid := fmt.Sprintf("w%02d-%03d", w, i)
				switch i % 3 {
				case 0:
					e.Submit(buy(oid, "taker", "BTCUSD", domain.OrderMarket, 0, 1))
				case 1:
					e.Submit(sell(oid, "maker", "BTCUSD", domain.OrderLimit, int64(90+(w*perWorker+i)%40), 2))
				case 2:
					e.Submit(buy(oid, "taker", "BTCUSD", domain.OrderLimit, 105, 1))
					e.Cancel(CancelRequest{OrderID: oid})
				}
			}
		}(w)
	}

	done := make(chan struct{})
	go func() { writers.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("撮合被卡住：并发提交未在限时内完成（疑似快照阻塞撮合）")
	}
	close(stop)

	readersDone := make(chan struct{})
	go func() { readers.Wait(); close(readersDone) }()
	select {
	case <-readersDone:
	case <-time.After(10 * time.Second):
		t.Fatal("取快照的读者被卡住")
	}

	// 事件一条不丢、序号连续。
	events := e.Events().All()
	for i, ev := range events {
		if ev.Seq != int64(i+1) {
			t.Fatalf("事件序号断裂 at %d: got %d", i, ev.Seq)
		}
	}

	// 抽样快照：用事件流重放到 snap.Seq，深度必须与快照逐档一致——
	// 证明快照确实是"该序号时刻"的完整视图，而非中间态。
	sampledMu.Lock()
	samples := append([]DepthSnapshot(nil), sampled...)
	sampledMu.Unlock()
	for _, snap := range samples {
		ghost := New([]string{"BTCUSD"}, cfg, map[string]int64{"BTCUSD": 100},
			map[string]bool{"maker": true, "taker": true})
		if err := ghost.Replay(events[:snap.Seq]); err != nil {
			t.Fatalf("replay to seq %d: %v", snap.Seq, err)
		}
		gsnap := ghost.Depth()
		gbv := gsnap.Books["BTCUSD"]
		sbv := snap.Books["BTCUSD"]
		if fmt.Sprintf("%v", gbv.Bids) != fmt.Sprintf("%v", sbv.Bids) ||
			fmt.Sprintf("%v", gbv.Asks) != fmt.Sprintf("%v", sbv.Asks) {
			t.Fatalf("快照 seq=%d 与重放视图不符:\nsnap=%v\nreplay=%v", snap.Seq, sbv, gbv)
		}
	}

	// 静态一致性：最终快照与注册表明细严格对得上。
	checkDepthAgainstRegistry(t, e, "BTCUSD", e.Depth())
	t.Logf("并发下取快照: 事件数=%d 连续无丢失, 抽样快照=%d 个均与重放视图一致, 撮合未被阻塞 ✔",
		len(events), len(samples))
}
