package engine

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/lisama569/order-matching-engine-go/internal/domain"
	"github.com/lisama569/order-matching-engine-go/internal/risk"
)

// depthInconsistency 校验快照每档聚合与明细严格对得上；返回空串表示自洽。
func depthInconsistency(v DepthView) string {
	for sym, sd := range v.Symbols {
		for _, side := range []struct {
			name   string
			levels []DepthLevel
		}{{"bids", sd.Bids}, {"asks", sd.Asks}} {
			for _, lv := range side.levels {
				var sum int64
				for _, d := range lv.Details {
					sum += d.Remaining
				}
				if sum != lv.Quantity || len(lv.Details) != lv.Orders {
					return fmt.Sprintf("%s %s@%d 聚合与明细不符: qty=%d sum(remaining)=%d orders=%d len(details)=%d (lastSeq=%d)",
						sym, side.name, lv.Price, lv.Quantity, sum, lv.Orders, len(lv.Details), v.LastSeq)
				}
			}
		}
	}
	return ""
}

// TestDepthSnapshotMatchesRestingOrders：盘口快照聚合与同一时刻挂单明细严格一致。
func TestDepthSnapshotMatchesRestingOrders(t *testing.T) {
	e := testEngine(t)
	e.Submit(sell("s1", "alice", "BTCUSD", domain.OrderLimit, 100, 10))
	e.Submit(sell("s2", "bob", "BTCUSD", domain.OrderLimit, 100, 5)) // 同价位第二笔
	e.Submit(sell("s3", "bob", "BTCUSD", domain.OrderLimit, 101, 8)) // 更高卖价
	e.Submit(buy("b1", "carol", "BTCUSD", domain.OrderLimit, 99, 4)) // 挂买
	e.Submit(buy("b2", "carol", "BTCUSD", domain.OrderMarket, 0, 6)) // 吃掉 s1 的 6 个
	e.Cancel(CancelRequest{OrderID: "s3"})

	v := e.DepthSnapshot()
	if msg := depthInconsistency(v); msg != "" {
		t.Fatal(msg)
	}

	if v.LastSeq != e.Events().LastSeq() {
		t.Fatalf("snapshot LastSeq=%d want %d", v.LastSeq, e.Events().LastSeq())
	}
	btc := v.Symbols["BTCUSD"]
	// 卖档 100: s1 剩 4 + s2 剩 5 = 9，2 笔
	if len(btc.Asks) != 1 || btc.Asks[0].Price != 100 || btc.Asks[0].Quantity != 9 || btc.Asks[0].Orders != 2 {
		t.Fatalf("asks=%+v want 100x9 orders=2", btc.Asks)
	}
	if len(btc.Bids) != 1 || btc.Bids[0].Price != 99 || btc.Bids[0].Quantity != 4 || btc.Bids[0].Orders != 1 {
		t.Fatalf("bids=%+v want 99x4 orders=1", btc.Bids)
	}
	// 与引擎可解释快照的聚合视图逐档一致
	snap := e.Snapshot()
	for i, pl := range snap.Books["BTCUSD"].Asks {
		dl := btc.Asks[i]
		if pl.Price != dl.Price || pl.Quantity != dl.Quantity || pl.Orders != dl.Orders {
			t.Fatalf("ask level %d: Snapshot=%+v DepthSnapshot=%+v", i, pl, dl)
		}
	}
	t.Logf("盘口快照: lastSeq=%d asks=%v bids=%v，每档剩余总量=明细之和 ✔",
		v.LastSeq, btc.Asks, btc.Bids)
}

// TestDepthSnapshotConcurrentWithMatching：并发下单/撤单/成交期间取快照，
// 不阻塞撮合、每个快照都是自洽的一致视图、事件不丢。
func TestDepthSnapshotConcurrentWithMatching(t *testing.T) {
	cfg := risk.Config{MaxPrice: 10000, Default: risk.Limits{
		MaxPosition: 1_000_000, MaxOpenOrders: 100_000, MaxExposure: 1 << 60, MaxSingleOrderNotional: 1 << 60}}
	e := New([]string{"BTCUSD"}, cfg, map[string]int64{"BTCUSD": 100},
		map[string]bool{"maker": true, "taker": true})
	e.SetLogger(t.Logf)

	// 铺 50 档卖单
	for i := 0; i < 50; i++ {
		e.Submit(sell(fmt.Sprintf("seed-%03d", i), "maker", "BTCUSD", domain.OrderLimit, int64(100+i), 5))
	}

	const writers = 8
	const readers = 4
	const perWriter = 60

	var writersWG, readersWG sync.WaitGroup
	stop := make(chan struct{})

	// 读方：持续取快照并校验自洽；记录耗时尖刺
	var maxSnapNanos int64
	var mu sync.Mutex
	var snapCount int
	var firstBad string
	for r := 0; r < readers; r++ {
		readersWG.Add(1)
		go func() {
			defer readersWG.Done()
			for {
				select {
				case <-stop:
					return
				default:
				}
				start := time.Now()
				v := e.DepthSnapshot()
				elapsed := time.Since(start).Nanoseconds()
				bad := depthInconsistency(v)
				mu.Lock()
				snapCount++
				if elapsed > maxSnapNanos {
					maxSnapNanos = elapsed
				}
				if bad != "" && firstBad == "" {
					firstBad = bad
				}
				mu.Unlock()
			}
		}()
	}

	// 写方：并发下单（限价挂单 + 市价吃单 + 撤单）
	for w := 0; w < writers; w++ {
		writersWG.Add(1)
		go func(w int) {
			defer writersWG.Done()
			for i := 0; i < perWriter; i++ {
				oid := fmt.Sprintf("w%02d-%03d", w, i)
				switch i % 3 {
				case 0: // 挂买（低价，不穿越）
					e.Submit(buy(oid, "taker", "BTCUSD", domain.OrderLimit, int64(50+w), 2))
				case 1: // 市价吃卖档
					e.Submit(buy(oid, "taker", "BTCUSD", domain.OrderMarket, 0, 1))
				default: // 撤掉自己 case 0 挂的单
					e.Cancel(CancelRequest{OrderID: fmt.Sprintf("w%02d-%03d", w, i-2)})
				}
			}
		}(w)
	}

	// 等写方全部完成（若快照阻塞撮合，这里会明显变慢甚至超时）
	writersDone := make(chan struct{})
	go func() { writersWG.Wait(); close(writersDone) }()
	select {
	case <-writersDone:
	case <-time.After(30 * time.Second):
		t.Fatal("撮合被快照卡住：写方未在 30s 内完成")
	}
	close(stop)
	readersWG.Wait()
	if firstBad != "" {
		t.Fatalf("并发期间取到不自洽快照: %s", firstBad)
	}

	// 事件不丢：序号严格连续
	events := e.Events().All()
	for i, ev := range events {
		if ev.Seq != int64(i+1) {
			t.Fatalf("事件序号在 %d 处断开: got %d", i, ev.Seq)
		}
	}
	// 最终快照与最终状态一致
	final := e.DepthSnapshot()
	if final.LastSeq != e.Events().LastSeq() {
		t.Fatalf("final snapshot LastSeq=%d want %d", final.LastSeq, e.Events().LastSeq())
	}
	if msg := depthInconsistency(final); msg != "" {
		t.Fatal(msg)
	}
	t.Logf("并发完成: 事件=%d 快照读取=%d 次(单次最长=%dµs)，全部自洽，撮合未被阻塞",
		len(events), snapCount, maxSnapNanos/1000)
}
