package engine

import (
	"fmt"
	"sync"
	"testing"

	"github.com/lisama569/order-matching-engine-go/internal/domain"
	"github.com/lisama569/order-matching-engine-go/internal/risk"
)

// TestConcurrentSubmitDeterministicReplay：并发下单后，事件流仍可重放出与实时完全一致的状态。
func TestConcurrentSubmitDeterministicReplay(t *testing.T) {
	cfg := risk.Config{MaxPrice: 1000, Default: risk.Limits{
		MaxPosition: 1_000_000, MaxOpenOrders: 1000, MaxExposure: 1 << 50, MaxSingleOrderNotional: 1 << 50}}
	e := New([]string{"BTCUSD"}, cfg, map[string]int64{"BTCUSD": 100},
		map[string]bool{"maker": true, "taker": true})

	// 先确定性地铺 200 个卖单（价格分散，避免同价）
	for i := 0; i < 200; i++ {
		oid := fmt.Sprintf("sell-%03d", i)
		r := e.Submit(sell(oid, "maker", "BTCUSD", domain.OrderLimit, int64(100+i), 2))
		if !r.Accepted {
			t.Fatalf("seed %s rejected: %s", oid, r.Reject)
		}
	}
	seqAfterSeed := e.Events().LastSeq()

	// 50 个 goroutine 并发市价买入，每个 1 个
	var wg sync.WaitGroup
	for g := 0; g < 50; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			oid := fmt.Sprintf("buy-%03d", g)
			r := e.Submit(buy(oid, "taker", "BTCUSD", domain.OrderMarket, 0, 1))
			if !r.Accepted {
				t.Errorf("concurrent %s rejected: %s", oid, r.Reject)
			}
		}(g)
	}
	wg.Wait()

	events := e.Events().All()
	if len(events) != int(seqAfterSeed)+100 { // 每笔市价成交 = 1 accepted + 1 filled
		t.Fatalf("event count=%d want %d", len(events), int(seqAfterSeed)+100)
	}
	t.Logf("并发完成: 事件序号 1..%d 连续无缺口（种子后 seq=%d）", len(events), seqAfterSeed)

	// 序号连续性
	for i, ev := range events {
		if ev.Seq != int64(i+1) {
			t.Fatalf("sequence broken at %d: got %d", i, ev.Seq)
		}
	}

	// 从事件流重建，必须与实时状态一致
	ghost := New([]string{"BTCUSD"}, cfg, map[string]int64{"BTCUSD": 100},
		map[string]bool{"maker": true, "taker": true})
	if err := ghost.Replay(events); err != nil {
		t.Fatalf("replay: %v", err)
	}
	live, rebuilt := e.Snapshot(), ghost.Snapshot()
	if !booksEqual(live.Books, rebuilt.Books) {
		t.Fatalf("books mismatch after concurrent workload")
	}
	if !accountsEqual(live.Accounts, rebuilt.Accounts) {
		t.Fatalf("accounts mismatch: live=%v rebuilt=%v", live.Accounts, rebuilt.Accounts)
	}
	if rebuilt.Accounts["taker/BTCUSD"].Position != 50 {
		t.Fatalf("taker position=%d want 50", rebuilt.Accounts["taker/BTCUSD"].Position)
	}
	if rebuilt.Accounts["maker/BTCUSD"].Position != -50 {
		t.Fatalf("maker position=%d want -50", rebuilt.Accounts["maker/BTCUSD"].Position)
	}
	t.Logf("并发事件流重建一致: taker净仓=+50 maker净仓=-50，簿子档位完全相同")
}

// TestSubscriberGetsAllEvents：订阅者按序收到全部撮合事件。
func TestSubscriberGetsAllEvents(t *testing.T) {
	e := testEngine(t)
	sub := e.Subscribe("srv-1", 128)
	e.Submit(sell("s1", "alice", "BTCUSD", domain.OrderLimit, 100, 5))
	e.Submit(buy("b1", "bob", "BTCUSD", domain.OrderMarket, 0, 3))

	wantTypes := []string{"ORDER_ACCEPTED", "ORDER_ACCEPTED", "ORDER_FILLED"}
	for i, want := range wantTypes {
		ev := <-sub.Events
		if string(ev.Payload.Type) != want || ev.Seq != int64(i+1) {
			t.Fatalf("sub event %d = seq=%d type=%s want %s", i, ev.Seq, ev.Payload.Type, want)
		}
		t.Logf("订阅者收到 seq=%d type=%s", ev.Seq, want)
	}
}

// TestMarketPartialFillCancelsRemainder：市价部分成交后残余撤销，且不挂入簿子。
func TestMarketPartialFillCancelsRemainder(t *testing.T) {
	e := testEngine(t)
	e.Submit(sell("s1", "alice", "BTCUSD", domain.OrderLimit, 100, 3))
	r := e.Submit(buy("b1", "bob", "BTCUSD", domain.OrderMarket, 0, 10))
	if !r.Accepted || r.Status != domain.StatusCancelled || len(r.Fills) != 1 || r.Fills[0].Quantity != 3 {
		t.Fatalf("r=%+v", r)
	}
	snap := e.Snapshot()
	if len(snap.Books["BTCUSD"].Asks) != 0 || len(snap.Books["BTCUSD"].Bids) != 0 {
		t.Fatalf("book should be empty, got %+v", snap.Books["BTCUSD"])
	}
	if snap.Accounts["bob/BTCUSD"].Position != 3 {
		t.Fatalf("taker position=%d want 3 (filled part kept)", snap.Accounts["bob/BTCUSD"].Position)
	}
	// 事件: s1 accepted, b1 accepted, fill@3, b1 cancelled
	if snap.LastSeq != 4 {
		t.Fatalf("seq=%d want 4", snap.LastSeq)
	}
	t.Logf("市价部分成交: 成交3保留持仓，残余7撤销；事件序号1..4，簿子清空")
}

// TestSlowSubscriberCatchupFromStore：慢订阅者被断开后，可依据权威事件日志从上次序号追赶，事件不丢。
func TestSlowSubscriberCatchupFromStore(t *testing.T) {
	e := testEngine(t)
	slow := e.Subscribe("slow", 1) // 缓冲为 1
	e.Submit(sell("s1", "alice", "BTCUSD", domain.OrderLimit, 100, 5))
	// 再产生多于缓冲的事件，slow 应被断开
	e.Submit(buy("b1", "bob", "BTCUSD", domain.OrderMarket, 0, 2))
	<-slow.Overflow
	t.Log("慢订阅者已断开（未阻塞撮合）")

	// 权威日志保留全部事件；从 seq=1 完整追赶
	caught := e.Events().From(1)
	if len(caught) != int(e.Events().LastSeq()) {
		t.Fatalf("catchup events=%d want %d", len(caught), e.Events().LastSeq())
	}
	for i, ev := range caught {
		if ev.Seq != int64(i+1) {
			t.Fatalf("catchup sequence broken at %d", i)
		}
		t.Logf("追赶事件 seq=%d type=%s", ev.Seq, ev.Payload.Type)
	}
	t.Logf("慢订阅者从 Store.From 重放 %d 条事件，无丢失", len(caught))
}
