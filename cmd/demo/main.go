// demo 是本地端到端示例：并发下单/撤单 → 停机 → 从事件流确定性重建 → 比对状态。
// 运行：go run ./cmd/demo
package main

import (
	"fmt"
	"log"
	"sync"

	"github.com/lisama569/order-matching-engine-go/internal/engine"
	"github.com/lisama569/order-matching-engine-go/internal/market"
	"github.com/lisama569/order-matching-engine-go/internal/risk"
)

func main() {
	cfg := engine.Config{
		Symbol:           "BTCUSD",
		Limits:           market.Limits{MinTickSize: 1, MinPrice: 1, MaxPrice: 100000, MinQty: 1, MaxQty: 1000},
		Risk:             risk.Config{MaxLongPosition: 200, MaxShortPosition: 200, MaxExposure: 1_000_000},
		SubscriberBuffer: 4,
		AllowedAccounts:  map[string]struct{}{"alice": {}, "bob": {}, "carol": {}},
	}
	e := engine.New(cfg)
	e.Start()

	// 订阅撮合结果。
	id, evCh, dropCh := e.Bus().Subscribe()
	var wgSub sync.WaitGroup
	wgSub.Add(1)
	go func() {
		defer wgSub.Done()
		for {
			select {
			case ev, ok := <-evCh:
				if !ok {
					return
				}
				log.Printf("[subscriber %d] %s", id, ev)
			case d := <-dropCh:
				log.Printf("[subscriber %d] DROPPED total=%d at seq=%d (消费过慢)", id, d.DroppedCount, d.LastSeq)
			}
		}
	}()

	// 做市：bob/carol 挂卖单。
	e.SubmitAndWait(engine.SubmitRequest{OrderID: "o1", AccountID: "bob", Symbol: "BTCUSD",
		Side: market.SideSell, Type: market.OrderTypeLimit, Price: 100, Qty: 30})
	e.SubmitAndWait(engine.SubmitRequest{OrderID: "o2", AccountID: "carol", Symbol: "BTCUSD",
		Side: market.SideSell, Type: market.OrderTypeLimit, Price: 101, Qty: 20})

	// 并发：alice 部分成交买单、撤单尝试、非法单、市价单。
	var wg sync.WaitGroup
	submit := func(req engine.SubmitRequest) {
		defer wg.Done()
		if ok, rej := e.SubmitAndWait(req); ok {
			log.Printf("accepted: %s", req.OrderID)
		} else {
			log.Printf("rejected: %s reason=%s", req.OrderID, rej.Reason)
		}
	}
	wg.Add(4)
	go submit(engine.SubmitRequest{OrderID: "o3", AccountID: "alice", Symbol: "BTCUSD",
		Side: market.SideBuy, Type: market.OrderTypeLimit, Price: 100, Qty: 40})
	go submit(engine.SubmitRequest{OrderID: "o4", AccountID: "alice", Symbol: "BTCUSD",
		Side: market.SideBuy, Type: market.OrderTypeLimit, Price: 101, Qty: 25})
	go submit(engine.SubmitRequest{OrderID: "bad", AccountID: "alice", Symbol: "BTCUSD",
		Side: market.SideBuy, Type: market.OrderTypeLimit, Price: 0, Qty: 10})
	go func() {
		defer wg.Done()
		log.Printf("cancel o2 result=%v", e.CancelAndWait(engine.CancelRequest{OrderID: "o2"}))
	}()
	wg.Wait()

	live := e.Snapshot()
	events := e.Events()
	log.Printf("live: lastSeq=%d sellLevels=%+v accounts=%+v", live.LastSeq, live.SellLevels, live.Accounts)

	e.Stop()
	wgSub.Wait()

	// 断点重建。
	r := engine.New(cfg)
	if err := r.Replay(events); err != nil {
		log.Fatalf("replay failed: %v", err)
	}
	rebuilt := r.Snapshot()
	log.Printf("rebuilt: lastSeq=%d sellLevels=%+v accounts=%+v",
		rebuilt.LastSeq, rebuilt.SellLevels, rebuilt.Accounts)

	fmt.Printf("live seq=%d == rebuilt seq=%d\n", live.LastSeq, rebuilt.LastSeq)
	fmt.Printf("books equal: %v\n",
		fmt.Sprint(live.BuyLevels) == fmt.Sprint(rebuilt.BuyLevels) &&
			fmt.Sprint(live.SellLevels) == fmt.Sprint(rebuilt.SellLevels))
	fmt.Printf("accounts equal: %v\n", fmt.Sprint(live.Accounts) == fmt.Sprint(rebuilt.Accounts))
}
