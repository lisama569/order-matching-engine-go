// matchingd 是本地确定性撮合服务的演示入口（纯内存状态，无外部依赖）。
//
// 运行固定脚本：下单 -> 部分成交 -> 撤单 -> 风控拒绝，打印每个事件的序号与判定依据，
// 随后从事件流重建状态并校验与实时状态完全一致。
package main

import (
	"fmt"
	"os"

	"github.com/lisama569/order-matching-engine-go/internal/domain"
	"github.com/lisama569/order-matching-engine-go/internal/engine"
	"github.com/lisama569/order-matching-engine-go/internal/event"
	"github.com/lisama569/order-matching-engine-go/internal/risk"
)

func main() {
	cfg := risk.Config{
		MaxPrice: 1000,
		Default: risk.Limits{
			MaxPosition:            100,
			MaxOpenOrders:          5,
			MaxExposure:            100_000,
			MaxSingleOrderNotional: 50_000,
		},
	}
	accounts := map[string]bool{"alice": true, "bob": true}
	e := engine.New([]string{"BTCUSD"}, cfg, map[string]int64{"BTCUSD": 100}, accounts)
	e.SetLogger(func(format string, args ...any) { fmt.Printf("  "+format+"\n", args...) })

	sub := e.Subscribe("demo-writer", 64)
	done := make(chan struct{})
	go func() { // 演示订阅者：按序接收事件
		for ev := range sub.Events {
			fmt.Printf("[subscriber] seq=%d type=%s\n", ev.Seq, ev.Payload.Type)
		}
		close(done)
	}()

	fmt.Println("== 1) 挂卖单 100x10 ==")
	e.Submit(engine.SubmitRequest{OrderID: "s1", AccountID: "alice", Symbol: "BTCUSD",
		Side: domain.SideSell, Type: domain.OrderLimit, Price: 100, Quantity: 10})

	fmt.Println("== 2) bob 市价买 6（部分吃掉 s1）==")
	r := e.Submit(engine.SubmitRequest{OrderID: "m1", AccountID: "bob", Symbol: "BTCUSD",
		Side: domain.SideBuy, Type: domain.OrderMarket, Quantity: 6})
	fmt.Printf("   成交 %d 笔，taker 状态=%s\n", len(r.Fills), r.Status)

	fmt.Println("== 3) bob 再买 200（限价 100），净仓将达 206 > 100，触发持仓限额拒绝 ==")
	if rej := e.Submit(engine.SubmitRequest{OrderID: "m2", AccountID: "bob", Symbol: "BTCUSD",
		Side: domain.SideBuy, Type: domain.OrderLimit, Price: 100, Quantity: 200}); rej.Accepted {
		fmt.Println("   不应被接受")
		os.Exit(1)
	} else {
		fmt.Printf("   拒绝原因=%s 说明=%s\n", rej.Reject, rej.RejectDetail)
	}

	fmt.Println("== 4) alice 撤销剩余 s1 ==")
	fmt.Printf("   %+v\n", e.Cancel(engine.CancelRequest{OrderID: "s1"}))

	snap := e.Snapshot()
	fmt.Printf("== 实时状态: LastSeq=%d 账户=%v 簿子=%v ==\n", snap.LastSeq, snap.Accounts, snap.Books)

	fmt.Println("== 5) 从事件流重建（断点重放）==")
	events := e.Events().All()
	ghost := engine.New([]string{"BTCUSD"}, cfg, map[string]int64{"BTCUSD": 100}, accounts)
	if err := ghost.Replay(events); err != nil {
		var se *event.SequenceError
		fmt.Printf("重放被拒绝: %v\n", err)
		_ = se
		os.Exit(1)
	}
	gs := ghost.Snapshot()
	fmt.Printf("== 重建状态: LastSeq=%d 账户=%v 簿子=%v ==\n", gs.LastSeq, gs.Accounts, gs.Books)
	if gs.LastSeq != snap.LastSeq {
		fmt.Println("REPLAY MISMATCH")
		os.Exit(1)
	}
	fmt.Println("重放结果与实时状态一致 ✔")
}
