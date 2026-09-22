package engine

import (
	"testing"

	"github.com/lisama569/order-matching-engine-go/internal/domain"
	"github.com/lisama569/order-matching-engine-go/internal/risk"
)

func testEngine(t *testing.T) *Engine {
	cfg := risk.Config{
		MaxPrice: 1000,
		Default: risk.Limits{
			MaxPosition:            100,
			MaxOpenOrders:          5,
			MaxExposure:            100_000,
			MaxSingleOrderNotional: 50_000,
		},
	}
	e := New([]string{"BTCUSD", "ETHUSD"}, cfg, map[string]int64{"BTCUSD": 100, "ETHUSD": 50},
		map[string]bool{"alice": true, "bob": true, "carol": true})
	e.SetLogger(t.Logf)
	return e
}

func buy(oid, who, sym string, typ domain.OrderType, price, qty int64) SubmitRequest {
	return SubmitRequest{OrderID: oid, AccountID: who, Symbol: sym, Side: domain.SideBuy, Type: typ, Price: price, Quantity: qty}
}
func sell(oid, who, sym string, typ domain.OrderType, price, qty int64) SubmitRequest {
	return SubmitRequest{OrderID: oid, AccountID: who, Symbol: sym, Side: domain.SideSell, Type: typ, Price: price, Quantity: qty}
}

func TestPartialFillAndResting(t *testing.T) {
	e := testEngine(t)
	r1 := e.Submit(sell("s1", "alice", "BTCUSD", domain.OrderLimit, 100, 10))
	if !r1.Accepted || r1.Status != domain.StatusNew {
		t.Fatalf("r1=%+v", r1)
	}
	// 市价买 6：吃 s1 6 个，s1 部分成交剩余 4
	r2 := e.Submit(buy("b1", "bob", "BTCUSD", domain.OrderMarket, 0, 6))
	if !r2.Accepted || len(r2.Fills) != 1 || r2.Fills[0].Quantity != 6 || r2.Fills[0].Price != 100 {
		t.Fatalf("r2=%+v", r2)
	}
	if r2.Status != domain.StatusFilled {
		t.Fatalf("taker market status=%s", r2.Status)
	}
	snap := e.Snapshot()
	lv := snap.Books["BTCUSD"].Asks[0]
	if lv.Price != 100 || lv.Quantity != 4 {
		t.Fatalf("ask level=%+v want 100x4", lv)
	}
	if got := snap.Accounts["alice/BTCUSD"].Position; got != -6 {
		t.Fatalf("alice position=%d want -6", got)
	}
	if got := snap.Accounts["bob/BTCUSD"].Position; got != 6 {
		t.Fatalf("bob position=%d want 6", got)
	}
	t.Logf("部分成交后: 卖档 100x%d; alice净仓=%d bob净仓=%d; 事件数=%d",
		lv.Quantity, snap.Accounts["alice/BTCUSD"].Position, snap.Accounts["bob/BTCUSD"].Position, snap.LastSeq)
}

func TestRejectionReasonsDistinct(t *testing.T) {
	e := testEngine(t)
	e.Submit(sell("dup", "alice", "BTCUSD", domain.OrderLimit, 100, 1))

	cases := []struct {
		name string
		req  SubmitRequest
		want domain.RejectReason
	}{
		{"数量非法", buy("q1", "alice", "BTCUSD", domain.OrderLimit, 100, 0), domain.RejectInvalidQuantity},
		{"负数量", buy("q2", "alice", "BTCUSD", domain.OrderLimit, 100, -3), domain.RejectInvalidQuantity},
		{"价格非正", buy("p1", "alice", "BTCUSD", domain.OrderLimit, 0, 1), domain.RejectInvalidPrice},
		{"价格越界", buy("p2", "alice", "BTCUSD", domain.OrderLimit, 1001, 1), domain.RejectInvalidPrice},
		{"未知账户", buy("u1", "mallory", "BTCUSD", domain.OrderLimit, 100, 1), domain.RejectUnknownAccount},
		{"未知品种", buy("u2", "alice", "DOGE", domain.OrderLimit, 100, 1), domain.RejectUnknownSymbol},
		{"重复ID", sell("dup", "bob", "BTCUSD", domain.OrderLimit, 100, 1), domain.RejectDuplicateID},
		{"市价无流动性", buy("m1", "alice", "ETHUSD", domain.OrderMarket, 0, 1), domain.RejectNoLiquidity},
		{"单笔名义超限", buy("n1", "alice", "BTCUSD", domain.OrderLimit, 100, 600), domain.RejectRiskMaxNotional},
	}
	seqBefore := e.Events().LastSeq()
	for _, tc := range cases {
		r := e.Submit(tc.req)
		if r.Accepted || r.Reject != tc.want {
			t.Errorf("%s: got reject=%q want=%q (detail=%s)", tc.name, r.Reject, tc.want, r.RejectDetail)
		}
		t.Logf("场景[%s] 判定原因=%s 依据=%s", tc.name, r.Reject, r.RejectDetail)
	}
	// 被拒委托不得改变已确定状态：事件序号与簿子不变
	if e.Events().LastSeq() != seqBefore {
		t.Fatalf("rejections appended events: before=%d after=%d", seqBefore, e.Events().LastSeq())
	}
	t.Logf("全部拒绝场景后事件序号仍为=%d（拒绝不产生状态变更）", seqBefore)
}

func TestCancel(t *testing.T) {
	e := testEngine(t)
	e.Submit(sell("s1", "alice", "BTCUSD", domain.OrderLimit, 100, 10))

	// 撤不存在的单
	if r := e.Cancel(CancelRequest{OrderID: "nope"}); r.Reject != domain.RejectCancelNotFound {
		t.Fatalf("cancel missing got=%s", r.Reject)
	}
	r := e.Cancel(CancelRequest{OrderID: "s1"})
	if !r.Cancelled {
		t.Fatal("cancel failed")
	}
	snap := e.Snapshot()
	if len(snap.Books["BTCUSD"].Asks) != 0 {
		t.Fatalf("asks=%v want empty after cancel", snap.Books["BTCUSD"].Asks)
	}
	if st := snap.Accounts["alice/BTCUSD"]; st.OpenCount != 0 || st.OpenSell != 0 {
		t.Fatalf("risk state after cancel=%+v", st)
	}
	// 重复撤单必须拒绝
	if r2 := e.Cancel(CancelRequest{OrderID: "s1"}); r2.Reject != domain.RejectCancelNotFound {
		t.Fatalf("double cancel got=%s", r2.Reject)
	}
	t.Logf("撤单成功: 簿子清空，挂单敞口释放; 事件序号=%d", snap.LastSeq)
}

func TestRiskPositionRejectDoesNotChangeState(t *testing.T) {
	cfg := risk.Config{Default: risk.Limits{MaxPosition: 5}}
	e := New([]string{"BTCUSD"}, cfg, map[string]int64{"BTCUSD": 100}, map[string]bool{"alice": true, "bob": true})
	e.SetLogger(t.Logf)
	e.Submit(sell("s1", "alice", "BTCUSD", domain.OrderLimit, 100, 5))
	// bob 试图买 10：最坏净仓 +10 > 5 => 拒绝
	r := e.Submit(buy("b1", "bob", "BTCUSD", domain.OrderLimit, 100, 10))
	if r.Accepted || r.Reject != domain.RejectRiskMaxPosition {
		t.Fatalf("r=%+v", r)
	}
	snap := e.Snapshot()
	if len(snap.Books["BTCUSD"].Asks) != 1 || snap.Books["BTCUSD"].Asks[0].Quantity != 5 {
		t.Fatalf("book mutated by rejected order: %+v", snap.Books["BTCUSD"].Asks)
	}
	t.Logf("风控拒绝后簿子仍为 100x5，事件序号=%d，状态不变", snap.LastSeq)
}
