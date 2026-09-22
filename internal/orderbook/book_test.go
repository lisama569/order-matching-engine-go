package orderbook

import (
	"testing"

	"github.com/lisama569/order-matching-engine-go/internal/domain"
)

func limit(id string, side domain.Side, price, qty int64) *domain.Order {
	return &domain.Order{ID: id, AccountID: "acc-" + id, Symbol: "BTCUSD",
		Side: side, Type: domain.OrderLimit, Price: price, Quantity: qty, Status: domain.StatusNew}
}

func TestPriceTimePriority(t *testing.T) {
	b := New("BTCUSD")
	b.Add(limit("s1", domain.SideSell, 100, 5))
	b.Add(limit("s2", domain.SideSell, 100, 5))
	b.Add(limit("s3", domain.SideSell, 101, 5))

	pfs := b.Simulate(domain.SideBuy, 7, 101, false)
	if len(pfs) != 2 {
		t.Fatalf("projected fills=%d want 2", len(pfs))
	}
	if pfs[0].Maker.ID != "s1" || pfs[0].Quantity != 5 {
		t.Fatalf("first fill=%+v want s1 x5", pfs[0])
	}
	if pfs[1].Maker.ID != "s2" || pfs[1].Quantity != 2 {
		t.Fatalf("second fill=%+v want s2 x2", pfs[1])
	}
	t.Log("判定依据: 价格优先(100<101) 时间优先(s1早于s2) => s1@100x5, s2@100x2")
}

func TestApplyPartialAndFullFill(t *testing.T) {
	b := New("BTCUSD")
	b.Add(limit("s1", domain.SideSell, 100, 5))

	pfs := b.Simulate(domain.SideBuy, 3, 100, false)
	if len(pfs) != 1 || pfs[0].Quantity != 3 {
		t.Fatalf("projection=%+v", pfs)
	}
	closed := b.ApplyFill(pfs[0], pfs[0].Quantity)
	if closed {
		t.Fatal("s1 should remain after partial fill")
	}
	maker, ok := b.Get("s1")
	if !ok || maker.FilledQty != 3 || maker.Remaining() != 2 || maker.Status != domain.StatusNew {
		t.Fatalf("maker after partial: %+v ok=%v", maker, ok)
	}
	bids, asks := b.Depth()
	if len(bids) != 0 || len(asks) != 1 || asks[0].Quantity != 2 {
		t.Fatalf("depth bids=%v asks=%v", bids, asks)
	}
	t.Logf("部分成交: s1 filled=3 remaining=2 档位剩余量=%d", asks[0].Quantity)

	// 再吃 2，全部成交并移出
	pfs2 := b.Simulate(domain.SideBuy, 2, 100, false)
	closed2 := b.ApplyFill(pfs2[0], 2)
	if !closed2 {
		t.Fatal("s1 should be fully filled and removed")
	}
	if _, ok := b.Get("s1"); ok {
		t.Fatal("s1 must be removed from book")
	}
	_, asks = b.Depth()
	if len(asks) != 0 {
		t.Fatalf("asks=%v want empty", asks)
	}
	t.Log("全部成交: s1 status=FILLED 并移出订单簿，档位清空")
}

func TestRemoveAndNoCross(t *testing.T) {
	b := New("BTCUSD")
	b.Add(limit("s1", domain.SideSell, 100, 5))
	if !b.Remove("s1") {
		t.Fatal("remove failed")
	}
	if b.Remove("s1") {
		t.Fatal("second remove must fail")
	}
	// 出价 99 不应越过 100 的卖价
	b.Add(limit("s2", domain.SideSell, 100, 5))
	if pfs := b.Simulate(domain.SideBuy, 5, 99, false); len(pfs) != 0 {
		t.Fatalf("crossed non-crossable book: %+v", pfs)
	}
	t.Log("判定依据: 买限价99 < 最优卖价100，不可成交")
}

func TestMarketConsumesAllLevels(t *testing.T) {
	b := New("BTCUSD")
	b.Add(limit("m1", domain.SideSell, 100, 2))
	b.Add(limit("m2", domain.SideSell, 101, 3))
	pfs := b.Simulate(domain.SideBuy, 4, 0, true)
	if len(pfs) != 2 || pfs[0].Price != 100 || pfs[1].Price != 101 {
		t.Fatalf("market projection=%+v", pfs)
	}
	if pfs[1].Quantity != 2 {
		t.Fatalf("second leg qty=%d want 2 (unfilled market remainder stays unfilled)", pfs[1].Quantity)
	}
	t.Log("市价单依次吃掉 100x2、101x2，剩余1无流动性，由引擎撤销")
}
