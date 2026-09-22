package match

import (
	"testing"

	"github.com/lisama569/order-matching-engine-go/internal/market"
)

func lo(id, acct string, side market.Side, price, qty int64) *market.Order {
	return &market.Order{ID: id, AccountID: acct, Symbol: "S", Side: side,
		Type: market.OrderTypeLimit, Price: price, Qty: qty}
}

func mo(id, acct string, side market.Side, qty int64) *market.Order {
	return &market.Order{ID: id, AccountID: acct, Symbol: "S", Side: side,
		Type: market.OrderTypeMarket, Qty: qty}
}

func TestPriceTimePriority(t *testing.T) {
	e := New("S")
	e.Submit(lo("a1", "A", market.SideSell, 101, 10))
	e.Submit(lo("a2", "B", market.SideSell, 100, 5)) // 更优价
	e.Submit(lo("a3", "C", market.SideSell, 100, 7)) // 同价时间靠后
	res := e.Submit(lo("b1", "D", market.SideBuy, 100, 8))
	if len(res.Fills) != 2 {
		t.Fatalf("want 2 fills got %d", len(res.Fills))
	}
	// 必须先成交更优价 100 的 a2，再成交 a3。
	if res.Fills[0].MakerOrderID != "a2" || res.Fills[0].Qty != 5 {
		t.Fatalf("fill0 = %s/%d", res.Fills[0].MakerOrderID, res.Fills[0].Qty)
	}
	if res.Fills[1].MakerOrderID != "a3" || res.Fills[1].Qty != 3 {
		t.Fatalf("fill1 = %s/%d", res.Fills[1].MakerOrderID, res.Fills[1].Qty)
	}
	// 成交价取挂单价 100，而非吃单价。
	if res.Fills[0].Price != 100 {
		t.Fatalf("price = %d", res.Fills[0].Price)
	}
	// 101 的卖单不应被动到。
	sells := e.SellLevels()
	if len(sells) != 2 || sells[0].Price != 100 || sells[0].Qty != 4 || sells[1].Price != 101 {
		t.Fatalf("sells = %+v", sells)
	}
}

func TestMarketIOC(t *testing.T) {
	e := New("S")
	e.Submit(lo("s1", "A", market.SideSell, 50, 5))
	// HasLiquidity(takerSide) 报告对手盘是否有挂单。
	if !e.HasLiquidity(market.SideBuy) {
		t.Fatal("market BUY must find sell-side liquidity")
	}
	if e.HasLiquidity(market.SideSell) {
		t.Fatal("market SELL must find no buy-side liquidity")
	}
	// 部分成交：市价买 10，只成交 5，剩余放弃，不挂簿。
	res := e.Submit(mo("m1", "B", market.SideBuy, 10))
	if res.Booked || res.TakerRemaining != 5 || len(res.Fills) != 1 {
		t.Fatalf("market partial: %+v", res)
	}
	if len(e.SellLevels()) != 0 || len(e.BuyLevels()) != 0 {
		t.Fatalf("market remainder must not be booked")
	}
}

func TestNoCrossNoTrade(t *testing.T) {
	e := New("S")
	e.Submit(lo("s1", "A", market.SideSell, 100, 5))
	res := e.Submit(lo("b1", "B", market.SideBuy, 99, 5))
	if len(res.Fills) != 0 || !res.Booked {
		t.Fatalf("non-crossing order should rest: %+v", res)
	}
	if len(e.BuyLevels()) != 1 || e.BuyLevels()[0].Price != 99 {
		t.Fatalf("buy level = %+v", e.BuyLevels())
	}
}

func TestCancelFromLevel(t *testing.T) {
	e := New("S")
	e.Submit(lo("b1", "A", market.SideBuy, 90, 3))
	e.Submit(lo("b2", "B", market.SideBuy, 90, 4))
	qty, ok := e.Cancel("b1")
	if !ok || qty != 3 {
		t.Fatalf("cancel b1 = %d/%v", qty, ok)
	}
	lv := e.BuyLevels()
	if len(lv) != 1 || lv[0].Qty != 4 {
		t.Fatalf("after cancel level = %+v", lv)
	}
	if _, ok := e.Cancel("b1"); ok {
		t.Fatal("double cancel must fail")
	}
	if _, ok := e.Cancel("nope"); ok {
		t.Fatal("cancel missing must fail")
	}
}
