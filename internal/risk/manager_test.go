package risk

import (
	"testing"

	"github.com/lisama569/order-matching-engine-go/internal/market"
)

func TestPositionAndExposure(t *testing.T) {
	m := NewManager(Config{MaxLongPosition: 100, MaxShortPosition: 50, MaxExposure: 5000})
	if r := m.CheckPreTrade("a", market.SideBuy, 10, 101); r != market.RejectRiskPositionLimit {
		t.Fatalf("long breach = %s", r)
	}
	if r := m.CheckPreTrade("a", market.SideBuy, 60, 100); r != market.RejectRiskExposureLimit {
		t.Fatalf("exposure breach = %s", r)
	}
	if r := m.CheckPreTrade("a", market.SideSell, 10, 51); r != market.RejectRiskPositionLimit {
		t.Fatalf("short breach = %s", r)
	}
	if r := m.CheckPreTrade("a", market.SideBuy, 10, 10); r != market.RejectNone {
		t.Fatalf("valid order rejected: %s", r)
	}
	m.ApplyFill("a", market.SideBuy, 10, 10)
	st, _ := m.State("a")
	if st.Position != 10 || st.Exposure != 100 {
		t.Fatalf("state = %+v", st)
	}
	// 再买 95 会使持仓达 105 > 100。
	if r := m.CheckPreTrade("a", market.SideBuy, 10, 95); r != market.RejectRiskPositionLimit {
		t.Fatalf("post-fill long breach = %s", r)
	}
	// 市价无有效参考价且配置了敞口 → 保守拒绝。
	if r := m.CheckPreTrade("a", market.SideBuy, 0, 1); r != market.RejectRiskExposureLimit {
		t.Fatalf("no reference price = %s", r)
	}
}

func TestNoLimit(t *testing.T) {
	m := NewManager(Config{})
	if r := m.CheckPreTrade("a", market.SideBuy, 0, 1<<40); r != market.RejectNone {
		t.Fatalf("unlimited config should pass, got %s", r)
	}
}
