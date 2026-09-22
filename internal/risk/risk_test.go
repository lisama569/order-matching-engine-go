package risk

import (
	"testing"

	"github.com/lisama569/order-matching-engine-go/internal/domain"
)

func newTestManager() *Manager {
	cfg := Config{
		MaxPrice: 200,
		Default: Limits{
			MaxPosition:            10,
			MaxOpenOrders:          2,
			MaxExposure:            1000,
			MaxSingleOrderNotional: 500,
		},
	}
	return NewManager(cfg, map[string]int64{"BTCUSD": 100}, map[string]bool{"a1": true}, map[string]bool{"BTCUSD": true})
}

func TestRejectionsAreDistinguishable(t *testing.T) {
	m := newTestManager()
	cases := []struct {
		name   string
		check  Check
		reason domain.RejectReason
	}{
		{"未知账户", Check{AccountID: "ghost", Symbol: "BTCUSD", Side: domain.SideBuy, HasLimitPrice: true, Price: 100, Quantity: 1}, domain.RejectUnknownAccount},
		{"未知品种", Check{AccountID: "a1", Symbol: "X", Side: domain.SideBuy, HasLimitPrice: true, Price: 100, Quantity: 1}, domain.RejectUnknownSymbol},
		{"单笔名义金额超限", Check{AccountID: "a1", Symbol: "BTCUSD", Side: domain.SideBuy, HasLimitPrice: true, Price: 100, Quantity: 6}, domain.RejectRiskMaxNotional},
		{"净持仓超限", Check{AccountID: "a1", Symbol: "BTCUSD", Side: domain.SideBuy, HasLimitPrice: true, Price: 1, Quantity: 1, ProjectedFillQty: 11}, domain.RejectRiskMaxPosition},
		{"挂单笔数超限", Check{AccountID: "a1", Symbol: "BTCUSD", Side: domain.SideBuy, HasLimitPrice: true, Price: 1, Quantity: 1, WillRest: true}, domain.RejectRiskMaxOrders},
		{"敞口超限", Check{AccountID: "a1", Symbol: "BTCUSD", Side: domain.SideBuy, HasLimitPrice: true, Price: 1, Quantity: 1, ProjectedFillQty: 11}, domain.RejectRiskMaxPosition},
	}
	// 先制造 2 笔挂单以触发挂单笔数上限
	m.OnAccepted("a1", "BTCUSD", domain.SideBuy, 1, 1)
	m.OnAccepted("a1", "BTCUSD", domain.SideBuy, 1, 1)
	for _, tc := range cases {
		got := m.CheckOrder(tc.check)
		t.Logf("场景=%s => 判定原因=%s (期望 %s)", tc.name, got, tc.reason)
		if got != tc.reason {
			t.Errorf("%s: got=%s want=%s", tc.name, got, tc.reason)
		}
	}
}

func TestExposureReject(t *testing.T) {
	cfg := Config{Default: Limits{MaxExposure: 500}}
	m := NewManager(cfg, map[string]int64{"BTCUSD": 100}, nil, nil)
	// 买入 6 个 => 敞口 6*100=600 > 500
	got := m.CheckOrder(Check{AccountID: "a", Symbol: "BTCUSD", Side: domain.SideBuy,
		HasLimitPrice: true, Price: 100, Quantity: 6, ProjectedFillQty: 6, ProjectedNotional: 600})
	if got != domain.RejectRiskMaxExposure {
		t.Fatalf("got=%s want RISK_MAX_EXPOSURE", got)
	}
	t.Logf("敞口判定: |净仓|6*参考价100=600 > 上限500 => %s", got)
}

func TestStateFromFillsReconstructs(t *testing.T) {
	// 同一条事件序列应用两遍，状态必须一致（重放幂等性在 Manager 层面的体现）
	run := func() AccountState {
		m := newTestManager()
		// 买入限价 10 全部挂单
		m.OnAccepted("a1", "BTCUSD", domain.SideBuy, 10, 10)
		// 其中 4 个被成交
		m.OnFill("a1", "BTCUSD", domain.SideBuy, 10, 4, true)
		// 剩余 6 撤单
		m.OnCancelled("a1", "BTCUSD", domain.SideBuy, 10, 6)
		return m.State("a1", "BTCUSD")
	}
	s1, s2 := run(), run()
	if s1 != s2 {
		t.Fatalf("replay mismatch: %+v vs %+v", s1, s2)
	}
	if s1.Position != 4 || s1.OpenCount != 0 || s1.OpenBuy != 0 {
		t.Fatalf("state=%+v want position=4 open cleared", s1)
	}
	t.Logf("事件重建状态: %+v (净仓=4, 挂单占用已释放)", s1)
}

func TestCheckDoesNotMutateState(t *testing.T) {
	m := newTestManager()
	before := m.State("a1", "BTCUSD")
	_ = m.CheckOrder(Check{AccountID: "a1", Symbol: "BTCUSD", Side: domain.SideBuy,
		HasLimitPrice: true, Price: 100, Quantity: 100}) // 触发名义金额拒绝
	after := m.State("a1", "BTCUSD")
	if before != after {
		t.Fatalf("reject must not mutate risk state: before=%+v after=%+v", before, after)
	}
}
