package risk

import (
	"sort"

	"github.com/lisama569/order-matching-engine-go/internal/market"
)

// AccountState 账户持仓与敞口状态。
type AccountState struct {
	AccountID string
	Position  int64 // 净持仓：买入为正，卖出为负
	Exposure  int64 // 名义敞口：每笔成交 |qty*price| 的累计
}

// Config 风控配置。0 表示对应项不限制。
type Config struct {
	MaxLongPosition  int64 // 最大多头持仓
	MaxShortPosition int64 // 最大空头持仓绝对值
	MaxExposure      int64 // 最大累计名义敞口
}

// Manager 账户维度持仓/敞口风控。非并发安全，由上层串行驱动。
type Manager struct {
	cfg      Config
	accounts map[string]*AccountState
}

// NewManager 创建风控管理器。
func NewManager(cfg Config) *Manager {
	return &Manager{cfg: cfg, accounts: make(map[string]*AccountState)}
}

// CheckPreTrade 下单前风控：以委托最坏情形（全部立即成交）预判。
// 价格必须为预期成交参考价：限价单用其限价；市价单由调用方给出
// 最坏参考价（如品种上限价），无法给出参考价而又配置了敞口限额时拒绝。
// 返回 RejectNone 表示通过，否则返回可区分拒绝原因。
func (m *Manager) CheckPreTrade(accountID string, side market.Side, price, qty int64) market.RejectReason {
	st := m.accounts[accountID]
	if st == nil {
		st = &AccountState{AccountID: accountID}
	}
	// 持仓限额预判。
	if side == market.SideBuy && m.cfg.MaxLongPosition > 0 {
		if st.Position+qty > m.cfg.MaxLongPosition {
			return market.RejectRiskPositionLimit
		}
	}
	if side == market.SideSell && m.cfg.MaxShortPosition > 0 {
		if st.Position-qty < -m.cfg.MaxShortPosition {
			return market.RejectRiskPositionLimit
		}
	}
	// 敞口限额预判。
	if m.cfg.MaxExposure > 0 {
		if price <= 0 {
			// 无有效参考价无法保证不越限，保守拒绝。
			return market.RejectRiskExposureLimit
		}
		notional := price * qty
		if st.Exposure+notional > m.cfg.MaxExposure {
			return market.RejectRiskExposureLimit
		}
	}
	return market.RejectNone
}

// ApplyFill 成交后更新账户状态。side 为该账户在此笔成交中的方向。
func (m *Manager) ApplyFill(accountID string, side market.Side, price, qty int64) {
	st := m.accounts[accountID]
	if st == nil {
		st = &AccountState{AccountID: accountID}
		m.accounts[accountID] = st
	}
	switch side {
	case market.SideBuy:
		st.Position += qty
	case market.SideSell:
		st.Position -= qty
	}
	st.Exposure += price * qty
}

// State 返回账户状态副本。
func (m *Manager) State(accountID string) (AccountState, bool) {
	st, ok := m.accounts[accountID]
	if !ok {
		return AccountState{AccountID: accountID}, false
	}
	return *st, true
}

// States 返回全部账户状态副本（按账户ID排序，保证输出确定性）。
func (m *Manager) States() []AccountState {
	ids := make([]string, 0, len(m.accounts))
	for id := range m.accounts {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	out := make([]AccountState, 0, len(ids))
	for _, id := range ids {
		out = append(out, *m.accounts[id])
	}
	return out
}
