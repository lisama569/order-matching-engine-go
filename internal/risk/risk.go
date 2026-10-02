// Package risk 提供账户持仓与敞口风控，并可从成交事件重建风控状态。
//
// 风控状态全部是成交/挂单事件的派生结果：实时撮合与事件重放走同一套更新函数，
// 因此重建后的账户敞口与实时状态严格一致。零值限额表示该检查项关闭。
package risk

import (
	"sort"

	"github.com/lisama569/order-matching-engine-go/internal/domain"
)

// Limits 是单账户风控限额配置（零值表示不限制）。
type Limits struct {
	MaxPosition            int64 // 净持仓数量绝对值上限（按预计成交后的最坏净仓判定）
	MaxOpenOrders          int   // 最大同时挂单（未成交委托）笔数
	MaxExposure            int64 // 持仓名义敞口上限 = |净持仓| * 参考价
	MaxSingleOrderNotional int64 // 单笔委托名义金额上限
}

// Config 为风控配置。
type Config struct {
	MaxPrice int64 // 限价单允许的最高价格（0 表示不限制）
	Default  Limits
	Accounts map[string]Limits // accountID -> 覆盖
}

// AccountState 为账户在某品种上的风控视图。
type AccountState struct {
	Position  int64 // 净持仓（买为正、卖为负）
	OpenBuy   int64 // 未成交买单占用名义金额
	OpenSell  int64 // 未成交卖单占用名义金额
	OpenCount int   // 未成交委托笔数
}

// Check 是委托受理前的风控判定输入。
type Check struct {
	AccountID         string
	Symbol            string
	Side              domain.Side
	HasLimitPrice     bool  // 限价单为 true
	Price             int64 // 限价单价格
	Quantity          int64 // 委托总量
	ProjectedFillQty  int64 // 本次确定将成交的数量
	ProjectedNotional int64 // 本次确定将成交的名义金额（价*量之和）
	RestingQty        int64 // 成交后将挂入簿子的剩余数量
	WillRest          bool  // 成交后是否会有剩余挂入簿子
}

type acctKey struct {
	account string
	symbol  string
}

// Manager 管理全部账户的风控状态，线程安全性由 engine 串行化保证。
type Manager struct {
	cfg             Config
	limits          map[string]Limits
	refPrices       map[string]int64
	allowedAccounts map[string]bool // nil 表示不限制
	allowedSymbols  map[string]bool // nil 表示不限制
	states          map[acctKey]*AccountState
}

// NewManager 创建风控管理器。
func NewManager(cfg Config, refPrices map[string]int64, allowedAccounts, allowedSymbols map[string]bool) *Manager {
	limits := make(map[string]Limits, len(cfg.Accounts))
	for k, v := range cfg.Accounts {
		limits[k] = v
	}
	return &Manager{
		cfg:             cfg,
		limits:          limits,
		refPrices:       refPrices,
		allowedAccounts: allowedAccounts,
		allowedSymbols:  allowedSymbols,
		states:          make(map[acctKey]*AccountState),
	}
}

// RegisterAccount 登记账户及其限额覆盖。
func (m *Manager) RegisterAccount(id string, l Limits) {
	m.limits[id] = l
	if m.allowedAccounts != nil {
		m.allowedAccounts[id] = true
	}
}

func (m *Manager) limitOf(account string) Limits {
	if l, ok := m.limits[account]; ok {
		return l
	}
	return m.cfg.Default
}

// IsAccountKnown / IsSymbolKnown 判定委托主体是否可接受。
func (m *Manager) IsAccountKnown(id string) bool {
	if m.allowedAccounts == nil {
		return true
	}
	return m.allowedAccounts[id]
}

func (m *Manager) IsSymbolKnown(s string) bool {
	if m.allowedSymbols == nil {
		return true
	}
	return m.allowedSymbols[s]
}

func (m *Manager) state(k acctKey) *AccountState {
	st := m.states[k]
	if st == nil {
		st = &AccountState{}
		m.states[k] = st
	}
	return st
}

// CheckOrder 执行委托受理前校验，返回第一个触发的拒绝原因；通过返回空串。
// 校验只读，不改变任何状态——被拒委托不产生已确定状态变更。
func (m *Manager) CheckOrder(c Check) domain.RejectReason {
	if !m.IsAccountKnown(c.AccountID) {
		return domain.RejectUnknownAccount
	}
	if !m.IsSymbolKnown(c.Symbol) {
		return domain.RejectUnknownSymbol
	}
	lim := m.limitOf(c.AccountID)
	st := m.state(acctKey{c.AccountID, c.Symbol})

	// 1) 单笔委托名义金额：限价单按限价位全额（最坏情形），市价单按预计成交额。
	notional := c.ProjectedNotional
	if c.HasLimitPrice {
		notional = c.Price * c.Quantity
	}
	if lim.MaxSingleOrderNotional > 0 && notional > lim.MaxSingleOrderNotional {
		return domain.RejectRiskMaxNotional
	}

	// 2) 净持仓（最坏情形）：已确定成交量立即改变净仓；
	// 限价挂单的剩余量按最坏假设（最终也成交）计入，避免挂单堆积在限额边缘。
	net := st.Position
	worst := c.ProjectedFillQty
	if c.HasLimitPrice {
		worst += c.RestingQty
	}
	if worst > 0 {
		if c.Side == domain.SideBuy {
			net += worst
		} else {
			net -= worst
		}
	}
	if lim.MaxPosition > 0 {
		abs := net
		if abs < 0 {
			abs = -abs
		}
		if abs > lim.MaxPosition {
			return domain.RejectRiskMaxPosition
		}
	}

	// 3) 挂单笔数：仅当成交后有剩余挂单时占用一笔。
	if c.WillRest && lim.MaxOpenOrders > 0 && st.OpenCount+1 > lim.MaxOpenOrders {
		return domain.RejectRiskMaxOrders
	}

	// 4) 持仓名义敞口：成交后 |净仓| * 参考价。
	if lim.MaxExposure > 0 {
		ref := m.refPrices[c.Symbol]
		if ref <= 0 {
			ref = c.Price
		}
		abs := net
		if abs < 0 {
			abs = -abs
		}
		if ref > 0 && abs*ref > lim.MaxExposure {
			return domain.RejectRiskMaxExposure
		}
	}
	return ""
}

// OnAccepted 记录一笔已受理且将挂入簿子的委托占用（挂单数量与名义金额）。
func (m *Manager) OnAccepted(accountID, symbol string, side domain.Side, price, qty int64) {
	st := m.state(acctKey{accountID, symbol})
	st.OpenCount++
	if side == domain.SideBuy {
		st.OpenBuy += price * qty
	} else {
		st.OpenSell += price * qty
	}
}

// OnFill 依据成交更新持仓，并释放被成交部分对应的挂单名义占用（resting=true 表示成交方为挂单方）。
func (m *Manager) OnFill(accountID, symbol string, side domain.Side, price, qty int64, resting bool) {
	st := m.state(acctKey{accountID, symbol})
	if resting {
		if side == domain.SideBuy {
			st.OpenBuy -= price * qty
		} else {
			st.OpenSell -= price * qty
		}
	}
	if side == domain.SideBuy {
		st.Position += qty
	} else {
		st.Position -= qty
	}
}

// OnRestingClosed 记录一笔挂单被完全成交而移出簿子：释放其挂单笔数占用。
// （名义占用由 OnFill 随每笔成交释放，这里只收尾笔数。）
func (m *Manager) OnRestingClosed(accountID, symbol string) {
	st := m.state(acctKey{accountID, symbol})
	if st.OpenCount > 0 {
		st.OpenCount--
	}
}

// OnCancelled 释放撤单的挂单占用。
func (m *Manager) OnCancelled(accountID, symbol string, side domain.Side, price, remainingQty int64) {
	st := m.state(acctKey{accountID, symbol})
	if st.OpenCount > 0 {
		st.OpenCount--
	}
	if side == domain.SideBuy {
		st.OpenBuy -= price * remainingQty
	} else {
		st.OpenSell -= price * remainingQty
	}
	if st.OpenBuy < 0 {
		st.OpenBuy = 0
	}
	if st.OpenSell < 0 {
		st.OpenSell = 0
	}
}

// State 返回账户-品种风控状态副本（可解释用）。
func (m *Manager) State(accountID, symbol string) AccountState {
	return *m.state(acctKey{accountID, symbol})
}

// AccountEntry 是一个账户-品种的风控状态条目（检查点导出/导入用）。
type AccountEntry struct {
	AccountID string
	Symbol    string
	State     AccountState
}

// ExportStates 导出全部账户风控状态（确定性排序：先账户后品种）。
func (m *Manager) ExportStates() []AccountEntry {
	out := make([]AccountEntry, 0, len(m.states))
	for k, st := range m.states {
		out = append(out, AccountEntry{AccountID: k.account, Symbol: k.symbol, State: *st})
	}
	sort.Slice(out, func(i, j int) bool {
		if out[i].AccountID != out[j].AccountID {
			return out[i].AccountID < out[j].AccountID
		}
		return out[i].Symbol < out[j].Symbol
	})
	return out
}

// ImportStates 以检查点中的账户状态整体替换当前状态（增量重建用）。
func (m *Manager) ImportStates(entries []AccountEntry) {
	m.states = make(map[acctKey]*AccountState, len(entries))
	for _, e := range entries {
		st := e.State
		m.states[acctKey{e.AccountID, e.Symbol}] = &st
	}
}
