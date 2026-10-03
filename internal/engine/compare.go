package engine

import (
	"sort"

	"github.com/lisama569/order-matching-engine-go/internal/orderbook"

	"github.com/lisama569/order-matching-engine-go/internal/domain"
)

// OrderView 是委托登记表中一笔委托的可比较视图（逐字段：最终状态/成交量/受理序号等）。
type OrderView struct {
	ID        string
	AccountID string
	Symbol    string
	Side      domain.Side
	Type      domain.OrderType
	Price     int64
	Quantity  int64
	FilledQty int64
	Status    domain.OrderStatus
	Sequence  int64
}

// Orders 返回全部委托的逐字段视图（按 ID 排序），用于重建一致性比对。
func (e *Engine) Orders() []OrderView {
	e.mu.Lock()
	defer e.mu.Unlock()
	out := make([]OrderView, 0, len(e.registry))
	for _, o := range e.registry {
		out = append(out, OrderView{
			ID: o.ID, AccountID: o.AccountID, Symbol: o.Symbol,
			Side: o.Side, Type: o.Type, Price: o.Price, Quantity: o.Quantity,
			FilledQty: o.FilledQty, Status: o.Status, Sequence: o.Sequence,
		})
	}
	sort.Slice(out, func(i, j int) bool { return out[i].ID < out[j].ID })
	return out
}

// StatesEqual 判定两台引擎的完整派生状态逐字段一致：
// LastSeq、每笔委托（最终状态/成交量/受理序号）、盘口档位、账户敞口。
func StatesEqual(a, b *Engine) bool {
	a.mu.Lock()
	defer a.mu.Unlock()
	b.mu.Lock()
	defer b.mu.Unlock()

	as, bs := a.snapshotLocked(), b.snapshotLocked()
	if as.LastSeq != bs.LastSeq {
		return false
	}
	if len(as.booksView) != len(bs.booksView) {
		return false
	}
	for sym, ab := range as.booksView {
		bb, ok := bs.booksView[sym]
		if !ok || !priceLevelsEqual(ab.Bids, bb.Bids) || !priceLevelsEqual(ab.Asks, bb.Asks) {
			return false
		}
	}
	if len(as.accountsView) != len(bs.accountsView) {
		return false
	}
	for k, av := range as.accountsView {
		if bv, ok := bs.accountsView[k]; !ok || av != bv {
			return false
		}
	}
	if len(a.registry) != len(b.registry) {
		return false
	}
	for id, ao := range a.registry {
		bo, ok := b.registry[id]
		if !ok || *ao != *bo {
			return false
		}
	}
	return true
}

type lockedSnapshot struct {
	LastSeq      int64
	booksView    map[string]BookView
	accountsView map[string]acctView
}

type acctView = struct {
	Position  int64
	OpenBuy   int64
	OpenSell  int64
	OpenCount int
}

func (e *Engine) snapshotLocked() lockedSnapshot {
	s := lockedSnapshot{LastSeq: e.store.LastSeq(),
		booksView:    map[string]BookView{},
		accountsView: map[string]acctView{}}
	for sym, bk := range e.books {
		bids, asks := bk.Depth()
		s.booksView[sym] = BookView{Bids: bids, Asks: asks}
	}
	seen := map[string]bool{}
	for _, o := range e.registry {
		key := o.AccountID + "/" + o.Symbol
		if seen[key] {
			continue
		}
		seen[key] = true
		st := e.riskMgr.State(o.AccountID, o.Symbol)
		s.accountsView[key] = acctView{
			Position: st.Position, OpenBuy: st.OpenBuy,
			OpenSell: st.OpenSell, OpenCount: st.OpenCount}
	}
	return s
}

func priceLevelsEqual(a, b []orderbook.PriceLevel) bool {
	if len(a) != len(b) {
		return false
	}
	for i := range a {
		if a[i] != b[i] {
			return false
		}
	}
	return true
}
