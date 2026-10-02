package engine

import (
	"github.com/lisama569/order-matching-engine-go/internal/audit"
	"github.com/lisama569/order-matching-engine-go/internal/domain"
	"github.com/lisama569/order-matching-engine-go/internal/orderbook"
)

// Audit 从事件流与当前状态出发做一次账实一致性对账：
// 盘口汇总 vs 挂单明细、账户敞口与名义占用 vs 挂单明细、
// 事件进度（序号连续性 + 滚动哈希）三者互相比对。
//
// 只要存在对不上的地方，本次审计即判定不通过；每条 Finding 都带有
// 不一致类别与差异大致所在的事件序号，可据此复现定位。
// 审计在锁内只读采集，不改变任何状态。
func (e *Engine) Audit() audit.Report {
	e.mu.Lock()
	defer e.mu.Unlock()

	in := audit.Input{
		Base:      e.store.Base(),
		Events:    e.store.All(),
		StoreHash: e.store.Hash(),
		Depth:     map[string]audit.BookDepth{},
	}
	for sym, bk := range e.books {
		bids, asks := bk.Depth()
		in.Depth[sym] = audit.BookDepth{
			Bids: toAuditLevels(domain.SideBuy, bids),
			Asks: toAuditLevels(domain.SideSell, asks),
		}
		for _, o := range bk.RestingOrders() {
			in.BookOrders = append(in.BookOrders, audit.OrderLine{
				OrderID: o.ID, AccountID: o.AccountID, Symbol: o.Symbol,
				Side: o.Side, Price: o.Price, Remaining: o.Remaining(), AcceptSeq: o.Sequence,
			})
		}
	}
	for _, o := range e.registry {
		// 注册表中"应处于挂单"的委托：限价单且未终结且有剩余。
		if o.Type == domain.OrderLimit && o.Remaining() > 0 &&
			(o.Status == domain.StatusNew || o.Status == domain.StatusPartiallyFilled) {
			in.Resting = append(in.Resting, audit.OrderLine{
				OrderID: o.ID, AccountID: o.AccountID, Symbol: o.Symbol,
				Side: o.Side, Price: o.Price, Remaining: o.Remaining(), AcceptSeq: o.Sequence,
			})
		}
	}
	for _, ae := range e.riskMgr.ExportStates() {
		in.Accounts = append(in.Accounts, audit.AccountLine{
			AccountID: ae.AccountID, Symbol: ae.Symbol, State: ae.State,
		})
	}

	rep := audit.Check(in)
	if rep.Pass {
		e.logf("AUDIT pass lastSeq=%d checkedEvents=%d skipped=%d", rep.LastSeq, rep.CheckedEvents, len(rep.Skipped))
	} else {
		for _, f := range rep.Findings {
			e.logf("AUDIT FAIL kind=%s symbol=%s account=%s order=%s nearSeq=%d detail=%s",
				f.Kind, f.Symbol, f.AccountID, f.OrderID, f.Seq, f.Detail)
		}
	}
	return rep
}

func toAuditLevels(side domain.Side, levels []orderbook.PriceLevel) []audit.Level {
	out := make([]audit.Level, len(levels))
	for i, lv := range levels {
		out[i] = audit.Level{Side: side, Price: lv.Price, Quantity: lv.Quantity, Orders: lv.Orders}
	}
	return out
}
