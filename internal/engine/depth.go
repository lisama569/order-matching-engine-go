package engine

import (
	"sync/atomic"

	"github.com/lisama569/order-matching-engine-go/internal/orderbook"
)

// DepthOrder 为盘口某价位档上一笔未成交挂单的明细视图。
type DepthOrder struct {
	OrderID   string
	AccountID string
	Remaining int64
}

// DepthLevel 为盘口某价位档的聚合视图：价位、该档剩余总量、挂单笔数，
// 并附带该档全部挂单明细（用于与聚合结果严格对账）。
type DepthLevel struct {
	Price    int64
	Quantity int64
	Orders   int
	Details  []DepthOrder
}

// SymbolDepth 为单品种盘口快照（买盘降序、卖盘升序）。
type SymbolDepth struct {
	Symbol string
	Bids   []DepthLevel
	Asks   []DepthLevel
}

// DepthView 为某一确定时刻（LastSeq 对应的状态）全部品种盘口的一致视图。
//
// 一致性保证：视图在撮合锁内、一次完整状态变更结束后一次性构建发布，
// 读取方通过原子加载获取，因此：
//   - 绝不会观察到“只成交了一半”的中间态；
//   - 读路径不阻塞撮合（见 DepthSnapshot 的懒发布说明）；
//   - 每档 Quantity 恒等于该档 Details 中各挂单 Remaining 之和。
//
// 返回的视图发布后不再被引擎修改，调用方应将其视为只读。
type DepthView struct {
	LastSeq int64
	Symbols map[string]SymbolDepth
}

// depthPub 保存最近一次发布的盘口视图（atomic.Value 存 *DepthView）。
// dirty 标记“状态已变、视图待重建”：撮合路径只置位（O(1)），
// 视图的重建被推迟到真正有人读取时，连续变更天然合并为一次重建。
type depthPub struct {
	v     atomic.Value
	dirty atomic.Bool
}

func (p *depthPub) load() *DepthView {
	if v := p.v.Load(); v != nil {
		return v.(*DepthView)
	}
	return nil
}

func (p *depthPub) store(d *DepthView) { p.v.Store(d) }

// DepthSnapshot 返回盘口一致视图，绝不阻塞撮合：
//   - 状态未变：直接返回已发布视图（原子加载，无锁）；
//   - 状态已变且撮合锁空闲：就地重建并发布最新视图；
//   - 状态已变但撮合正在进行：返回最近一次发布的一致视图
//     （对应某个完整变更批次之后的状态，绝不是中间态）。
//
// 无论走哪条路径，返回的视图都是自洽的：每档剩余总量等于该档明细之和。
func (e *Engine) DepthSnapshot() DepthView {
	if e.depth.dirty.Load() && e.mu.TryLock() {
		e.publishDepth()
		e.mu.Unlock()
	}
	if d := e.depth.load(); d != nil {
		return *d
	}
	return DepthView{Symbols: map[string]SymbolDepth{}}
}

// publishDepth 在当前状态上重建并发布盘口视图（调用方持锁）。
// 发布出去的永远是一个完整变更批次之后的状态。
func (e *Engine) publishDepth() {
	defer e.depth.dirty.Store(false)
	view := &DepthView{LastSeq: e.store.LastSeq(), Symbols: make(map[string]SymbolDepth, len(e.books))}
	for sym, bk := range e.books {
		bids, asks := bk.DepthDetailed()
		view.Symbols[sym] = SymbolDepth{
			Symbol: sym,
			Bids:   convertLevels(bids),
			Asks:   convertLevels(asks),
		}
	}
	e.depth.store(view)
}

func convertLevels(in []orderbook.LevelDetail) []DepthLevel {
	out := make([]DepthLevel, 0, len(in))
	for _, lv := range in {
		dl := DepthLevel{Price: lv.Price, Quantity: lv.Quantity, Orders: len(lv.Orders)}
		for _, o := range lv.Orders {
			dl.Details = append(dl.Details, DepthOrder{
				OrderID: o.OrderID, AccountID: o.AccountID, Remaining: o.Remaining})
		}
		out = append(out, dl)
	}
	return out
}
