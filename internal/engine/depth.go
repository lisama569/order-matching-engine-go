package engine

// DepthSnapshot 是某一事件序号时刻的盘口汇总视图（买盘/卖盘各档：
// 价位、该档剩余总量、该档挂单笔数）。
//
// 一致性保证：快照只在每次状态变更批次（一笔提交/撤单的全部事件）提交后、
// 同一把锁内重新发布，因此任何时刻取到的都是某个事件序号上的完整视图，
// 绝不暴露"只成交了一半"的中间态；每档剩余总量严格等于该价位全部
// 未成交挂单剩余量之和。
type DepthSnapshot struct {
	Seq   int64               // 该视图对应的事件序号
	Books map[string]BookView // 品种 -> 买卖档位
}

// Depth 返回最近一次发布的盘口快照。
//
// 非阻塞：读取走 atomic.Value，不取撮合锁，并发下单/撤单/成交期间
// 随时可取，既不会卡住撮合，也不会影响事件追加与分发。
func (e *Engine) Depth() DepthSnapshot {
	p, _ := e.depth.Load().(*DepthSnapshot)
	if p == nil {
		return DepthSnapshot{Books: map[string]BookView{}}
	}
	return *p
}

// publishDepthLocked 在持锁状态下重建并发布盘口视图（调用方须已持有 e.mu）。
// 每次发布都是全新分配的不可变对象，读者持有的旧快照不受影响。
func (e *Engine) publishDepthLocked() {
	snap := &DepthSnapshot{Seq: e.store.LastSeq(), Books: make(map[string]BookView, len(e.books))}
	for sym, bk := range e.books {
		bids, asks := bk.Depth()
		snap.Books[sym] = BookView{Bids: bids, Asks: asks}
	}
	e.depth.Store(snap)
}
