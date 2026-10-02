// Package orderbook 实现单品种限价订单簿：价格优先、时间优先撮合。
package orderbook

import "github.com/lisama569/order-matching-engine-go/internal/domain"

// ProjectedFill 描述一笔吃单委托的预撮合成交（不改写簿子状态）。
type ProjectedFill struct {
	Maker    *domain.Order
	Price    int64
	Quantity int64
}

// level 是同一价格的时间优先队列。
type level struct {
	price    int64
	totalQty int64
	orders   []*domain.Order // 队首最早，FIFO
}

type loc struct {
	side  domain.Side
	price int64
}

// Book 是单品种订单簿。所有方法非线程安全；由 engine 串行化调用以保证确定性。
type Book struct {
	symbol string
	bids   []*level // 价格降序
	asks   []*level // 价格升序
	index  map[string]loc
}

// New 创建指定品种的订单簿。
func New(symbol string) *Book {
	return &Book{symbol: symbol, index: make(map[string]loc)}
}

// Symbol 返回品种。
func (b *Book) Symbol() string { return b.symbol }

// sideLevels 返回该方向挂单所在档位切片（买单在 bids，卖单在 asks）。
func (b *Book) sideLevels(side domain.Side) []*level {
	if side == domain.SideBuy {
		return b.bids
	}
	return b.asks
}

// levelAt 返回（不存在则创建）指定价格档位，保持价格有序。
func (b *Book) levelAt(side domain.Side, price int64, create bool) *level {
	levels := b.sideLevels(side)
	i := 0
	if side == domain.SideBuy {
		// bids 降序：找到第一个 <= price 的位置
		for i < len(levels) && levels[i].price > price {
			i++
		}
	} else {
		// asks 升序：找到第一个 >= price 的位置
		for i < len(levels) && levels[i].price < price {
			i++
		}
	}
	if i < len(levels) && levels[i].price == price {
		return levels[i]
	}
	if !create {
		return nil
	}
	lv := &level{price: price}
	levels = append(levels, nil)
	copy(levels[i+1:], levels[i:])
	levels[i] = lv
	if side == domain.SideBuy {
		b.bids = levels
	} else {
		b.asks = levels
	}
	return lv
}

// Add 将限价委托挂入簿子尾部（同价位时间优先）。
func (b *Book) Add(o *domain.Order) {
	lv := b.levelAt(o.Side, o.Price, true)
	lv.orders = append(lv.orders, o)
	lv.totalQty += o.Remaining()
	b.index[o.ID] = loc{side: o.Side, price: o.Price}
}

// Remove 从簿子移除委托；不存在返回 false。
func (b *Book) Remove(orderID string) bool {
	where, ok := b.index[orderID]
	if !ok {
		return false
	}
	lv := b.levelAt(where.side, where.price, false)
	if lv == nil {
		return false
	}
	for i, o := range lv.orders {
		if o.ID == orderID {
			lv.totalQty -= o.Remaining()
			lv.orders = append(lv.orders[:i], lv.orders[i+1:]...)
			break
		}
	}
	delete(b.index, orderID)
	if len(lv.orders) == 0 {
		b.dropLevel(where.side, where.price)
	}
	return true
}

func (b *Book) dropLevel(side domain.Side, price int64) {
	var levels []*level
	if side == domain.SideBuy {
		levels = b.bids
	} else {
		levels = b.asks
	}
	for i, lv := range levels {
		if lv.price == price {
			levels = append(levels[:i], levels[i+1:]...)
			break
		}
	}
	if side == domain.SideBuy {
		b.bids = levels
	} else {
		b.asks = levels
	}
}

// Get 返回簿中委托。
func (b *Book) Get(orderID string) (*domain.Order, bool) {
	where, ok := b.index[orderID]
	if !ok {
		return nil, false
	}
	lv := b.levelAt(where.side, where.price, false)
	if lv == nil {
		return nil, false
	}
	for _, o := range lv.orders {
		if o.ID == orderID {
			return o, true
		}
	}
	return nil, false
}

// crossable 判断挂单价能否与吃单成交。
func crossable(takerSide domain.Side, makerPrice, limitPrice int64, market bool) bool {
	if market {
		return true
	}
	if takerSide == domain.SideBuy {
		return makerPrice <= limitPrice // 买：卖价不高于出价
	}
	return makerPrice >= limitPrice // 卖：买价不低于要价
}

// Simulate 在不改写状态的前提下预撮合一笔吃单：
// 吃买单遍历 asks（升序），吃卖单遍历 bids（降序）；同价位按 FIFO。
func (b *Book) Simulate(side domain.Side, remaining int64, limitPrice int64, market bool) []ProjectedFill {
	if remaining <= 0 {
		return nil
	}
	levels := b.asks
	if side == domain.SideSell {
		levels = b.bids
	}
	var out []ProjectedFill
	for _, lv := range levels {
		if remaining == 0 {
			break
		}
		if !crossable(side, lv.price, limitPrice, market) {
			break // 价格已越过可成交范围（对手盘单调排序，后续更差）
		}
		for _, maker := range lv.orders {
			if remaining == 0 {
				break
			}
			qty := maker.Remaining()
			if qty > remaining {
				qty = remaining
			}
			if qty <= 0 {
				continue
			}
			out = append(out, ProjectedFill{Maker: maker, Price: lv.price, Quantity: qty})
			remaining -= qty
		}
	}
	return out
}

// ApplyFill 将一笔预撮合成交落实到挂单方：累加成交数量，全成则移出队列。
// 返回 maker 是否被完全成交。
func (b *Book) ApplyFill(pf ProjectedFill, qty int64) bool {
	where, ok := b.index[pf.Maker.ID]
	if !ok {
		return false
	}
	lv := b.levelAt(where.side, where.price, false)
	if lv == nil {
		return false
	}
	lv.totalQty -= qty
	pf.Maker.FilledQty += qty
	closed := false
	if pf.Maker.Remaining() == 0 {
		pf.Maker.Status = domain.StatusFilled
		for i, o := range lv.orders {
			if o.ID == pf.Maker.ID {
				lv.orders = append(lv.orders[:i], lv.orders[i+1:]...)
				break
			}
		}
		delete(b.index, pf.Maker.ID)
		closed = true
	}
	if len(lv.orders) == 0 {
		b.dropLevel(where.side, where.price)
	}
	return closed
}

// SettleFill 吃单方成交回写：限价吃单方在受理时已整单入簿，
// 其每笔成交必须同步扣减所在档位总量；剩余量降为 0 时移出簿子。
// 返回 (该委托是否在簿, 是否因本次成交被移出)。不在簿（市价吃单方）时返回 (false, false)。
// 注意：委托的 FilledQty 由调用方（引擎状态转移）先行累加，本函数只维护档位结构。
func (b *Book) SettleFill(orderID string, qty int64) (resting bool, closed bool) {
	where, ok := b.index[orderID]
	if !ok {
		return false, false
	}
	lv := b.levelAt(where.side, where.price, false)
	if lv == nil {
		return false, false
	}
	lv.totalQty -= qty
	for i, o := range lv.orders {
		if o.ID == orderID {
			if o.Remaining() == 0 {
				lv.orders = append(lv.orders[:i], lv.orders[i+1:]...)
				delete(b.index, orderID)
				closed = true
			}
			break
		}
	}
	if len(lv.orders) == 0 {
		b.dropLevel(where.side, where.price)
	}
	return true, closed
}

// BestBid / BestAsk 返回最优价；无档位返回 (0,false)。
func (b *Book) BestBid() (int64, bool) {
	if len(b.bids) == 0 {
		return 0, false
	}
	return b.bids[0].price, true
}

func (b *Book) BestAsk() (int64, bool) {
	if len(b.asks) == 0 {
		return 0, false
	}
	return b.asks[0].price, true
}

// PriceLevel 为某价格档位的聚合视图。
type PriceLevel struct {
	Price    int64
	Quantity int64
	Orders   int
}

// Depth 返回买卖档位深度（用于可解释输出/测试）。
func (b *Book) Depth() (bids []PriceLevel, asks []PriceLevel) {
	for _, lv := range b.bids {
		bids = append(bids, PriceLevel{Price: lv.price, Quantity: lv.totalQty, Orders: len(lv.orders)})
	}
	for _, lv := range b.asks {
		asks = append(asks, PriceLevel{Price: lv.price, Quantity: lv.totalQty, Orders: len(lv.orders)})
	}
	return bids, asks
}

// LevelState 是一个价格档位的可序列化状态：价位 + 该档挂单的 FIFO 顺序。
// 委托本体由引擎注册表持有，这里只记录 ID 顺序，供检查点恢复簿子队列。
type LevelState struct {
	Side     domain.Side
	Price    int64
	OrderIDs []string // 队首最早，FIFO
}

// ExportLevels 按档位导出簿子结构（买盘降序、卖盘升序，档内 FIFO）。
func (b *Book) ExportLevels() []LevelState {
	var out []LevelState
	for _, lv := range b.bids {
		out = append(out, exportLevel(domain.SideBuy, lv))
	}
	for _, lv := range b.asks {
		out = append(out, exportLevel(domain.SideSell, lv))
	}
	return out
}

func exportLevel(side domain.Side, lv *level) LevelState {
	ids := make([]string, len(lv.orders))
	for i, o := range lv.orders {
		ids[i] = o.ID
	}
	return LevelState{Side: side, Price: lv.price, OrderIDs: ids}
}

// RestingOrders 返回簿中全部挂单（按档位顺序），供审计与检查点核对明细。
func (b *Book) RestingOrders() []*domain.Order {
	var out []*domain.Order
	for _, lv := range b.bids {
		out = append(out, lv.orders...)
	}
	for _, lv := range b.asks {
		out = append(out, lv.orders...)
	}
	return out
}
