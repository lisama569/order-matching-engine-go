package match

import (
	"container/list"
	"sort"

	"github.com/lisama569/order-matching-engine-go/internal/market"
)

// Fill 表示一笔成交的双方数量与价格。
type Fill struct {
	MakerOrderID string
	TakerOrderID string
	MakerAccount string
	TakerAccount string
	Price        int64
	Qty          int64
}

// Result 一次撮合的确定性结果。
type Result struct {
	Fills          []Fill
	TakerRemaining int64 // 撮合后 taker 剩余数量
	Booked         bool  // 剩余部分是否已挂入簿中（限价单）
}

// Level 价位聚合视图。
type Level struct {
	Price int64
	Qty   int64
}

type level struct {
	price  int64
	orders *list.List // 元素值为 *market.Order，FIFO
}

// Engine 单品种限价订单簿撮合器。
// 非并发安全：由上层 engine 单 goroutine 串行驱动。
type Engine struct {
	symbol     string
	orderElem  map[string]*list.Element
	buyLevels  []*level // 买档按价格降序
	sellLevels []*level // 卖档按价格升序
}

// New 创建撮合器。
func New(symbol string) *Engine {
	return &Engine{
		symbol:    symbol,
		orderElem: make(map[string]*list.Element),
	}
}

// HasLiquidity 返回对手盘是否存在任意挂单（用于市价单可接受性判定）。
func (e *Engine) HasLiquidity(takerSide market.Side) bool {
	if takerSide == market.SideBuy {
		return len(e.sellLevels) > 0
	}
	return len(e.buyLevels) > 0
}

// Submit 提交委托并立即撮合；限价单剩余量挂簿，市价单剩余量放弃（IOC 语义）。
// 调用方需保证委托已通过基础合法性与风控校验。
func (e *Engine) Submit(o *market.Order) Result {
	res := Result{TakerRemaining: o.Qty}
	if o.Side == market.SideBuy {
		e.matchBuy(o, &res)
	} else {
		e.matchSell(o, &res)
	}
	// 限价单有剩余 → 挂簿；市价单剩余直接放弃。
	if o.Qty > 0 && o.Type == market.OrderTypeLimit {
		e.book(o)
		res.Booked = true
	}
	res.TakerRemaining = o.Qty
	return res
}

// matchBuy 与卖档（升序，从最低价）撮合。
func (e *Engine) matchBuy(o *market.Order, res *Result) {
	for len(e.sellLevels) > 0 && o.Qty > 0 {
		lv := e.sellLevels[0]
		if o.Type == market.OrderTypeLimit && o.Price < lv.price {
			return
		}
		if !e.consumeLevel(o, res, lv) {
			return
		}
		e.sellLevels = e.sellLevels[1:]
	}
}

// matchSell 与买档（降序，从最高价）撮合。
func (e *Engine) matchSell(o *market.Order, res *Result) {
	for len(e.buyLevels) > 0 && o.Qty > 0 {
		lv := e.buyLevels[0]
		if o.Type == market.OrderTypeLimit && o.Price > lv.price {
			return
		}
		if !e.consumeLevel(o, res, lv) {
			return
		}
		e.buyLevels = e.buyLevels[1:]
	}
}

// consumeLevel 消费一个价位的 FIFO 队列。
// 返回 false 表示 taker 量已耗尽（价位可能仍有剩余，不得摘除该价位）；
// 返回 true 表示该价位已被完全打空，可由调用方摘除。
func (e *Engine) consumeLevel(o *market.Order, res *Result, lv *level) bool {
	for lv.orders.Len() > 0 {
		if o.Qty == 0 {
			return false
		}
		front := lv.orders.Front()
		maker := front.Value.(*market.Order)
		qty := o.Qty
		if maker.Qty < qty {
			qty = maker.Qty
		}
		res.Fills = append(res.Fills, Fill{
			MakerOrderID: maker.ID,
			TakerOrderID: o.ID,
			MakerAccount: maker.AccountID,
			TakerAccount: o.AccountID,
			Price:        lv.price,
			Qty:          qty,
		})
		o.Qty -= qty
		maker.Qty -= qty
		if maker.Qty == 0 {
			lv.orders.Remove(front)
			delete(e.orderElem, maker.ID)
		}
	}
	return true
}

func (e *Engine) book(o *market.Order) {
	if o.Side == market.SideBuy {
		e.buyLevels = e.insertLevel(e.buyLevels, o, false) // 买盘降序
	} else {
		e.sellLevels = e.insertLevel(e.sellLevels, o, true) // 卖盘升序
	}
}

// insertLevel 按价格序插入价位，并把委托追加到价位 FIFO 队尾（时间优先）。
// ascending=true 价位升序（卖盘），false 价位降序（买盘）。
func (e *Engine) insertLevel(lvls []*level, o *market.Order, ascending bool) []*level {
	idx := sort.Search(len(lvls), func(i int) bool {
		if ascending {
			return lvls[i].price >= o.Price
		}
		return lvls[i].price <= o.Price
	})
	if idx < len(lvls) && lvls[idx].price == o.Price {
		el := lvls[idx].orders.PushBack(o)
		e.orderElem[o.ID] = el
		return lvls
	}
	lv := &level{price: o.Price, orders: list.New()}
	el := lv.orders.PushBack(o)
	e.orderElem[o.ID] = el
	lvls = append(lvls, nil)
	copy(lvls[idx+1:], lvls[idx:])
	lvls[idx] = lv
	return lvls
}

// Cancel 撤销委托；返回撤掉的剩余数量；第二个返回值 false 表示订单不在簿中。
func (e *Engine) Cancel(orderID string) (int64, bool) {
	o, qty, ok := e.CancelWithOrder(orderID)
	_ = o
	return qty, ok
}

// CancelWithOrder 撤销委托并返回其在簿中的剩余委托副本（含账户/方向等）与撤销量。
func (e *Engine) CancelWithOrder(orderID string) (*market.Order, int64, bool) {
	el, ok := e.orderElem[orderID]
	if !ok {
		return nil, 0, false
	}
	o := el.Value.(*market.Order)
	qty := o.Qty
	if o.Side == market.SideBuy {
		e.buyLevels = e.removeFromSide(e.buyLevels, el, o.Price, false)
	} else {
		e.sellLevels = e.removeFromSide(e.sellLevels, el, o.Price, true)
	}
	delete(e.orderElem, orderID)
	cp := *o
	return &cp, qty, true
}

// removeFromSide 从指定一侧摘除委托元素；若价位被清空则同时摘除价位。
func (e *Engine) removeFromSide(lvls []*level, el *list.Element, price int64, ascending bool) []*level {
	idx := sort.Search(len(lvls), func(i int) bool {
		if ascending {
			return lvls[i].price >= price
		}
		return lvls[i].price <= price
	})
	if idx >= len(lvls) || lvls[idx].price != price {
		return lvls
	}
	lv := lvls[idx]
	lv.orders.Remove(el)
	if lv.orders.Len() == 0 {
		copy(lvls[idx:], lvls[idx+1:])
		lvls[len(lvls)-1] = nil
		return lvls[:len(lvls)-1]
	}
	return lvls
}

// Levels 返回各价位聚合量。
func (e *Engine) BuyLevels() []Level  { return aggregate(e.buyLevels) }
func (e *Engine) SellLevels() []Level { return aggregate(e.sellLevels) }

func aggregate(lvls []*level) []Level {
	out := make([]Level, 0, len(lvls))
	for _, lv := range lvls {
		var qty int64
		for el := lv.orders.Front(); el != nil; el = el.Next() {
			qty += el.Value.(*market.Order).Qty
		}
		out = append(out, Level{Price: lv.price, Qty: qty})
	}
	return out
}
