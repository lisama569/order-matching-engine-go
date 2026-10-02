// Package engine 串接订单簿、事件日志、风控与发布总线，提供确定性撮合服务。
//
// 确定性保证：
//   - 一把互斥锁串行化全部状态变更，事件在锁内按提交先后取得连续序号；
//   - 实时撮合与断点重放使用同一个 mutate(e) 状态转移函数，因此同一事件流
//     无论重放多少次、从实时还是重建路径得到的订单簿与账户敞口完全一致；
//   - 被拒委托在校验阶段只读判定，不调用 mutate、不追加事件，状态保持不变。
package engine

import (
	"fmt"
	"sync"

	"github.com/lisama569/order-matching-engine-go/internal/domain"
	"github.com/lisama569/order-matching-engine-go/internal/event"
	"github.com/lisama569/order-matching-engine-go/internal/orderbook"
	"github.com/lisama569/order-matching-engine-go/internal/pubsub"
	"github.com/lisama569/order-matching-engine-go/internal/risk"
)

// SubmitRequest 为下单请求。
type SubmitRequest struct {
	OrderID   string
	AccountID string
	Symbol    string
	Side      domain.Side
	Type      domain.OrderType
	Price     int64
	Quantity  int64
}

// SubmitResult 为下单结论（被拒时 Reject 非空且不产生任何状态变更）。
type SubmitResult struct {
	Accepted     bool
	Fills        []domain.Fill
	OrderID      string
	Status       domain.OrderStatus
	Reject       domain.RejectReason
	RejectDetail string
}

// CancelRequest 为撤单请求。
type CancelRequest struct {
	OrderID string
}

// CancelResult 为撤单结论。
type CancelResult struct {
	Cancelled bool
	OrderID   string
	Reject    domain.RejectReason
}

// Snapshot 为引擎当前状态快照（可解释输出）。
type Snapshot struct {
	LastSeq  int64
	Books    map[string]BookView
	Accounts map[string]risk.AccountState // key: accountID + "/" + symbol
}

// BookView 为单品种视图。
type BookView struct {
	Bids []orderbook.PriceLevel
	Asks []orderbook.PriceLevel
}

// Engine 是撮合服务入口。
type Engine struct {
	mu      sync.Mutex
	store   *event.Store
	bus     *pubsub.Bus
	riskMgr *risk.Manager

	symbols         []string
	riskCfg         risk.Config
	refPrices       map[string]int64
	allowedAccounts map[string]bool

	books    map[string]*orderbook.Book
	registry map[string]*domain.Order
	logf     func(format string, args ...any)
	depth    depthPub // 最近一次发布的盘口一致视图（读路径免锁）
}

// New 创建引擎并登记品种与账户风控配置。
func New(symbols []string, riskCfg risk.Config, refPrices map[string]int64,
	allowedAccounts map[string]bool) *Engine {
	e := &Engine{
		store:           event.NewStore(),
		bus:             pubsub.NewBus(),
		symbols:         append([]string(nil), symbols...),
		riskCfg:         riskCfg,
		refPrices:       refPrices,
		allowedAccounts: allowedAccounts,
		books:           make(map[string]*orderbook.Book),
		registry:        make(map[string]*domain.Order),
		logf:            func(string, ...any) {},
	}
	for _, s := range symbols {
		e.books[s] = orderbook.New(s)
	}
	e.riskMgr = risk.NewManager(riskCfg, refPrices, allowedAccounts, symbolSet(symbols))
	e.publishDepth() // 初始空盘口，保证 DepthSnapshot 总有可用视图
	return e
}

// SetLogger 注入判定日志（测试中用于打印事件序号与判定依据）。
func (e *Engine) SetLogger(f func(format string, args ...any)) {
	if f == nil {
		f = func(string, ...any) {}
	}
	e.logf = f
}

func symbolSet(symbols []string) map[string]bool {
	m := make(map[string]bool, len(symbols))
	for _, s := range symbols {
		m[s] = true
	}
	return m
}

func (e *Engine) book(symbol string) *orderbook.Book {
	b := e.books[symbol]
	if b == nil {
		b = orderbook.New(symbol)
		e.books[symbol] = b
	}
	return b
}

func (e *Engine) reject(req SubmitRequest, reason domain.RejectReason, detail string) SubmitResult {
	e.logf("REJECT order=%s account=%s reason=%s detail=%s | 判定依据: %s",
		req.OrderID, req.AccountID, reason, detail, detail)
	return SubmitResult{OrderID: req.OrderID, Reject: reason, RejectDetail: detail}
}

// emit 校验序号、追加事件日志并向总线发布（调用方持锁）。
func (e *Engine) emit(ev event.Event) event.Event {
	if err := e.store.Append(ev); err != nil {
		// 串行化下不应发生；出现即编程错误，直接暴露。
		panic(fmt.Sprintf("append event: %v", err))
	}
	e.logf("EVENT seq=%d type=%s", ev.Seq, ev.Payload.Type)
	e.bus.Publish(ev)
	return ev
}

// nextEvent 为完整负载分配下一个连续序号并 emit。
func (e *Engine) nextEvent(p event.Payload) event.Event {
	return e.emit(event.Event{Seq: e.store.LastSeq() + 1, Payload: p})
}

// Submit 受理并撮合一笔委托。
func (e *Engine) Submit(req SubmitRequest) SubmitResult {
	e.mu.Lock()
	defer e.mu.Unlock()

	// ---- 1. 静态校验（只读，不改变任何状态）----
	if req.Quantity <= 0 {
		return e.reject(req, domain.RejectInvalidQuantity,
			fmt.Sprintf("quantity=%d 必须为正整数", req.Quantity))
	}
	if req.Type == domain.OrderLimit {
		if req.Price <= 0 {
			return e.reject(req, domain.RejectInvalidPrice,
				fmt.Sprintf("limit price=%d 必须为正整数", req.Price))
		}
		if e.riskCfg.MaxPrice > 0 && req.Price > e.riskCfg.MaxPrice {
			return e.reject(req, domain.RejectInvalidPrice,
				fmt.Sprintf("limit price=%d 超过价格上限 %d", req.Price, e.riskCfg.MaxPrice))
		}
	}
	if req.Side != domain.SideBuy && req.Side != domain.SideSell {
		return e.reject(req, domain.RejectInvalidQuantity, "未知委托方向")
	}
	if _, dup := e.registry[req.OrderID]; dup {
		return e.reject(req, domain.RejectDuplicateID, "委托 ID 已存在: "+req.OrderID)
	}

	order := &domain.Order{
		ID:        req.OrderID,
		AccountID: req.AccountID,
		Symbol:    req.Symbol,
		Side:      req.Side,
		Type:      req.Type,
		Price:     req.Price,
		Quantity:  req.Quantity,
		Status:    domain.StatusNew,
	}

	// ---- 2. 只读预撮合（Simulate 不改写簿子）----
	projections := e.book(req.Symbol).Simulate(req.Side, req.Quantity, req.Price, req.Type == domain.OrderMarket)

	var projQty, projNotional int64
	for _, pf := range projections {
		projQty += pf.Quantity
		projNotional += pf.Price * pf.Quantity
	}
	if req.Type == domain.OrderMarket && projQty == 0 {
		return e.reject(req, domain.RejectNoLiquidity, "市价单无对手盘可成交")
	}
	remaining := req.Quantity - projQty
	willRest := req.Type == domain.OrderLimit && remaining > 0

	// ---- 3. 风控校验（只读；拒绝则前面的预撮合结果全部丢弃，状态不变）----
	if reason := e.riskMgr.CheckOrder(risk.Check{
		AccountID:         req.AccountID,
		Symbol:            req.Symbol,
		Side:              req.Side,
		HasLimitPrice:     req.Type == domain.OrderLimit,
		Price:             req.Price,
		Quantity:          req.Quantity,
		ProjectedFillQty:  projQty,
		ProjectedNotional: projNotional,
		RestingQty:        remaining,
		WillRest:          willRest,
	}); reason != "" {
		return e.reject(req, reason, fmt.Sprintf(
			"qty=%d projectedFill=%d projectedNotional=%d willRest=%t",
			req.Quantity, projQty, projNotional, willRest))
	}

	// ---- 4. 提交：确定性事件顺序为“先受理吃单方(原始委托) -> 逐笔成交 -> 市价残余撤销”。
	// 实时撮合与重放走同一个 mutate：受理事件先把吃单方登记进 registry，
	// 成交事件才能同时更新吃单方与挂单方；撤单事件决定市价残余是否离场。
	e.registry[order.ID] = order
	accEv := e.nextEvent(event.Payload{Type: event.TypeOrderAccepted, Order: cloneOrder(order)})
	e.mutate(accEv)

	var fills []domain.Fill
	for _, pf := range projections {
		seq := e.store.LastSeq() + 1
		fill := domain.Fill{
			Sequence:     seq,
			TradeID:      fmt.Sprintf("T%d", seq),
			TakerOrderID: order.ID,
			MakerOrderID: pf.Maker.ID,
			AccountTaker: order.AccountID,
			AccountMaker: pf.Maker.AccountID,
			Symbol:       req.Symbol,
			TakerSide:    req.Side,
			Price:        pf.Price,
			Quantity:     pf.Quantity,
		}
		ev := event.Event{Seq: seq, Payload: event.Payload{Type: event.TypeOrderFilled, Fill: &fill}}
		e.mutate(ev)
		e.emit(ev)
		fills = append(fills, fill)
	}

	// ---- 5. 成交后确定吃单方最终状态；市价单残余立即撤销（IOC），确定性不入簿 ----
	if remaining == 0 {
		order.Status = domain.StatusFilled
	} else if willRest {
		if order.FilledQty > 0 {
			order.Status = domain.StatusPartiallyFilled
		} else {
			order.Status = domain.StatusNew
		}
	} else {
		order.Status = domain.StatusCancelled // 市价单：已成交部分保留，残余撤销
		if !willRest {
			cancelEv := e.nextEvent(event.Payload{Type: event.TypeOrderCancelled, Order: cloneOrder(order)})
			e.mutate(cancelEv)
		}
	}

	e.logf("ACCEPT order=%s status=%s fills=%d remaining=%d | 判定依据: 价格优先时间优先预撮合 + 风控通过",
		order.ID, order.Status, len(fills), remaining)

	e.depth.dirty.Store(true) // 撮合路径只置脏标记（O(1)），视图重建推迟到读取时

	return SubmitResult{
		Accepted: true,
		Fills:    fills,
		OrderID:  order.ID,
		Status:   order.Status,
	}
}

// Cancel 撤销一笔在簿委托。
func (e *Engine) Cancel(req CancelRequest) CancelResult {
	e.mu.Lock()
	defer e.mu.Unlock()

	order, ok := e.registry[req.OrderID]
	if !ok {
		e.logf("REJECT cancel order=%s reason=%s", req.OrderID, domain.RejectCancelNotFound)
		return CancelResult{OrderID: req.OrderID, Reject: domain.RejectCancelNotFound}
	}
	if order.Status == domain.StatusFilled || order.Status == domain.StatusCancelled {
		e.logf("REJECT cancel order=%s reason=%s status=%s", req.OrderID, domain.RejectCancelNotFound, order.Status)
		return CancelResult{OrderID: req.OrderID, Reject: domain.RejectCancelNotFound}
	}
	order.Status = domain.StatusCancelled
	ev := e.nextEvent(event.Payload{Type: event.TypeOrderCancelled, Order: order})
	e.mutate(ev)
	e.depth.dirty.Store(true)
	return CancelResult{Cancelled: true, OrderID: order.ID}
}

// mutate 是唯一的状态转移函数：实时撮合与事件重放共用。
func (e *Engine) mutate(ev event.Event) {
	p := ev.Payload
	switch p.Type {
	case event.TypeOrderFilled:
		f := p.Fill
		bk := e.book(f.Symbol)
		if maker, ok := bk.Get(f.MakerOrderID); ok {
			pf := orderbook.ProjectedFill{Maker: maker, Price: f.Price, Quantity: f.Quantity}
			if closed := bk.ApplyFill(pf, f.Quantity); closed {
				p.ClosedOrder = cloneOrder(maker)
				e.riskMgr.OnRestingClosed(f.AccountMaker, f.Symbol)
			}
		}
		// 吃单方此刻必然已由受理事件登记（实时）或可在重放时登记（前向兼容）。
		taker, exists := e.registry[f.TakerOrderID]
		if !exists {
			taker = &domain.Order{ID: f.TakerOrderID, AccountID: f.AccountTaker,
				Symbol: f.Symbol, Side: f.TakerSide, Type: domain.OrderMarket}
			e.registry[f.TakerOrderID] = taker
		}
		if restingTaker, ok := bk.Get(f.TakerOrderID); ok {
			// 限价吃单方在受理时已挂簿：成交事件同时扣减其档位与成交量，
			// 全成则移出簿子（簿中绝不残留已完全成交的委托）。
			if closed := bk.ApplyFill(orderbook.ProjectedFill{Maker: restingTaker, Price: f.Price, Quantity: f.Quantity}, f.Quantity); closed {
				e.riskMgr.OnRestingClosed(f.AccountTaker, f.Symbol)
			}
		} else {
			taker.FilledQty += f.Quantity
		}
		opp := domain.SideSell
		if f.TakerSide == domain.SideSell {
			opp = domain.SideBuy
		}
		// 限价吃单方的挂单名义占用按限价位随成交释放（受理时按限价位全额计入）；
		// 市价吃单方从未占用，仅更新持仓。
		if taker.Type == domain.OrderLimit {
			e.riskMgr.OnFill(f.AccountTaker, f.Symbol, f.TakerSide, taker.Price, f.Quantity, true)
		} else {
			e.riskMgr.OnFill(f.AccountTaker, f.Symbol, f.TakerSide, f.Price, f.Quantity, false)
		}
		e.riskMgr.OnFill(f.AccountMaker, f.Symbol, opp, f.Price, f.Quantity, true)

	case event.TypeOrderAccepted:
		o := p.Order
		existing, ok := e.registry[o.ID]
		if !ok {
			// 重放路径：登记原始委托（FilledQty=0, Status=NEW），后续成交事件推进状态。
			existing = cloneOrder(o)
			existing.FilledQty = 0
			existing.Status = domain.StatusNew
			e.registry[o.ID] = existing
		}
		existing.Sequence = ev.Seq
		if o.Type == domain.OrderLimit && o.Quantity > o.FilledQty {
			// 受理的是原始委托：挂入全部数量；后续成交事件再逐档扣减。
			e.book(o.Symbol).Add(existing)
			e.riskMgr.OnAccepted(o.AccountID, o.Symbol, o.Side, o.Price, o.Quantity)
		}

	case event.TypeOrderCancelled:
		o := p.Order
		existing, ok := e.registry[o.ID]
		if !ok {
			existing = cloneOrder(o)
			e.registry[o.ID] = existing
		}
		if e.book(o.Symbol).Remove(o.ID) {
			// 市价残余撤销时不在簿中、不占用挂单敞口，仅限价挂单撤单才释放。
			e.riskMgr.OnCancelled(o.AccountID, o.Symbol, o.Side, o.Price, existing.Remaining())
		}
		existing.Status = domain.StatusCancelled
	}
}

func cloneOrder(o *domain.Order) *domain.Order {
	cp := *o
	return &cp
}

// Subscribe 订阅撮合事件。
func (e *Engine) Subscribe(id string, buffer int) *pubsub.Subscriber {
	return e.bus.Subscribe(id, buffer)
}

// Events 返回权威事件存储（重放/追赶用）。
func (e *Engine) Events() *event.Store { return e.store }

// Replay 在干净状态上从事件流重建订单簿与风控状态。
// 序号重复、乱序或存在缺口时由 event.Replay 明确拒绝，状态不会被部分推进。
func (e *Engine) Replay(events []event.Event) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	// 先在临时区域重放，全部成功后才替换线上状态。
	ghost := New(e.symbols, e.riskCfg, e.refPrices, e.allowedAccounts)
	err := event.Replay(events, func(ev event.Event) error {
		if appendErr := ghost.store.Append(ev); appendErr != nil {
			return appendErr
		}
		ghost.mutate(ev)
		return nil
	})
	if err != nil {
		return err
	}

	// 提交重建结果（保留当前总线与订阅者）。
	e.books = ghost.books
	e.registry = ghost.registry
	e.riskMgr = ghost.riskMgr
	e.store = ghost.store
	e.depth.dirty.Store(true)
	return nil
}

// Snapshot 返回可解释的状态视图。
func (e *Engine) Snapshot() Snapshot {
	e.mu.Lock()
	defer e.mu.Unlock()

	snap := Snapshot{LastSeq: e.store.LastSeq(), Books: map[string]BookView{}, Accounts: map[string]risk.AccountState{}}
	for sym, bk := range e.books {
		bids, asks := bk.Depth()
		snap.Books[sym] = BookView{Bids: bids, Asks: asks}
	}
	seen := map[string]bool{}
	for _, o := range e.registry {
		key := o.AccountID + "/" + o.Symbol
		if seen[key] {
			continue
		}
		seen[key] = true
		snap.Accounts[key] = e.riskMgr.State(o.AccountID, o.Symbol)
	}
	return snap
}
