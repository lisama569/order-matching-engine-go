package engine

import (
	"errors"
	"log"

	"github.com/lisama569/order-matching-engine-go/internal/event"
	"github.com/lisama569/order-matching-engine-go/internal/market"
	"github.com/lisama569/order-matching-engine-go/internal/match"
	"github.com/lisama569/order-matching-engine-go/internal/pubsub"
	"github.com/lisama569/order-matching-engine-go/internal/risk"
)

// SubmitRequest 下单请求。
type SubmitRequest struct {
	OrderID   string
	AccountID string
	Symbol    string
	Side      market.Side
	Type      market.OrderType
	Price     int64 // 限价单价格（Tick 单位）
	Qty       int64
}

// CancelRequest 撤单请求。
type CancelRequest struct {
	OrderID string
}

// Rejected 拒绝回执。
type Rejected struct {
	OrderID string
	Reason  market.RejectReason
}

func (r Rejected) Error() string {
	return "order " + r.OrderID + " rejected: " + r.Reason.String()
}

// Config 服务配置。
type Config struct {
	Symbol           string
	Limits           market.Limits
	Risk             risk.Config
	SubscriberBuffer int
	// AllowedAccounts 非空时仅允许列名账户提交委托。
	AllowedAccounts map[string]struct{}
}

// state 撮合确定性状态的可整体替换容器（实时与重放共用）。
type state struct {
	book       *match.Engine
	risk       *risk.Manager
	knownOrder map[string]struct{} // 所有曾提交的订单ID（含被拒）
}

func newState(cfg Config) *state {
	return &state{
		book:       match.New(cfg.Symbol),
		risk:       risk.NewManager(cfg.Risk),
		knownOrder: make(map[string]struct{}),
	}
}

// Engine 串行驱动撮合、事件、风控与分发的确定性服务。
type Engine struct {
	cfg   Config
	store *event.Store
	bus   *pubsub.Bus
	st    *state

	submitCh chan submitJob
	cancelCh chan cancelJob
	snapCh   chan chan Snapshot
	eventsCh chan chan []event.Event
	stopCh   chan struct{}
	doneCh   chan struct{}
	running  bool
}

type submitJob struct {
	req      SubmitRequest
	acceptCh chan struct{} // 接受时关闭
	rejectCh chan Rejected // 拒绝时发送原因后关闭
}

type cancelJob struct {
	req      CancelRequest
	resultCh chan bool
}

// Snapshot 订单簿与账户敞口的只读快照（均为聚合/副本值，可安全持有）。
type Snapshot struct {
	BuyLevels  []match.Level
	SellLevels []match.Level
	Accounts   []risk.AccountState
	LastSeq    int64
}

// New 创建服务。
func New(cfg Config) *Engine {
	return &Engine{
		cfg:   cfg,
		store: event.NewStore(),
		bus:   pubsub.New(cfg.SubscriberBuffer),
		st:    newState(cfg),
	}
}

// Start 启动内部串行处理 goroutine。
func (e *Engine) Start() {
	e.submitCh = make(chan submitJob, 128)
	e.cancelCh = make(chan cancelJob, 128)
	e.snapCh = make(chan chan Snapshot)
	e.eventsCh = make(chan chan []event.Event)
	e.stopCh = make(chan struct{})
	e.doneCh = make(chan struct{})
	e.running = true
	go e.loop()
}

func (e *Engine) loop() {
	defer close(e.doneCh)
	for {
		select {
		case <-e.stopCh:
			return
		case job := <-e.submitCh:
			rej := e.processSubmit(job.req)
			if rej.Reason != market.RejectNone {
				job.rejectCh <- rej
				close(job.rejectCh)
			} else {
				close(job.acceptCh)
			}
		case job := <-e.cancelCh:
			job.resultCh <- e.processCancel(job.req)
		case reply := <-e.snapCh:
			reply <- Snapshot{
				BuyLevels:  e.st.book.BuyLevels(),
				SellLevels: e.st.book.SellLevels(),
				Accounts:   e.st.risk.States(),
				LastSeq:    e.store.LastSeq(),
			}
		case ch := <-e.eventsCh:
			ch <- e.store.Events()
		}
	}
}

// Stop 停止服务并关闭分发总线。
func (e *Engine) Stop() {
	if !e.running {
		return
	}
	close(e.stopCh)
	<-e.doneCh
	e.running = false
	e.bus.Close()
}

// Submit 异步提交委托。接受时 acceptCh 关闭；拒绝时 rejectCh 收到原因。
func (e *Engine) Submit(req SubmitRequest) (<-chan struct{}, <-chan Rejected) {
	job := submitJob{
		req:      req,
		acceptCh: make(chan struct{}),
		rejectCh: make(chan Rejected, 1),
	}
	e.submitCh <- job
	return job.acceptCh, job.rejectCh
}

// Cancel 异步撤单，返回是否撤销成功。
func (e *Engine) Cancel(req CancelRequest) <-chan bool {
	job := cancelJob{req: req, resultCh: make(chan bool, 1)}
	e.cancelCh <- job
	return job.resultCh
}

// SubmitAndWait 同步下单，便于测试确定性断言。
func (e *Engine) SubmitAndWait(req SubmitRequest) (bool, Rejected) {
	acc, rej := e.Submit(req)
	select {
	case <-acc:
		return true, Rejected{}
	case r := <-rej:
		return false, r
	}
}

// CancelAndWait 同步撤单。
func (e *Engine) CancelAndWait(req CancelRequest) bool {
	return <-e.Cancel(req)
}

// rejectEvent 在不改变任何确定状态的前提下生成拒绝事件并发布。
// knownOrder 记录属于"请求已受理"层面，仍记录以防同ID重复尝试。
func (e *Engine) rejectAndPublish(req SubmitRequest, reason market.RejectReason) Rejected {
	ev := event.Event{
		Seq:       e.store.LastSeq() + 1,
		Kind:      event.KindOrderRejected,
		OrderID:   req.OrderID,
		AccountID: req.AccountID,
		Symbol:    req.Symbol,
		Side:      req.Side,
		OrderType: req.Type,
		Price:     req.Price,
		Qty:       req.Qty,
		Reason:    reason,
	}
	if err := e.store.Append(ev); err != nil {
		// 理论不可达：单线程序号必然连续。
		log.Printf("engine: append reject event failed: %v", err)
	}
	e.bus.Publish(ev)
	log.Printf("engine: seq=%d REJECT order=%s acct=%s reason=%s", ev.Seq, req.OrderID, req.AccountID, reason)
	return Rejected{OrderID: req.OrderID, Reason: reason}
}

// processSubmit 在串行 goroutine 中处理下单，返回非 None 原因即拒绝。
func (e *Engine) processSubmit(req SubmitRequest) Rejected {
	reason, derived := e.simulate(e.st, req)
	if reason != market.RejectNone {
		return e.rejectAndPublish(req, reason)
	}
	for _, ev := range derived {
		ev.Seq = e.store.LastSeq() + 1
		if err := e.store.Append(ev); err != nil {
			log.Printf("engine: append event failed: %v", err)
		}
		e.bus.Publish(ev)
		log.Printf("engine: %s", ev)
	}
	return Rejected{}
}

// simulate 在给定状态容器上纯本地评估一次下单：
//   - 拒绝时返回可区分原因，derived 为空，除已知ID登记外不改变簿/风控状态；
//   - 接受时返回 RejectNone 与完整派生事件（Seq 未赋值），并就地更新状态。
//
// 实时路径与重放路径共用本函数，以保证撮合结论确定且一致。
func (e *Engine) simulate(st *state, req SubmitRequest) (market.RejectReason, []event.Event) {
	o := &market.Order{
		ID:        req.OrderID,
		AccountID: req.AccountID,
		Symbol:    req.Symbol,
		Side:      req.Side,
		Type:      req.Type,
		Price:     req.Price,
		Qty:       req.Qty,
	}
	// 无论接受与否，同ID再次出现视为重复。
	if _, dup := st.knownOrder[o.ID]; o.ID != "" && dup {
		return market.RejectDuplicateOrderID, nil
	}
	if len(e.cfg.AllowedAccounts) > 0 {
		if _, ok := e.cfg.AllowedAccounts[o.AccountID]; !ok {
			st.knownOrder[o.ID] = struct{}{}
			return market.RejectUnknownAccount, nil
		}
	}
	if reason := market.Validate(o, e.cfg.Limits); reason != market.RejectNone {
		st.knownOrder[o.ID] = struct{}{}
		return reason, nil
	}
	// 市价单若无任何对手流动性，属于不可接受委托（不会产生任何状态变更）。
	if o.Type == market.OrderTypeMarket && !st.book.HasLiquidity(o.Side) {
		st.knownOrder[o.ID] = struct{}{}
		return market.RejectUnacceptableOrder, nil
	}
	// 风控预判：限价以限价为最坏成交价；市价以品种上限价为最坏参考。
	refPrice := o.Price
	if o.Type == market.OrderTypeMarket {
		refPrice = e.cfg.Limits.MaxPrice
	}
	if reason := st.risk.CheckPreTrade(o.AccountID, o.Side, refPrice, o.Qty); reason != market.RejectNone {
		st.knownOrder[o.ID] = struct{}{}
		return reason, nil
	}
	evs := e.deriveSubmit(st, o)
	st.knownOrder[o.ID] = struct{}{}
	return market.RejectNone, evs
}

// deriveSubmit 在给定状态上纯本地驱动一次下单，返回除 Seq 外完整的事件列表，
// 并就地更新状态容器（book/risk）。实时与重放共用，保证结论一致。
func (e *Engine) deriveSubmit(st *state, o *market.Order) []event.Event {
	accepted := event.Event{
		Kind:      event.KindOrderAccepted,
		OrderID:   o.ID,
		AccountID: o.AccountID,
		Symbol:    o.Symbol,
		Side:      o.Side,
		OrderType: o.Type,
		Price:     o.Price,
		Qty:       o.Qty,
	}
	res := st.book.Submit(o)
	evs := []event.Event{accepted}
	for _, f := range res.Fills {
		// taker 方向即下单方向；maker 为对手方向。
		makerSide := market.SideSell
		if o.Side == market.SideSell {
			makerSide = market.SideBuy
		}
		st.risk.ApplyFill(o.AccountID, o.Side, f.Price, f.Qty)
		st.risk.ApplyFill(f.MakerAccount, makerSide, f.Price, f.Qty)
		evs = append(evs, event.Event{
			Kind:         event.KindOrderFilled,
			OrderID:      f.TakerOrderID,
			AccountID:    f.TakerAccount,
			Symbol:       o.Symbol,
			Side:         o.Side,
			OrderType:    o.Type,
			Price:        f.Price,
			Qty:          f.Qty,
			MakerOrderID: f.MakerOrderID,
			MakerAccount: f.MakerAccount,
		})
	}
	if res.Booked {
		evs = append(evs, event.Event{
			Kind:      event.KindOrderBooked,
			OrderID:   o.ID,
			AccountID: o.AccountID,
			Symbol:    o.Symbol,
			Side:      o.Side,
			OrderType: o.Type,
			Price:     o.Price,
			Qty:       res.TakerRemaining,
		})
	}
	return evs
}

func (e *Engine) processCancel(req CancelRequest) bool {
	ev, ok := e.deriveCancel(e.st, req)
	if !ok {
		log.Printf("engine: cancel failed order=%s reason=%s", req.OrderID, market.RejectOrderNotFound)
		return false
	}
	ev.Seq = e.store.LastSeq() + 1
	if err := e.store.Append(ev); err != nil {
		log.Printf("engine: append cancel event failed: %v", err)
	}
	e.bus.Publish(ev)
	log.Printf("engine: %s", ev)
	return true
}

// deriveCancel 在给定状态上派生撤单事件（Seq 未赋值），并就地更新簿。
// 账户信息取自簿中委托，保证重放时撤单事件可完整比对。
func (e *Engine) deriveCancel(st *state, req CancelRequest) (event.Event, bool) {
	if el, qty, ok := st.book.CancelWithOrder(req.OrderID); ok {
		o := el
		return event.Event{
			Kind:      event.KindOrderCanceled,
			OrderID:   o.ID,
			AccountID: o.AccountID,
			Symbol:    o.Symbol,
			Side:      o.Side,
			OrderType: o.Type,
			Price:     o.Price,
			Qty:       qty,
		}, true
	}
	return event.Event{}, false
}

// snapshot 返回当前确定性状态的只读快照。
// 服务运行时经串行循环执行以避免数据竞争；未启动时直接读取。
func (e *Engine) snapshot() Snapshot {
	if e.snapCh != nil {
		ch := make(chan Snapshot, 1)
		e.snapCh <- ch
		return <-ch
	}
	return Snapshot{
		BuyLevels:  e.st.book.BuyLevels(),
		SellLevels: e.st.book.SellLevels(),
		Accounts:   e.st.risk.States(),
		LastSeq:    e.store.LastSeq(),
	}
}

// Snapshot 返回订单簿聚合价位、全部账户敞口与当前事件序号。
func (e *Engine) Snapshot() Snapshot { return e.snapshot() }

// Events 返回已提交事件副本。运行时经串行循环读取。
func (e *Engine) Events() []event.Event {
	if e.eventsCh != nil {
		ch := make(chan []event.Event, 1)
		e.eventsCh <- ch
		return <-ch
	}
	return e.store.Events()
}

// LastSeq 返回当前事件序号。
func (e *Engine) LastSeq() int64 { return e.snapshot().LastSeq }

// BuyLevels / SellLevels 暴露订单簿聚合视图（经串行循环快照）。
func (e *Engine) BuyLevels() []match.Level  { return e.snapshot().BuyLevels }
func (e *Engine) SellLevels() []match.Level { return e.snapshot().SellLevels }

// AccountState 返回账户风控状态（副本）。
func (e *Engine) AccountState(id string) (risk.AccountState, bool) {
	snap := e.snapshot()
	for i := range snap.Accounts {
		if snap.Accounts[i].AccountID == id {
			return snap.Accounts[i], true
		}
	}
	return risk.AccountState{AccountID: id}, false
}

// Bus 返回分发总线以订阅撮合结果。
func (e *Engine) Bus() *pubsub.Bus { return e.bus }

// ErrReplay 事件流重放失败的统一包装。
var ErrReplay = errors.New("replay rejected")
