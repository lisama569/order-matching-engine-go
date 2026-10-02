// Package audit 账实一致性审计：把盘口汇总、账户敞口与名义占用、事件进度
// 三者互相比对。任何一处对不上都判定不通过，并给出不一致类别与
// 差异大致所在的事件序号，保证可复现定位。
//
// 本包只做纯函数判定（Check），数据由引擎在锁内采集后传入。
package audit

import (
	"fmt"
	"sort"

	"github.com/lisama569/order-matching-engine-go/internal/domain"
	"github.com/lisama569/order-matching-engine-go/internal/event"
	"github.com/lisama569/order-matching-engine-go/internal/risk"
)

// Kind 为不一致类别编码。
type Kind string

const (
	KindSeqDiscontinuity  Kind = "SEQ_DISCONTINUITY"   // 事件序号不连续
	KindEventHashMismatch Kind = "EVENT_HASH_MISMATCH" // 事件流滚动哈希与日志记录不符
	KindBookDepthMismatch Kind = "BOOK_DEPTH_MISMATCH" // 盘口汇总与挂单明细对不上
	KindOpenOrderMismatch Kind = "OPEN_ORDER_MISMATCH" // 账户挂单笔数/名义占用与挂单明细对不上
	KindPositionMismatch  Kind = "POSITION_MISMATCH"   // 账户净持仓与成交事件推导结果对不上
)

// Finding 为一条不一致记录：类别 + 涉及对象 + 差异大致所在事件序号 + 细节。
type Finding struct {
	Kind      Kind
	Symbol    string
	AccountID string
	OrderID   string
	Seq       int64 // 差异大致落在该事件序号附近
	Detail    string
}

// Report 为一次审计的结论。Pass=false 时 Findings 非空。
type Report struct {
	Pass          bool
	Findings      []Finding
	Skipped       []string // 因检查点基线等原因降级跳过的检查项
	LastSeq       int64
	CheckedEvents int64 // 本次核对过的事件条数
}

// Level 为一个价格档位的汇总（价位、剩余总量、挂单笔数）。
type Level struct {
	Side     domain.Side
	Price    int64
	Quantity int64
	Orders   int
}

// BookDepth 为单品种买卖盘口汇总。
type BookDepth struct {
	Bids []Level
	Asks []Level
}

// OrderLine 为一笔挂单的明细行。
type OrderLine struct {
	OrderID   string
	AccountID string
	Symbol    string
	Side      domain.Side
	Price     int64
	Remaining int64
	AcceptSeq int64 // 受理事件序号（定位用）
}

// AccountLine 为一个账户-品种的风控状态行。
type AccountLine struct {
	AccountID string
	Symbol    string
	State     risk.AccountState
}

// Input 为审计输入：事件进度 + 盘口汇总 + 挂单明细 + 账户状态。
type Input struct {
	Base       int64         // 事件日志的检查点基线（0 表示持有完整事件流）
	Events     []event.Event // 当前持有的事件（基线之后）
	StoreHash  uint64        // 事件日志记录的滚动哈希
	Depth      map[string]BookDepth
	BookOrders []OrderLine // 订单簿中实际挂着的委托明细
	Resting    []OrderLine // 注册表中按状态应当挂着的委托明细
	Accounts   []AccountLine
}

type acctSym struct {
	account string
	symbol  string
}

// Check 执行全部一致性比对，返回审计结论。
func Check(in Input) Report {
	rep := Report{Pass: true}
	if n := len(in.Events); n > 0 {
		rep.CheckedEvents = int64(n)
		rep.LastSeq = in.Events[n-1].Seq
	} else {
		rep.LastSeq = in.Base
	}

	checkSequence(in, &rep)
	checkBookDepth(in, &rep)
	checkOpenOrders(in, &rep)
	checkPositions(in, &rep)

	rep.Pass = len(rep.Findings) == 0
	return rep
}

func (r *Report) fail(f Finding) {
	r.Findings = append(r.Findings, f)
}

// checkSequence 核对事件进度：序号必须从基线 +1 开始严格连续；
// 持有完整事件流时（Base==0）同时重算滚动哈希与日志记录比对。
func checkSequence(in Input, rep *Report) {
	for i, ev := range in.Events {
		expected := in.Base + int64(i) + 1
		if ev.Seq != expected {
			rep.fail(Finding{
				Kind:   KindSeqDiscontinuity,
				Symbol: eventSymbol(ev),
				Seq:    ev.Seq,
				Detail: fmt.Sprintf("事件序号断裂: 位置 %d 处序号=%d, 期望=%d", i, ev.Seq, expected),
			})
			return // 序号已断，后续比对意义不大，定位第一处即可
		}
	}
	if in.Base == 0 {
		if got := event.HashOf(in.Events); got != in.StoreHash {
			rep.fail(Finding{
				Kind:   KindEventHashMismatch,
				Seq:    rep.LastSeq,
				Detail: fmt.Sprintf("事件流重算哈希=%d 与日志记录=%d 不符", got, in.StoreHash),
			})
		}
	} else {
		rep.Skipped = append(rep.Skipped,
			fmt.Sprintf("EVENT_HASH: 基线 seq=%d 之前的事件已由检查点覆盖，仅校验基线后序号连续性", in.Base))
	}
}

func eventSymbol(ev event.Event) string {
	if ev.Payload.Order != nil {
		return ev.Payload.Order.Symbol
	}
	if ev.Payload.Fill != nil {
		return ev.Payload.Fill.Symbol
	}
	return ""
}

type levelKey struct {
	symbol string
	side   domain.Side
	price  int64
}

// checkBookDepth 核对盘口汇总与挂单明细：
// 每档剩余总量必须等于该价位全部挂单剩余量之和，笔数等于挂单笔数；
// 且簿中明细与注册表"应挂单"集合必须互为充要。
func checkBookDepth(in Input, rep *Report) {
	// 1) 簿中明细聚合成档位，与盘口汇总逐档比对。
	agg := map[levelKey]Level{}
	for _, ol := range in.BookOrders {
		k := levelKey{ol.Symbol, ol.Side, ol.Price}
		lv := agg[k]
		lv.Side, lv.Price = ol.Side, ol.Price
		lv.Quantity += ol.Remaining
		lv.Orders++
		agg[k] = lv
	}
	seen := map[levelKey]bool{}
	for sym, bd := range in.Depth {
		for _, lv := range append(append([]Level(nil), bd.Bids...), bd.Asks...) {
			k := levelKey{sym, lv.Side, lv.Price}
			seen[k] = true
			a, ok := agg[k]
			if !ok {
				rep.fail(Finding{Kind: KindBookDepthMismatch, Symbol: sym, Seq: rep.LastSeq,
					Detail: fmt.Sprintf("盘口档位 %s@%d(%d笔,%d量) 在挂单明细中不存在",
						lv.Side, lv.Price, lv.Orders, lv.Quantity)})
				continue
			}
			if a.Quantity != lv.Quantity || a.Orders != lv.Orders {
				rep.fail(Finding{Kind: KindBookDepthMismatch, Symbol: sym, Seq: rep.LastSeq,
					Detail: fmt.Sprintf("档位 %s@%d: 盘口汇总=%d量/%d笔, 明细合计=%d量/%d笔",
						lv.Side, lv.Price, lv.Quantity, lv.Orders, a.Quantity, a.Orders)})
			}
		}
	}
	for k, a := range agg {
		if !seen[k] {
			rep.fail(Finding{Kind: KindBookDepthMismatch, Symbol: k.symbol, Seq: rep.LastSeq,
				Detail: fmt.Sprintf("挂单明细存在档位 %s@%d(%d笔,%d量)，但盘口汇总缺失",
					k.side, k.price, a.Orders, a.Quantity)})
		}
	}

	// 2) 簿中明细与注册表应挂单集合必须一致（双向）。
	bookByID := map[string]OrderLine{}
	for _, ol := range in.BookOrders {
		bookByID[ol.OrderID] = ol
	}
	restByID := map[string]OrderLine{}
	for _, ol := range in.Resting {
		restByID[ol.OrderID] = ol
		b, ok := bookByID[ol.OrderID]
		if !ok {
			rep.fail(Finding{Kind: KindBookDepthMismatch, Symbol: ol.Symbol, OrderID: ol.OrderID, Seq: ol.AcceptSeq,
				Detail: fmt.Sprintf("委托 %s 状态为挂单(剩余%d)但不在订单簿中", ol.OrderID, ol.Remaining)})
			continue
		}
		if b.Remaining != ol.Remaining || b.Price != ol.Price || b.Side != ol.Side {
			rep.fail(Finding{Kind: KindBookDepthMismatch, Symbol: ol.Symbol, OrderID: ol.OrderID, Seq: ol.AcceptSeq,
				Detail: fmt.Sprintf("委托 %s 簿中(%s@%d 剩%d)与注册表(%s@%d 剩%d)不符",
					ol.OrderID, b.Side, b.Price, b.Remaining, ol.Side, ol.Price, ol.Remaining)})
		}
	}
	for _, ol := range in.BookOrders {
		if _, ok := restByID[ol.OrderID]; !ok {
			rep.fail(Finding{Kind: KindBookDepthMismatch, Symbol: ol.Symbol, OrderID: ol.OrderID, Seq: ol.AcceptSeq,
				Detail: fmt.Sprintf("委托 %s 挂在簿中(剩余%d)但注册表状态不是挂单", ol.OrderID, ol.Remaining)})
		}
	}
}

// checkOpenOrders 核对账户挂单笔数与名义占用：由挂单明细重算期望占用，
// 与风控状态逐账户比对。
func checkOpenOrders(in Input, rep *Report) {
	type expect struct {
		count             int
		openBuy, openSell int64
		maxSeq            int64
	}
	exp := map[acctSym]*expect{}
	for _, ol := range in.Resting {
		k := acctSym{ol.AccountID, ol.Symbol}
		ex := exp[k]
		if ex == nil {
			ex = &expect{}
			exp[k] = ex
		}
		ex.count++
		if ol.Side == domain.SideBuy {
			ex.openBuy += ol.Price * ol.Remaining
		} else {
			ex.openSell += ol.Price * ol.Remaining
		}
		if ol.AcceptSeq > ex.maxSeq {
			ex.maxSeq = ol.AcceptSeq
		}
	}
	actual := map[acctSym]risk.AccountState{}
	for _, al := range in.Accounts {
		actual[acctSym{al.AccountID, al.Symbol}] = al.State
	}
	keys := map[acctSym]bool{}
	for k := range exp {
		keys[k] = true
	}
	for k := range actual {
		keys[k] = true
	}
	sorted := make([]acctSym, 0, len(keys))
	for k := range keys {
		sorted = append(sorted, k)
	}
	sort.Slice(sorted, func(i, j int) bool {
		if sorted[i].account != sorted[j].account {
			return sorted[i].account < sorted[j].account
		}
		return sorted[i].symbol < sorted[j].symbol
	})
	for _, k := range sorted {
		ex := exp[k]
		if ex == nil {
			ex = &expect{}
		}
		st := actual[k]
		if st.OpenCount == ex.count && st.OpenBuy == ex.openBuy && st.OpenSell == ex.openSell {
			continue
		}
		seq := ex.maxSeq
		if seq == 0 {
			seq = rep.LastSeq
		}
		rep.fail(Finding{Kind: KindOpenOrderMismatch, Symbol: k.symbol, AccountID: k.account, Seq: seq,
			Detail: fmt.Sprintf("账户 %s/%s 挂单占用: 风控记录=%d笔/买%d/卖%d, 挂单明细重算=%d笔/买%d/卖%d",
				k.account, k.symbol, st.OpenCount, st.OpenBuy, st.OpenSell, ex.count, ex.openBuy, ex.openSell)})
	}
}

// checkPositions 从成交事件重算账户净持仓，与风控状态比对。
// 基线之前的事件不在手边时（Base>0）无法完整重算，降级跳过。
func checkPositions(in Input, rep *Report) {
	if in.Base > 0 {
		rep.Skipped = append(rep.Skipped,
			fmt.Sprintf("POSITION: 基线 seq=%d 之前的成交事件不在手边，跳过重算比对", in.Base))
		return
	}
	pos := map[acctSym]int64{}
	lastSeq := map[acctSym]int64{}
	for _, ev := range in.Events {
		if ev.Payload.Type != event.TypeOrderFilled || ev.Payload.Fill == nil {
			continue
		}
		f := ev.Payload.Fill
		apply := func(account string, side domain.Side) {
			k := acctSym{account, f.Symbol}
			if side == domain.SideBuy {
				pos[k] += f.Quantity
			} else {
				pos[k] -= f.Quantity
			}
			lastSeq[k] = ev.Seq
		}
		apply(f.AccountTaker, f.TakerSide)
		opp := domain.SideSell
		if f.TakerSide == domain.SideSell {
			opp = domain.SideBuy
		}
		apply(f.AccountMaker, opp)
	}
	actual := map[acctSym]risk.AccountState{}
	for _, al := range in.Accounts {
		actual[acctSym{al.AccountID, al.Symbol}] = al.State
	}
	keys := map[acctSym]bool{}
	for k := range pos {
		keys[k] = true
	}
	for k := range actual {
		keys[k] = true
	}
	for k := range keys {
		want := pos[k]
		got := actual[k].Position
		if want != got {
			rep.fail(Finding{Kind: KindPositionMismatch, Symbol: k.symbol, AccountID: k.account,
				Seq: lastSeq[k],
				Detail: fmt.Sprintf("账户 %s/%s 净持仓: 风控记录=%d, 成交事件重算=%d",
					k.account, k.symbol, got, want)})
		}
	}
}
