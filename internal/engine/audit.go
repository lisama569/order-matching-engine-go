package engine

import (
	"fmt"
	"sort"

	"github.com/lisama569/order-matching-engine-go/internal/domain"
	"github.com/lisama569/order-matching-engine-go/internal/event"
	"github.com/lisama569/order-matching-engine-go/internal/orderbook"
)

// DiscrepancyKind 为账实不一致的分类。
type DiscrepancyKind string

const (
	// DiscSequence 事件进度不一致（序号缺口/重复/乱序）。
	DiscSequence DiscrepancyKind = "SEQUENCE"
	// DiscBook 盘口汇总与挂单明细不一致。
	DiscBook DiscrepancyKind = "BOOK"
	// DiscAccount 账户敞口/名义占用与事件流重推结果不一致。
	DiscAccount DiscrepancyKind = "ACCOUNT"
)

// Discrepancy 为一条可复现定位的不一致记录。
type Discrepancy struct {
	Kind     DiscrepancyKind
	Subject  string // 品种+价位 / 账户键 / event-log
	Expected string
	Actual   string
	NearSeq  int64 // 差异大致落在哪个事件序号附近
}

// AuditReport 为一次账实一致性审计的结论。
type AuditReport struct {
	OK            bool
	LastSeq       int64
	CheckedEvents int64 // 参与对账的事件数
	FullCoverage  bool  // false 表示日志含已归档前缀（检查点之前的事件不在本日志），持仓绝对值未独立重推
	Discrepancies []Discrepancy
}

// Audit 从事件流与当前状态出发对账，三方互相比对：
//
//  1. 事件进度：日志持有的事件序号必须从 base+1 起严格连续；
//  2. 盘口汇总 vs 挂单明细：registry 中未成交挂单按价位重算的聚合，
//     必须与订单簿档位（剩余总量/笔数）逐项相等；
//  3. 账户敞口 vs 事件重推：用日志持有的事件在影子引擎重推，
//     与当前风控状态逐字段比对（全覆盖时）；名义占用另以
//     “registry 在簿挂单重算 vs 当前风控占用”自洽校验（总是生效）。
//
// 任一不一致都使审计不通过；每条差异都带类别与大致事件序号（NearSeq）。
func (e *Engine) Audit() AuditReport {
	e.mu.Lock()
	defer e.mu.Unlock()

	events := e.store.All()
	rep := AuditReport{
		OK:            true,
		LastSeq:       e.store.LastSeq(),
		CheckedEvents: int64(len(events)),
		FullCoverage:  e.store.Base() == 0,
	}
	add := func(d Discrepancy) {
		rep.OK = false
		rep.Discrepancies = append(rep.Discrepancies, d)
		e.logf("AUDIT 不一致 kind=%s subject=%s expected=%s actual=%s nearSeq=%d",
			d.Kind, d.Subject, d.Expected, d.Actual, d.NearSeq)
	}

	// ---- 1. 事件进度连续性 ----
	seqOK := true
	expected := e.store.Base() + 1
	seen := map[int64]bool{}
	for _, ev := range events {
		if ev.Seq != expected {
			kind := "GAP"
			if ev.Seq < expected {
				if seen[ev.Seq] {
					kind = "DUPLICATE"
				} else {
					kind = "OUT_OF_ORDER"
				}
			}
			add(Discrepancy{
				Kind: DiscSequence, Subject: "event-log",
				Expected: fmt.Sprintf("seq=%d", expected),
				Actual:   fmt.Sprintf("seq=%d (%s)", ev.Seq, kind),
				NearSeq:  ev.Seq,
			})
			seqOK = false
			break // 序号链已断，后续位置无意义
		}
		seen[ev.Seq] = true
		expected++
	}

	// ---- 2. 盘口汇总 vs 挂单明细（registry 重算 vs 订单簿档位）----
	type lvlKey struct {
		symbol string
		side   domain.Side
		price  int64
	}
	type lvlAgg struct {
		qty     int64
		orders  int
		lastSeq int64 // 该档挂单的最大受理序号，用于定位
	}
	sideName := func(s domain.Side) string { return s.String() }

	want := map[lvlKey]*lvlAgg{}
	for _, o := range e.registry {
		if o.Type != domain.OrderLimit || o.Remaining() <= 0 ||
			(o.Status != domain.StatusNew && o.Status != domain.StatusPartiallyFilled) {
			continue
		}
		k := lvlKey{o.Symbol, o.Side, o.Price}
		a := want[k]
		if a == nil {
			a = &lvlAgg{}
			want[k] = a
		}
		a.qty += o.Remaining()
		a.orders++
		if o.Sequence > a.lastSeq {
			a.lastSeq = o.Sequence
		}
	}
	got := map[lvlKey]*lvlAgg{}
	for sym, bk := range e.books {
		bids, asks := bk.DepthDetailed()
		collect := func(side domain.Side, lvs []orderbook.LevelDetail) {
			for _, lv := range lvs {
				a := &lvlAgg{qty: lv.Quantity, orders: len(lv.Orders)}
				for _, o := range lv.Orders {
					if o.Seq > a.lastSeq {
						a.lastSeq = o.Seq
					}
				}
				got[lvlKey{sym, side, lv.Price}] = a
			}
		}
		collect(domain.SideBuy, bids)
		collect(domain.SideSell, asks)
	}
	keys := map[lvlKey]bool{}
	for k := range want {
		keys[k] = true
	}
	for k := range got {
		keys[k] = true
	}
	sorted := make([]lvlKey, 0, len(keys))
	for k := range keys {
		sorted = append(sorted, k)
	}
	sort.Slice(sorted, func(i, j int) bool {
		a, b := sorted[i], sorted[j]
		if a.symbol != b.symbol {
			return a.symbol < b.symbol
		}
		if a.side != b.side {
			return a.side < b.side
		}
		return a.price < b.price
	})
	for _, k := range sorted {
		w, g := want[k], got[k]
		wq, wo := int64(0), 0
		var near int64
		if w != nil {
			wq, wo, near = w.qty, w.orders, w.lastSeq
		}
		gq, go_ := int64(0), 0
		if g != nil {
			gq, go_ = g.qty, g.orders
			if g.lastSeq > near {
				near = g.lastSeq // 簿中档位一侧也能提供定位（明细被抹掉时）
			}
		}
		if wq != gq || wo != go_ {
			add(Discrepancy{
				Kind:     DiscBook,
				Subject:  fmt.Sprintf("%s %s@%d", k.symbol, sideName(k.side), k.price),
				Expected: fmt.Sprintf("挂单明细重算 qty=%d orders=%d", wq, wo),
				Actual:   fmt.Sprintf("盘口档位 qty=%d orders=%d", gq, go_),
				NearSeq:  near,
			})
		}
	}

	// ---- 3. 账户敞口 vs 事件重推 / 名义占用自洽 ----
	// 3a. 影子引擎重推日志持有的事件。
	var ghost *Engine
	if seqOK {
		ghost = New(e.symbols, e.riskCfg, e.refPrices, e.allowedAccounts)
		ghost.store.SetBase(e.store.Base())
		if err := event.ReplayFrom(events, e.store.Base()+1, func(ev event.Event) error {
			if appendErr := ghost.store.Append(ev); appendErr != nil {
				return appendErr
			}
			ghost.mutate(ev)
			return nil
		}); err != nil {
			ghost = nil // 重推失败（序号问题已在第 1 步报告）
		}
	}

	// 账户键集合：registry 中出现的 account/symbol。
	type acctKey struct {
		account, symbol string
	}
	accts := map[acctKey]bool{}
	for _, o := range e.registry {
		accts[acctKey{o.AccountID, o.Symbol}] = true
	}
	// 每个账户键最后被事件触及的序号（定位用）。
	lastTouch := map[acctKey]int64{}
	touch := func(account, symbol string, seq int64) {
		k := acctKey{account, symbol}
		if seq > lastTouch[k] {
			lastTouch[k] = seq
		}
	}
	for _, ev := range events {
		p := ev.Payload
		switch p.Type {
		case event.TypeOrderFilled:
			touch(p.Fill.AccountTaker, p.Fill.Symbol, ev.Seq)
			touch(p.Fill.AccountMaker, p.Fill.Symbol, ev.Seq)
		case event.TypeOrderAccepted, event.TypeOrderCancelled:
			if p.Order != nil {
				touch(p.Order.AccountID, p.Order.Symbol, ev.Seq)
			}
		}
	}

	// 3b. registry 在簿挂单重算名义占用（总是可做的自洽校验）。
	type openAgg struct {
		openBuy, openSell int64
		count             int
	}
	openWant := map[acctKey]*openAgg{}
	for _, o := range e.registry {
		if o.Type != domain.OrderLimit || o.Remaining() <= 0 ||
			(o.Status != domain.StatusNew && o.Status != domain.StatusPartiallyFilled) {
			continue
		}
		k := acctKey{o.AccountID, o.Symbol}
		a := openWant[k]
		if a == nil {
			a = &openAgg{}
			openWant[k] = a
		}
		a.count++
		if o.Side == domain.SideBuy {
			a.openBuy += o.Price * o.Remaining()
		} else {
			a.openSell += o.Price * o.Remaining()
		}
	}

	acctList := make([]acctKey, 0, len(accts))
	for k := range accts {
		acctList = append(acctList, k)
	}
	sort.Slice(acctList, func(i, j int) bool {
		if acctList[i].account != acctList[j].account {
			return acctList[i].account < acctList[j].account
		}
		return acctList[i].symbol < acctList[j].symbol
	})

	for _, k := range acctList {
		cur := e.riskMgr.State(k.account, k.symbol)
		subject := k.account + "/" + k.symbol
		near := lastTouch[k]

		// 名义占用自洽：registry 重算 vs 当前风控占用。
		ow := openWant[k]
		wb, ws, wc := int64(0), int64(0), 0
		if ow != nil {
			wb, ws, wc = ow.openBuy, ow.openSell, ow.count
		}
		if cur.OpenBuy != wb || cur.OpenSell != ws || cur.OpenCount != wc {
			add(Discrepancy{
				Kind:     DiscAccount,
				Subject:  subject,
				Expected: fmt.Sprintf("挂单重算 openBuy=%d openSell=%d openCount=%d", wb, ws, wc),
				Actual:   fmt.Sprintf("风控占用 openBuy=%d openSell=%d openCount=%d", cur.OpenBuy, cur.OpenSell, cur.OpenCount),
				NearSeq:  near,
			})
		}

		// 事件重推全量比对（仅全覆盖时持仓绝对值可独立验证）。
		if ghost != nil && rep.FullCoverage {
			re := ghost.riskMgr.State(k.account, k.symbol)
			if re != cur {
				add(Discrepancy{
					Kind:     DiscAccount,
					Subject:  subject,
					Expected: fmt.Sprintf("事件重推 %+v", re),
					Actual:   fmt.Sprintf("当前状态 %+v", cur),
					NearSeq:  near,
				})
			}
		}
	}

	if rep.OK {
		e.logf("AUDIT 通过 lastSeq=%d checkedEvents=%d fullCoverage=%t",
			rep.LastSeq, rep.CheckedEvents, rep.FullCoverage)
	}
	return rep
}
