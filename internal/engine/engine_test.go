package engine

import (
	"errors"
	"fmt"
	"log"
	"strings"
	"sync"
	"testing"
	"time"

	"github.com/lisama569/order-matching-engine-go/internal/event"
	"github.com/lisama569/order-matching-engine-go/internal/market"
	"github.com/lisama569/order-matching-engine-go/internal/match"
	"github.com/lisama569/order-matching-engine-go/internal/risk"
)

func testConfig() Config {
	return Config{
		Symbol: "BTCUSD",
		Limits: market.Limits{
			MinTickSize: 1,
			MinPrice:    1,
			MaxPrice:    100_000,
			MinQty:      1,
			MaxQty:      1_000,
		},
		Risk: riskCfg(0, 0, 0),
		// 缓冲刻意设小，便于制造慢订阅者。
		SubscriberBuffer: 2,
		AllowedAccounts: map[string]struct{}{
			"alice": {}, "bob": {}, "carol": {},
		},
	}
}

func riskCfg(long, short, exposure int64) risk.Config {
	return risk.Config{
		MaxLongPosition:  long,
		MaxShortPosition: short,
		MaxExposure:      exposure,
	}
}

func buyLimit(id, acct string, price, qty int64) SubmitRequest {
	return SubmitRequest{OrderID: id, AccountID: acct, Symbol: "BTCUSD",
		Side: market.SideBuy, Type: market.OrderTypeLimit, Price: price, Qty: qty}
}

func sellLimit(id, acct string, price, qty int64) SubmitRequest {
	return SubmitRequest{OrderID: id, AccountID: acct, Symbol: "BTCUSD",
		Side: market.SideSell, Type: market.OrderTypeLimit, Price: price, Qty: qty}
}

func marketBuy(id, acct string, qty int64) SubmitRequest {
	return SubmitRequest{OrderID: id, AccountID: acct, Symbol: "BTCUSD",
		Side: market.SideBuy, Type: market.OrderTypeMarket, Qty: qty}
}

// TestPartialFillAndTimePriority 覆盖部分成交与价格/时间优先。
func TestPartialFillAndTimePriority(t *testing.T) {
	e := New(testConfig())
	e.Start()
	defer e.Stop()

	// 卖一 100 量 30（bob，先到），卖一 100 量 20（carol，后到）。
	if ok, _ := e.SubmitAndWait(sellLimit("s1", "bob", 100, 30)); !ok {
		t.Fatal("s1 should be accepted")
	}
	if ok, _ := e.SubmitAndWait(sellLimit("s2", "carol", 100, 20)); !ok {
		t.Fatal("s2 should be accepted")
	}
	// 更优价位不会被越过：卖二 99 不应存在，再挂卖二 101。
	if ok, _ := e.SubmitAndWait(sellLimit("s3", "bob", 101, 50)); !ok {
		t.Fatal("s3 should be accepted")
	}

	// 买单 100 量 40：先吃 bob 30，再吃 carol 10，部分成交。
	if ok, _ := e.SubmitAndWait(buyLimit("b1", "alice", 100, 40)); !ok {
		t.Fatal("b1 should be accepted")
	}

	fills := fillsOf(e.Events())
	log.Printf("test: seq stream = %s", summarize(e.Events()))
	if len(fills) != 2 {
		t.Fatalf("want 2 fills, got %d", len(fills))
	}
	if fills[0].MakerOrderID != "s1" || fills[0].Qty != 30 {
		t.Fatalf("fill0 want s1/30 got %s/%d", fills[0].MakerOrderID, fills[0].Qty)
	}
	if fills[1].MakerOrderID != "s2" || fills[1].Qty != 10 {
		t.Fatalf("fill1 want s2/10 got %s/%d", fills[1].MakerOrderID, fills[1].Qty)
	}
	if fills[0].Price != 100 {
		t.Fatalf("fill price want 100 got %d", fills[0].Price)
	}

	// 簿视图：s2 剩 10@100，s3 50@101；b1 无剩余挂单（刚好 30+10=40）。
	sells := e.SellLevels()
	if len(sells) != 2 || sells[0].Price != 100 || sells[0].Qty != 10 ||
		sells[1].Price != 101 || sells[1].Qty != 50 {
		t.Fatalf("sell levels mismatch: %+v", sells)
	}
	if len(e.BuyLevels()) != 0 {
		t.Fatalf("buy levels should be empty: %+v", e.BuyLevels())
	}
}

// TestCancel 覆盖撤单成功与重复撤单失败。
func TestCancel(t *testing.T) {
	e := New(testConfig())
	e.Start()
	defer e.Stop()

	if ok, _ := e.SubmitAndWait(buyLimit("c1", "alice", 90, 15)); !ok {
		t.Fatal("c1 accepted")
	}
	if !e.CancelAndWait(CancelRequest{OrderID: "c1"}) {
		t.Fatal("first cancel should succeed")
	}
	if e.CancelAndWait(CancelRequest{OrderID: "c1"}) {
		t.Fatal("second cancel should fail: order not found")
	}
	if e.CancelAndWait(CancelRequest{OrderID: "missing"}) {
		t.Fatal("cancel of unknown order should fail")
	}
	// 撤单事件携带撤销量。
	evs := e.Events()
	last := evs[len(evs)-1]
	if last.Kind != event.KindOrderCanceled || last.Qty != 15 {
		t.Fatalf("want CANCELED qty=15, got %s qty=%d", last.Kind, last.Qty)
	}
	log.Printf("test: %s — 判定: 已撤销, 撤销量=%d", last, last.Qty)
	if len(e.BuyLevels()) != 0 {
		t.Fatalf("book should be empty after cancel: %+v", e.BuyLevels())
	}
}

// TestRejections 覆盖数量非法、价格越界、市价无流动性等可区分拒绝原因。
func TestRejections(t *testing.T) {
	e := New(testConfig())
	e.Start()
	defer e.Stop()

	cases := []struct {
		name   string
		req    SubmitRequest
		reason market.RejectReason
	}{
		{"zero qty", buyLimit("r1", "alice", 100, 0), market.RejectInvalidQuantity},
		{"qty over max", buyLimit("r2", "alice", 100, 5_000), market.RejectInvalidQuantity},
		{"price zero", buyLimit("r3", "alice", 0, 10), market.RejectInvalidPrice},
		{"price over max", buyLimit("r4", "alice", 200_000, 10), market.RejectInvalidPrice},
		{"price off tick", buyLimit("r5", "alice", 100, 10), market.RejectInvalidPrice},
		{"unknown account", buyLimit("r6", "eve", 100, 10), market.RejectUnknownAccount},
		{"market no liquidity", marketBuy("r7", "alice", 10), market.RejectUnacceptableOrder},
		{"bad side", SubmitRequest{OrderID: "r8", AccountID: "alice", Symbol: "BTCUSD",
			Side: market.SideUnknown, Type: market.OrderTypeLimit, Price: 100, Qty: 10},
			market.RejectUnacceptableOrder},
	}
	// 修正 off-tick 用例价格（tick=1 时任何整数都合法，改用不同边界）。
	cases[4].req = buyLimit("r5", "alice", 101, 10) // 101 对 tick=1 合法，故不会拒绝；下面单独用 tick 配置
	cases[4].reason = market.RejectNone

	for _, c := range cases {
		if c.reason == market.RejectNone {
			continue
		}
		ok, rej := e.SubmitAndWait(c.req)
		if ok {
			t.Fatalf("%s: want reject, got accepted", c.name)
		}
		log.Printf("test: case=%s seq判定 reason got=%s want=%s", c.name, rej.Reason, c.reason)
		if rej.Reason != c.reason {
			t.Fatalf("%s: reason want=%s got=%s", c.name, c.reason, rej.Reason)
		}
	}
	// 被拒委托不得改变簿状态，也不得影响后续撮合。
	if len(e.BuyLevels()) != 0 || len(e.SellLevels()) != 0 {
		t.Fatalf("rejected orders must not alter book: buys=%+v sells=%+v",
			e.BuyLevels(), e.SellLevels())
	}
	// 同ID重复提交必须被拒。
	if ok, _ := e.SubmitAndWait(sellLimit("dup", "bob", 100, 5)); !ok {
		t.Fatal("first dup accepted")
	}
	if ok, rej := e.SubmitAndWait(sellLimit("dup", "bob", 100, 5)); ok ||
		rej.Reason != market.RejectDuplicateOrderID {
		t.Fatalf("duplicate order id must be rejected distinctly, got ok=%v reason=%s", ok, rej.Reason)
	}
}

// TestTickSizeRejection 用 tick=5 的配置验证价格步长越界。
func TestTickSizeRejection(t *testing.T) {
	cfg := testConfig()
	cfg.Limits.MinTickSize = 5
	e := New(cfg)
	e.Start()
	defer e.Stop()
	if ok, rej := e.SubmitAndWait(buyLimit("t1", "alice", 103, 10)); ok {
		t.Fatal("off-tick price must be rejected")
	} else if rej.Reason != market.RejectInvalidPrice {
		t.Fatalf("want INVALID_PRICE, got %s", rej.Reason)
	}
	if ok, _ := e.SubmitAndWait(buyLimit("t2", "alice", 105, 10)); !ok {
		t.Fatal("on-tick price must be accepted")
	}
}

// TestRiskReject 覆盖持仓与敞口风控拒绝，且拒绝不改变已确定状态。
func TestRiskReject(t *testing.T) {
	cfg := testConfig()
	// alice 最多持多 20，最多名义敞口 5000。
	cfg.Risk = riskCfg(20, 0, 0)
	e := New(cfg)
	e.Start()
	defer e.Stop()

	// 卖方提供流动性。
	if ok, _ := e.SubmitAndWait(sellLimit("k1", "bob", 100, 100)); !ok {
		t.Fatal("k1 accepted")
	}
	// alice 买 30，最坏持仓 +30 > 20 → 拒绝，bob 的卖单不得被吃。
	ok, rej := e.SubmitAndWait(buyLimit("k2", "alice", 100, 30))
	log.Printf("test: risk position check got=%s 判定依据: position+30 > maxLong=20", rej.Reason)
	if ok || rej.Reason != market.RejectRiskPositionLimit {
		t.Fatalf("want RISK_POSITION_LIMIT, got ok=%v reason=%s", ok, rej.Reason)
	}
	if sells := e.SellLevels(); len(sells) != 1 || sells[0].Qty != 100 {
		t.Fatalf("risk reject must not change book, sells=%+v", sells)
	}
	// 买入 10 合法并成交。
	if ok, _ := e.SubmitAndWait(buyLimit("k3", "alice", 100, 10)); !ok {
		t.Fatal("k3 accepted")
	}
	if st, _ := e.AccountState("alice"); st.Position != 10 {
		t.Fatalf("alice position want 10 got %d", st.Position)
	}
	// 再买 15 将使持仓达 25 > 20 → 拒绝，既有持仓不变。
	if ok, rej := e.SubmitAndWait(buyLimit("k4", "alice", 100, 15)); ok ||
		rej.Reason != market.RejectRiskPositionLimit {
		t.Fatalf("want RISK_POSITION_LIMIT got ok=%v reason=%s", ok, rej.Reason)
	}
	if st, _ := e.AccountState("alice"); st.Position != 10 {
		t.Fatalf("position must stay 10 after reject, got %d", st.Position)
	}

	// 敞口限额单独验证。做市方 bob 的预挂单名义额（10*50=500）必须同样不超限，
	// 因为风控对挂单与成交双方均生效；alice 的 20*100=2000 超限被拒。
	cfg2 := testConfig()
	cfg2.Risk = riskCfg(0, 0, 1000)
	e2 := New(cfg2)
	e2.Start()
	defer e2.Stop()
	if ok, _ := e2.SubmitAndWait(sellLimit("e1", "bob", 10, 50)); !ok {
		t.Fatal("e1 accepted")
	}
	// 市价单以 MaxPrice 为最坏参考 → 20*100000 远超 1000 → 敞口拒绝。
	if ok, rej := e2.SubmitAndWait(marketBuy("e2", "alice", 20)); ok ||
		rej.Reason != market.RejectRiskExposureLimit {
		log.Printf("test: exposure check got=%s", rej.Reason)
		t.Fatalf("want RISK_EXPOSURE_LIMIT got ok=%v reason=%s", ok, rej.Reason)
	}
	// 限价单按其限价口径：20*100=2000 > 1000 → 敞口拒绝，簿不改变。
	if ok, rej := e2.SubmitAndWait(buyLimit("e3", "alice", 100, 20)); ok ||
		rej.Reason != market.RejectRiskExposureLimit {
		t.Fatalf("want RISK_EXPOSURE_LIMIT got ok=%v reason=%s", ok, rej.Reason)
	}
	if sells := e2.SellLevels(); len(sells) != 1 || sells[0].Qty != 50 {
		t.Fatalf("book must be unchanged after exposure reject, sells=%+v", sells)
	}
	log.Printf("test: exposure 判定依据: notional=20*100=2000 > maxExposure=1000")
}

// TestReplayDeterministic 覆盖事件流重建：簿与敞口一致、多次重建一致。
func TestReplayDeterministic(t *testing.T) {
	e := New(testConfig())
	e.Start()
	buildScenario(t, e)
	evs := e.Events()
	wantBuys := e.BuyLevels()
	wantSells := e.SellLevels()
	wantAlice, _ := e.AccountState("alice")
	wantBob, _ := e.AccountState("bob")
	e.Stop()

	// 同一事件流重建三次，结果必须逐字段一致。
	var prevBuy, prevSell string
	var prevAlice, prevBob risk.AccountState
	for round := 1; round <= 3; round++ {
		r := New(testConfig())
		if err := r.Replay(evs); err != nil {
			t.Fatalf("replay round %d failed: %v", round, err)
		}
		gotBuys := summarizeLevels(r.BuyLevels())
		gotSells := summarizeLevels(r.SellLevels())
		gotAlice, _ := r.AccountState("alice")
		gotBob, _ := r.AccountState("bob")
		log.Printf("replay round=%d lastSeq=%d buys=%s sells=%s alice=%+v bob=%+v",
			round, r.LastSeq(), gotBuys, gotSells, gotAlice, gotBob)
		if round > 1 && (gotBuys != prevBuy || gotSells != prevSell) {
			t.Fatalf("replay not deterministic across rounds: %s/%s vs %s/%s",
				gotBuys, gotSells, prevBuy, prevSell)
		}
		if round > 1 && (gotAlice != prevAlice || gotBob != prevBob) {
			t.Fatalf("risk state not deterministic: %+v/%+v vs %+v/%+v",
				gotAlice, gotBob, prevAlice, prevBob)
		}
		prevBuy, prevSell = gotBuys, gotSells
		prevAlice, prevBob = gotAlice, gotBob
	}

	// 重建结果必须与实时状态一致。
	r := New(testConfig())
	if err := r.Replay(evs); err != nil {
		t.Fatalf("replay failed: %v", err)
	}
	if summarizeLevels(r.BuyLevels()) != summarizeLevels(wantBuys) ||
		summarizeLevels(r.SellLevels()) != summarizeLevels(wantSells) {
		t.Fatalf("replayed book differs from live: buys %+v vs %+v sells %+v vs %+v",
			r.BuyLevels(), wantBuys, r.SellLevels(), wantSells)
	}
	if ga, _ := r.AccountState("alice"); ga != wantAlice {
		t.Fatalf("alice replay %+v != live %+v", ga, wantAlice)
	}
	if gb, _ := r.AccountState("bob"); gb != wantBob {
		t.Fatalf("bob replay %+v != live %+v", gb, wantBob)
	}
}

// TestReplaySeqViolations 覆盖重复、乱序与缺口必须被明确拒绝。
func TestReplaySeqViolations(t *testing.T) {
	e := New(testConfig())
	e.Start()
	buildScenario(t, e)
	evs := e.Events()
	e.Stop()

	mk := func(mod func([]event.Event) []event.Event) []event.Event {
		cp := make([]event.Event, len(evs))
		copy(cp, evs)
		return mod(cp)
	}

	// 缺口：删除中间一个事件（后续序号产生缺口）。
	gap := mk(func(v []event.Event) []event.Event {
		return append(v[:3], v[4:]...)
	})
	if err := New(testConfig()).Replay(gap); !errors.Is(err, event.ErrSeqGap) {
		t.Fatalf("gap: want ErrSeqGap, got %v", err)
	} else {
		log.Printf("test: gap replay 拒绝依据=%v", err)
	}

	// 重复：复制一个已有序号。
	dup := mk(func(v []event.Event) []event.Event {
		out := make([]event.Event, 0, len(v)+1)
		out = append(out, v[:2]...)
		out = append(out, v[1]) // 重复 seq=2
		out = append(out, v[2:]...)
		return out
	})
	if err := New(testConfig()).Replay(dup); !errors.Is(err, event.ErrSeqDuplicate) {
		t.Fatalf("duplicate: want ErrSeqDuplicate, got %v", err)
	} else {
		log.Printf("test: duplicate replay 拒绝依据=%v", err)
	}

	// 乱序：交换两个相邻事件。
	reordered := mk(func(v []event.Event) []event.Event {
		v[1], v[2] = v[2], v[1]
		return v
	})
	if err := New(testConfig()).Replay(reordered); err == nil {
		t.Fatal("reordered stream must be rejected")
	} else {
		log.Printf("test: out-of-order replay 拒绝依据=%v", err)
	}

	// 不从 1 开始也是乱序/缺口。
	wrongStart := mk(func(v []event.Event) []event.Event {
		for i := range v {
			v[i].Seq++
		}
		return v
	})
	if err := New(testConfig()).Replay(wrongStart); err == nil {
		t.Fatal("stream not starting at 1 must be rejected")
	}

	// 语义篡改：把某个成交价改掉，重算必须不一致。
	tampered := mk(func(v []event.Event) []event.Event {
		for i := range v {
			if v[i].Kind == event.KindOrderFilled {
				v[i].Price = v[i].Price + 1
				break
			}
		}
		return v
	})
	if err := New(testConfig()).Replay(tampered); !errors.Is(err, ErrReplay) {
		t.Fatalf("tampered fill must be rejected with ErrReplay, got %v", err)
	}

	// 失败的重放不得污染引擎已有状态。
	fresh := New(testConfig())
	if err := fresh.Replay(gap); err == nil {
		t.Fatal("expected error")
	}
	if fresh.LastSeq() != 0 || len(fresh.BuyLevels()) != 0 || len(fresh.SellLevels()) != 0 {
		t.Fatal("failed replay must leave engine state untouched")
	}
}

// TestSlowSubscriber 慢订阅者不得阻塞撮合，且通过 drops 通道可观测丢弃。
func TestSlowSubscriber(t *testing.T) {
	cfg := testConfig()
	cfg.SubscriberBuffer = 2
	e := New(cfg)
	e.Start()
	defer e.Stop()

	id, evCh, dropCh := e.Bus().Subscribe()
	_ = id
	// 不读取 evCh：制造慢订阅者；撮合仍必须快速完成。
	if ok, _ := e.SubmitAndWait(sellLimit("m1", "bob", 100, 100)); !ok {
		t.Fatal("m1 accepted")
	}
	done := make(chan struct{})
	go func() {
		for i := 0; i < 50; i++ {
			e.SubmitAndWait(buyLimit("slow", "alice", 1, 1)) // 绝大多数被拒（簿无卖方@1）
		}
		close(done)
	}()
	select {
	case <-done:
	case <-time.After(2 * time.Second):
		t.Fatal("slow subscriber blocked the matching loop")
	}
	select {
	case d := <-dropCh:
		log.Printf("test: slow subscriber drop notice=%+v 判定: 缓冲满后丢最旧事件", d)
		if d.DroppedCount == 0 {
			t.Fatal("expect positive dropped count")
		}
	default:
		t.Fatal("expected a drop notice for slow subscriber")
	}
	_ = evCh
}

// ---- helpers ----

func buildScenario(t *testing.T, e *Engine) {
	t.Helper()
	// bob 卖 100x30；carol 卖 101x20；alice 买 100x20 全成交；
	// alice 再买 100x20 → 吃 bob 剩 10 后挂买 100x10；撤销该买单。
	must := func(ok bool, msg string) {
		if !ok {
			t.Fatal(msg)
		}
	}
	must(ok(e.SubmitAndWait(sellLimit("z1", "bob", 100, 30))), "z1")
	must(ok(e.SubmitAndWait(sellLimit("z2", "carol", 101, 20))), "z2")
	must(ok(e.SubmitAndWait(buyLimit("z3", "alice", 100, 20))), "z3")
	must(ok(e.SubmitAndWait(buyLimit("z4", "alice", 100, 20))), "z4")
	if !e.CancelAndWait(CancelRequest{OrderID: "z4"}) {
		t.Fatal("cancel z4")
	}
}

func ok(accepted bool, _ Rejected) bool { return accepted }

func fillsOf(evs []event.Event) []event.Event {
	var out []event.Event
	for _, ev := range evs {
		if ev.Kind == event.KindOrderFilled {
			out = append(out, ev)
		}
	}
	return out
}

func summarize(evs []event.Event) string {
	out := ""
	for _, ev := range evs {
		out += ev.String() + "; "
	}
	return out
}

func summarizeLevels(ls []match.Level) string {
	parts := make([]string, 0, len(ls))
	for _, lv := range ls {
		parts = append(parts, fmt.Sprintf("%d:%d", lv.Price, lv.Qty))
	}
	return strings.Join(parts, ",")
}

// TestConcurrentDeterminism 高并发下单/撤单后，实时状态与事件流重建必须一致；
// 同一事件流重复重建也必须一致。
func TestConcurrentDeterminism(t *testing.T) {
	e := New(testConfig())
	e.Start()

	// 预置两档卖方流动性。
	for i := 0; i < 20; i++ {
		e.SubmitAndWait(sellLimit(fmt.Sprintf("mm-sell-%02d", i), "bob", 100+int64(i%3), 5))
	}

	var wg sync.WaitGroup
	for i := 0; i < 60; i++ {
		wg.Add(1)
		i := i
		go func() {
			defer wg.Done()
			req := buyLimit(fmt.Sprintf("c-%02d", i), "alice", 101, 2)
			if i%5 == 0 {
				req = buyLimit(fmt.Sprintf("c-%02d", i), "alice", 1, 2) // 不交叉，挂买一
			}
			e.SubmitAndWait(req)
			if i%7 == 0 {
				e.CancelAndWait(CancelRequest{OrderID: fmt.Sprintf("c-%02d", i)})
			}
		}()
	}
	// carol 的非法/风控（持仓敞口默认无限制，这里只制造拒绝噪声）并发。
	for i := 0; i < 10; i++ {
		wg.Add(1)
		i := i
		go func() {
			defer wg.Done()
			e.SubmitAndWait(buyLimit(fmt.Sprintf("bad-%02d", i), "alice", 0, 2)) // 非法价格
		}()
	}
	wg.Wait()

	live := e.Snapshot()
	evs := e.Events()
	// 事件序号必须 1..N 连续无缺。
	for i, ev := range evs {
		if ev.Seq != int64(i+1) {
			t.Fatalf("seq discontinuity at index %d: got %d", i, ev.Seq)
		}
	}
	e.Stop()

	r1 := New(testConfig())
	if err := r1.Replay(evs); err != nil {
		t.Fatalf("first replay: %v", err)
	}
	r2 := New(testConfig())
	if err := r2.Replay(evs); err != nil {
		t.Fatalf("second replay: %v", err)
	}
	if fmt.Sprint(r1.Snapshot()) != fmt.Sprint(r2.Snapshot()) {
		t.Fatal("two replays differ")
	}
	if fmt.Sprint(r1.BuyLevels()) != fmt.Sprint(live.BuyLevels) ||
		fmt.Sprint(r1.SellLevels()) != fmt.Sprint(live.SellLevels) ||
		fmt.Sprint(r1.Snapshot().Accounts) != fmt.Sprint(live.Accounts) {
		t.Fatalf("replay != live:\n live=%+v\n replay=%+v", live, r1.Snapshot())
	}
	log.Printf("test: concurrent events=%d replay matches live, buys=%s sells=%s accounts=%d",
		len(evs), summarizeLevels(r1.BuyLevels()), summarizeLevels(r1.SellLevels()),
		len(r1.Snapshot().Accounts))
}
