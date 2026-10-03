// 检查点 / 增量重建一致性回归测试（bugfix-b）。
//
// 覆盖：
//   - 从【实时运行中】的引擎落检查点（而非重放中间态），增量重建 vs 从零全量重建
//     逐字段一致：每笔委托最终状态/成交量/序号、盘口档位、账户敞口；
//   - 重建后继续下单/撤单（含重复撤单、撤已成交/已撤销单）的结论与实时一致；
//   - 市价全成、市价残余撤销、限价部分成交后继续挂簿、主动撤单混合事件流；
//   - 并发下单/撤单期间落检查点同样一致（配合 -race）；
//   - 检查点缺失/损坏/序号对不上/放错位置一律安全退回全量重建，结果与实时一致。
package engine

import (
	"fmt"
	"sort"
	"sync"
	"testing"
	"time"

	"github.com/lisama569/order-matching-engine-go/internal/domain"
	"github.com/lisama569/order-matching-engine-go/internal/event"
	"github.com/lisama569/order-matching-engine-go/internal/risk"
)

// engineState 为逐字段对比用的完整派生状态。
type engineState struct {
	lastSeq  int64
	orders   map[string]domain.Order
	books    map[string]BookView
	accounts map[string]risk.AccountState
}

func captureState(t *testing.T, e *Engine) engineState {
	t.Helper()
	s := e.Snapshot()
	st := engineState{
		lastSeq:  s.LastSeq,
		books:    s.Books,
		accounts: s.Accounts,
		orders:   map[string]domain.Order{},
	}
	for id, o := range e.registry {
		st.orders[id] = *o
	}
	return st
}

func assertStatesEqual(t *testing.T, tag string, got, want engineState) {
	t.Helper()
	if got.lastSeq != want.lastSeq {
		t.Fatalf("[%s] LastSeq got=%d want=%d", tag, got.lastSeq, want.lastSeq)
	}
	if !booksEqual(got.books, want.books) {
		t.Fatalf("[%s] books diverge:\n got=%v\nwant=%v", tag, got.books, want.books)
	}
	if !accountsEqual(got.accounts, want.accounts) {
		t.Fatalf("[%s] accounts diverge:\n got=%v\nwant=%v", tag, got.accounts, want.accounts)
	}
	if len(got.orders) != len(want.orders) {
		t.Fatalf("[%s] registry size got=%d want=%d", tag, len(got.orders), len(want.orders))
	}
	ids := make([]string, 0, len(got.orders))
	for id := range got.orders {
		ids = append(ids, id)
	}
	sort.Strings(ids)
	for _, id := range ids {
		a, b := got.orders[id], want.orders[id]
		if a != b {
			t.Fatalf("[%s] order %s 逐字段不一致:\n got=%+v\nwant=%+v", tag, id, a, b)
		}
	}
}

// op 为脚本中的一步（下单或撤单）。
type op struct {
	kind string // "sub" | "cancel"
	req  SubmitRequest
	id   string
}

// mixedOps 混合场景：限价挂单、限价吃单部分成交继续挂簿、市价全成、
// 市价残余撤销、主动撤单、重复撤单、撤已成交单、风控/参数拒绝、多品种。
func mixedOps() []op {
	return []op{
		{"sub", sell("s1", "alice", "BTCUSD", domain.OrderLimit, 100, 10), ""},
		{"sub", sell("s2", "bob", "BTCUSD", domain.OrderLimit, 100, 5), ""},
		{"sub", sell("s3", "bob", "BTCUSD", domain.OrderLimit, 101, 8), ""},
		{"sub", buy("s4", "carol", "BTCUSD", domain.OrderLimit, 102, 4), ""},
		{"sub", buy("s5", "carol", "ETHUSD", domain.OrderLimit, 50, 6), ""}, // ETH 买档 50x6
		// 限价吃单 12：s1 x10@100 + s2 x2@100，全成
		{"sub", buy("b1", "carol", "BTCUSD", domain.OrderLimit, 101, 12), ""},
		// 限价吃单 20：s2 剩 3 + s3 x8 = 11，残余 9 挂买 @101（PARTIALLY_FILLED 继续挂簿）
		{"sub", buy("b2", "bob", "BTCUSD", domain.OrderLimit, 101, 20), ""},
		// 市价卖 4：吃 b2（买档 101）x4，市价单全成；b2 剩 5 继续挂
		{"sub", sell("m1", "alice", "BTCUSD", domain.OrderMarket, 0, 4), ""},
		// 市价残余撤销：ETH 买档 s5 x6，市价卖 10 吃 6，残余 4 撤销，s5 全成
		{"sub", sell("m2", "alice", "ETHUSD", domain.OrderMarket, 0, 10), ""},
		// 无流动性市价拒绝（s5 已被吃光，ETH 买档为空）
		{"sub", sell("m3", "bob", "ETHUSD", domain.OrderMarket, 0, 1), ""},
		// 主动撤单：b2 部分成交后继续挂簿，撤掉
		{"cancel", SubmitRequest{}, "b2"},
		// 重复撤单：必须拒绝
		{"cancel", SubmitRequest{}, "b2"},
		// 撤已全成的单：必须拒绝
		{"cancel", SubmitRequest{}, "b1"},
		// 撤不存在的单
		{"cancel", SubmitRequest{}, "ghost"},
		// 非法数量 / 重复 ID 拒绝（不产生事件）
		{"sub", buy("badq", "alice", "BTCUSD", domain.OrderLimit, 100, 0), ""},
		{"sub", sell("s1", "alice", "BTCUSD", domain.OrderLimit, 100, 1), ""},
		// 结束时簿上仍有挂单
		{"sub", sell("s6", "alice", "BTCUSD", domain.OrderLimit, 103, 7), ""},
		{"sub", buy("b3", "carol", "BTCUSD", domain.OrderLimit, 98, 3), ""},
	}
}

func applyOp(e *Engine, o op) (label string, ok bool, reason domain.RejectReason) {
	if o.kind == "cancel" {
		r := e.Cancel(CancelRequest{OrderID: o.id})
		return "cancel:" + o.id, r.Cancelled, r.Reject
	}
	r := e.Submit(o.req)
	return "submit:" + o.req.OrderID, r.Accepted, r.Reject
}

// TestLiveCheckpointIncrementalEqualsFull：核心回归——检查点直接从实时引擎落，
// 在事件流每个批次边界都验证“实时 = 增量重建 = 全量重建”逐字段一致。
func TestLiveCheckpointIncrementalEqualsFull(t *testing.T) {
	live := testEngine(t)
	for i, o := range mixedOps() {
		label, ok, reason := applyOp(live, o)
		t.Logf("op%02d %-18s accepted/cancelled=%-5t reject=%-22s lastSeq=%d",
			i, label, ok, reason, live.Events().LastSeq())

		events := live.Events().All()
		if len(events) == 0 {
			continue
		}
		cp := live.Checkpoint() // 关键：从实时运行中的引擎落盘
		if cp.LastSeq != int64(len(events)) {
			t.Fatalf("op%d checkpoint LastSeq=%d want %d", i, cp.LastSeq, len(events))
		}

		inc := testEngine(t)
		if err := inc.Rebuild(&cp, events); err != nil {
			t.Fatalf("op%d incremental rebuild: %v", i, err)
		}
		full := testEngine(t)
		if err := full.Replay(events); err != nil {
			t.Fatalf("op%d full replay: %v", i, err)
		}

		liveSt, incSt, fullSt := captureState(t, live), captureState(t, inc), captureState(t, full)
		assertStatesEqual(t, fmt.Sprintf("op%d 增量重建 vs 全量重建", i), incSt, fullSt)
		assertStatesEqual(t, fmt.Sprintf("op%d 实时 vs 全量重建", i), liveSt, fullSt)

		// 增量重建日志只持有尾部事件
		if base, n := inc.Events().Base(), len(inc.Events().All()); base != cp.LastSeq || n != len(events)-int(cp.LastSeq) {
			t.Fatalf("op%d tail store base=%d n=%d want %d/%d",
				i, base, n, cp.LastSeq, len(events)-int(cp.LastSeq))
		}
	}
	t.Log("全部批次边界：实时 / 实时检查点增量重建 / 从零全量重建 三者逐字段一致 ✔")
}

// TestPostRebuildBehaviorMatchesLive：重建后继续下单/撤单的结论必须与实时一致，
// 已全成/已撤销委托不能再次撤销成功。
func TestPostRebuildBehaviorMatchesLive(t *testing.T) {
	src := testEngine(t)
	head := []op{
		{"sub", sell("s1", "alice", "BTCUSD", domain.OrderLimit, 100, 10), ""},
		{"sub", sell("s2", "bob", "BTCUSD", domain.OrderLimit, 100, 5), ""},
		{"sub", buy("b1", "carol", "BTCUSD", domain.OrderLimit, 101, 14), ""}, // s1x10+s2x4 全成；s2 剩1
		{"sub", buy("b2", "carol", "BTCUSD", domain.OrderLimit, 99, 3), ""},
		{"sub", sell("m1", "alice", "BTCUSD", domain.OrderMarket, 0, 9), ""}, // s2x1，残余8撤销
		{"cancel", SubmitRequest{}, "b2"},                                    // 主动撤单
	}
	for _, o := range head {
		applyOp(src, o)
	}
	events := src.Events().All()
	cp := src.Checkpoint() // 实时检查点

	inc := testEngine(t)
	if err := inc.Rebuild(&cp, events); err != nil {
		t.Fatalf("incremental rebuild: %v", err)
	}
	full := testEngine(t)
	if err := full.Replay(events); err != nil {
		t.Fatalf("full replay: %v", err)
	}
	// 实时对照：另起引擎把同一段脚本实时提交一遍
	live := testEngine(t)
	for _, o := range head {
		applyOp(live, o)
	}

	cont := []op{
		{"sub", sell("s6", "bob", "BTCUSD", domain.OrderLimit, 104, 6), ""},
		{"sub", buy("m2", "bob", "BTCUSD", domain.OrderMarket, 0, 6), ""}, // 市价全成 s6
		{"cancel", SubmitRequest{}, "s6"},                                 // 已全成 -> 拒绝
		{"cancel", SubmitRequest{}, "b1"},                                 // 历史已全成 -> 拒绝
		{"cancel", SubmitRequest{}, "b2"},                                 // 历史已撤销 -> 拒绝
		{"cancel", SubmitRequest{}, "b2"},                                 // 重复撤单 -> 拒绝
		{"sub", buy("b4", "carol", "BTCUSD", domain.OrderLimit, 97, 2), ""},
		{"sub", sell("s7", "alice", "BTCUSD", domain.OrderLimit, 105, 4), ""},
		{"sub", buy("m3", "carol", "BTCUSD", domain.OrderMarket, 0, 4), ""},     // 全成 s7
		{"sub", buy("m4", "bob", "BTCUSD", domain.OrderMarket, 0, 9), ""},       // 无流动性，残余全撤
		{"sub", buy("bad", "bob", "BTCUSD", domain.OrderLimit, 96, 100000), ""}, // 名义超限拒绝
	}
	engines := []struct {
		name string
		e    *Engine
	}{{"实时", live}, {"增量重建", inc}, {"全量重建", full}}
	for i, o := range cont {
		results := map[string]struct {
			ok     bool
			reason string
		}{}
		for _, en := range engines {
			_, ok, reason := applyOp(en.e, o)
			results[en.name] = struct {
				ok     bool
				reason string
			}{ok, string(reason)}
		}
		for _, n := range []string{"增量重建", "全量重建"} {
			if results[n] != results["实时"] {
				t.Fatalf("cont op%d %s: %s 结论=%+v 实时=%+v",
					i, firstID(o), n, results[n], results["实时"])
			}
		}
		t.Logf("cont op%02d %-10s 三路径结论一致: accepted/cancelled=%-5t reject=%s",
			i, firstID(o), results["实时"].ok, results["实时"].reason)
		assertStatesEqual(t, fmt.Sprintf("cont op%d 增量 vs 实时", i),
			captureState(t, inc), captureState(t, live))
		assertStatesEqual(t, fmt.Sprintf("cont op%d 全量 vs 实时", i),
			captureState(t, full), captureState(t, live))
	}
	t.Log("重建后续行为（下单/撤单/重复撤单/撤终态单/拒绝）与实时完全一致 ✔")
}

func firstID(o op) string {
	if o.kind == "cancel" {
		return o.id
	}
	return o.req.OrderID
}

// TestConcurrentCheckpointRebuild：并发下单/撤单期间反复落检查点，
// 每个检查点的增量重建都与全量重建、实时终态逐字段一致。
func TestConcurrentCheckpointRebuild(t *testing.T) {
	cfg := risk.Config{MaxPrice: 10000, Default: risk.Limits{
		MaxPosition: 1 << 40, MaxOpenOrders: 1 << 30, MaxExposure: 1 << 60, MaxSingleOrderNotional: 1 << 60}}
	newEng := func() *Engine {
		return New([]string{"BTCUSD", "ETHUSD"}, cfg,
			map[string]int64{"BTCUSD": 100, "ETHUSD": 50},
			map[string]bool{"maker": true, "taker": true})
	}

	live := newEng()
	live.SetLogger(t.Logf)
	for i := 0; i < 60; i++ {
		if r := live.Submit(sell(fmt.Sprintf("seed-s-%03d", i), "maker", "BTCUSD",
			domain.OrderLimit, int64(100+i%40), 2)); !r.Accepted {
			t.Fatalf("seed sell %d: %s", i, r.Reject)
		}
		if i%2 == 0 {
			if r := live.Submit(buy(fmt.Sprintf("seed-b-%03d", i), "maker", "ETHUSD",
				domain.OrderLimit, int64(40+i%10), 2)); !r.Accepted {
				t.Fatalf("seed buy %d: %s", i, r.Reject)
			}
		}
	}

	const writers, perW = 8, 40
	var writerWG, cpWG sync.WaitGroup
	var cpMu sync.Mutex
	var cps []Checkpoint
	stop := make(chan struct{})

	// 检查点线程：写方并发期间持续从实时引擎落检查点
	cpWG.Add(1)
	go func() {
		defer cpWG.Done()
		for {
			select {
			case <-stop:
				cp := live.Checkpoint() // 停写后再落一个终态检查点
				cpMu.Lock()
				cps = append(cps, cp)
				cpMu.Unlock()
				return
			default:
			}
			cp := live.Checkpoint()
			cpMu.Lock()
			cps = append(cps, cp)
			cpMu.Unlock()
		}
	}()

	// 写方：限价挂单 + 市价吃单(BTC/ETH) + 撤单混合
	for w := 0; w < writers; w++ {
		writerWG.Add(1)
		go func(w int) {
			defer writerWG.Done()
			for i := 0; i < perW; i++ {
				oid := fmt.Sprintf("w%02d-%03d", w, i)
				switch i % 4 {
				case 0:
					live.Submit(sell(oid, "maker", "BTCUSD", domain.OrderLimit, int64(120+w), 2))
				case 1:
					live.Submit(buy(oid, "taker", "BTCUSD", domain.OrderMarket, 0, 1))
				case 2:
					live.Submit(buy(oid, "taker", "ETHUSD", domain.OrderMarket, 0, 1))
				case 3:
					live.Cancel(CancelRequest{OrderID: fmt.Sprintf("w%02d-%03d", w, i-3)})
				}
			}
		}(w)
	}

	done := make(chan struct{})
	go func() { writerWG.Wait(); close(done) }()
	select {
	case <-done:
	case <-time.After(30 * time.Second):
		t.Fatal("写方 30s 未完成：Checkpoint 可能与撮合锁死锁")
	}
	close(stop)
	cpWG.Wait()

	events := live.Events().All()
	for i, ev := range events {
		if ev.Seq != int64(i+1) {
			t.Fatalf("并发后事件序号断开 at %d: %d", i, ev.Seq)
		}
	}
	finalLive := captureState(t, live)

	// 每个并发期间落的检查点都必须可做增量重建，且终态逐字段一致
	n := 0
	for idx, cp := range cps {
		inc := newEng()
		if err := inc.Rebuild(&cp, events); err != nil {
			t.Fatalf("cp#%d(seq=%d) rebuild: %v", idx, cp.LastSeq, err)
		}
		assertStatesEqual(t, fmt.Sprintf("并发检查点#%d@seq=%d", idx, cp.LastSeq),
			captureState(t, inc), finalLive)
		n++
	}

	// 全量重建同样一致
	full := newEng()
	if err := full.Replay(events); err != nil {
		t.Fatalf("full replay: %v", err)
	}
	assertStatesEqual(t, "并发后全量重建", captureState(t, full), finalLive)
	t.Logf("并发混合负载完成: 事件=%d，并发期间落检查点=%d 个，全部增量重建终态与实时/全量逐字段一致 ✔",
		len(events), n)
}

// TestMixedScenarioFinalOrderStates：混合事件流结束后，
// 各委托的最终状态/成交量/受理序号符合业务语义，且实时/全量重建/多个实时检查点
// 增量重建给出完全相同的逐笔结论。
func TestMixedScenarioFinalOrderStates(t *testing.T) {
	live := testEngine(t)
	var checkpoints []Checkpoint
	events := []event.Event{}
	for i, o := range mixedOps() {
		applyOp(live, o)
		if n := len(live.Events().All()); n > len(events) {
			events = live.Events().All()
			if i%4 == 0 { // 多个实时检查点（覆盖不同事件类型之后）
				cp := live.Checkpoint()
				checkpoints = append(checkpoints, cp)
			}
		}
	}
	checkpoints = append(checkpoints, live.Checkpoint()) // 终态实时检查点

	want := map[string]struct {
		status    domain.OrderStatus
		filledQty int64
	}{
		"s1": {domain.StatusFilled, 10},    // 被 b1 全吃
		"s2": {domain.StatusFilled, 5},     // b1 吃 2、b2 吃 3，全成
		"s3": {domain.StatusFilled, 8},     // 被 b2 全吃
		"s4": {domain.StatusFilled, 4},     // BTC 买档，被市价卖 m1 全吃
		"s5": {domain.StatusFilled, 6},     // ETH 买档，被市价卖 m2 全吃
		"b1": {domain.StatusFilled, 12},    // 限价吃单全成
		"b2": {domain.StatusCancelled, 11}, // 限价吃单部分成交(11)后残余挂簿、主动撤单
		"m1": {domain.StatusFilled, 4},     // 市价卖全成
		"m2": {domain.StatusCancelled, 6},  // 市价卖残余撤销（成交6，残余4）
		"s6": {domain.StatusNew, 0},
		"b3": {domain.StatusNew, 0},
	}

	assertOrders := func(tag string, e *Engine) {
		t.Helper()
		for id, w := range want {
			o, ok := e.registry[id]
			if !ok {
				t.Fatalf("[%s] 委托 %s 丢失", tag, id)
			}
			if o.Status != w.status || o.FilledQty != w.filledQty {
				t.Fatalf("[%s] 委托 %s: status=%s filled=%d, want status=%s filled=%d",
					tag, id, o.Status, o.FilledQty, w.status, w.filledQty)
			}
		}
		// m3 是无流动性拒绝，不应进入登记表
		if _, ok := e.registry["m3"]; ok {
			t.Fatalf("[%s] 被拒市价单 m3 不应登记", tag)
		}
	}

	assertOrders("实时", live)

	full := testEngine(t)
	if err := full.Replay(events); err != nil {
		t.Fatal(err)
	}
	assertOrders("全量重建", full)
	assertStatesEqual(t, "全量重建 vs 实时", captureState(t, full), captureState(t, live))

	for idx, cp := range checkpoints {
		inc := testEngine(t)
		if err := inc.Rebuild(&cp, events); err != nil {
			t.Fatalf("cp#%d: %v", idx, err)
		}
		assertOrders(fmt.Sprintf("实时检查点#%d@seq=%d 增量重建", idx, cp.LastSeq), inc)
		assertStatesEqual(t, fmt.Sprintf("cp#%d 增量 vs 实时", idx),
			captureState(t, inc), captureState(t, live))
	}

	for id, w := range want {
		t.Logf("委托 %-3s 最终状态=%-16s 成交量=%d（实时=全量=%d 个实时检查点增量重建）",
			id, w.status, w.filledQty, len(checkpoints))
	}
	t.Logf("混合流终态逐笔语义正确，且 %d 个实时检查点的增量重建全部与实时/全量逐字段一致 ✔",
		len(checkpoints))
}
