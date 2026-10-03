package engine

import (
	"fmt"
	"sync"
	"testing"
	"time"

	"github.com/lisama569/order-matching-engine-go/internal/domain"
	"github.com/lisama569/order-matching-engine-go/internal/risk"
)

// ordersEqual 逐字段比对两条路径的委托登记表（状态、成交量、序号等全部字段）。
func ordersEqual(a, b []domain.Order) (string, bool) {
	if len(a) != len(b) {
		return "orders count differs", false
	}
	for i := range a {
		if a[i] != b[i] {
			return "order " + a[i].ID + " differs", false
		}
	}
	return "", true
}

// runMixedScenario 在实时引擎上跑一段混合事件流：
// 市价全成、市价残余撤销、部分成交后挂簿、主动撤单、重复撤单混在一起。
// 返回引擎与"阶段一结束时落下的检查点"（从正在运行的实时引擎上取）。
func runMixedScenario(t *testing.T) (live *Engine, cpFromLive Checkpoint) {
	t.Helper()
	live = testEngine(t)

	// ---- 阶段一 ----
	live.Submit(sell("s1", "alice", "BTCUSD", domain.OrderLimit, 100, 10))
	live.Submit(sell("s2", "bob", "BTCUSD", domain.OrderLimit, 101, 8))
	live.Submit(buy("b1", "carol", "BTCUSD", domain.OrderLimit, 101, 12)) // 限价吃两档，残余挂簿
	live.Submit(buy("m1", "bob", "BTCUSD", domain.OrderMarket, 0, 3))     // 市价全成
	live.Submit(buy("m2", "alice", "BTCUSD", domain.OrderMarket, 0, 50))  // 市价部分成交，残余撤销
	live.Cancel(CancelRequest{OrderID: "b1"})                             // 主动撤部分成交后的挂簿单
	live.Cancel(CancelRequest{OrderID: "b1"})                             // 重复撤单（拒绝，无事件）
	live.Cancel(CancelRequest{OrderID: "m1"})                             // 撤已全成市价单（拒绝，无事件）

	// 在实时运行中的引擎上落检查点（不是从重放出来的中间状态）
	cpFromLive = live.Checkpoint()

	// ---- 阶段二：检查点之后事件流继续推进 ----
	live.Submit(sell("s3", "carol", "ETHUSD", domain.OrderLimit, 50, 6))
	live.Submit(buy("b2", "alice", "ETHUSD", domain.OrderLimit, 50, 4)) // 部分成交 s3，残余挂簿
	live.Submit(sell("m3", "bob", "ETHUSD", domain.OrderMarket, 0, 2))  // 市价全成
	live.Cancel(CancelRequest{OrderID: "s3"})                           // 撤部分成交后的 s3
	live.Cancel(CancelRequest{OrderID: "m3"})                           // 撤已全成市价单（拒绝）
	live.Submit(buy("b3", "carol", "BTCUSD", domain.OrderLimit, 99, 2)) // 挂簿不成交
	return live, cpFromLive
}

// TestCheckpointFromLiveEngineEqualsFullRebuild 复现并验证修复：
// 检查点从实时运行中的引擎落下，增量重建结果必须与从零全量重建、
// 以及实时状态逐字段一致（每笔委托的最终状态、成交量、序号，
// 以及账户敞口、盘口档位）。
func TestCheckpointFromLiveEngineEqualsFullRebuild(t *testing.T) {
	live, cp := runMixedScenario(t)
	events := live.Events().All()
	t.Logf("混合场景事件流长度=%d，检查点取自实时引擎 seq=%d", len(events), cp.LastSeq)

	// 路径一：实时引擎落检查点 -> 增量重建
	inc := testEngine(t)
	if err := inc.Rebuild(&cp, events); err != nil {
		t.Fatalf("incremental rebuild: %v", err)
	}
	// 路径二：从零全量重建
	full := testEngine(t)
	if err := full.Replay(events); err != nil {
		t.Fatalf("full replay: %v", err)
	}

	ls, is, fs := live.Snapshot(), inc.Snapshot(), full.Snapshot()
	if is.LastSeq != ls.LastSeq || fs.LastSeq != ls.LastSeq {
		t.Fatalf("LastSeq: live=%d incremental=%d full=%d", ls.LastSeq, is.LastSeq, fs.LastSeq)
	}
	if msg, ok := ordersEqual(is.Orders, fs.Orders); !ok {
		t.Fatalf("incremental vs full orders mismatch: %s", msg)
	}
	if msg, ok := ordersEqual(is.Orders, ls.Orders); !ok {
		t.Fatalf("incremental vs live orders mismatch: %s", msg)
	}
	if !booksEqual(is.Books, fs.Books) || !accountsEqual(is.Accounts, fs.Accounts) {
		t.Fatalf("incremental vs full books/accounts mismatch")
	}
	if !booksEqual(is.Books, ls.Books) || !accountsEqual(is.Accounts, ls.Accounts) {
		t.Fatalf("incremental vs live books/accounts mismatch")
	}
	t.Logf("实时落检查点增量重建 == 全量重建 == 实时：orders=%d 逐字段一致，LastSeq=%d",
		len(is.Orders), is.LastSeq)
}

// TestCheckpointFromLiveOrReplayIdentical：同一事件流推进到同一序号时，
// 从实时引擎落的检查点与从重放引擎落的检查点必须完全相同（校验值一致），
// 两条增量重建路径的结果也逐字段一致。
func TestCheckpointFromLiveOrReplayIdentical(t *testing.T) {
	live, cpLive := runMixedScenario(t)
	events := live.Events().All()

	// 从重放路径推进到同一序号再落检查点
	replayed := testEngine(t)
	if err := replayed.Replay(events[:cpLive.LastSeq]); err != nil {
		t.Fatalf("prefix replay: %v", err)
	}
	cpReplay := replayed.Checkpoint()

	if cpLive.Checksum != cpReplay.Checksum {
		t.Fatalf("checkpoint from live vs replay differ: live=%016x replay=%016x",
			cpLive.Checksum, cpReplay.Checksum)
	}
	t.Logf("实时与重放落出的检查点完全一致: seq=%d checksum=%016x", cpLive.LastSeq, cpLive.Checksum)

	full := testEngine(t)
	if err := full.Replay(events); err != nil {
		t.Fatal(err)
	}
	fs := full.Snapshot()
	for name, cp := range map[string]Checkpoint{"实时落检查点": cpLive, "重放落检查点": cpReplay} {
		inc := testEngine(t)
		if err := inc.Rebuild(&cp, events); err != nil {
			t.Fatalf("%s: %v", name, err)
		}
		is := inc.Snapshot()
		if msg, ok := ordersEqual(is.Orders, fs.Orders); !ok {
			t.Fatalf("%s增量重建 vs 全量重建: %s", name, msg)
		}
		if !booksEqual(is.Books, fs.Books) || !accountsEqual(is.Accounts, fs.Accounts) || is.LastSeq != fs.LastSeq {
			t.Fatalf("%s增量重建 vs 全量重建: 盘口/账户/序号不一致", name)
		}
		t.Logf("%s增量重建与全量重建逐字段一致 (LastSeq=%d)", name, is.LastSeq)
	}
}

// TestRebuildContinuationMatchesLive：重建后的引擎继续下单/撤单，
// 结论必须与实时引擎逐步一致：已全成或已撤销的委托不能再被撤销成功，
// 重复撤单拒绝，新单撮合结论相同。
func TestRebuildContinuationMatchesLive(t *testing.T) {
	live, cp := runMixedScenario(t)
	events := live.Events().All()

	inc := testEngine(t)
	if err := inc.Rebuild(&cp, events); err != nil {
		t.Fatal(err)
	}
	full := testEngine(t)
	if err := full.Replay(events); err != nil {
		t.Fatal(err)
	}
	engines := map[string]*Engine{"实时": live, "增量重建": inc, "全量重建": full}

	// 已终结委托的撤单必须在三条路径上同样被拒绝
	for _, oid := range []string{"m1", "m2", "b1", "s3"} { // 全成市价/残余撤销市价/已撤限价/已撤限价
		for name, e := range engines {
			if r := e.Cancel(CancelRequest{OrderID: oid}); r.Cancelled {
				t.Fatalf("%s: 已终结委托 %s 被撤销成功 (status 应拒绝)", name, oid)
			}
		}
	}
	t.Logf("已全成/已撤销委托(m1,m2,b1,s3)在实时/增量/全量三条路径上均拒绝撤单")

	// 逐步锁步推进：撤单成功 -> 重复撤单拒绝 -> 新单撮合，结论逐步一致
	type step struct {
		desc string
		act  func(e *Engine) string
	}
	steps := []step{
		{"撤在簿挂单 b3", func(e *Engine) string {
			r := e.Cancel(CancelRequest{OrderID: "b3"})
			return cancelOutcome(r)
		}},
		{"重复撤 b3", func(e *Engine) string {
			return cancelOutcome(e.Cancel(CancelRequest{OrderID: "b3"}))
		}},
		{"市价买 2（吃 s2 残余）", func(e *Engine) string {
			return submitOutcome(e.Submit(buy("c1", "alice", "BTCUSD", domain.OrderMarket, 0, 2)))
		}},
		{"限价卖挂簿", func(e *Engine) string {
			return submitOutcome(e.Submit(sell("c2", "bob", "BTCUSD", domain.OrderLimit, 105, 4)))
		}},
		{"撤 c2", func(e *Engine) string {
			return cancelOutcome(e.Cancel(CancelRequest{OrderID: "c2"}))
		}},
	}
	for _, st := range steps {
		want := st.act(live)
		for _, name := range []string{"增量重建", "全量重建"} {
			if got := st.act(engines[name]); got != want {
				t.Fatalf("步骤[%s] %s=%s 实时=%s", st.desc, name, got, want)
			}
		}
		t.Logf("步骤[%s]: 三条路径结论一致 => %s", st.desc, want)
	}

	ls := live.Snapshot()
	for _, name := range []string{"增量重建", "全量重建"} {
		s := engines[name].Snapshot()
		if msg, ok := ordersEqual(s.Orders, ls.Orders); !ok {
			t.Fatalf("锁步推进后 %s vs 实时: %s", name, msg)
		}
		if !booksEqual(s.Books, ls.Books) || !accountsEqual(s.Accounts, ls.Accounts) || s.LastSeq != ls.LastSeq {
			t.Fatalf("锁步推进后 %s vs 实时: 盘口/账户/序号不一致", name)
		}
	}
	t.Logf("锁步推进结束: 三路径 orders/盘口/账户/LastSeq=%d 逐字段一致", ls.LastSeq)
}

func cancelOutcome(r CancelResult) string {
	if r.Cancelled {
		return "cancelled"
	}
	return "rejected:" + string(r.Reject)
}

func submitOutcome(r SubmitResult) string {
	if !r.Accepted {
		return "rejected:" + string(r.Reject)
	}
	return "accepted:" + r.Status.String()
}

// TestCheckpointDuringConcurrentMatching：并发下单/撤单期间落的每一个检查点，
// 做增量重建都必须与"从零全量重建到同一序号"逐字段一致。
func TestCheckpointDuringConcurrentMatching(t *testing.T) {
	cfg := riskConfigLoose()
	newEng := func() *Engine {
		e := New([]string{"BTCUSD"}, cfg, map[string]int64{"BTCUSD": 100},
			map[string]bool{"maker": true, "taker": true})
		e.SetLogger(t.Logf)
		return e
	}
	live := newEng()

	// 种子挂单：一部分会被市价单吃掉，一部分被撤，一部分留簿
	const seeds = 100
	for i := 0; i < seeds; i++ {
		oid := fmt.Sprintf("seed-%03d", i)
		if r := live.Submit(sell(oid, "maker", "BTCUSD", domain.OrderLimit, int64(100+i), 2)); !r.Accepted {
			t.Fatalf("seed %s rejected: %s", oid, r.Reject)
		}
	}

	var wg sync.WaitGroup
	// 并发市价吃单（全成）+ 限价挂簿 + 撤单（含对可能已终结委托的撤单）
	for g := 0; g < 30; g++ {
		wg.Add(1)
		go func(g int) {
			defer wg.Done()
			live.Submit(buy(fmt.Sprintf("tk-%03d", g), "taker", "BTCUSD", domain.OrderMarket, 0, 1))
			live.Submit(sell(fmt.Sprintf("lm-%03d", g), "maker", "BTCUSD", domain.OrderLimit, int64(200+g), 1))
			live.Cancel(CancelRequest{OrderID: fmt.Sprintf("seed-%03d", g*2)})
			live.Cancel(CancelRequest{OrderID: fmt.Sprintf("seed-%03d", g*2)}) // 重复撤单
			live.Cancel(CancelRequest{OrderID: fmt.Sprintf("tk-%03d", g)})     // 撤已全成市价单
		}(g)
	}
	// 并发落检查点（实时引擎运行中）
	const checkpoints = 12
	cps := make([]Checkpoint, checkpoints)
	var cpWg sync.WaitGroup
	for c := 0; c < checkpoints; c++ {
		cpWg.Add(1)
		go func(c int) {
			defer cpWg.Done()
			time.Sleep(time.Duration(c) * time.Millisecond) // 错开落点，覆盖推进中的不同阶段
			cps[c] = live.Checkpoint()
		}(c)
	}
	wg.Wait()
	cpWg.Wait()

	events := live.Events().All()
	t.Logf("并发阶段结束: 事件流长度=%d，运行期间落检查点 %d 个", len(events), checkpoints)

	// 运行期间落的每个检查点：增量重建到 LastSeq 必须与全量重建到同一序号逐字段一致
	seen := map[int64]bool{}
	for i, cp := range cps {
		if seen[cp.LastSeq] {
			continue
		}
		seen[cp.LastSeq] = true
		prefix := events[:cp.LastSeq]
		inc := newEng()
		if err := inc.Rebuild(&cp, prefix); err != nil {
			t.Fatalf("checkpoint#%d (seq=%d) incremental rebuild: %v", i, cp.LastSeq, err)
		}
		full := newEng()
		if err := full.Replay(prefix); err != nil {
			t.Fatalf("checkpoint#%d (seq=%d) full replay: %v", i, cp.LastSeq, err)
		}
		is, fs := inc.Snapshot(), full.Snapshot()
		if msg, ok := ordersEqual(is.Orders, fs.Orders); !ok {
			t.Fatalf("checkpoint#%d (seq=%d): %s", i, cp.LastSeq, msg)
		}
		if !booksEqual(is.Books, fs.Books) || !accountsEqual(is.Accounts, fs.Accounts) || is.LastSeq != fs.LastSeq {
			t.Fatalf("checkpoint#%d (seq=%d): books/accounts/lastSeq mismatch", i, cp.LastSeq)
		}
		t.Logf("运行中检查点#%d seq=%d: 增量重建 == 全量重建（orders=%d 逐字段一致）",
			i, cp.LastSeq, len(is.Orders))
	}

	// 收尾：终态检查点 + 全事件流增量重建，与实时、全量重建三方一致
	finalCp := live.Checkpoint()
	inc := newEng()
	if err := inc.Rebuild(&finalCp, events); err != nil {
		t.Fatal(err)
	}
	full := newEng()
	if err := full.Replay(events); err != nil {
		t.Fatal(err)
	}
	ls, is, fs := live.Snapshot(), inc.Snapshot(), full.Snapshot()
	if msg, ok := ordersEqual(is.Orders, ls.Orders); !ok {
		t.Fatalf("终态: 增量 vs 实时: %s", msg)
	}
	if msg, ok := ordersEqual(is.Orders, fs.Orders); !ok {
		t.Fatalf("终态: 增量 vs 全量: %s", msg)
	}
	if !booksEqual(is.Books, ls.Books) || !accountsEqual(is.Accounts, ls.Accounts) {
		t.Fatalf("终态: 增量 vs 实时 books/accounts mismatch")
	}
	t.Logf("终态检查点 seq=%d: 实时 == 增量重建 == 全量重建，三方逐字段一致", finalCp.LastSeq)
}

func riskConfigLoose() risk.Config {
	return risk.Config{MaxPrice: 100000, Default: risk.Limits{
		MaxPosition: 1 << 40, MaxOpenOrders: 1 << 30, MaxExposure: 1 << 62, MaxSingleOrderNotional: 1 << 62}}
}
