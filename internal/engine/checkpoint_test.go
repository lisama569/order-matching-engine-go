package engine

import (
	"fmt"
	"testing"
	"time"

	"github.com/lisama569/order-matching-engine-go/internal/domain"
	"github.com/lisama569/order-matching-engine-go/internal/risk"
)

// TestCheckpointIncrementalRebuildEqualsFull：检查点增量重建与从零全量重建逐字段一致。
func TestCheckpointIncrementalRebuildEqualsFull(t *testing.T) {
	live := buildScenario(t)
	events := live.Events().All()

	// 在事件流推进过程中（约一半处）落检查点：
	// 用前缀事件重建一个中间引擎，从它身上取检查点。
	mid := len(events) / 2
	midEngine := testEngine(t)
	if err := midEngine.Replay(events[:mid]); err != nil {
		t.Fatalf("mid replay: %v", err)
	}
	cp := midEngine.Checkpoint()
	if cp.LastSeq != int64(mid) {
		t.Fatalf("checkpoint LastSeq=%d want %d", cp.LastSeq, mid)
	}
	t.Logf("检查点落于 seq=%d（事件流总长 %d）", cp.LastSeq, len(events))

	// 增量重建
	inc := testEngine(t)
	if err := inc.Rebuild(&cp, events); err != nil {
		t.Fatalf("incremental rebuild: %v", err)
	}
	// 全量重建
	full := testEngine(t)
	if err := full.Replay(events); err != nil {
		t.Fatalf("full replay: %v", err)
	}

	is, fs := inc.Snapshot(), full.Snapshot()
	if is.LastSeq != fs.LastSeq {
		t.Fatalf("LastSeq: incremental=%d full=%d", is.LastSeq, fs.LastSeq)
	}
	if !booksEqual(is.Books, fs.Books) {
		t.Fatalf("books mismatch:\ninc=%v\nfull=%v", is.Books, fs.Books)
	}
	if !accountsEqual(is.Accounts, fs.Accounts) {
		t.Fatalf("accounts mismatch:\ninc=%v\nfull=%v", is.Accounts, fs.Accounts)
	}
	// 增量重建的日志只持有检查点之后的事件（内存不随历史事件数增长）
	if got := inc.Events().Base(); got != int64(mid) {
		t.Fatalf("incremental store base=%d want %d", got, mid)
	}
	if got := len(inc.Events().All()); got != len(events)-mid {
		t.Fatalf("incremental store holds %d events, want tail %d", got, len(events)-mid)
	}
	// 增量重建后可继续撮合，序号严格衔接
	r := inc.Submit(buy("cont1", "bob", "BTCUSD", domain.OrderLimit, 99, 1))
	if !r.Accepted || inc.Events().LastSeq() != int64(len(events))+1 {
		t.Fatalf("continuation after rebuild: accepted=%t lastSeq=%d", r.Accepted, inc.Events().LastSeq())
	}
	t.Logf("增量重建与全量重建逐字段一致: LastSeq=%d，续撮合序号衔接至 %d",
		is.LastSeq, inc.Events().LastSeq())
}

// TestCheckpointFallbackToFullRebuild：缺失/损坏/放错位置的检查点安全退回全量重建。
func TestCheckpointFallbackToFullRebuild(t *testing.T) {
	live := buildScenario(t)
	events := live.Events().All()
	want := live.Snapshot()

	mid := len(events) / 2
	midEngine := testEngine(t)
	if err := midEngine.Replay(events[:mid]); err != nil {
		t.Fatal(err)
	}
	good := midEngine.Checkpoint()

	// 损坏：篡改检查点内容但不重算校验值
	corrupt := good
	corrupt.Orders = append([]domain.Order(nil), good.Orders...)
	corrupt.Orders[0].FilledQty += 777

	// 放错位置：LastSeq 指向事件流中不存在的位置（重算校验值使其"完好"）
	misplaced := good
	misplaced.LastSeq = int64(len(events)) + 100
	misplaced.sortAndSeal()

	// 放错位置（不衔接）：LastSeq 有效但事件流从别处开始
	wrongStream := good
	wrongStream.LastSeq = 2
	wrongStream.sortAndSeal()

	cases := []struct {
		name string
		cp   *Checkpoint
	}{
		{"检查点缺失(nil)", nil},
		{"检查点损坏(校验值不匹配)", &corrupt},
		{"检查点放错位置(超出事件流)", &misplaced},
		{"检查点放错位置(与事件流不衔接)", &wrongStream},
	}
	for _, tc := range cases {
		e := testEngine(t)
		if err := e.Rebuild(tc.cp, events); err != nil {
			t.Fatalf("%s: rebuild should fall back to full, got err=%v", tc.name, err)
		}
		s := e.Snapshot()
		if s.LastSeq != want.LastSeq || !booksEqual(s.Books, want.Books) || !accountsEqual(s.Accounts, want.Accounts) {
			t.Fatalf("%s: fallback full rebuild diverges from live", tc.name)
		}
		if e.Events().Base() != 0 {
			t.Fatalf("%s: fallback must rebuild from seq 1, base=%d", tc.name, e.Events().Base())
		}
		t.Logf("%s => 安全退回全量重建，结果与实时一致 (LastSeq=%d)", tc.name, s.LastSeq)
	}

	// 对照：完好的检查点绝不触发全量
	e := testEngine(t)
	if err := e.Rebuild(&good, events); err != nil {
		t.Fatal(err)
	}
	if e.Events().Base() != int64(mid) {
		t.Fatalf("valid checkpoint must be honored, base=%d want %d", e.Events().Base(), mid)
	}
	t.Logf("完好检查点正常生效: base=%d，只重放尾部 %d 个事件", mid, len(events)-mid)
}

// TestCheckpointRebuildFasterThanFull：大事件量下带检查点的重建明显快于从零重建。
func TestCheckpointRebuildFasterThanFull(t *testing.T) {
	cfg := risk.Config{MaxPrice: 100000, Default: risk.Limits{
		MaxPosition: 1 << 40, MaxOpenOrders: 1 << 30, MaxExposure: 1 << 62, MaxSingleOrderNotional: 1 << 62}}
	newEng := func() *Engine {
		return New([]string{"BTCUSD"}, cfg, map[string]int64{"BTCUSD": 100},
			map[string]bool{"maker": true, "taker": true})
	}

	// 构造大事件流：100 档价位、每档多笔挂单 + 市价吃单，交替三轮，约 4.5 万事件
	live := newEng()
	for round := 0; round < 3; round++ {
		for i := 0; i < 5000; i++ {
			oid := fmt.Sprintf("r%d-s%04d", round, i)
			live.Submit(sell(oid, "maker", "BTCUSD", domain.OrderLimit, int64(100+i%100), 2))
		}
		for i := 0; i < 5000; i++ {
			oid := fmt.Sprintf("r%d-b%04d", round, i)
			live.Submit(buy(oid, "taker", "BTCUSD", domain.OrderMarket, 0, 1))
		}
	}
	events := live.Events().All()
	if len(events) < 40000 {
		t.Fatalf("scenario too small: %d events", len(events))
	}

	// 在 95% 处落检查点
	cut := len(events) * 95 / 100
	midEng := newEng()
	if err := midEng.Replay(events[:cut]); err != nil {
		t.Fatal(err)
	}
	cp := midEng.Checkpoint()

	// 各测 5 次取最优，降低环境噪声
	fullDur := time.Duration(1 << 62)
	for i := 0; i < 5; i++ {
		start := time.Now()
		full := newEng()
		if err := full.Replay(events); err != nil {
			t.Fatal(err)
		}
		if d := time.Since(start); d < fullDur {
			fullDur = d
		}
	}
	var inc *Engine
	incDur := time.Duration(1 << 62)
	for i := 0; i < 5; i++ {
		start := time.Now()
		inc = newEng()
		if err := inc.Rebuild(&cp, events); err != nil {
			t.Fatal(err)
		}
		if d := time.Since(start); d < incDur {
			incDur = d
		}
	}
	full := newEng()
	if err := full.Replay(events); err != nil {
		t.Fatal(err)
	}

	// 结果逐字段一致
	is, fs := inc.Snapshot(), full.Snapshot()
	if is.LastSeq != fs.LastSeq || !booksEqual(is.Books, fs.Books) || !accountsEqual(is.Accounts, fs.Accounts) {
		t.Fatal("incremental rebuild diverges from full rebuild on large stream")
	}
	// 增量路径只处理尾部事件
	if got := len(inc.Events().All()); got != len(events)-cut {
		t.Fatalf("incremental processed %d events, want %d", got, len(events)-cut)
	}
	// 耗时明显更低（尾部仅 5% 事件，要求至少快 2 倍；
	// -race 下计时被插桩扭曲，放宽为严格更快即可）
	if raceEnabled {
		if incDur >= fullDur {
			t.Fatalf("incremental rebuild not faster under -race: full=%v incremental=%v", fullDur, incDur)
		}
	} else if incDur*2 >= fullDur {
		t.Fatalf("incremental rebuild not significantly faster: full=%v incremental=%v", fullDur, incDur)
	}
	t.Logf("事件=%d 检查点@seq=%d: 全量重建=%v 增量重建=%v (%.1fx)，结果逐字段一致",
		len(events), cp.LastSeq, fullDur, incDur, float64(fullDur)/float64(incDur))
}
