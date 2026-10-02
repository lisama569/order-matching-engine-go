package engine

import (
	"encoding/json"
	"errors"
	"fmt"
	"os"
	"path/filepath"
	"testing"
	"time"

	"github.com/lisama569/order-matching-engine-go/internal/checkpoint"
	"github.com/lisama569/order-matching-engine-go/internal/domain"
	"github.com/lisama569/order-matching-engine-go/internal/risk"
)

// newLoadEngine 创建不限额、不限制账户的引擎（压测/重建用）。
func newLoadEngine(t *testing.T) *Engine {
	t.Helper()
	e := New([]string{"BTCUSD"}, risk.Config{}, map[string]int64{"BTCUSD": 100}, nil)
	e.SetLogger(t.Logf)
	return e
}

// compactResting 过滤掉已终结的委托 ID，控制跟踪列表体积。
func compactResting(e *Engine, ids []string) []string {
	out := ids[:0]
	for _, id := range ids {
		o := e.registry[id]
		if o != nil && o.Remaining() > 0 &&
			(o.Status == domain.StatusNew || o.Status == domain.StatusPartiallyFilled) {
			out = append(out, id)
		}
	}
	return out
}

// generateLoad 确定性地制造事件流：限价挂单（40 档价位带）、市价吃单、撤单混合，
// 敞口挂单规模有界（定期撤最老挂单），事件数推进到 targetEvents。
func generateLoad(t *testing.T, e *Engine, targetEvents int64) {
	t.Helper()
	resting, _ := e.loadResting(), 0
	i := int64(0)
	for e.Events().LastSeq() < targetEvents {
		i++
		if i > targetEvents*20+1000 {
			t.Fatalf("generateLoad: 迭代超限（事件推进过慢）")
		}
		oid := fmt.Sprintf("L%06d", i)
		acct := fmt.Sprintf("acct-%03d", i%100)
		switch i % 10 {
		case 0, 1, 2, 3, 4, 5: // 60% 限价挂单：卖 95..134 / 买 65..104，部分交叉成交
			var r SubmitResult
			if i%2 == 0 {
				r = e.Submit(sell(oid, acct, "BTCUSD", domain.OrderLimit, 95+(i%40), 1+(i%3)))
			} else {
				r = e.Submit(buy(oid, acct, "BTCUSD", domain.OrderLimit, 65+(i%40), 1+(i%3)))
			}
			if r.Accepted && (r.Status == domain.StatusNew || r.Status == domain.StatusPartiallyFilled) {
				resting = append(resting, oid)
			}
		case 6, 7: // 20% 市价吃单（方向交替）
			if i%20 < 10 {
				e.Submit(buy(oid, acct, "BTCUSD", domain.OrderMarket, 0, 1+(i%2)))
			} else {
				e.Submit(sell(oid, acct, "BTCUSD", domain.OrderMarket, 0, 1+(i%2)))
			}
		case 8: // 10% 撤掉最老的两笔挂单（控制敞口规模）
			for len(resting) > 0 {
				victim := resting[0]
				resting = resting[1:]
				if e.Cancel(CancelRequest{OrderID: victim}).Cancelled {
					break
				}
			}
			if len(resting) > 0 {
				victim := resting[0]
				resting = resting[1:]
				e.Cancel(CancelRequest{OrderID: victim})
			}
		case 9: // 10% 整理跟踪列表
			resting = compactResting(e, resting)
		}
	}
}

// loadResting 返回当前簿中挂单 ID（测试辅助）。
func (e *Engine) loadResting() []string {
	var out []string
	for _, bk := range e.books {
		for _, o := range bk.RestingOrders() {
			out = append(out, o.ID)
		}
	}
	return out
}

// assertEnginesEqual 逐字段比对两个引擎的可见状态：序号、盘口、账户、注册表、事件哈希。
func assertEnginesEqual(t *testing.T, want, got *Engine, ctx string) {
	t.Helper()
	ws, gs := want.Snapshot(), got.Snapshot()
	if ws.LastSeq != gs.LastSeq {
		t.Fatalf("%s: LastSeq %d != %d", ctx, gs.LastSeq, ws.LastSeq)
	}
	if !booksEqual(ws.Books, gs.Books) {
		t.Fatalf("%s: 盘口不一致:\nwant=%v\ngot=%v", ctx, ws.Books, gs.Books)
	}
	if !accountsEqual(ws.Accounts, gs.Accounts) {
		t.Fatalf("%s: 账户敞口不一致:\nwant=%v\ngot=%v", ctx, ws.Accounts, gs.Accounts)
	}
	if len(want.registry) != len(got.registry) {
		t.Fatalf("%s: 注册表大小 %d != %d", ctx, len(got.registry), len(want.registry))
	}
	for id, wo := range want.registry {
		go_, ok := got.registry[id]
		if !ok {
			t.Fatalf("%s: 注册表缺少委托 %s", ctx, id)
		}
		if *go_ != *wo {
			t.Fatalf("%s: 委托 %s 字段不一致:\nwant=%+v\ngot=%+v", ctx, id, wo, go_)
		}
	}
	if want.Events().Hash() != got.Events().Hash() {
		t.Fatalf("%s: 事件流哈希不一致: %d != %d", ctx, got.Events().Hash(), want.Events().Hash())
	}
}

// TestCheckpointRebuildEqualsFullRebuild：检查点增量重建与从零全量重建逐字段一致。
func TestCheckpointRebuildEqualsFullRebuild(t *testing.T) {
	live := newLoadEngine(t)
	generateLoad(t, live, 3000)
	cp := live.CaptureCheckpoint()
	mid := cp.Seq
	generateLoad(t, live, 6000)
	events := live.Events().All()
	t.Logf("事件流=%d 条，检查点 seq=%d（覆盖前缀哈希=%d）", len(events), mid, cp.EventHash)

	// 全量重建（对照组）
	full := newLoadEngine(t)
	if err := full.Replay(events); err != nil {
		t.Fatalf("full replay: %v", err)
	}
	assertEnginesEqual(t, live, full, "全量重建 vs 实时")

	// 路径 1：检查点 + 全量事件流（哈希校验前缀后增量续放）
	inc := newLoadEngine(t)
	res, err := inc.Rebuild(&cp, events)
	if err != nil {
		t.Fatalf("rebuild with checkpoint: %v", err)
	}
	if !res.UsedCheckpoint || res.FallbackReason != FallbackNone {
		t.Fatalf("应走检查点路径: %+v", res)
	}
	if res.FromSeq != mid+1 || res.ReplayedEvents != len(events)-int(mid) {
		t.Fatalf("增量工作量不符: %+v", res)
	}
	assertEnginesEqual(t, full, inc, "检查点+全量流 vs 全量重建")

	// 路径 2：检查点 + 仅增量段事件
	inc2 := newLoadEngine(t)
	res2, err := inc2.Rebuild(&cp, events[cp.Seq:])
	if err != nil {
		t.Fatalf("rebuild tail-only: %v", err)
	}
	if !res2.UsedCheckpoint || res2.ReplayedEvents != len(events)-int(mid) {
		t.Fatalf("增量段路径不符: %+v", res2)
	}
	assertEnginesEqual(t, full, inc2, "检查点+增量段 vs 全量重建")
	// 内存有界：增量重建的日志只持有增量段，不随历史事件总数增长
	if got := len(inc2.Events().All()); got != len(events)-int(mid) {
		t.Fatalf("增量重建日志应只持有增量段: got %d want %d", got, len(events)-int(mid))
	}
	if inc2.Events().Base() != mid {
		t.Fatalf("日志基线=%d want %d", inc2.Events().Base(), mid)
	}
	t.Logf("增量重建 %d 条事件即补齐 %d 条历史，结果与全量重建逐字段一致 ✔",
		res.ReplayedEvents, len(events))
}

// TestCheckpointFallbacks：缺失/损坏/放错位置的检查点都被识别并安全退回全量重建。
func TestCheckpointFallbacks(t *testing.T) {
	live := newLoadEngine(t)
	generateLoad(t, live, 800)
	cp := live.CaptureCheckpoint()
	generateLoad(t, live, 1500)
	events := live.Events().All()

	full := newLoadEngine(t)
	if err := full.Replay(events); err != nil {
		t.Fatal(err)
	}

	// 1) 检查点缺失（nil）→ 全量重建
	g1 := newLoadEngine(t)
	res, err := g1.Rebuild(nil, events)
	if err != nil || res.UsedCheckpoint || res.FallbackReason != FallbackNoCheckpoint {
		t.Fatalf("缺失检查点: res=%+v err=%v", res, err)
	}
	assertEnginesEqual(t, full, g1, "缺失检查点退回全量")
	t.Logf("缺失检查点: 退回全量重建 reason=%s ✔", res.FallbackReason)

	// 2) 放错位置：检查点序号超出事件流范围
	misplaced := cp
	misplaced.Seq = int64(len(events)) + 100
	g2 := newLoadEngine(t)
	res, err = g2.Rebuild(&misplaced, events)
	if err != nil || res.UsedCheckpoint || res.FallbackReason != FallbackMisplaced {
		t.Fatalf("放错位置: res=%+v err=%v", res, err)
	}
	assertEnginesEqual(t, full, g2, "放错位置退回全量")
	t.Logf("放错位置（seq 越界）: 退回全量重建 reason=%s ✔", res.FallbackReason)

	// 3) 放错流：另一条事件流的检查点（前缀哈希不符）
	other := newLoadEngine(t)
	generateLoad(t, other, 900) // 不同事件流
	cpOther := other.CaptureCheckpoint()
	cpOther.Seq = cp.Seq // 伪装成同一位置
	g3 := newLoadEngine(t)
	res, err = g3.Rebuild(&cpOther, events)
	if err != nil || res.UsedCheckpoint || res.FallbackReason != FallbackHashMismatch {
		t.Fatalf("放错流: res=%+v err=%v", res, err)
	}
	assertEnginesEqual(t, full, g3, "放错流退回全量")
	t.Logf("放错流（前缀哈希不符）: 退回全量重建 reason=%s ✔", res.FallbackReason)

	// 4) 内容损坏：检查点内部引用不一致（档位指向不存在的委托）
	corrupt := cp
	corrupt.Orders = cp.Orders[:len(cp.Orders)/2]
	g4 := newLoadEngine(t)
	res, err = g4.Rebuild(&corrupt, events)
	if err != nil || res.UsedCheckpoint || res.FallbackReason != FallbackCorrupt {
		t.Fatalf("内容损坏: res=%+v err=%v", res, err)
	}
	assertEnginesEqual(t, full, g4, "内容损坏退回全量")
	t.Logf("内容损坏（内部引用不一致）: 退回全量重建 reason=%s ✔", res.FallbackReason)

	// 5) 编码层损坏：落盘字节被篡改 → 解码即识别，按缺失处理
	data, err := EncodeCheckpoint(cp)
	if err != nil {
		t.Fatal(err)
	}
	data[len(data)/2] ^= 0xFF
	if _, err := DecodeCheckpoint(data); !errors.Is(err, checkpoint.ErrCorrupt) {
		t.Fatalf("篡改字节应被识别为损坏, got %v", err)
	}
	g5 := newLoadEngine(t)
	res, err = g5.Rebuild(nil, events) // 损坏 => 视为缺失 => 全量
	if err != nil || res.FallbackReason != FallbackNoCheckpoint {
		t.Fatalf("编码损坏退回: res=%+v err=%v", res, err)
	}
	assertEnginesEqual(t, full, g5, "编码损坏退回全量")
	t.Logf("编码层损坏: Decode 识别为 ErrCorrupt，按缺失退回全量 ✔")

	// 6) 只有增量段、检查点又不可用：没有全量流可退，必须明确报错而非悄悄续放
	g6 := newLoadEngine(t)
	if _, err := g6.Rebuild(nil, events[cp.Seq:]); err == nil {
		t.Fatal("缺失检查点 + 仅增量段：应明确拒绝")
	} else {
		t.Logf("缺失检查点且只有增量段: 明确报错 %v ✔", err)
	}
	g7 := newLoadEngine(t)
	if _, err := g7.Rebuild(&misplaced, events[cp.Seq:]); err == nil {
		t.Fatal("放错位置 + 仅增量段：应明确拒绝")
	} else {
		t.Logf("放错位置且只有增量段: 明确报错 %v ✔", err)
	}
}

// TestCheckpointFileStore：检查点落盘后可加载续建；文件损坏/缺失安全退回。
func TestCheckpointFileStore(t *testing.T) {
	dir := t.TempDir()
	fs := checkpoint.FileStore{Dir: dir}

	live := newLoadEngine(t)
	generateLoad(t, live, 600)
	cp, err := live.SaveCheckpoint(fs)
	if err != nil {
		t.Fatal(err)
	}
	generateLoad(t, live, 1200)
	events := live.Events().All()

	loaded, err := LoadLatestCheckpoint(fs)
	if err != nil || loaded == nil {
		t.Fatalf("加载检查点: %v", err)
	}
	if loaded.Seq != cp.Seq || loaded.EventHash != cp.EventHash {
		t.Fatalf("加载的检查点不符: %+v", loaded)
	}
	g := newLoadEngine(t)
	res, err := g.Rebuild(loaded, events)
	if err != nil || !res.UsedCheckpoint {
		t.Fatalf("落盘检查点续建: res=%+v err=%v", res, err)
	}
	assertEnginesEqual(t, live, g, "落盘检查点续建 vs 实时")
	t.Logf("检查点落盘 %s/cp-%d.json，加载续建一致 ✔", dir, cp.Seq)

	// 文件损坏 → 解码失败 → 按缺失退回全量
	if err := os.WriteFile(filepath.Join(dir, fmt.Sprintf("cp-%d.json", cp.Seq)), []byte("{garbage"), 0o644); err != nil {
		t.Fatal(err)
	}
	if _, err := LoadLatestCheckpoint(fs); !errors.Is(err, checkpoint.ErrCorrupt) {
		t.Fatalf("损坏文件应返回 ErrCorrupt, got %v", err)
	}
	g2 := newLoadEngine(t)
	res, err = g2.Rebuild(nil, events)
	if err != nil || res.UsedCheckpoint {
		t.Fatalf("损坏退回: res=%+v err=%v", res, err)
	}
	assertEnginesEqual(t, live, g2, "文件损坏退回全量")

	// 目录为空 → 检查点缺失（nil, nil）
	empty, err := LoadLatestCheckpoint(checkpoint.FileStore{Dir: t.TempDir()})
	if err != nil || empty != nil {
		t.Fatalf("空目录应返回 (nil,nil), got %v %v", empty, err)
	}
	t.Logf("文件损坏/缺失均安全退回全量重建 ✔")
}

// TestCheckpointRebuildThenContinueLive：增量重建后可继续实时撮合，序号严格衔接。
func TestCheckpointRebuildThenContinueLive(t *testing.T) {
	live := newLoadEngine(t)
	generateLoad(t, live, 500)
	cp := live.CaptureCheckpoint()
	generateLoad(t, live, 1000)
	events := live.Events().All()

	inc := newLoadEngine(t)
	if _, err := inc.Rebuild(&cp, events[cp.Seq:]); err != nil {
		t.Fatal(err)
	}
	// 同一笔新单分别打到实时引擎与增量重建引擎：结论与后续状态必须一致
	lr := live.Submit(buy("cont-1", "acct-001", "BTCUSD", domain.OrderLimit, 96, 2))
	gr := inc.Submit(buy("cont-1", "acct-001", "BTCUSD", domain.OrderLimit, 96, 2))
	if lr.Accepted != gr.Accepted || lr.Status != gr.Status {
		t.Fatalf("续撮合结论不一致: live=%+v rebuilt=%+v", lr, gr)
	}
	if inc.Events().LastSeq() != live.Events().LastSeq() {
		t.Fatalf("序号未衔接: live=%d rebuilt=%d", live.Events().LastSeq(), inc.Events().LastSeq())
	}
	assertEnginesEqual(t, live, inc, "增量重建后续撮合 vs 实时")
	t.Logf("增量重建后继续撮合: 新事件 seq=%d 严格衔接，状态与实时一致 ✔", inc.Events().LastSeq())
}

// TestCheckpointMemoryBoundedVsFullRebuild：检查点重建的内存不随事件流总长失控——
// 检查点显著小于其替代的事件历史，且增量重建后日志只持有增量段。
func TestCheckpointMemoryBoundedVsFullRebuild(t *testing.T) {
	live := newLoadEngine(t)
	generateLoad(t, live, 19000)
	cp := live.CaptureCheckpoint()
	generateLoad(t, live, 20000)
	events := live.Events().All()

	evJSON, err := json.Marshal(events)
	if err != nil {
		t.Fatal(err)
	}
	cpJSON, err := EncodeCheckpoint(cp)
	if err != nil {
		t.Fatal(err)
	}
	t.Logf("事件流=%d 条: 全量重建需装载事件历史 %dB; 检查点仅 %dB (%.1f%%)，且与后续事件增长无关",
		len(events), len(evJSON), len(cpJSON), 100*float64(len(cpJSON))/float64(len(evJSON)))
	if len(cpJSON)*2 > len(evJSON) {
		t.Fatalf("检查点应显著小于其替代的事件历史: checkpoint=%dB events=%dB", len(cpJSON), len(evJSON))
	}

	// 定期落检查点时，重建只需持有"检查点 + 一个间隔的增量事件"：
	// 日志基线之后的持有量不随历史总长增长。
	g := newLoadEngine(t)
	tail := events[cp.Seq:]
	res, err := g.Rebuild(&cp, tail) // 只给检查点之后的增量段
	if err != nil {
		t.Fatal(err)
	}
	if held := len(g.Events().All()); held != len(tail) {
		t.Fatalf("重建后日志持有量=%d, 应只持有增量段 %d 条", held, len(tail))
	}
	assertEnginesEqual(t, live, g, "检查点+末尾增量 vs 实时")
	t.Logf("重建只装载末尾 %d 条增量事件即补齐 %d 条历史，日志持有量与历史总长脱钩 ✔",
		res.ReplayedEvents, len(events))
}

// TestCheckpointRebuildFasterThanFull：事件量大时，带检查点的重建耗时明显低于全量。
func TestCheckpointRebuildFasterThanFull(t *testing.T) {
	live := newLoadEngine(t)
	generateLoad(t, live, 27000)
	cp := live.CaptureCheckpoint()
	generateLoad(t, live, 30000)
	events := live.Events().All()
	tail := events[cp.Seq:]

	minOf := func(n int, f func()) time.Duration {
		best := time.Hour
		for i := 0; i < n; i++ {
			start := time.Now()
			f()
			if d := time.Since(start); d < best {
				best = d
			}
		}
		return best
	}
	fullDur := minOf(5, func() {
		g := New([]string{"BTCUSD"}, risk.Config{}, map[string]int64{"BTCUSD": 100}, nil)
		if err := g.Replay(events); err != nil {
			t.Fatal(err)
		}
	})
	var res RebuildResult
	incDur := minOf(5, func() {
		g := New([]string{"BTCUSD"}, risk.Config{}, map[string]int64{"BTCUSD": 100}, nil)
		var err error
		res, err = g.Rebuild(&cp, tail)
		if err != nil {
			t.Fatal(err)
		}
	})
	t.Logf("重建耗时: 全量(%d 事件)=%v, 检查点增量(%d 事件)=%v, 提速 %.1fx",
		len(events), fullDur, res.ReplayedEvents, incDur, float64(fullDur)/float64(incDur))
	if res.ReplayedEvents != len(tail) {
		t.Fatalf("增量重建应只处理增量段 %d 条, got %d", len(tail), res.ReplayedEvents)
	}
	if incDur*2 > fullDur {
		t.Fatalf("增量重建提速不足 2x: full=%v inc=%v", fullDur, incDur)
	}
}

func BenchmarkRebuildFull(b *testing.B) {
	live := New([]string{"BTCUSD"}, risk.Config{}, map[string]int64{"BTCUSD": 100}, nil)
	generateLoad(&testing.T{}, live, 30000)
	events := live.Events().All()
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		g := New([]string{"BTCUSD"}, risk.Config{}, map[string]int64{"BTCUSD": 100}, nil)
		if err := g.Replay(events); err != nil {
			b.Fatal(err)
		}
	}
}

func BenchmarkRebuildCheckpoint(b *testing.B) {
	live := New([]string{"BTCUSD"}, risk.Config{}, map[string]int64{"BTCUSD": 100}, nil)
	generateLoad(&testing.T{}, live, 27000)
	cp := live.CaptureCheckpoint()
	generateLoad(&testing.T{}, live, 30000)
	tail := live.Events().All()[cp.Seq:]
	b.ReportAllocs()
	b.ResetTimer()
	for i := 0; i < b.N; i++ {
		g := New([]string{"BTCUSD"}, risk.Config{}, map[string]int64{"BTCUSD": 100}, nil)
		if _, err := g.Rebuild(&cp, tail); err != nil {
			b.Fatal(err)
		}
	}
}
