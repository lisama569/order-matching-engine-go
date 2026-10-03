// 异常检查点退回全量重建的回归测试（bugfix-b）。
//
// 检查点缺失、内容损坏（校验值不匹配）、携带状态与 LastSeq 矛盾、
// 序号超出事件流、事件指纹对不上——一律识别出来并安全退回从零全量重建，
// 绝不顺着错误起点往下走；退回后的结果仍与实时逐字段一致。
// 若事件流本身（尾部）畸形，全量重放也无法成功：此时必须明确报错、
// 线上状态保持不变，绝不产出错误状态。
package engine

import (
	"errors"
	"testing"

	"github.com/lisama569/order-matching-engine-go/internal/domain"
	"github.com/lisama569/order-matching-engine-go/internal/event"
)

func ptr(c Checkpoint) *Checkpoint { return &c }

func TestCheckpointFallbackScenarios(t *testing.T) {
	live := testEngine(t)
	for _, o := range mixedOps() {
		applyOp(live, o)
	}
	events := live.Events().All()
	wantState := captureState(t, live)

	mid := len(events) * 2 / 3
	midEng := testEngine(t)
	if err := midEng.Replay(events[:mid]); err != nil {
		t.Fatal(err)
	}
	good := midEng.Checkpoint() // 便于构造“合法检查点”；实时引擎落盘路径由
	// TestLiveCheckpointIncrementalEqualsFull 覆盖。

	corruptFilled := good
	corruptFilled.Orders = append([]domain.Order(nil), good.Orders...)
	corruptFilled.Orders[0].FilledQty += 777

	corruptAccount := good
	corruptAccount.Accounts = append([]AccountCheckpoint(nil), good.Accounts...)
	corruptAccount.Accounts[0].State.Position += 555

	cases := []struct {
		name string
		cp   *Checkpoint
	}{
		{"检查点缺失(nil)", nil},
		{"检查点损坏(篡改成交量,校验值不匹配)", &corruptFilled},
		{"检查点损坏(篡改账户状态,校验值不匹配)", &corruptAccount},
		{"放错位置(LastSeq超出事件流)", ptr(sealed(good, func(c *Checkpoint) {
			c.LastSeq = int64(len(events)) + 100
		}))},
		{"放错位置(LastSeq=0却携带状态)", ptr(sealed(good, func(c *Checkpoint) {
			c.LastSeq = 0
		}))},
		{"放错位置(边界事件指纹对不上)", ptr(sealed(good, func(c *Checkpoint) {
			c.LastEventHash ^= 0xdeadbeef
		}))},
		{"放错位置(指向流中另一合法序号)", ptr(sealed(good, func(c *Checkpoint) {
			c.LastSeq = 2 // 重算后“完好”，但 seq2 处事件指纹对不上
		}))},
	}

	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			e := testEngine(t)
			if err := e.Rebuild(tc.cp, events); err != nil {
				t.Fatalf("异常检查点必须退回全量重建并成功，got err=%v", err)
			}
			got := captureState(t, e)
			assertStatesEqual(t, tc.name+" 退回结果 vs 实时", got, wantState)
			if e.Events().Base() != 0 {
				t.Fatalf("%s: 退回必须从序号1全量重建，base=%d", tc.name, e.Events().Base())
			}
			t.Logf("%s => 识别异常并安全退回全量重建，结果与实时一致 (lastSeq=%d, base=0)",
				tc.name, got.lastSeq)
		})
	}

	// 对照：完好检查点必须被采纳（走增量路径），结果同样逐字段一致
	e := testEngine(t)
	if err := e.Rebuild(&good, events); err != nil {
		t.Fatal(err)
	}
	if e.Events().Base() != int64(mid) {
		t.Fatalf("完好检查点应被采纳走增量，base=%d want %d", e.Events().Base(), mid)
	}
	assertStatesEqual(t, "完好检查点增量重建 vs 实时", captureState(t, e), wantState)
	t.Logf("完好检查点正常生效: base=%d，只重放尾部 %d 个事件，结果与实时逐字段一致",
		mid, len(events)-mid)
}

// sealed 篡改字段后重算校验值，得到“完好但不合法”的检查点（考验位置/指纹判定）。
func sealed(good Checkpoint, mutate func(*Checkpoint)) Checkpoint {
	bad := good
	mutate(&bad)
	bad.sortAndSeal()
	return bad
}

// TestRebuildMalformedStreamReturnsErrorAndKeepsState：
// 无检查点、事件流本身畸形（缺口/重复/乱序）时，Rebuild 明确报错且不替换线上状态。
func TestRebuildMalformedStreamReturnsErrorAndKeepsState(t *testing.T) {
	live := testEngine(t)
	for _, o := range mixedOps() {
		applyOp(live, o)
	}
	goodEvents := live.Events().All()

	cases := map[string]struct {
		events []event.Event
		kind   string
	}{
		"缺口": {malformed(goodEvents, "gap"), event.SeqGap},
		"重复": {malformed(goodEvents, "dup"), event.SeqDuplicate},
		"乱序": {malformed(goodEvents, "outOfOrder"), event.SeqDuplicate},
	}
	for name, tc := range cases {
		e := testEngine(t)
		e.Submit(sell("keep", "alice", "ETHUSD", domain.OrderLimit, 40, 2))
		before := captureState(t, e)

		err := e.Rebuild(nil, tc.events)
		var se *event.SequenceError
		if !errors.As(err, &se) {
			t.Fatalf("%s: want SequenceError got %v", name, err)
		}
		if se.Kind != tc.kind {
			t.Fatalf("%s: kind=%s want %s", name, se.Kind, tc.kind)
		}
		t.Logf("坏流(%s) => kind=%s got=%d expected=%d，不替换线上状态", name, se.Kind, se.Got, se.Expected)
		assertStatesEqual(t, "坏流("+name+")后线上状态必须保持", captureState(t, e), before)
	}
}

// TestCheckpointTailBrokenFallsBackThenErrors：
// 检查点定位/指纹合法，但尾部事件序号链畸形（缺口/重复/乱序）：
// 必须先安全退回全量重放，再由全量重放明确报错；线上状态保持干净，不产出错误状态。
func TestCheckpointTailBrokenFallsBackThenErrors(t *testing.T) {
	live := testEngine(t)
	for _, o := range mixedOps() {
		applyOp(live, o)
	}
	events := live.Events().All()

	cut := len(events) / 2
	midEng := testEngine(t)
	if err := midEng.Replay(events[:cut]); err != nil {
		t.Fatal(err)
	}
	cp := midEng.Checkpoint()

	cases := map[string][]event.Event{
		"尾部缺口":    malformed(events, "gap"),
		"尾部重复":    malformed(events, "dup"),
		"尾部乱序":    malformed(events, "outOfOrder"),
		"尾部首事件跳号": withGapAt(events, cut),
	}
	for name, bad := range cases {
		e := testEngine(t)
		err := e.Rebuild(&cp, bad)
		var se *event.SequenceError
		if !errors.As(err, &se) {
			t.Fatalf("%s: 退回全量后应报 SequenceError，got %v", name, err)
		}
		if e.Events().Base() != 0 || e.Snapshot().LastSeq != 0 {
			t.Fatalf("%s: 失败的重建不得部分推进状态: base=%d lastSeq=%d",
				name, e.Events().Base(), e.Snapshot().LastSeq)
		}
		t.Logf("%s => 不顺着检查点走、退回全量后报 %s(got=%d,expected=%d)，引擎保持干净 ✔",
			name, se.Kind, se.Got, se.Expected)
	}
}

func malformed(base []event.Event, kind string) []event.Event {
	out := append([]event.Event(nil), base...)
	switch kind {
	case "gap":
		return append(out[:len(out)-2], out[len(out)-1])
	case "dup":
		return append(out, out[len(out)-2]) // 末尾加旧序号
	default: // outOfOrder
		old := out[len(out)-3]
		out[len(out)-1] = old
		return out
	}
}

func withGapAt(base []event.Event, idx int) []event.Event {
	out := append([]event.Event(nil), base...)
	out[idx].Seq += 5
	return out
}
