package engine

import (
	"errors"
	"reflect"
	"testing"

	"github.com/lisama569/order-matching-engine-go/internal/domain"
	"github.com/lisama569/order-matching-engine-go/internal/event"
	"github.com/lisama569/order-matching-engine-go/internal/risk"
)

func buildScenario(t *testing.T) *Engine {
	e := testEngine(t)
	e.Submit(sell("s1", "alice", "BTCUSD", domain.OrderLimit, 100, 10))
	e.Submit(sell("s2", "bob", "BTCUSD", domain.OrderLimit, 101, 8))
	e.Submit(buy("b1", "carol", "BTCUSD", domain.OrderLimit, 101, 12)) // 吃 s1 x10@100 + s2 x2@101
	e.Submit(buy("b2", "alice", "ETHUSD", domain.OrderLimit, 50, 4))   // 挂买
	e.Submit(sell("s3", "bob", "BTCUSD", domain.OrderLimit, 102, 3))   // 挂卖
	e.Cancel(CancelRequest{OrderID: "s3"})
	e.Submit(sell("x1", "alice", "ETHUSD", domain.OrderMarket, 0, 2)) // 吃 b2 x2，剩余市价撤销
	return e
}

func TestReplayEqualsLiveAndIsDeterministic(t *testing.T) {
	live := buildScenario(t)
	events := live.Events().All()
	t.Logf("事件流长度=%d，将重建 3 次并逐字段对比实时状态", len(events))

	liveSnap := live.Snapshot()
	var prev Snapshot
	for i := 1; i <= 3; i++ {
		ghost := testEngine(t)
		if err := ghost.Replay(events); err != nil {
			t.Fatalf("replay #%d: %v", i, err)
		}
		gs := ghost.Snapshot()
		if gs.LastSeq != liveSnap.LastSeq {
			t.Fatalf("replay #%d LastSeq=%d want %d", i, gs.LastSeq, liveSnap.LastSeq)
		}
		if !booksEqual(gs.Books, liveSnap.Books) {
			t.Fatalf("replay #%d books mismatch:\nreplay=%v\nlive=%v", i, gs.Books, liveSnap.Books)
		}
		if !accountsEqual(gs.Accounts, liveSnap.Accounts) {
			t.Fatalf("replay #%d accounts mismatch:\nreplay=%v\nlive=%v", i, gs.Accounts, liveSnap.Accounts)
		}
		if i > 1 && (!booksEqual(gs.Books, prev.Books) || !accountsEqual(gs.Accounts, prev.Accounts)) {
			t.Fatalf("replay #%d differs from replay #%d (non-deterministic)", i, i-1)
		}
		prev = gs
		t.Logf("重建 #%d: LastSeq=%d 买卖档位与账户敞口与实时状态一致", i, gs.LastSeq)
	}

	// 重建后的引擎可继续撮合：新事件序号必须严格衔接，且与"实时引擎提交同一单"结果一致。
	contGhost := testEngine(t)
	if err := contGhost.Replay(events); err != nil {
		t.Fatal(err)
	}
	gr := contGhost.Submit(buy("g1", "bob", "BTCUSD", domain.OrderLimit, 99, 1)) // 不越过卖价 => 挂买
	lr := live.Submit(buy("g1", "bob", "BTCUSD", domain.OrderLimit, 99, 1))
	if gr.Status != lr.Status || contGhost.Events().LastSeq() != live.Events().LastSeq() {
		t.Fatalf("post-replay continuation diverges: ghost=%d live=%d",
			contGhost.Events().LastSeq(), live.Events().LastSeq())
	}
	if !booksEqual(contGhost.Snapshot().Books, live.Snapshot().Books) {
		t.Fatalf("post-replay books diverge")
	}
	t.Logf("重建后续撮合序号正确衔接至 %d，结论与实时一致", contGhost.Events().LastSeq())
}

func TestReplayMalformedStreamsRejected(t *testing.T) {
	live := buildScenario(t)
	base := live.Events().All()

	drop := func(idx int) []event.Event { // 0-based 删除制造缺口
		out := append([]event.Event(nil), base...)
		return append(out[:idx], out[idx+1:]...)
	}
	dup := func() []event.Event {
		out := append([]event.Event(nil), base...)
		out = append(out, base[0]) // 末尾追加旧序号
		return out
	}
	swap := func() []event.Event {
		out := append([]event.Event(nil), base...)
		out[2], out[3] = out[3], out[2]
		return out
	}

	cases := []struct {
		name     string
		events   []event.Event
		wantKind string
	}{
		{"存在缺口", drop(2), event.SeqGap},
		{"序号重复", dup(), event.SeqDuplicate},
		{"乱序交换", swap(), event.SeqGap},
	}
	for _, tc := range cases {
		ghost := testEngine(t)
		err := ghost.Replay(tc.events)
		var se *event.SequenceError
		if !errors.As(err, &se) {
			t.Fatalf("%s: want SequenceError got %v", tc.name, err)
		}
		t.Logf("%s => 拒绝重建 kind=%s got=%d expected=%d (%v)，未推进任何状态",
			tc.name, se.Kind, se.Got, se.Expected, err)
		if ghost.Snapshot().LastSeq != 0 {
			t.Fatalf("%s: failed replay must leave engine clean, LastSeq=%d", tc.name, ghost.Snapshot().LastSeq)
		}
	}
}

func booksEqual(a, b map[string]BookView) bool {
	if len(a) != len(b) {
		return false
	}
	for k, va := range a {
		vb, ok := b[k]
		if !ok {
			return false
		}
		if !reflect.DeepEqual(va.Bids, vb.Bids) || !reflect.DeepEqual(va.Asks, vb.Asks) {
			return false
		}
	}
	return true
}

func accountsEqual(a, b map[string]risk.AccountState) bool {
	return reflect.DeepEqual(a, b)
}
