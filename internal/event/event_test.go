package event

import (
	"errors"
	"testing"
)

func mkEvent(seq int64) Event { return Event{Seq: seq, Payload: Payload{Type: TypeOrderAccepted}} }

func TestStoreAppendSequential(t *testing.T) {
	s := NewStore()
	for seq := int64(1); seq <= 3; seq++ {
		if err := s.Append(mkEvent(seq)); err != nil {
			t.Fatalf("append seq=%d: %v", seq, err)
		}
	}
	if s.LastSeq() != 3 {
		t.Fatalf("LastSeq=%d want 3", s.LastSeq())
	}
	got := s.From(2)
	if len(got) != 2 || got[0].Seq != 2 {
		t.Fatalf("From(2) = %+v", got)
	}
}

func TestStoreRejectsDuplicateAndGap(t *testing.T) {
	s := NewStore()
	if err := s.Append(mkEvent(1)); err != nil {
		t.Fatal(err)
	}
	// 重复序号
	err := s.Append(mkEvent(1))
	var se *SequenceError
	if !errors.As(err, &se) || se.Kind != SeqDuplicate {
		t.Fatalf("want DUPLICATE, got %v", err)
	}
	t.Logf("判定 seq=1: %v (序号重复，必须拒绝)", err)

	// 缺口：期望 2 却给 3
	err = s.Append(mkEvent(3))
	if !errors.As(err, &se) || se.Kind != SeqGap {
		t.Fatalf("want GAP, got %v", err)
	}
	t.Logf("判定 seq=3: %v (存在缺口，必须拒绝)", err)

	// 缺口拒绝后日志未推进，序号 2 仍可正常追加
	if err := s.Append(mkEvent(2)); err != nil {
		t.Fatalf("gap must not advance state: %v", err)
	}
	if s.LastSeq() != 2 {
		t.Fatalf("LastSeq=%d want 2", s.LastSeq())
	}
}

func TestReplayRejectsDuplicateOutOfOrderGap(t *testing.T) {
	cases := []struct {
		name     string
		events   []Event
		wantKind string
	}{
		{"乱序", []Event{mkEvent(2), mkEvent(1)}, SeqGap}, // 2 先到 => 期望1，给2 => GAP
		{"真正乱序回退", []Event{mkEvent(1), mkEvent(3), mkEvent(2)}, SeqGap},
		{"重复", []Event{mkEvent(1), mkEvent(1)}, SeqDuplicate},
		{"缺口", []Event{mkEvent(1), mkEvent(3)}, SeqGap},
		{"乱序未见过的小序号", func() []Event {
			// 构造 1,2,3,4,5 然后把 3 以乱序形式再次给 Replay 不会重复 seen?
			return []Event{mkEvent(1), mkEvent(2), mkEvent(4)}
		}(), SeqGap},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			applied := 0
			err := Replay(tc.events, func(Event) error { applied++; return nil })
			var se *SequenceError
			if !errors.As(err, &se) {
				t.Fatalf("want SequenceError, got %v", err)
			}
			t.Logf("事件序号判定: got=%d expected=%d kind=%s => %v", se.Got, se.Expected, se.Kind, err)
		})
	}

	// 严格乱序（回退到未见过的小序号）：1,2,3 后又给 1 -> duplicate
	t.Run("回退重复", func(t *testing.T) {
		err := Replay([]Event{mkEvent(1), mkEvent(2), mkEvent(1)}, func(Event) error { return nil })
		var se *SequenceError
		if !errors.As(err, &se) || se.Kind != SeqDuplicate {
			t.Fatalf("want duplicate, got %v", err)
		}
	})
}

func TestReplayValid(t *testing.T) {
	var seen []int64
	err := Replay([]Event{mkEvent(1), mkEvent(2), mkEvent(3)}, func(e Event) error {
		seen = append(seen, e.Seq)
		t.Logf("应用事件 seq=%d，判定依据：序号严格等于前一序号+1", e.Seq)
		return nil
	})
	if err != nil {
		t.Fatal(err)
	}
	if len(seen) != 3 || seen[2] != 3 {
		t.Fatalf("applied %v", seen)
	}
}
