package event

import (
	"errors"
	"testing"
)

func TestAppendSeqProtocol(t *testing.T) {
	s := NewStore()
	mk := func(seq int64) Event { return Event{Seq: seq, Kind: KindOrderAccepted, OrderID: "x"} }
	if err := s.Append(mk(2)); !errors.Is(err, ErrSeqGap) {
		t.Fatalf("first seq 2 must be gap, got %v", err)
	}
	if err := s.Append(mk(1)); err != nil {
		t.Fatal(err)
	}
	if err := s.Append(mk(1)); !errors.Is(err, ErrSeqDuplicate) {
		t.Fatalf("dup seq must be ErrSeqDuplicate, got %v", err)
	}
	if err := s.Append(mk(3)); !errors.Is(err, ErrSeqGap) {
		t.Fatalf("seq 3 after 1 must be gap, got %v", err)
	}
	if err := s.Append(mk(2)); err != nil {
		t.Fatalf("seq 2 should now be accepted, got %v", err)
	}
	if err := s.Append(mk(0)); !errors.Is(err, ErrSeqOutOfOrder) {
		t.Fatalf("seq 0 must be out-of-order, got %v", err)
	}
	// 任何拒绝都不得推进序号。
	if s.LastSeq() != 2 || len(s.Events()) != 2 {
		t.Fatalf("store state after rejects: last=%d len=%d", s.LastSeq(), len(s.Events()))
	}
}
