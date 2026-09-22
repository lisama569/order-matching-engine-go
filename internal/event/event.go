// Package event 定义有序状态变更事件、事件存储与重放校验。
package event

import (
	"errors"
	"fmt"

	"github.com/lisama569/order-matching-engine-go/internal/domain"
)

// Type 标识事件种类。
type Type string

const (
	TypeOrderAccepted  Type = "ORDER_ACCEPTED"
	TypeOrderFilled    Type = "ORDER_FILLED"
	TypeOrderCancelled Type = "ORDER_CANCELLED"
)

// Event 是状态机的唯一变更来源。序号必须从 1 开始严格连续递增（无重复、无乱序、无缺口）。
type Event struct {
	Seq     int64
	Payload Payload
}

// Payload 为事件负载（按 Type 区分）。
type Payload struct {
	Type        Type
	Order       *domain.Order // ORDER_ACCEPTED / ORDER_CANCELLED
	Fill        *domain.Fill  // ORDER_FILLED
	ClosedOrder *domain.Order // ORDER_FILLED 时被完全成交并移出簿子的挂单方（部分成交时为 nil）
}

// 序号非法的三类明确判定。
const (
	SeqDuplicate  = "DUPLICATE"    // 序号重复
	SeqOutOfOrder = "OUT_OF_ORDER" // 序号乱序（小于下一个期望序号且未曾出现）
	SeqGap        = "GAP"          // 序号缺口（大于下一个期望序号）
)

// SequenceError 描述序号非法的具体类别，绝不静默跳过。
type SequenceError struct {
	Kind     string // DUPLICATE / OUT_OF_ORDER / GAP
	Got      int64
	Expected int64
}

func (e *SequenceError) Error() string {
	switch e.Kind {
	case SeqDuplicate:
		return fmt.Sprintf("event sequence duplicate: got %d, expected %d", e.Got, e.Expected)
	case SeqOutOfOrder:
		return fmt.Sprintf("event sequence out of order: got %d, expected %d", e.Got, e.Expected)
	default:
		return fmt.Sprintf("event sequence gap: got %d, expected %d", e.Got, e.Expected)
	}
}

// AsSequenceError 提取序号错误。
func AsSequenceError(err error) (*SequenceError, bool) {
	var se *SequenceError
	return se, errors.As(err, &se)
}

// Store 是线程安全的追加式事件日志，是全部状态变更的权威来源。
type Store struct {
	events []Event
}

// NewStore 创建空事件日志。
func NewStore() *Store {
	return &Store{}
}

// Append 校验序号并追加事件：
//   - 序号必须等于 LastSeq()+1；
//   - 小于等于已存在序号 => DUPLICATE；
//   - 大于期望序号 => GAP。
func (s *Store) Append(e Event) error {
	expected := int64(len(s.events)) + 1
	if e.Seq <= int64(len(s.events)) {
		return &SequenceError{Kind: SeqDuplicate, Got: e.Seq, Expected: expected}
	}
	if e.Seq > expected {
		return &SequenceError{Kind: SeqGap, Got: e.Seq, Expected: expected}
	}
	s.events = append(s.events, e)
	return nil
}

// All 按序号返回全部事件副本（切片拷贝，避免外部修改）。
func (s *Store) All() []Event {
	out := make([]Event, len(s.events))
	copy(out, s.events)
	return out
}

// From 返回序号 >= fromSeq 的事件副本（订阅者断线追赶用）。
func (s *Store) From(fromSeq int64) []Event {
	if fromSeq < 1 {
		fromSeq = 1
	}
	if fromSeq > int64(len(s.events)) {
		return nil
	}
	out := make([]Event, int64(len(s.events))-fromSeq+1)
	copy(out, s.events[fromSeq-1:])
	return out
}

// LastSeq 返回当前最大序号（空日志为 0）。
func (s *Store) LastSeq() int64 { return int64(len(s.events)) }
