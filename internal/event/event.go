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
// base 记录已被检查点归档、不再由本日志持有的事件数：
// 实时撮合与全量重建时 base=0；基于检查点增量重建时 base=检查点序号，
// 日志只持有检查点之后的事件，内存不随历史事件数增长。
type Store struct {
	base   int64
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
	expected := s.LastSeq() + 1
	if e.Seq <= s.LastSeq() {
		return &SequenceError{Kind: SeqDuplicate, Got: e.Seq, Expected: expected}
	}
	if e.Seq > expected {
		return &SequenceError{Kind: SeqGap, Got: e.Seq, Expected: expected}
	}
	s.events = append(s.events, e)
	return nil
}

// SetBase 设置已归档事件数（检查点增量重建用）。
// 设置后本日志只持有 base 之后的事件，序号校验以 base 为起点继续。
func (s *Store) SetBase(base int64) { s.base = base }

// Base 返回已归档事件数。
func (s *Store) Base() int64 { return s.base }

// All 按序号返回本日志持有的全部事件副本（切片拷贝，避免外部修改）。
// 注意：base>0 时只含检查点之后的事件。
func (s *Store) All() []Event {
	out := make([]Event, len(s.events))
	copy(out, s.events)
	return out
}

// From 返回序号 >= fromSeq 的事件副本（订阅者断线追赶用）。
// fromSeq 落在已归档区间（<= base）时，只能从 base+1 起返回本日志持有的部分。
func (s *Store) From(fromSeq int64) []Event {
	if fromSeq < s.base+1 {
		fromSeq = s.base + 1
	}
	if fromSeq > s.LastSeq() {
		return nil
	}
	out := make([]Event, s.LastSeq()-fromSeq+1)
	copy(out, s.events[fromSeq-s.base-1:])
	return out
}

// LastSeq 返回当前最大序号（空日志为 base）。
func (s *Store) LastSeq() int64 { return s.base + int64(len(s.events)) }

// Last 返回日志持有的最后一个事件；空日志返回 false。
func (s *Store) Last() (Event, bool) {
	if len(s.events) == 0 {
		return Event{}, false
	}
	return s.events[len(s.events)-1], true
}
