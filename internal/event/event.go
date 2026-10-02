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
//
// 支持检查点基线：base > 0 时，序号 1..base 的事件已由某个检查点覆盖而不再持有，
// 日志只保存 base 之后的事件；滚动哈希 hash 仍然覆盖全部已追加事件（含基线之前），
// 因此可以从检查点继续追加且哈希与完整日志逐位一致。
type Store struct {
	base   int64  // 检查点覆盖的最大序号（0 表示完整日志）
	hash   uint64 // 覆盖 1..LastSeq() 全部事件的滚动哈希
	events []Event
}

// NewStore 创建空事件日志。
func NewStore() *Store {
	return &Store{hash: offsetBasis64}
}

// NewStoreAt 创建带检查点基线的事件日志：下一个期望序号为 base+1，
// 滚动哈希从 baseHash（即序号 1..base 的事件哈希）继续累积。
func NewStoreAt(base int64, baseHash uint64) *Store {
	if base < 0 {
		base = 0
	}
	return &Store{base: base, hash: baseHash}
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
	s.hash = hashEvent(s.hash, e)
	return nil
}

// All 按序号返回当前持有的全部事件副本（切片拷贝，避免外部修改）。
// 带基线时只含基线之后的事件。
func (s *Store) All() []Event {
	out := make([]Event, len(s.events))
	copy(out, s.events)
	return out
}

// From 返回序号 >= fromSeq 的事件副本（订阅者断线追赶用）。
func (s *Store) From(fromSeq int64) []Event {
	if fromSeq < s.base+1 {
		fromSeq = s.base + 1
	}
	if fromSeq > s.LastSeq() {
		return nil
	}
	start := fromSeq - s.base - 1
	out := make([]Event, s.LastSeq()-fromSeq+1)
	copy(out, s.events[start:])
	return out
}

// LastSeq 返回当前最大序号（空日志且基线为 0 时为 0）。
func (s *Store) LastSeq() int64 { return s.base + int64(len(s.events)) }

// Base 返回检查点基线序号（0 表示完整日志）。
func (s *Store) Base() int64 { return s.base }

// Hash 返回覆盖序号 1..LastSeq() 全部事件的滚动哈希。
// 同一事件流无论从零累积还是从检查点基线继续累积，结果逐位一致。
func (s *Store) Hash() uint64 { return s.hash }
