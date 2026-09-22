package event

import (
	"errors"
	"fmt"

	"github.com/lisama569/order-matching-engine-go/internal/market"
)

// Kind 事件类型。
type Kind int

const (
	KindUnknown Kind = 0
	// KindOrderAccepted 委托通过校验与风控，进入撮合/挂簿流程。
	KindOrderAccepted Kind = 1
	// KindOrderRejected 委托在产生任何确定状态变更前被拒绝（含风控拒绝）。
	KindOrderRejected Kind = 2
	// KindOrderFilled 产生一笔成交（taker 与某 maker）。
	KindOrderFilled Kind = 3
	// KindOrderBooked 委托有剩余量挂入订单簿。
	KindOrderBooked Kind = 4
	// KindOrderCanceled 簿中委托被撤销，含撤掉的剩余数量。
	KindOrderCanceled Kind = 5
)

func (k Kind) String() string {
	switch k {
	case KindOrderAccepted:
		return "ORDER_ACCEPTED"
	case KindOrderRejected:
		return "ORDER_REJECTED"
	case KindOrderFilled:
		return "ORDER_FILLED"
	case KindOrderBooked:
		return "ORDER_BOOKED"
	case KindOrderCanceled:
		return "ORDER_CANCELED"
	default:
		return "UNKNOWN"
	}
}

// Event 有序状态变更事件。Seq 从 1 起严格连续递增。
type Event struct {
	Seq       int64
	Kind      Kind
	OrderID   string
	AccountID string
	Symbol    string
	Side      market.Side
	OrderType market.OrderType
	Price     int64
	Qty       int64 // Fill 数量 / Cancel 的撤销量 / Booked 的挂单量
	Reason    market.RejectReason
	// Fill 相关
	MakerOrderID string
	MakerAccount string
}

func (e Event) String() string {
	switch e.Kind {
	case KindOrderFilled:
		return fmt.Sprintf("seq=%d %s taker=%s(%s) maker=%s(%s) price=%d qty=%d",
			e.Seq, e.Kind, e.OrderID, e.AccountID, e.MakerOrderID, e.MakerAccount, e.Price, e.Qty)
	case KindOrderRejected:
		return fmt.Sprintf("seq=%d %s order=%s acct=%s reason=%s",
			e.Seq, e.Kind, e.OrderID, e.AccountID, e.Reason)
	default:
		return fmt.Sprintf("seq=%d %s order=%s acct=%s side=%s type=%s price=%d qty=%d",
			e.Seq, e.Kind, e.OrderID, e.AccountID, e.Side, e.OrderType, e.Price, e.Qty)
	}
}

// 序号违规错误，调用方可 errors.Is 判定。
var (
	ErrSeqOutOfOrder = errors.New("event seq out of order: expected strictly increasing continuation")
	ErrSeqDuplicate  = errors.New("event seq duplicated")
	ErrSeqGap        = errors.New("event seq gap detected")
)

// Store 有序事件存储（内存）。非并发安全，由上层串行驱动。
type Store struct {
	events []Event
}

// NewStore 创建事件存储。
func NewStore() *Store { return &Store{} }

// Append 追加事件；序号必须恰好等于 LastSeq()+1，
// 否则区分返回重复、乱序或缺口错误，绝不静默推进。
func (s *Store) Append(ev Event) error {
	want := int64(len(s.events)) + 1
	if ev.Seq == want {
		s.events = append(s.events, ev)
		return nil
	}
	if ev.Seq < want {
		// 判断是落在已存在区间（重复）还是非法小值。
		if ev.Seq >= 1 && ev.Seq <= int64(len(s.events)) {
			return fmt.Errorf("%w: got=%d last=%d", ErrSeqDuplicate, ev.Seq, s.LastSeq())
		}
		return fmt.Errorf("%w: got=%d expected=%d", ErrSeqOutOfOrder, ev.Seq, want)
	}
	return fmt.Errorf("%w: got=%d expected=%d", ErrSeqGap, ev.Seq, want)
}

// Events 返回事件副本切片。
func (s *Store) Events() []Event {
	out := make([]Event, len(s.events))
	copy(out, s.events)
	return out
}

// LastSeq 返回最后一个事件序号，空存储为 0。
func (s *Store) LastSeq() int64 { return int64(len(s.events)) }
