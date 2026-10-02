package event

import (
	"encoding/binary"

	"github.com/lisama569/order-matching-engine-go/internal/domain"
)

// FNV-1a 64 常量：事件流滚动哈希的基础。
const (
	offsetBasis64 = 14695981039346656037
	prime64       = 1099511628211
)

// hashEvent 将单个事件折入滚动哈希：覆盖序号、类型与全部负载字段，
// 同一事件流无论从零累积还是从检查点基线继续累积，结果逐位一致。
func hashEvent(h uint64, e Event) uint64 {
	h = hashUint64(h, uint64(e.Seq))
	h = hashString(h, string(e.Payload.Type))
	h = hashOrder(h, e.Payload.Order)
	h = hashFill(h, e.Payload.Fill)
	h = hashOrder(h, e.Payload.ClosedOrder)
	return h
}

func hashUint64(h, v uint64) uint64 {
	var b [8]byte
	binary.LittleEndian.PutUint64(b[:], v)
	for _, c := range b {
		h ^= uint64(c)
		h *= prime64
	}
	return h
}

func hashString(h uint64, s string) uint64 {
	for i := 0; i < len(s); i++ {
		h ^= uint64(s[i])
		h *= prime64
	}
	h ^= 0xff // 字段分隔，避免拼接歧义
	h *= prime64
	return h
}

func hashOrder(h uint64, o *domain.Order) uint64 {
	if o == nil {
		return hashUint64(h, 0)
	}
	h = hashUint64(h, 1)
	h = hashString(h, o.ID)
	h = hashString(h, o.AccountID)
	h = hashString(h, o.Symbol)
	h = hashUint64(h, uint64(o.Side))
	h = hashUint64(h, uint64(o.Type))
	h = hashUint64(h, uint64(o.Price))
	h = hashUint64(h, uint64(o.Quantity))
	h = hashUint64(h, uint64(o.FilledQty))
	h = hashUint64(h, uint64(o.Status))
	h = hashUint64(h, uint64(o.Sequence))
	return h
}

func hashFill(h uint64, f *domain.Fill) uint64 {
	if f == nil {
		return hashUint64(h, 0)
	}
	h = hashUint64(h, 1)
	h = hashUint64(h, uint64(f.Sequence))
	h = hashString(h, f.TradeID)
	h = hashString(h, f.TakerOrderID)
	h = hashString(h, f.MakerOrderID)
	h = hashString(h, f.AccountTaker)
	h = hashString(h, f.AccountMaker)
	h = hashString(h, f.Symbol)
	h = hashUint64(h, uint64(f.TakerSide))
	h = hashUint64(h, uint64(f.Price))
	h = hashUint64(h, uint64(f.Quantity))
	return h
}

// HashOf 从零计算一段事件流的滚动哈希（用于校验检查点覆盖的事件前缀）。
// 返回的哈希与 Store 逐条 Append 同样事件后的 Hash() 一致。
func HashOf(events []Event) uint64 {
	h := uint64(offsetBasis64)
	for _, e := range events {
		h = hashEvent(h, e)
	}
	return h
}
