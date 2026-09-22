package engine

import (
	"errors"
	"fmt"
	"log"

	"github.com/lisama569/order-matching-engine-go/internal/event"
	"github.com/lisama569/order-matching-engine-go/internal/market"
)

// Replay 从事件流确定性重建状态。
//
// 协议：
//  1. Seq 必须从 1 开始严格连续递增；重复、乱序或存在缺口一律拒绝并返回错误，
//     绝不静默跳过；校验失败时服务原状态保持不变。
//  2. 事件语义必须可重算：以 ACCEPTED 为起点重放撮合，重算出的 FILLED/BOOKED
//     序列必须与流中完全一致；REJECTED 必须独立成单事件；CANCELED 必须能在
//     重建出的簿中找到对应委托。任何不一致都明确拒绝。
//  3. 重放只允许在服务未启动时进行；成功后实时状态与事件流均被替换为重建结果，
//     同一事件流多次重建必然得到相同订单簿与账户敞口。
func (e *Engine) Replay(events []event.Event) (retErr error) {
	if e.running {
		return fmt.Errorf("%w: engine is running, stop it before replay", ErrReplay)
	}
	// 全部校验与重算在临时状态上进行，任何一步失败都不影响现有状态。
	fresh := newState(e.cfg)
	store := event.NewStore()

	// Phase 1: 序号协议校验。
	for _, ev := range events {
		if err := store.Append(ev); err != nil {
			if errors.Is(err, event.ErrSeqDuplicate) {
				return fmt.Errorf("%w: %w", ErrReplay, err)
			}
			return fmt.Errorf("%w: %w", ErrReplay, err)
		}
	}

	// Phase 2: 按事件语义重算并逐字段比对。
	var pending []event.Event // 当前 ACCEPTED 后期望出现的派生事件
	for i := range events {
		ev := events[i]
		log.Printf("replay: inspect seq=%d kind=%s order=%s", ev.Seq, ev.Kind, ev.OrderID)
		if len(pending) > 0 {
			if err := compareEvent(pending[0], ev); err != nil {
				return fmt.Errorf("%w: seq=%d state derivation mismatch: %v", ErrReplay, ev.Seq, err)
			}
			pending = pending[1:]
			continue
		}
		switch ev.Kind {
		case event.KindOrderAccepted:
			req := submitReqFromEvent(ev)
			reason, derived := e.simulate(fresh, req)
			// 流中声明接受，则重算也必须接受，且首个派生事件就是该 ACCEPTED 本身。
			if reason != market.RejectNone {
				return fmt.Errorf("%w: seq=%d stream says ACCEPTED but re-derivation rejects: %s",
					ErrReplay, ev.Seq, reason)
			}
			if len(derived) == 0 {
				return fmt.Errorf("%w: seq=%d accepted event produced no derivation", ErrReplay, ev.Seq)
			}
			if err := compareEvent(derived[0], ev); err != nil {
				return fmt.Errorf("%w: seq=%d accepted mismatch: %v", ErrReplay, ev.Seq, err)
			}
			pending = derived[1:]
		case event.KindOrderRejected:
			req := submitReqFromEvent(ev)
			reason, derived := e.simulate(fresh, req)
			if len(derived) != 0 {
				return fmt.Errorf("%w: seq=%d stream says REJECTED but re-derivation accepts",
					ErrReplay, ev.Seq)
			}
			if reason != ev.Reason {
				return fmt.Errorf("%w: seq=%d reject reason mismatch: stream=%s replay=%s",
					ErrReplay, ev.Seq, ev.Reason, reason)
			}
			// 其余静态字段也必须与流中记录一致。
			want := event.Event{
				Kind:      event.KindOrderRejected,
				OrderID:   ev.OrderID,
				AccountID: ev.AccountID,
				Symbol:    ev.Symbol,
				Side:      ev.Side,
				OrderType: ev.OrderType,
				Price:     ev.Price,
				Qty:       ev.Qty,
				Reason:    reason,
			}
			if err := compareEvent(want, ev); err != nil {
				return fmt.Errorf("%w: seq=%d rejected mismatch: %v", ErrReplay, ev.Seq, err)
			}
		case event.KindOrderCanceled:
			got, ok := e.deriveCancel(fresh, CancelRequest{OrderID: ev.OrderID})
			if !ok {
				return fmt.Errorf("%w: seq=%d cancel of unknown/inactive order=%s",
					ErrReplay, ev.Seq, ev.OrderID)
			}
			if err := compareEvent(got, ev); err != nil {
				return fmt.Errorf("%w: seq=%d canceled mismatch: %v", ErrReplay, ev.Seq, err)
			}
		case event.KindOrderFilled, event.KindOrderBooked:
			return fmt.Errorf("%w: seq=%d unexpected %s without preceding ACCEPTED",
				ErrReplay, ev.Seq, ev.Kind)
		default:
			return fmt.Errorf("%w: seq=%d unknown event kind=%d", ErrReplay, ev.Seq, ev.Kind)
		}
	}
	if len(pending) > 0 {
		return fmt.Errorf("%w: stream truncated: %d derived event(s) missing", ErrReplay, len(pending))
	}

	// Phase 3: 提交重建结果。
	e.st = fresh
	e.store = store
	log.Printf("replay: rebuilt successfully, lastSeq=%d", store.LastSeq())
	return nil
}

func submitReqFromEvent(ev event.Event) SubmitRequest {
	return SubmitRequest{
		OrderID:   ev.OrderID,
		AccountID: ev.AccountID,
		Symbol:    ev.Symbol,
		Side:      ev.Side,
		Type:      ev.OrderType,
		Price:     ev.Price,
		Qty:       ev.Qty,
	}
}

// compareEvent 逐字段比对两个除 Seq 外应当完全一致的事件。
func compareEvent(want, got event.Event) error {
	if want.Kind != got.Kind {
		return fmt.Errorf("kind want=%s got=%s", want.Kind, got.Kind)
	}
	if want.OrderID != got.OrderID {
		return fmt.Errorf("orderID want=%s got=%s", want.OrderID, got.OrderID)
	}
	if want.AccountID != got.AccountID {
		return fmt.Errorf("accountID want=%s got=%s", want.AccountID, got.AccountID)
	}
	if want.Symbol != got.Symbol {
		return fmt.Errorf("symbol want=%s got=%s", want.Symbol, got.Symbol)
	}
	if want.Side != got.Side {
		return fmt.Errorf("side want=%s got=%s", want.Side, got.Side)
	}
	if want.OrderType != got.OrderType {
		return fmt.Errorf("orderType want=%s got=%s", want.OrderType, got.OrderType)
	}
	if want.Price != got.Price {
		return fmt.Errorf("price want=%d got=%d", want.Price, got.Price)
	}
	if want.Qty != got.Qty {
		return fmt.Errorf("qty want=%d got=%d", want.Qty, got.Qty)
	}
	if want.MakerOrderID != got.MakerOrderID {
		return fmt.Errorf("makerOrderID want=%s got=%s", want.MakerOrderID, got.MakerOrderID)
	}
	if want.MakerAccount != got.MakerAccount {
		return fmt.Errorf("makerAccount want=%s got=%s", want.MakerAccount, got.MakerAccount)
	}
	if want.Reason != got.Reason {
		return fmt.Errorf("reason want=%s got=%s", want.Reason, got.Reason)
	}
	return nil
}
