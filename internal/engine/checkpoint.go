package engine

import (
	"encoding/binary"
	"fmt"
	"hash/fnv"
	"sort"

	"github.com/lisama569/order-matching-engine-go/internal/domain"
	"github.com/lisama569/order-matching-engine-go/internal/event"
	"github.com/lisama569/order-matching-engine-go/internal/risk"
)

// AccountCheckpoint 为单账户-品种的风控状态检查点项。
type AccountCheckpoint struct {
	AccountID string
	Symbol    string
	State     risk.AccountState
}

// Checkpoint 为事件流推进到 LastSeq 时全部派生状态的可校验快照。
// 之后可只重放 LastSeq 之后的剩余事件把状态补齐，
// 补齐结果与从零完整重建逐字段一致。
//
// 盘口不单独落盘：在簿挂单（限价、未终结、有剩余）按受理序号（Sequence）
// 排序重挂即可恢复价格-时间优先队列，与事件重放得到的簿子逐字段一致。
type Checkpoint struct {
	LastSeq       int64
	LastEventHash uint64              // 序号 LastSeq 处事件的内容指纹：识别"放错位置"的检查点
	Orders        []domain.Order      // 委托登记表（含已终结委托，保持撤单判定行为）
	Accounts      []AccountCheckpoint // 账户敞口与名义占用
	Checksum      uint64              // 覆盖上述全部字段的完整性校验值
}

// Checkpoint 落当前状态的检查点；可在事件流推进过程中随时调用。
func (e *Engine) Checkpoint() Checkpoint {
	e.mu.Lock()
	defer e.mu.Unlock()

	cp := Checkpoint{LastSeq: e.store.LastSeq()}
	for _, o := range e.registry {
		cp.Orders = append(cp.Orders, *o)
	}
	seen := map[string]bool{}
	for _, o := range e.registry {
		key := o.AccountID + "/" + o.Symbol
		if seen[key] {
			continue
		}
		seen[key] = true
		cp.Accounts = append(cp.Accounts, AccountCheckpoint{
			AccountID: o.AccountID, Symbol: o.Symbol,
			State: e.riskMgr.State(o.AccountID, o.Symbol),
		})
	}
	if last, ok := e.store.Last(); ok {
		cp.LastEventHash = hashEvent(last)
	}
	cp.sortAndSeal()
	e.logf("CHECKPOINT 落盘 seq=%d orders=%d accounts=%d checksum=%016x",
		cp.LastSeq, len(cp.Orders), len(cp.Accounts), cp.Checksum)
	return cp
}

// sortAndSeal 确定性排序并计算校验值。
func (cp *Checkpoint) sortAndSeal() {
	sort.Slice(cp.Orders, func(i, j int) bool { return cp.Orders[i].ID < cp.Orders[j].ID })
	sort.Slice(cp.Accounts, func(i, j int) bool {
		a, b := cp.Accounts[i], cp.Accounts[j]
		if a.AccountID != b.AccountID {
			return a.AccountID < b.AccountID
		}
		return a.Symbol < b.Symbol
	})
	cp.Checksum = cp.computeChecksum()
}

func (cp *Checkpoint) computeChecksum() uint64 {
	h := fnv.New64a()
	var buf [8]byte
	put := func(v int64) { binary.LittleEndian.PutUint64(buf[:], uint64(v)); h.Write(buf[:]) }
	put(cp.LastSeq)
	put(int64(cp.LastEventHash))
	for _, o := range cp.Orders {
		h.Write([]byte(o.ID))
		h.Write([]byte{0})
		h.Write([]byte(o.AccountID))
		h.Write([]byte{0})
		h.Write([]byte(o.Symbol))
		h.Write([]byte{0})
		put(int64(o.Side))
		put(int64(o.Type))
		put(o.Price)
		put(o.Quantity)
		put(o.FilledQty)
		put(int64(o.Status))
		put(o.Sequence)
	}
	for _, a := range cp.Accounts {
		h.Write([]byte(a.AccountID))
		h.Write([]byte{0})
		h.Write([]byte(a.Symbol))
		h.Write([]byte{0})
		put(a.State.Position)
		put(a.State.OpenBuy)
		put(a.State.OpenSell)
		put(int64(a.State.OpenCount))
	}
	return h.Sum64()
}

// validate 校验检查点完整性与相对事件流的位置。
func (cp *Checkpoint) validate(events []event.Event) error {
	if cp.Checksum != cp.computeChecksum() {
		return fmt.Errorf("检查点损坏: 校验值不匹配 (checksum=%016x)", cp.Checksum)
	}
	if cp.LastSeq < 0 {
		return fmt.Errorf("检查点损坏: LastSeq=%d 为负", cp.LastSeq)
	}
	// LastSeq=0 合法的唯一形态是空检查点（事件流也为空）；
	// 携带状态却声称位于起点的检查点必然是损坏/放错，按全量重放会与状态重复。
	if cp.LastSeq == 0 && (len(cp.Orders) > 0 || len(cp.Accounts) > 0) {
		return fmt.Errorf("检查点放错位置: LastSeq=0 却携带 orders=%d accounts=%d 状态",
			len(cp.Orders), len(cp.Accounts))
	}
	if cp.LastSeq > int64(len(events)) {
		return fmt.Errorf("检查点放错位置: LastSeq=%d 超出事件流长度 %d", cp.LastSeq, len(events))
	}
	if cp.LastSeq >= 1 {
		if events[cp.LastSeq-1].Seq != cp.LastSeq {
			return fmt.Errorf("检查点放错位置: 事件流第 %d 个事件序号为 %d，与 LastSeq=%d 不衔接",
				cp.LastSeq, events[cp.LastSeq-1].Seq, cp.LastSeq)
		}
		if h := hashEvent(events[cp.LastSeq-1]); h != cp.LastEventHash {
			return fmt.Errorf("检查点放错位置: 序号 %d 处事件指纹=%016x，检查点记录=%016x",
				cp.LastSeq, h, cp.LastEventHash)
		}
	}
	if cp.LastSeq < int64(len(events)) && events[cp.LastSeq].Seq != cp.LastSeq+1 {
		return fmt.Errorf("检查点放错位置: 剩余事件从序号 %d 开始，期望 %d",
			events[cp.LastSeq].Seq, cp.LastSeq+1)
	}
	return nil
}

// restore 把检查点状态装入 ghost 引擎（调用方持锁）。
func (e *Engine) restore(ghost *Engine, cp *Checkpoint) {
	ghost.store.SetBase(cp.LastSeq)
	// 一次性分配并整体拷贝委托表（检查点本身不被引擎后续修改），
	// 避免逐条堆分配；大检查点下恢复耗时可降数倍。
	orders := make([]domain.Order, len(cp.Orders))
	copy(orders, cp.Orders)
	ghost.registry = make(map[string]*domain.Order, len(orders))
	for i := range orders {
		ghost.registry[orders[i].ID] = &orders[i]
	}
	// 在簿挂单按受理序号重挂，恢复价格-时间优先队列。
	var resting []*domain.Order
	for _, o := range ghost.registry {
		if o.Type == domain.OrderLimit && o.Remaining() > 0 &&
			(o.Status == domain.StatusNew || o.Status == domain.StatusPartiallyFilled) {
			resting = append(resting, o)
		}
	}
	sort.Slice(resting, func(i, j int) bool { return resting[i].Sequence < resting[j].Sequence })
	for _, o := range resting {
		ghost.book(o.Symbol).Add(o)
	}
	for _, a := range cp.Accounts {
		ghost.riskMgr.RestoreState(a.AccountID, a.Symbol, a.State)
	}
}

// Rebuild 基于检查点增量重建：
//   - cp 为 nil（缺失）：直接从序号 1 全量重建；
//   - cp 损坏（校验值不匹配）或放错位置（与事件流不衔接）：
//     记录原因并安全退回全量重建，绝不顺着错误起点继续；
//   - cp 合法：从 cp.LastSeq+1 起按序重放剩余事件，序号连续性判定规则不变。
//
// 与 Replay 相同，先在影子引擎上完成全部重放，成功后才替换线上状态。
func (e *Engine) Rebuild(cp *Checkpoint, events []event.Event) error {
	e.mu.Lock()
	defer e.mu.Unlock()

	ghost := New(e.symbols, e.riskCfg, e.refPrices, e.allowedAccounts)
	start := 0
	if cp != nil {
		if err := cp.validate(events); err != nil {
			e.logf("CHECKPOINT 不可用: %v | 判定依据: 安全退回全量重建(从序号 1 开始)", err)
		} else if err := verifySequence(events[int(cp.LastSeq):], cp.LastSeq+1); err != nil {
			// 定位校验通过（指纹能对上），但尾部事件自身的序号链缺口/重复/乱序：
			// 同样不顺着检查点往下走，安全退回全量重建。
			e.logf("CHECKPOINT 尾部序号对不上: %v | 判定依据: 安全退回全量重建(从序号 1 开始)", err)
		} else {
			e.restore(ghost, cp)
			start = int(cp.LastSeq)
			e.logf("CHECKPOINT 生效 seq=%d，增量重放剩余 %d 个事件", cp.LastSeq, len(events)-start)
		}
	} else {
		e.logf("CHECKPOINT 缺失 | 判定依据: 全量重建(从序号 1 开始)")
	}

	err := event.ReplayFrom(events[start:], int64(start)+1, func(ev event.Event) error {
		if appendErr := ghost.store.Append(ev); appendErr != nil {
			return appendErr
		}
		ghost.mutate(ev)
		return nil
	})
	if err != nil {
		// 退回全量后仍失败：事件流本身畸形（缺口/重复/乱序），保持线上状态不变并明确报错。
		if start != 0 {
			return fmt.Errorf("incremental rebuild from checkpoint: %w", err)
		}
		return err
	}

	e.books = ghost.books
	e.registry = ghost.registry
	e.riskMgr = ghost.riskMgr
	e.store = ghost.store
	e.depth.dirty.Store(true)
	return nil
}

// verifySequence 只校验事件序号链是否严格从 startSeq 连续（不应用状态）。
func verifySequence(events []event.Event, startSeq int64) error {
	return event.ReplayFrom(events, startSeq, func(event.Event) error { return nil })
}

// hashEvent 计算事件内容指纹（检查点定位用）：覆盖序号、类型与全部负载字段。
func hashEvent(ev event.Event) uint64 {
	h := fnv.New64a()
	var buf [8]byte
	put := func(v int64) { binary.LittleEndian.PutUint64(buf[:], uint64(v)); h.Write(buf[:]) }
	put(ev.Seq)
	h.Write([]byte(ev.Payload.Type))
	h.Write([]byte{0})
	if o := ev.Payload.Order; o != nil {
		h.Write([]byte(o.ID))
		h.Write([]byte{0})
		h.Write([]byte(o.AccountID))
		h.Write([]byte{0})
		h.Write([]byte(o.Symbol))
		h.Write([]byte{0})
		put(int64(o.Side))
		put(int64(o.Type))
		put(o.Price)
		put(o.Quantity)
		put(o.FilledQty)
		put(int64(o.Status))
	}
	if f := ev.Payload.Fill; f != nil {
		h.Write([]byte(f.TradeID))
		h.Write([]byte{0})
		h.Write([]byte(f.TakerOrderID))
		h.Write([]byte{0})
		h.Write([]byte(f.MakerOrderID))
		h.Write([]byte{0})
		put(f.Price)
		put(f.Quantity)
	}
	if c := ev.Payload.ClosedOrder; c != nil {
		h.Write([]byte(c.ID))
		h.Write([]byte{0})
		put(int64(c.Status))
	}
	return h.Sum64()
}
