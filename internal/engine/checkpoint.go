package engine

import (
	"errors"
	"fmt"
	"sort"

	"github.com/lisama569/order-matching-engine-go/internal/checkpoint"
	"github.com/lisama569/order-matching-engine-go/internal/domain"
	"github.com/lisama569/order-matching-engine-go/internal/event"
	"github.com/lisama569/order-matching-engine-go/internal/orderbook"
	"github.com/lisama569/order-matching-engine-go/internal/risk"
)

// CheckpointState 是某一事件序号上的完整撮合状态（检查点内容）。
// 体积只与"未成交挂单 + 账户数"相关，与历史事件总数无关。
type CheckpointState struct {
	Seq       int64                             // 覆盖到的事件序号（含）
	EventHash uint64                            // 序号 1..Seq 事件流的滚动哈希
	Orders    []domain.Order                    // 注册表全部委托（含状态与已成交量）
	Levels    map[string][]orderbook.LevelState // 品种 -> 簿子档位（FIFO 顺序）
	Accounts  []risk.AccountEntry               // 账户风控状态
}

// 检查点退回全量重建的原因编码（可判定、可复现）。
const (
	FallbackNone         = ""                         // 使用了检查点，未退回
	FallbackNoCheckpoint = "NO_CHECKPOINT"            // 检查点缺失
	FallbackMisplaced    = "CHECKPOINT_MISPLACED"     // 检查点位置与事件流对不上
	FallbackHashMismatch = "CHECKPOINT_HASH_MISMATCH" // 检查点覆盖的事件前缀哈希不符（放错流/放错位置）
	FallbackCorrupt      = "CHECKPOINT_CORRUPT"       // 检查点内容自身不一致
)

// RebuildResult 描述一次重建走的路径与工作量。
type RebuildResult struct {
	UsedCheckpoint bool   // 是否基于检查点增量重建
	FallbackReason string // 退回全量重建的原因（未退回为空）
	FromSeq        int64  // 本次实际重放的起始事件序号
	ReplayedEvents int    // 本次实际重放的事件条数
	LastSeq        int64  // 重建完成后的最大事件序号
}

// CaptureCheckpoint 在当前状态上落检查点（深拷贝，之后的撮合不会影响它）。
func (e *Engine) CaptureCheckpoint() CheckpointState {
	e.mu.Lock()
	defer e.mu.Unlock()
	return e.captureCheckpointLocked()
}

func (e *Engine) captureCheckpointLocked() CheckpointState {
	cp := CheckpointState{
		Seq:       e.store.LastSeq(),
		EventHash: e.store.Hash(),
		Levels:    make(map[string][]orderbook.LevelState, len(e.books)),
	}
	for _, o := range e.registry {
		cp.Orders = append(cp.Orders, *o)
	}
	sort.Slice(cp.Orders, func(i, j int) bool { return cp.Orders[i].ID < cp.Orders[j].ID })
	for sym, bk := range e.books {
		cp.Levels[sym] = bk.ExportLevels()
	}
	cp.Accounts = e.riskMgr.ExportStates()
	return cp
}

// EncodeCheckpoint 将检查点封装为带校验和的字节流（落盘/传输用）。
func EncodeCheckpoint(cp CheckpointState) ([]byte, error) {
	return checkpoint.Encode(cp)
}

// DecodeCheckpoint 解开检查点封装；数据损坏返回 checkpoint.ErrCorrupt，
// 调用方应将其视为"检查点缺失"并退回全量重建。
func DecodeCheckpoint(data []byte) (CheckpointState, error) {
	var cp CheckpointState
	if err := checkpoint.Decode(data, &cp); err != nil {
		return CheckpointState{}, err
	}
	return cp, nil
}

// restoreFromCheckpoint 用检查点内容整体替换引擎状态（调用方持锁）。
// 检查点内容自身不一致（如档位引用了注册表中不存在的委托）返回错误。
func (e *Engine) restoreFromCheckpoint(cp CheckpointState) error {
	registry := make(map[string]*domain.Order, len(cp.Orders))
	for i := range cp.Orders {
		o := cp.Orders[i]
		registry[o.ID] = &o
	}
	books := make(map[string]*orderbook.Book, len(e.symbols))
	for _, s := range e.symbols {
		books[s] = orderbook.New(s)
	}
	for sym, levels := range cp.Levels {
		bk := books[sym]
		if bk == nil {
			bk = orderbook.New(sym)
			books[sym] = bk
		}
		for _, lv := range levels {
			for _, id := range lv.OrderIDs {
				o, ok := registry[id]
				if !ok {
					return fmt.Errorf("checkpoint level %s %s@%d references unknown order %s",
						sym, lv.Side, lv.Price, id)
				}
				if o.Side != lv.Side || o.Price != lv.Price {
					return fmt.Errorf("checkpoint level %s %s@%d holds inconsistent order %s (%s@%d)",
						sym, lv.Side, lv.Price, id, o.Side, o.Price)
				}
				bk.Add(o)
			}
		}
	}
	e.registry = registry
	e.books = books
	e.riskMgr.ImportStates(cp.Accounts)
	e.store = event.NewStoreAt(cp.Seq, cp.EventHash)
	return nil
}

// Rebuild 基于检查点（可选）+ 事件流重建状态，是 Replay 的增量版。
//
// 路径选择（任何一步对不上都安全退回全量重建，绝不顺着错误起点往下走）：
//   - cp 为 nil：直接全量重放（要求事件流从序号 1 开始）；
//   - 事件流从 1 开始（全量流）：校验 cp.Seq 不越界且 cp.EventHash 等于
//     事件流前 cp.Seq 条的滚动哈希，相符则从检查点续放剩余事件，否则退回全量；
//   - 事件流首条序号恰为 cp.Seq+1（只给了增量段）：信任该配对，从检查点续放，
//     序号连续性由 ReplayFrom 照常校验；
//   - 其它任何组合：判定为 CHECKPOINT_MISPLACED，若事件流从 1 开始则退回全量，
//     否则明确报错（没有全量流可退，拒绝静默继续）。
//
// 无论走哪条路径，重放都在影子引擎上完成，全部成功后才替换线上状态。
func (e *Engine) Rebuild(cp *CheckpointState, events []event.Event) (RebuildResult, error) {
	e.mu.Lock()
	defer e.mu.Unlock()

	res := RebuildResult{}
	useCp := false

	switch {
	case cp == nil:
		res.FallbackReason = FallbackNoCheckpoint
	case cp.Seq < 0:
		res.FallbackReason = FallbackMisplaced
	case len(events) == 0:
		// 空事件流：只有"空检查点 + 空流"可以走检查点路径（即重建空状态）。
		if cp.Seq == 0 {
			useCp = true
		} else {
			res.FallbackReason = FallbackMisplaced
		}
	case events[0].Seq == 1:
		// 全量流：用滚动哈希校验检查点确实覆盖该流的前 cp.Seq 条。
		switch {
		case cp.Seq > int64(len(events)):
			res.FallbackReason = FallbackMisplaced
		case event.HashOf(events[:cp.Seq]) != cp.EventHash:
			res.FallbackReason = FallbackHashMismatch
		default:
			useCp = true
		}
	case events[0].Seq == cp.Seq+1:
		// 只给了增量段：配对检查点续放（连续性由 ReplayFrom 校验）。
		useCp = true
	default:
		res.FallbackReason = FallbackMisplaced
	}

	if res.FallbackReason != "" && (len(events) == 0 || events[0].Seq != 1) {
		// 检查点不可用，而手上的事件流又不是从 1 开始的全量流：
		// 无法安全退回全量重建，明确拒绝而不是顺着错误起点往下走。
		return res, fmt.Errorf("rebuild: checkpoint unusable (%s) and event stream does not start at seq 1 (got %s)",
			res.FallbackReason, firstSeqDesc(events))
	}

	ghost := New(e.symbols, e.riskCfg, e.refPrices, e.allowedAccounts)
	ghost.logf = e.logf

	if useCp {
		if err := ghost.restoreFromCheckpoint(*cp); err != nil {
			// 检查点内容自身损坏：若手上有全量流则退回全量，否则报错。
			res.FallbackReason = FallbackCorrupt
			useCp = false
			if len(events) == 0 || events[0].Seq != 1 {
				return res, fmt.Errorf("rebuild: %v and no full event stream to fall back on", err)
			}
			e.logf("REBUILD fallback reason=%s detail=%v", FallbackCorrupt, err)
		}
	}

	if useCp {
		tail := events
		if len(tail) > 0 && tail[0].Seq == 1 {
			tail = tail[cp.Seq:]
		}
		res.FromSeq = cp.Seq + 1
		res.ReplayedEvents = len(tail)
		err := event.ReplayFrom(tail, cp.Seq+1, func(ev event.Event) error {
			if appendErr := ghost.store.Append(ev); appendErr != nil {
				return appendErr
			}
			ghost.mutate(ev)
			return nil
		})
		if err != nil {
			return res, err
		}
		res.UsedCheckpoint = true
		e.logf("REBUILD checkpoint seq=%d replayed=%d tail events", cp.Seq, len(tail))
	} else {
		res.FromSeq = 1
		res.ReplayedEvents = len(events)
		err := event.Replay(events, func(ev event.Event) error {
			if appendErr := ghost.store.Append(ev); appendErr != nil {
				return appendErr
			}
			ghost.mutate(ev)
			return nil
		})
		if err != nil {
			return res, err
		}
		e.logf("REBUILD full replay events=%d fallbackReason=%s", len(events), res.FallbackReason)
	}

	// 全部成功后才替换线上状态（保留总线与订阅者）。
	e.books = ghost.books
	e.registry = ghost.registry
	e.riskMgr = ghost.riskMgr
	e.store = ghost.store
	e.publishDepthLocked()
	res.LastSeq = e.store.LastSeq()
	return res, nil
}

func firstSeqDesc(events []event.Event) string {
	if len(events) == 0 {
		return "empty stream"
	}
	return fmt.Sprintf("first seq %d", events[0].Seq)
}

// SaveCheckpoint 捕获当前检查点并写入文件存储。
func (e *Engine) SaveCheckpoint(fs checkpoint.FileStore) (CheckpointState, error) {
	cp := e.CaptureCheckpoint()
	data, err := EncodeCheckpoint(cp)
	if err != nil {
		return CheckpointState{}, err
	}
	if err := fs.Save(cp.Seq, data); err != nil {
		return CheckpointState{}, err
	}
	return cp, nil
}

// LoadLatestCheckpoint 读取最新检查点；缺失返回 (nil, nil)，损坏返回错误。
func LoadLatestCheckpoint(fs checkpoint.FileStore) (*CheckpointState, error) {
	_, data, err := fs.Latest()
	if errors.Is(err, checkpoint.ErrNotFound) {
		return nil, nil
	}
	if err != nil {
		return nil, err
	}
	cp, err := DecodeCheckpoint(data)
	if err != nil {
		return nil, err
	}
	return &cp, nil
}
