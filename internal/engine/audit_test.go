package engine

import (
	"testing"

	"github.com/lisama569/order-matching-engine-go/internal/audit"
	"github.com/lisama569/order-matching-engine-go/internal/domain"
	"github.com/lisama569/order-matching-engine-go/internal/event"
)

func findFinding(rep audit.Report, kind audit.Kind) *audit.Finding {
	for i := range rep.Findings {
		if rep.Findings[i].Kind == kind {
			return &rep.Findings[i]
		}
	}
	return nil
}

// TestAuditPassOnHealthyEngine：健康引擎审计通过，且日志含事件进度。
func TestAuditPassOnHealthyEngine(t *testing.T) {
	e := buildScenario(t)
	rep := e.Audit()
	if !rep.Pass {
		t.Fatalf("健康引擎审计应通过: %+v", rep.Findings)
	}
	if rep.LastSeq != e.Events().LastSeq() || rep.CheckedEvents != e.Events().LastSeq() {
		t.Fatalf("审计事件进度不符: %+v", rep)
	}
	t.Logf("健康引擎: 审计通过 lastSeq=%d checkedEvents=%d ✔", rep.LastSeq, rep.CheckedEvents)
}

// TestAuditDetectsBookDepthMismatch：人为把一笔挂单从簿中摘掉（注册表仍认为在挂单），
// 审计必须报 BOOK_DEPTH_MISMATCH 并指到该委托与其受理序号附近。
func TestAuditDetectsBookDepthMismatch(t *testing.T) {
	e := buildScenario(t)
	// 找到一笔在簿挂单，直接从簿中移除（模拟簿子与注册表不一致）
	var victim *domain.Order
	for _, o := range e.registry {
		if o.Symbol == "BTCUSD" && o.Type == domain.OrderLimit && o.Remaining() > 0 &&
			(o.Status == domain.StatusNew || o.Status == domain.StatusPartiallyFilled) {
			victim = o
			break
		}
	}
	if victim == nil {
		t.Skip("场景中没有在簿挂单")
	}
	if !e.books["BTCUSD"].Remove(victim.ID) {
		t.Fatalf("注入失败: %s 不在簿中", victim.ID)
	}
	rep := e.Audit()
	if rep.Pass {
		t.Fatal("注入簿子不一致后审计必须判定不通过")
	}
	f := findFinding(rep, audit.KindBookDepthMismatch)
	if f == nil {
		t.Fatalf("应报告 BOOK_DEPTH_MISMATCH: %+v", rep.Findings)
	}
	if f.OrderID != victim.ID || f.Seq != victim.Sequence {
		t.Fatalf("定位信息不符: got order=%s seq=%d, want order=%s seq=%d",
			f.OrderID, f.Seq, victim.ID, victim.Sequence)
	}
	t.Logf("审计识别: kind=%s order=%s nearSeq=%d detail=%s ✔", f.Kind, f.OrderID, f.Seq, f.Detail)
}

// TestAuditDetectsOpenOrderMismatch：人为虚增账户挂单占用，审计报 OPEN_ORDER_MISMATCH。
func TestAuditDetectsOpenOrderMismatch(t *testing.T) {
	e := buildScenario(t)
	// 虚增 alice 的卖单名义占用与笔数（绕过事件流直接改风控状态）
	e.riskMgr.OnAccepted("alice", "BTCUSD", domain.SideSell, 100, 5)

	rep := e.Audit()
	if rep.Pass {
		t.Fatal("虚增挂单占用后审计必须判定不通过")
	}
	f := findFinding(rep, audit.KindOpenOrderMismatch)
	if f == nil {
		t.Fatalf("应报告 OPEN_ORDER_MISMATCH: %+v", rep.Findings)
	}
	if f.AccountID != "alice" || f.Symbol != "BTCUSD" {
		t.Fatalf("定位信息不符: %+v", f)
	}
	if f.Seq <= 0 || f.Seq > e.Events().LastSeq() {
		t.Fatalf("差异定位序号超出事件范围: %d", f.Seq)
	}
	t.Logf("审计识别: kind=%s account=%s nearSeq=%d detail=%s ✔", f.Kind, f.AccountID, f.Seq, f.Detail)
}

// TestAuditDetectsPositionMismatch：人为改持仓，审计报 POSITION_MISMATCH 并指到该账户最后一笔成交附近。
func TestAuditDetectsPositionMismatch(t *testing.T) {
	e := buildScenario(t)
	// bob 在 BTCUSD 有成交（s2 部分被吃）；人为加一笔绕过事件流的"成交"
	e.riskMgr.OnFill("bob", "BTCUSD", domain.SideBuy, 100, 3, false)

	rep := e.Audit()
	if rep.Pass {
		t.Fatal("篡改持仓后审计必须判定不通过")
	}
	f := findFinding(rep, audit.KindPositionMismatch)
	if f == nil {
		t.Fatalf("应报告 POSITION_MISMATCH: %+v", rep.Findings)
	}
	if f.AccountID != "bob" || f.Symbol != "BTCUSD" {
		t.Fatalf("定位信息不符: %+v", f)
	}
	// 定位序号应是 bob/BTCUSD 最后一笔成交事件的序号
	var lastFillSeq int64
	for _, ev := range e.Events().All() {
		if ev.Payload.Fill != nil && ev.Payload.Fill.Symbol == "BTCUSD" &&
			(ev.Payload.Fill.AccountTaker == "bob" || ev.Payload.Fill.AccountMaker == "bob") {
			lastFillSeq = ev.Seq
		}
	}
	if f.Seq != lastFillSeq {
		t.Fatalf("定位序号=%d, 应为 bob 最后一笔成交 seq=%d", f.Seq, lastFillSeq)
	}
	t.Logf("审计识别: kind=%s account=%s nearSeq=%d(最后成交) detail=%s ✔", f.Kind, f.AccountID, f.Seq, f.Detail)
}

// TestAuditDetectsSequenceIssues：事件序号断裂与哈希不符都能被识别（纯函数层注入）。
func TestAuditDetectsSequenceIssues(t *testing.T) {
	e := buildScenario(t)
	events := e.Events().All()

	// 1) 序号断裂：抽掉中间一条事件
	broken := append(append([]event.Event{}, events[:3]...), events[4:]...)
	rep := audit.Check(audit.Input{
		Base: 0, Events: broken, StoreHash: event.HashOf(broken),
		Depth: map[string]audit.BookDepth{},
	})
	if rep.Pass || findFinding(rep, audit.KindSeqDiscontinuity) == nil {
		t.Fatalf("序号断裂应被识别: pass=%v findings=%+v", rep.Pass, rep.Findings)
	}
	f := findFinding(rep, audit.KindSeqDiscontinuity)
	t.Logf("审计识别: kind=%s nearSeq=%d detail=%s ✔", f.Kind, f.Seq, f.Detail)

	// 2) 哈希不符：事件流与日志记录哈希对不上
	rep2 := audit.Check(audit.Input{
		Base: 0, Events: events, StoreHash: event.HashOf(events) + 1,
		Depth: map[string]audit.BookDepth{},
	})
	if rep2.Pass || findFinding(rep2, audit.KindEventHashMismatch) == nil {
		t.Fatalf("哈希不符应被识别: pass=%v findings=%+v", rep2.Pass, rep2.Findings)
	}
	t.Logf("审计识别: kind=%s detail=%s ✔", audit.KindEventHashMismatch,
		findFinding(rep2, audit.KindEventHashMismatch).Detail)
}

// TestAuditAfterCheckpointRebuild：检查点增量重建后的引擎审计仍通过；
// 基线之前的事件不在手边，对应检查项降级跳过并明确标注。
func TestAuditAfterCheckpointRebuild(t *testing.T) {
	live := newLoadEngine(t)
	generateLoad(t, live, 500)
	cp := live.CaptureCheckpoint()
	generateLoad(t, live, 1000)

	g := newLoadEngine(t)
	if _, err := g.Rebuild(&cp, live.Events().All()[cp.Seq:]); err != nil {
		t.Fatal(err)
	}
	rep := g.Audit()
	if !rep.Pass {
		t.Fatalf("增量重建后审计应通过: %+v", rep.Findings)
	}
	if len(rep.Skipped) == 0 {
		t.Fatal("基线场景应标注降级跳过的检查项")
	}
	t.Logf("增量重建后审计通过 lastSeq=%d, 降级跳过项=%v ✔", rep.LastSeq, rep.Skipped)
}
