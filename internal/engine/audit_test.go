package engine

import (
	"testing"

	"github.com/lisama569/order-matching-engine-go/internal/risk"
)

// TestAuditHealthyEnginePasses：健康引擎（含部分成交/撤单/市价残余）审计通过。
func TestAuditHealthyEnginePasses(t *testing.T) {
	e := buildScenario(t)
	rep := e.Audit()
	if !rep.OK {
		t.Fatalf("healthy engine audit failed: %+v", rep.Discrepancies)
	}
	if !rep.FullCoverage || rep.CheckedEvents != rep.LastSeq {
		t.Fatalf("report=%+v", rep)
	}
	t.Logf("健康引擎审计通过: lastSeq=%d checkedEvents=%d fullCoverage=%t",
		rep.LastSeq, rep.CheckedEvents, rep.FullCoverage)
}

// TestAuditDetectsInjectedAccountDrift：人为注入账户敞口不一致，审计必须判不通过并给出类别与位置。
func TestAuditDetectsInjectedAccountDrift(t *testing.T) {
	e := buildScenario(t)

	// 注入：篡改 alice/BTCUSD 的净持仓与挂单占用（绕过事件流直接改状态）
	drifted := risk.AccountState{Position: 999, OpenBuy: 1, OpenSell: 12345, OpenCount: 7}
	e.mu.Lock()
	e.riskMgr.RestoreState("alice", "BTCUSD", drifted)
	e.mu.Unlock()

	rep := e.Audit()
	if rep.OK {
		t.Fatal("audit must fail after injected account drift")
	}
	var found *Discrepancy
	for i := range rep.Discrepancies {
		d := &rep.Discrepancies[i]
		t.Logf("差异 kind=%s subject=%s expected=%s actual=%s nearSeq=%d",
			d.Kind, d.Subject, d.Expected, d.Actual, d.NearSeq)
		if d.Kind == DiscAccount && d.Subject == "alice/BTCUSD" {
			found = d
		}
	}
	if found == nil {
		t.Fatalf("no ACCOUNT discrepancy reported for alice/BTCUSD: %+v", rep.Discrepancies)
	}
	// alice/BTCUSD 最后被事件触及的是 seq=4（b1×s1 成交），定位应落在它附近
	if found.NearSeq != 4 {
		t.Fatalf("NearSeq=%d want 4 (last event touching alice/BTCUSD)", found.NearSeq)
	}
	t.Logf("审计判定不通过: 类别=%s 定位 nearSeq=%d（可复现）", found.Kind, found.NearSeq)
}

// TestAuditDetectsInjectedBookDrift：人为注入盘口/明细不一致，审计必须判不通过并定位到档位。
func TestAuditDetectsInjectedBookDrift(t *testing.T) {
	e := buildScenario(t)

	// 注入：从委托登记表抹掉在簿卖单 s2（seq=2 受理，价位 101 剩 6）——簿子与明细从此对不上
	e.mu.Lock()
	delete(e.registry, "s2")
	e.mu.Unlock()

	rep := e.Audit()
	if rep.OK {
		t.Fatal("audit must fail after injected book drift")
	}
	var found *Discrepancy
	for i := range rep.Discrepancies {
		d := &rep.Discrepancies[i]
		t.Logf("差异 kind=%s subject=%s expected=%s actual=%s nearSeq=%d",
			d.Kind, d.Subject, d.Expected, d.Actual, d.NearSeq)
		if d.Kind == DiscBook {
			found = d
		}
	}
	if found == nil {
		t.Fatalf("no BOOK discrepancy reported: %+v", rep.Discrepancies)
	}
	if found.Subject != "BTCUSD SELL@101" {
		t.Fatalf("Subject=%q want %q", found.Subject, "BTCUSD SELL@101")
	}
	// 该档挂单 s2 的受理序号是 2，定位应落在 seq=2
	if found.NearSeq != 2 {
		t.Fatalf("NearSeq=%d want 2 (accept seq of erased order s2)", found.NearSeq)
	}
	t.Logf("审计判定不通过: 类别=%s 档位=%s 定位 nearSeq=%d", found.Kind, found.Subject, found.NearSeq)
}

// TestAuditAfterCheckpointRebuild：检查点增量重建后的引擎审计仍通过；
// 日志只持有检查点之后的事件，报告应标明非全覆盖。
func TestAuditAfterCheckpointRebuild(t *testing.T) {
	live := buildScenario(t)
	events := live.Events().All()

	mid := len(events) / 2
	midEngine := testEngine(t)
	if err := midEngine.Replay(events[:mid]); err != nil {
		t.Fatal(err)
	}
	cp := midEngine.Checkpoint()

	inc := testEngine(t)
	if err := inc.Rebuild(&cp, events); err != nil {
		t.Fatal(err)
	}
	rep := inc.Audit()
	if !rep.OK {
		t.Fatalf("audit after checkpoint rebuild failed: %+v", rep.Discrepancies)
	}
	if rep.FullCoverage {
		t.Fatal("incremental-rebuilt engine must report FullCoverage=false")
	}
	if rep.CheckedEvents != int64(len(events)-mid) {
		t.Fatalf("CheckedEvents=%d want %d (tail only)", rep.CheckedEvents, len(events)-mid)
	}
	t.Logf("增量重建引擎审计通过: lastSeq=%d checkedEvents=%d(尾部) fullCoverage=%t",
		rep.LastSeq, rep.CheckedEvents, rep.FullCoverage)
}
