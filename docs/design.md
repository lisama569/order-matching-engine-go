# 确定性限价订单簿撮合与执行服务 — 设计说明

纯内存实现的单进程撮合服务：限价单 / 市价单、价格优先时间优先撮合、部分成交、撤单、
事件溯源（有序事件 + 断点重建）、多订阅者分发、账户持仓与敞口风控。
无外部依赖，状态完全由有序事件流决定，同一事件流多次重放结果逐字节一致。

## 1. 撮合规则

- 订单簿按品种隔离（`internal/orderbook`）。买盘 `bids` 按价格**降序**、
  卖盘 `asks` 按价格**升序**维护；同一价格内为 FIFO 队列（时间优先）。
- 撮合判定：
  - 吃**买**单：从最低卖价起逐档吃；`makerPrice <= limitPrice` 可成交，越过即停止。
  - 吃**卖**单：从最高买价起逐档吃；`makerPrice >= limitPrice` 可成交，越过即停止。
  - 市价单（`OrderMarket`）按 IOC 语义：能吃多少吃多少，**残余立即撤销**，绝不挂簿；
    若对手盘为空、一口都吃不到，则以 `NO_LIQUIDITY` 拒绝。
  - 限价单残余（且仍可挂单）留在簿中，状态为 `NEW`（零成交）或 `PARTIALLY_FILLED`。
    `PARTIALLY_FILLED` 由统一状态转移函数 `mutate` 在每笔成交后即时推进
    （挂单方、限价吃单方、市价吃单方的全部终结状态都在 mutate 内推导，
    实时撮合与事件重放逐字段一致，见 §3.3）。
- 数量与价格校验（拒绝原因可区分，见 §4）：
  - 数量必须为正整数，否则 `INVALID_QUANTITY`；
  - 限价单价格必须为正且不超过 `risk.Config.MaxPrice`，否则 `INVALID_PRICE`；
  - 账户 / 品种不在登记集合内：`UNKNOWN_ACCOUNT` / `UNKNOWN_SYMBOL`；
  - 委托 ID 重复：`DUPLICATE_ORDER_ID`。
- 限价吃单方在受理时先按原始数量挂簿，随后的每笔成交事件同时扣减挂单方
  **与吃单方**的档位；吃单方全成即移出簿子，簿中绝不残留已完全成交的委托，
  其挂单名义占用按限价位随成交释放、挂单笔数同步收尾。
- 并发模型：`Engine` 内一把互斥锁串行化全部状态变更。并发下单 / 撤单时，
  事件序号即提交先后顺序，结论确定；`go test -race` 无竞争。

## 2. 事件模型

全部状态变更都是追加式事件（`internal/event`），序号从 1 开始，**严格连续递增**。

| 事件类型 | 时机 | 关键负载 |
| --- | --- | --- |
| `ORDER_ACCEPTED` | 委托通过静态与风控校验、进入撮合流程的第一步（携带原始委托） | `Order`（FilledQty=0, Status=NEW） |
| `ORDER_FILLED` | 每一笔吃单-挂单成交 | `Fill`（TradeID、双方账户、价格、数量、序号）；全成挂单方另附 `ClosedOrder` 快照 |
| `ORDER_CANCELLED` | 限价挂单主动撤销 / 市价单残余 IOC 撤销 | `Order` |

一笔提交的确定性事件序列固定为：

```
ORDER_ACCEPTED(taker, 原始委托)
  ORDER_FILLED × N          # 按价格优先时间优先得到的逐笔成交
  ORDER_CANCELLED(taker)    # 仅当市价单存在未成交残余时
```

`SubmitResult.Fills` 给出本次全部成交；委托最终状态为
`NEW / PARTIALLY_FILLED / FILLED / CANCELLED`。

**被拒绝的委托不是事件**：拒绝发生在只读校验阶段（静态校验 → 只读预撮合 `Book.Simulate`
→ 只读风控 `Manager.CheckOrder`），不追加事件、不改变簿子与敞口。

## 3. 重放协议

- 追加与重放共用同一套序号校验：
  - 下一序号必须恰为 `lastSeq + 1`；
  - 序号 ≤ 已存在序号 ⇒ `DUPLICATE`（重复）；
  - 给出回退但未见过的小序号 ⇒ `OUT_OF_ORDER`（乱序）；
  - 序号 > 期望值 ⇒ `GAP`（缺口）。
  - 任何违规都返回 `*event.SequenceError`（含 `Kind/Got/Expected`），**立即终止，
    绝不静默跳过或继续推进**。
- 实时撮合与断点重建使用**同一个状态转移函数** `Engine.mutate(ev)`：
  - `ORDER_ACCEPTED`：登记原始委托；限价单按原始数量挂簿并占用挂单敞口；
  - `ORDER_FILLED`：推进吃单方与挂单方的成交量、更新双方持仓与挂单释放，
    挂单方全成则移出档位；
  - `ORDER_CANCELLED`：从簿中移除并释放挂单占用（市价残余不在簿中，不重复释放）。
- `Engine.Replay` 先在“影子引擎”上完整重放，**全部成功后才替换线上状态**；
  若中途出现序号违规，线上引擎保持原状（`LastSeq=0` 的干净重建场景同理）。
- 重建后的引擎可用同一把锁继续撮合，新事件序号严格衔接，结论与“实时路径提交相同委托”一致
  （见 `TestReplayEqualsLiveAndDeterministic`）。
- 慢订阅者断线追赶：事件不会从权威日志丢失，重新 `Subscribe` 后用
  `Store.From(lastSeenSeq)` 拉取缺失事件即可补齐。
- `event.ReplayFrom(events, startSeq, apply)` 支持从任意期望序号开始重放
  （检查点增量重建用），序号连续性判定规则与 `Replay` 完全一致。
- `event.Store` 带 `base` 偏移：实时撮合与全量重建时 `base=0`；基于检查点
  增量重建时 `base=检查点序号`，日志只持有检查点之后的事件，
  内存占用不随历史事件数增长。

## 3.1 盘口快照

`Engine.DepthSnapshot()` 随时返回全部品种盘口的一致视图（`DepthView`）：
买盘降序、卖盘升序，每档给出价位、该档剩余总量、挂单笔数及全部挂单明细
（委托 ID、账户、剩余量、受理序号）。

- **严格对账**：每档剩余总量恒等于该档明细中各挂单剩余量之和
  （`TestDepthSnapshotMatchesRestingOrders` 逐档验证）。
- **一致视图**：视图只在撮合锁内、一次完整变更批次结束后构建发布，
  绝不暴露“只成交了一半”的中间态。
- **不阻塞撮合**：撮合路径只置脏标记（O(1)），视图重建推迟到读取时；
  读取时若锁空闲则就地重建最新视图，若撮合正在进行则返回最近一次发布的
  一致视图——读取永不等待撮合锁，事件也不会因取快照而丢失
  （`TestDepthSnapshotConcurrentWithMatching`：并发下上万次读取全部自洽）。

## 3.2 账实一致性审计

`Engine.Audit()` 从事件流与当前状态出发做一次对账，三方互相比对：

1. **事件进度**：日志持有的事件序号必须从 `base+1` 起严格连续；
2. **盘口汇总 vs 挂单明细**：委托登记表中未成交挂单按价位重算的聚合，
   必须与订单簿档位（剩余总量 / 笔数）逐项相等；
3. **账户敞口 vs 事件重推**：用日志持有的事件在影子引擎重推，与当前风控
   状态逐字段比对；名义占用另以“登记表在簿挂单重算 vs 当前风控占用”
   做自洽校验（即使日志只含检查点之后的尾部也始终生效）。

任一不一致都使审计判定为**不通过**。每条差异（`Discrepancy`）带有：

- `Kind`：`SEQUENCE` / `BOOK` / `ACCOUNT` 三类之一；
- `Subject`：品种+方向+价位 / 账户键 / `event-log`；
- `Expected` / `Actual`：两方各自的重算结果；
- `NearSeq`：差异大致落在哪个事件序号附近（取相关挂单的受理序号或
  该账户最后被事件触及的序号），可据此复现定位。

`AuditReport.FullCoverage=false` 表示引擎经检查点增量重建、日志只持有尾部
事件：此时持仓绝对值无法从尾部事件独立重推，对应比对自动降级并明确标注，
绝不把“没校验”当作“校验通过”。

## 3.3 检查点与增量重建

`Engine.Checkpoint()` 在事件流推进过程中随时落检查点（与撮合共用同一把互斥锁，
因此**并发下单 / 撤单期间落盘也只会观察到完整变更批次之后的状态**）：`LastSeq`、
序号 `LastSeq` 处事件的内容指纹、委托登记表（含已终结委托）、账户风控状态，
以及覆盖全部字段的 FNV-1a 校验值。盘口不单独落盘——在簿挂单按受理序号
（`Sequence`）排序重挂即可恢复价格-时间优先队列。

**一致性保证（实时落盘与重放落盘等价）。** 检查点无论来自正在运行的实时引擎，
还是来自重放前缀得到的中间引擎，`Rebuild` 的结果都与从零全量重建、与实时状态
**逐字段一致**：每笔委托的最终状态（`NEW/PARTIALLY_FILLED/FILLED/CANCELLED`）、
`FilledQty`、受理序号 `Sequence`，盘口各档价位 / 剩余总量 / 笔数，
以及全部账户-品种的净持仓与买 / 卖挂单名义占用、挂单笔数。

成立的根本原因是：委托状态的全部派生都在**唯一状态转移函数** `Engine.mutate(ev)`
内完成，实时撮合、全量重放、检查点后尾部重放走同一份代码——

- `ORDER_FILLED`：挂单方（含限价吃单方）部分成交即置 `PARTIALLY_FILLED`、
  全成置 `FILLED` 并移出簿子；**市价吃单方（不在簿中）全成即 `FILLED`，
  有残余时保持 `NEW` 由后续 `ORDER_CANCELLED` 终结**。
  此前市价吃单方的终结状态只在 `Submit` 尾部补写、`mutate` 不推进，
  导致“实时落检查点 + 尾部重放”保留 `FILLED/CANCELLED`，而从零全量重放停在
  `NEW`——这正是本次修复的分歧根因，修复后成交状态对三条路径完全一致。
- `ORDER_CANCELLED`：在簿则移除并释放挂单占用（市价残余不在簿、不重复释放），
  状态置 `CANCELLED`。已 `FILLED/CANCELLED` 的委托在受理撤单时即被拒绝，
  重建后该判定行为不变（检查点保留全部已终结委托）。

`Engine.Rebuild(cp, events)` 基于检查点增量重建，按以下顺序决策：

- `cp == nil`（缺失）：直接从序号 1 全量重建；
- 校验值不匹配（内容损坏）、`LastSeq` 为负或“为 0 却携带状态”、
  `LastSeq` 超出事件流、边界事件序号不是 `LastSeq` 或**事件内容指纹对不上**
  （放错位置 / 属于另一条事件流）：记录原因并**安全退回全量重建**；
- 检查点定位合法但**尾部事件序号链**存在缺口 / 重复 / 乱序：不顺着检查点继续，
  同样**先安全退回全量重建**；全量重放因此也失败时，明确返回
  `*event.SequenceError` 且**不替换线上状态**（重建在影子引擎上完成，
  成功后才提交），绝不产出“从错误起点悄悄重建”的状态；
- 检查点合法：恢复状态后从 `LastSeq+1` 起按序重放剩余事件，
  序号连续性判定规则不变；补齐结果与从零完整重建**逐字段一致**。

重建后的引擎可用同一把锁继续撮合：新事件序号从尾部 `LastSeq` 严格衔接，
继续下单 / 撤单 / 重复撤单 / 撤已终结单的结论与“实时引擎提交相同序列”完全一致。

与 `Replay` 相同，重建先在影子引擎上完成，成功后才替换线上状态。
大事件量下（4.5 万事件、100 档价位）带检查点的重建耗时约为全量重建的
1/2.2 且日志只保留尾部事件（`TestCheckpointRebuildFasterThanFull`）。

`engine.StatesEqual(a, b)` 与 `Engine.Orders()` 提供两台引擎完整派生状态
（LastSeq、逐笔委托字段、盘口、账户敞口）的逐字段比对，供校验与演示复用。

## 4. 风控配置项与拒绝原因

配置入口 `risk.Config`（零值限额表示该项关闭）：

| 配置项 | 含义 | 触发的拒绝原因 |
| --- | --- | --- |
| `MaxPrice` | 限价单允许的最高价格 | `INVALID_PRICE` |
| `Limits.MaxSingleOrderNotional` | 单笔委托名义金额上限；限价单按 `限价位 × 全额`（最坏情形），市价单按预计成交额 | `RISK_MAX_SINGLE_ORDER_NOTIONAL` |
| `Limits.MaxPosition` | 净持仓数量绝对值上限；按“已确定成交量 + 限价残余挂单最坏全成”后的净仓判定 | `RISK_MAX_POSITION` |
| `Limits.MaxOpenOrders` | 同时挂单笔数上限（仅对成交后仍挂簿的委托计数） | `RISK_MAX_OPEN_ORDERS` |
| `Limits.MaxExposure` | 持仓名义敞口上限 = `|净持仓| × 参考价`（`refPrices[symbol]`） | `RISK_MAX_EXPOSURE` |
| `Config.Accounts` | 按账户 ID 覆盖默认限额 | — |

其它拒绝原因：`INVALID_QUANTITY`、`UNKNOWN_ACCOUNT`、`UNKNOWN_SYMBOL`、
`DUPLICATE_ORDER_ID`、`NO_LIQUIDITY`、`CANCEL_ORDER_NOT_FOUND`。

派生风控状态（净持仓、买 / 卖挂单名义占用、挂单笔数）全部由事件更新，
重放同一事件流必然得到相同账户敞口。所有校验只读，拒绝不改变任何已确定状态。

## 5. 订阅分发

`internal/pubsub` 为一对多内存总线：

- 每个订阅者有独立缓冲；`Publish` **永不阻塞**撮合线程。
- 订阅者缓冲满（慢消费者）时立即关闭其 `Overflow` 信号与事件通道并摘除，
  其他订阅者与撮合主流程不受影响；事件仍完整保存在 `event.Store` 中。
- 慢订阅者可重新订阅并以 `Store.From(...)` 重放缺失事件，做到“不阻塞、不丢事件”。

## 6. 代码结构

```
internal/domain    委托 / 成交 / 状态 / 拒绝原因等核心模型
internal/orderbook 价格-时间优先订单簿（只读 Simulate + ApplyFill + 明细深度）
internal/event     追加式事件日志（带 base 偏移）、序号校验、Replay/ReplayFrom
internal/risk      限额配置、受理前只读校验、持仓/敞口状态更新与恢复
internal/pubsub    非阻塞多订阅者分发（慢消费者断开）
internal/engine    串行化撮合核心、统一 mutate、断点重建、快照
  engine/depth.go       盘口一致视图（懒发布，读取免锁）
  engine/audit.go       账实一致性审计（三方比对 + 差异分类定位）
  engine/checkpoint.go  检查点落盘 / 校验 / 增量重建
cmd/matchingd      本地确定性演示程序（含快照 / 审计 / 检查点演示）
```

## 7. 本地验证方法

```bash
# 构建
go build ./...

# 全量单测（日志打印事件序号与判定依据）
go test ./... -v

# 并发安全
go test -race -count=1 ./...

# 确定性演示：下单/部分成交/风控拒绝/撤单 + 事件流重建比对
go run ./cmd/matchingd

# 检查点一致性复现（bugfix-b）
# 1. 实时引擎在混合流每个批次边界落检查点，增量重建 == 全量重建 == 实时（逐字段）
go test ./internal/engine/ -run 'TestLiveCheckpointIncrementalEqualsFull|TestMixedScenarioFinalOrderStates' -v
# 2. 重建后继续下单/撤单/重复撤单结论与实时一致
go test ./internal/engine/ -run TestPostRebuildBehaviorMatchesLive -v
# 3. 并发下单/撤单期间落检查点（建议带 -race）
go test ./internal/engine/ -run TestConcurrentCheckpointRebuild -race -v
# 4. 检查点缺失/损坏/放错位置/尾部序号畸形的安全退回
go test ./internal/engine/ -run 'TestCheckpointFallback|TestRebuildMalformed|TestCheckpointTailBroken' -v
```

关键测试：

- `internal/orderbook`：价格时间优先、部分 / 全部成交、不可穿越价、市价吃多档；
- `internal/event`：序号重复 / 乱序 / 缺口拒绝、缺口不推进日志；
- `internal/engine`：部分成交、撤单（含重复撤单）、各类可区分拒绝、
  风控拒绝不改变状态、并发下单后重建一致、订阅者按序收事件、市价残余撤销；
- `internal/engine.TestReplay*`：同一事件流连续重建 3 次，订单簿档位与账户敞口
  与实时状态逐字段一致；畸形流（缺口 / 重复 / 乱序）明确拒绝且引擎保持干净；
- `internal/engine.TestDepthSnapshot*`：快照聚合与挂单明细逐档一致；
  并发下单 / 撤单 / 成交期间上万次取快照全部自洽、撮合不被阻塞、事件不丢；
- `internal/engine.TestCheckpoint*`：检查点增量重建与全量重建逐字段一致；
  缺失 / 损坏 / 放错位置的检查点安全退回全量重建；4.5 万事件下增量重建
  明显快于全量重建且日志只保留尾部事件；
- `internal/engine.TestLiveCheckpointIncrementalEqualsFull`：检查点**直接从实时
  运行中的引擎**在每个批次边界落盘，增量重建与从零全量重建、实时状态逐字段一致
  （市价全成 / 市价残余撤销 / 限价部分成交继续挂簿 / 主动撤单 / 重复撤单 /
  撤已成交单 / 参数与风控拒绝 / 重复 ID 混合流）；
- `internal/engine.TestMixedScenarioFinalOrderStates`：混合流逐笔最终状态 /
  成交量 / 受理序号符合业务语义，且多个实时检查点的增量重建都等于实时与全量；
- `internal/engine.TestPostRebuildBehaviorMatchesLive`：重建后继续下单 / 撤单 /
  重复撤单 / 撤已终结单的结论与实时完全相同；
- `internal/engine.TestConcurrentCheckpointRebuild`：并发下单 / 撤单期间持续落
  检查点（`-race`），每个检查点的增量重建终态都与实时 / 全量逐字段一致；
- `internal/engine.TestCheckpointFallback*` / `TestRebuildMalformed*` /
  `TestCheckpointTailBroken*`：检查点缺失、内容损坏、LastSeq 矛盾、指纹对不上、
  尾部序号链缺口 / 重复 / 乱序全部安全退回全量；事件流本身畸形时明确报错且
  线上状态保持不变；
- `internal/engine.TestAudit*`：健康引擎审计通过；人为注入账户敞口 /
  盘口明细不一致时审计判不通过，并给出差异类别与大致事件序号。

## 8. 兼容范围

- **老事件流零迁移**：事件模型（三类事件、序号规则）未变，既有事件流
  不做任何迁移即可继续 `Replay` 全量重放；`Rebuild(nil, events)` 与
  `Replay(events)` 等价。
- **检查点可选**：不落检查点不影响任何既有路径；检查点不可用时自动
  退回全量重建，结果与不用检查点完全一致。
- **行为修正（检查点一致性）**：本版修正“成交委托的终结状态只由实时路径
  `Submit` 尾部补写、统一转移函数 `mutate` 不推进”的疏漏。此前市价吃单方
  全成 / 残余撤销、以及部分成交挂单方的状态，在“从零全量重放”与
  “实时落检查点 + 尾部重放”两条路径上会给出不同结果（重放走完全部成交后
  市价单仍停在 `NEW`）。现全部收归 `mutate` 在成交 / 撤单事件内即时推导，
  实时、全量重放、任意检查点增量重建三条路径逐字段一致。
- **行为修正（异常检查点退回）**：`LastSeq=0` 却携带状态的矛盾检查点，以及
  定位合法但尾部事件序号链缺口 / 重复 / 乱序的情形，现在都会被识别并安全
  退回全量重建；全量重放也无法成功时明确报错、不替换线上状态，绝不从错误
  起点悄悄往下重建。
- **行为修正**：更早版本曾修正限价吃单方全成后仍残留簿中、其挂单名义占用
  与挂单笔数未随成交释放的记账疏漏（实现与 §1 文档原意对齐）。
  下单 / 撤单 / 重放 / 风控的受理与拒绝结论不变；仅派生视图
  （盘口档位、账户挂单占用）不再包含本应离场的残留项。
