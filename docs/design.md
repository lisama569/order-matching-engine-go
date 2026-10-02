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
- 数量与价格校验（拒绝原因可区分，见 §4）：
  - 数量必须为正整数，否则 `INVALID_QUANTITY`；
  - 限价单价格必须为正且不超过 `risk.Config.MaxPrice`，否则 `INVALID_PRICE`；
  - 账户 / 品种不在登记集合内：`UNKNOWN_ACCOUNT` / `UNKNOWN_SYMBOL`；
  - 委托 ID 重复：`DUPLICATE_ORDER_ID`。
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

## 6. 盘口快照查询

`Engine.Depth()` 返回某一事件序号时刻的盘口汇总视图（`DepthSnapshot`）：
买盘 / 卖盘各档的价位、该档剩余总量、该档挂单笔数。

- **一致性**：快照只在每次状态变更批次（一笔提交 / 撤单产生的全部事件）提交后、
  同一把锁内重新发布（`publishDepthLocked`），因此任何时刻取到的都是某个事件
  序号上的完整视图，绝不暴露"只成交了一半"的中间态；每档剩余总量严格等于
  该价位全部未成交挂单剩余量之和（由 `TestDepthSnapshotMatchesRestingDetail`
  与审计的 `BOOK_DEPTH_MISMATCH` 检查共同保证）。
- **非阻塞**：读取走 `atomic.Value`，不取撮合锁；并发下单 / 撤单 / 成交期间
  随时可取，既不卡住撮合，也不影响事件追加与订阅分发（事件仍完整落在
  `event.Store`，快照本身不产生也不消费事件）。
- 每次发布都是全新分配的不可变对象，读者持有的旧快照不被后续撮合影响。
- 边界：快照是内存态视图，不持久化；重启后由重放 / 检查点重建恢复。

## 7. 账实一致性审计

`Engine.Audit()` 从事件流与当前状态出发做对账（`internal/audit` 为纯函数判定，
引擎在锁内只读采集输入），三方互相比对：

| 检查项 | 比对内容 | 不一致类别 |
| --- | --- | --- |
| 事件进度 | 序号从基线 +1 严格连续；完整流在手时重算滚动哈希与日志记录比对 | `SEQ_DISCONTINUITY` / `EVENT_HASH_MISMATCH` |
| 盘口 vs 明细 | 每档剩余总量 = 该价位全部挂单剩余量之和、笔数相符；簿中明细与注册表"应挂单"集合互为充要 | `BOOK_DEPTH_MISMATCH` |
| 账户占用 vs 明细 | 挂单笔数、买 / 卖名义占用由挂单明细重算后与风控状态比对 | `OPEN_ORDER_MISMATCH` |
| 持仓 vs 成交事件 | 净持仓由全部成交事件重算后与风控状态比对 | `POSITION_MISMATCH` |

- 只要存在对不上的地方，本次审计即判定 `Pass=false`；每条 `Finding` 带类别、
  涉及品种 / 账户 / 委托与**差异大致所在的事件序号**（涉及委托的受理序号、
  该账户最后一笔成交序号等），可据此复现定位。
- 检查点基线场景（`Base>0`）：基线之前的事件不在手边，哈希与持仓重算
  降级跳过并在 `Skipped` 中明确标注，不会误报。
- 审计只读，不改变任何状态。

## 8. 检查点与增量重建

`Engine.CaptureCheckpoint()` 在事件流推进过程中落检查点（`CheckpointState`：
覆盖序号、事件流滚动哈希、注册表全部委托、簿子档位 FIFO 结构、账户风控状态），
`checkpoint.FileStore` 负责带 SHA-256 校验和的落盘 / 读取。

`Engine.Rebuild(cp, events)` 是 `Replay` 的增量版，路径选择：

1. `cp == nil`（缺失）→ 全量重放（事件流须从序号 1 开始）；
2. 事件流从 1 开始（全量流在手）→ 校验 `cp.Seq` 不越界且 `cp.EventHash`
   等于事件流前 `cp.Seq` 条的滚动哈希，相符则从检查点续放剩余事件；
   哈希不符（放错流 / 放错位置）→ 退回全量（`CHECKPOINT_HASH_MISMATCH` /
   `CHECKPOINT_MISPLACED`）；
3. 事件流首条序号恰为 `cp.Seq+1`（只给了增量段）→ 信任该配对从检查点续放，
   序号连续性由 `ReplayFrom` 照常校验；
4. 检查点内容自身不一致（档位引用不存在的委托等）→ `CHECKPOINT_CORRUPT`，
   有全量流则退回全量，否则明确报错；
5. 检查点不可用且手上的事件流又不是从 1 开始 → **明确报错**，
   绝不顺着错误的起点悄悄往下走。

保证：

- 无论走哪条路径，重放都在影子引擎上完成，全部成功后才替换线上状态；
- 增量重建与从零全量重建**逐字段一致**（盘口、账户、注册表、事件哈希）；
- 重建后日志基线（`Store.Base`）= 检查点序号，新事件序号严格衔接，
  原有连续性判定（重复 / 乱序 / 缺口）不受影响；
- 事件日志的滚动哈希覆盖基线前后的全部事件，从检查点继续累积与从零
  累积逐位一致，审计可照常校验基线后的事件进度；
- 耗时与内存：重建只处理检查点之后的增量事件（几万条事件、多档价位场景下
  提速明显，见 `TestCheckpointRebuildFasterThanFull` 与 Benchmark）；
  无需装载完整事件历史，日志只持有基线之后的增量段。检查点体积与引擎
  实时状态同阶（含委托注册表——去重语义要求记住全部历史委托 ID），
  定期落检查点时重建内存 = 检查点 + 一个间隔的增量事件，不随事件流总长失控。

兼容范围：

- **老事件流无需任何迁移**：事件格式、序号规则、重放判定均未改变，
  `Replay` 行为不变；不带检查点调用 `Rebuild(nil, events)` 即等价于全量重放。
- 本版修正了吃单方成交的回写：限价吃单方受理时整单入簿，此前版本其成交
  未同步扣减所在档位总量、未释放对应挂单名义占用与笔数（簿子残留幻影档位）。
  现成交事件会同时结算吃单方档位（`Book.SettleFill`）并释放其挂单占用
  （`risk.OnRestingClosed`）。实时与重放共用同一 `mutate`，结论依旧确定；
  用老事件流重放会得到修正后的（账实一致的）状态。

## 9. 代码结构

```
internal/domain    委托 / 成交 / 状态 / 拒绝原因等核心模型
internal/orderbook 价格-时间优先订单簿（只读 Simulate + ApplyFill + SettleFill）
internal/event     追加式事件日志、序号校验、滚动哈希、Replay / ReplayFrom
internal/risk      限额配置、受理前只读校验、持仓/敞口状态更新
internal/pubsub    非阻塞多订阅者分发（慢消费者断开）
internal/audit     账实一致性审计（纯函数判定，引擎锁内采集输入）
internal/checkpoint 检查点完整性封装（JSON + SHA-256）与文件存取
internal/engine    串行化撮合核心、统一 mutate、断点重建、盘口快照、检查点重建
cmd/matchingd      本地确定性演示程序
```

## 10. 本地验证方法

```bash
# 构建
go build ./...

# 全量单测（日志打印事件序号与判定依据）
go test ./... -v

# 并发安全
go test -race -count=1 ./...

# 确定性演示：下单/部分成交/风控拒绝/撤单 + 事件流重建比对
#             + 盘口快照 / 审计 / 检查点增量重建
go run ./cmd/matchingd

# 重建性能对比（全量 vs 检查点增量）
go test -bench=Rebuild -benchmem ./internal/engine/
```

关键测试：

- `internal/orderbook`：价格时间优先、部分 / 全部成交、不可穿越价、市价吃多档；
- `internal/event`：序号重复 / 乱序 / 缺口拒绝、缺口不推进日志；
- `internal/engine`：部分成交、撤单（含重复撤单）、各类可区分拒绝、
  风控拒绝不改变状态、并发下单后重建一致、订阅者按序收事件、市价残余撤销；
- `internal/engine.TestReplay*`：同一事件流连续重建 3 次，订单簿档位与账户敞口
  与实时状态逐字段一致；畸形流（缺口 / 重复 / 乱序）明确拒绝且引擎保持干净；
- `internal/engine.TestDepth*`：盘口快照与挂单明细严格一致；并发下取快照不阻塞
  撮合、不丢事件，抽样快照与重放到同一序号的视图逐档一致；
- `internal/engine.TestCheckpoint*`：检查点增量重建与全量重建逐字段一致；
  缺失 / 损坏 / 放错位置的检查点安全退回全量；增量重建明显提速且内存有界；
- `internal/engine.TestAudit*`：健康引擎审计通过；人为注入盘口 / 挂单占用 /
  持仓 / 序号不一致均被识别并给出类别与事件序号定位。
