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

## 6. 代码结构

```
internal/domain    委托 / 成交 / 状态 / 拒绝原因等核心模型
internal/orderbook 价格-时间优先订单簿（只读 Simulate + ApplyFill）
internal/event     追加式事件日志、序号校验、Replay
internal/risk      限额配置、受理前只读校验、持仓/敞口状态更新
internal/pubsub    非阻塞多订阅者分发（慢消费者断开）
internal/engine    串行化撮合核心、统一 mutate、断点重建、快照
cmd/matchingd      本地确定性演示程序
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
```

关键测试：

- `internal/orderbook`：价格时间优先、部分 / 全部成交、不可穿越价、市价吃多档；
- `internal/event`：序号重复 / 乱序 / 缺口拒绝、缺口不推进日志；
- `internal/engine`：部分成交、撤单（含重复撤单）、各类可区分拒绝、
  风控拒绝不改变状态、并发下单后重建一致、订阅者按序收事件、市价残余撤销；
- `internal/engine.TestReplay*`：同一事件流连续重建 3 次，订单簿档位与账户敞口
  与实时状态逐字段一致；畸形流（缺口 / 重复 / 乱序）明确拒绝且引擎保持干净。
