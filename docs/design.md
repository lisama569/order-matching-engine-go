# 可确定性重放的限价订单簿撮合与执行服务 — 设计说明

全部状态保存在本地内存，无外部依赖；所有状态变更被记录为**有序事件流**，
同一事件流在任意时刻重建都得到相同的订单簿与账户敞口。

## 1. 模块结构

| 包 | 职责 |
|---|---|
| `internal/market` | 方向/类型/拒绝原因枚举、委托结构、静态合法性校验 `Validate` |
| `internal/match` | 单品种限价订单簿：价格优先 + FIFO 时间优先撮合、挂簿、撤单 |
| `internal/event` | 事件模型与有序内存存储；序号重复/乱序/缺口明确拒绝 |
| `internal/risk` | 账户维度净持仓与累计名义敞口的下单前预判、成交后更新 |
| `internal/pubsub` | 撮合结果多订阅者非阻塞分发；慢订阅者丢最旧事件并计数通知 |
| `internal/engine` | 单 goroutine 串行驱动上述全部组件，生成事件、发布、重放 |
| `cmd/demo` | 端到端本地示例：并发下单/撤单 → 停机 → 重建比对 |

## 2. 撮合规则

### 2.1 优先级
1. **价格优先**：买单与最低卖价成交，卖单与最高买价成交；
   限价单只在价格可交叉时成交（买价 ≥ 卖一价，卖价 ≤ 买一价）。
2. **时间优先**：同一价位内按进入簿中的先后 FIFO 成交。
3. **成交价取挂单价（maker price）**，保证结论只依赖既有簿状态。

### 2.2 委托类型
- **限价单（LIMIT）**：立即撮合到不能再成交为止，剩余数量挂入订单簿。
- **市价单（MARKET）**：IOC 语义——立即吃掉对手盘，**剩余量直接放弃、不挂簿**；
  若提交时对手盘完全没有流动性，则以 `UNACCEPTABLE_ORDER` 拒绝（不会产生任何状态变更）。

### 2.3 部分成交与撤单
- taker 可与多个 maker 逐笔成交，每笔成交产生一条 `ORDER_FILLED` 事件。
- 撤单返回撤掉的剩余数量并产生 `ORDER_CANCELED`；订单不存在、已成交完或已撤销
  时返回失败（不产生事件）。重复撤单同样失败。

### 2.4 委托合法性与可区分拒绝原因

拒绝在撮合与簿/风控状态发生任何变更**之前**完成，并产生一条 `ORDER_REJECTED` 事件
（拒绝事件也占用连续序号，使拒绝本身可审计、可重放）。原因枚举：

| Reason | 含义 |
|---|---|
| `INVALID_QUANTITY` | 数量 ≤ 0、低于 `MinQty` 或高于 `MaxQty` |
| `INVALID_PRICE` | 限价 ≤ 0、越出 `[MinPrice, MaxPrice]`、不是 `MinTickSize` 整数倍；或市价单携带价格 |
| `UNACCEPTABLE_ORDER` | 未知方向/类型、ID/账户/品种为空、市价单无对手流动性 |
| `UNKNOWN_ACCOUNT` | 账户不在 `AllowedAccounts` 白名单（配置非空时） |
| `DUPLICATE_ORDER_ID` | 同一订单ID曾提交过（含被拒绝的ID） |
| `RISK_POSITION_LIMIT` | 预判将突破多/空头持仓限额 |
| `RISK_EXPOSURE_LIMIT` | 预判将突破名义敞口限额 |
| `ORDER_NOT_FOUND` | 撤单目标不在簿中（仅用于撤单失败回执，不产生事件） |

## 3. 事件模型

序号 `Seq` 从 **1** 开始、严格连续递增。事件类型：

| Kind | 触发时机 | 关键字段 |
|---|---|---|
| `ORDER_ACCEPTED` | 委托通过全部校验与风控，进入撮合 | 方向/类型/价格/原始数量 |
| `ORDER_FILLED` | 每一笔 maker-taker 成交 | `Price/Qty`、taker 与 `MakerOrderID/MakerAccount` |
| `ORDER_BOOKED` | 限价单剩余量挂簿 | 挂单 `Price/Qty` |
| `ORDER_CANCELED` | 簿中委托被撤销 | 撤销量 `Qty` |
| `ORDER_REJECTED` | 委托在确定状态变更前被拒 | 可区分 `Reason` |

一次被接受的限价单对应固定形态的事件组：
`ACCEPTED, FILLED..., [BOOKED]`；被拒绝的委托恰好只有一条 `REJECTED`。
`FILLED/BOOKED` 不允许脱离 `ACCEPTED` 单独出现在流中。

事件字段采用整数价格（最小变动单位 tick 的整数倍）与整数数量，避免浮点不确定性。

## 4. 重放协议（`Engine.Replay`）

重放只能在服务**停止**后调用（运行中调用返回 `ErrReplay`）。三个阶段：

1. **序号协议校验**：逐条 append 到临时存储，要求 `seq == lastSeq+1`：
   - 序号重复 → `event.ErrSeqDuplicate`
   - 序号倒退/非正 → `event.ErrSeqOutOfOrder`
   - 序号跳跃 → `event.ErrSeqGap`

   任何一种都**明确返回错误，绝不静默跳过或继续推进**。
2. **确定性重算与逐字段比对**：从空状态出发，对每个 `ACCEPTED` 用与实时路径
   **完全相同的 `simulate` 函数**重跑校验、风控与撮合，要求重算出的
   `FILLED/BOOKED` 序列与流中逐字段一致；`REJECTED` 要求重算结论为拒绝且
   `Reason` 一致；`CANCELED` 要求订单确实在重建出的簿中且撤销量一致。
   语义被篡改（如改成交价/量）会在此阶段被拒绝。
3. **原子提交**：仅当以上全部通过，才用重建出的状态容器与事件存储替换服务状态；
   中途失败保持原状态不变。

因此：同一事件流多次重建、以及重建结果与实时状态，在订单簿聚合价位与
账户 `Position/Exposure` 上完全一致。

## 5. 风控配置

`risk.Config`（`0` 表示该项不限制）：

| 字段 | 含义 |
|---|---|
| `MaxLongPosition` | 账户最大净多头持仓 |
| `MaxShortPosition` | 账户最大净空头持仓（绝对值） |
| `MaxExposure` | 账户累计名义额 ∑\|qty×price\| 上限 |

- **下单前预判（保守）**：以委托全部立即成交的最坏情形检查。限价单以其限价为
  最坏成交价；市价单以品种 `Limits.MaxPrice` 为参考价；若配置了敞口限额但
  无法给出有效参考价（`MaxPrice=0`），保守按 `RISK_EXPOSURE_LIMIT` 拒绝。
- 风控对挂单方与成交双方同等生效（做市挂单同样要通过预判）。
- 成交后按成交方向更新：买入 `Position += qty`，卖出 `Position -= qty`，
  双方各自累计 `Exposure += qty*price`。
- 被风控拒绝的委托不会产生成交、不会挂单，已确定的簿与敞口状态保持不变。

`engine.Config` 其他配置：`Limits`（价格/数量边界）、`SubscriberBuffer`
（每订阅者事件缓冲）、`AllowedAccounts`（账户白名单，nil/空表示不限制）。

## 6. 订阅者分发

- `Bus.Subscribe()` 返回 `(id, <-chan Event, <-chan Dropped)`。
- 发布在撮合串行 goroutine 内完成，**永不阻塞**：某订阅者缓冲满时，弹出其
  最旧事件再写入最新事件，累加丢弃计数，并通过 `Dropped` 通道异步通知
  （`DroppedCount` 为累计值，`LastSeq` 为丢弃发生时的最新序号）。
- 因此慢订阅者不会拖慢或阻塞撮合；最新事件始终可达，历史事件以事件流为准可重放补齐。

## 7. 并发模型

`Engine.Start()` 启动唯一的撮合 goroutine，下单、撤单、快照查询都通过
带缓冲的通道进入该 goroutine 串行执行；因此并发请求的确定顺序即通道接收顺序，
撮合结论与单机串行一致。状态查询返回的是聚合价位/账户副本快照。

## 8. 本地验证

```bash
# 构建与静态检查
go build ./...
go vet ./...

# 全部单测（含序号/判定日志）
go test ./... -v

# 竞态检测下的并发测试
go test -race ./...

# 端到端示例：并发下单/撤单 -> 停机 -> 事件流重建 -> 状态比对
go run ./cmd/demo
```

测试覆盖：价格/时间优先与部分成交、市价 IOC、撤单与重复撤单、数量/价格/tick
非法拒绝、未知账户与重复ID、市价无流动性、持仓与敞口风控拒绝且状态不变、
事件流重复/乱序/缺口/非1起始/语义篡改的拒绝、失败重放不污染状态、
同一事件流三次重建一致且与实时一致、慢订阅者不阻塞撮合并有丢弃通知。
