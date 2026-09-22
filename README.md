# order-matching-engine-go

本地内存、可确定性重放的限价订单簿撮合与执行服务示例。支持限价/市价单撮合、
部分成交、撤单、事件溯源重建、持仓与敞口风控、多订阅者非阻塞分发。

## 快速开始

```bash
go test ./... -v        # 单测（日志含事件序号与判定依据）
go test -race ./...     # 竞态检测
go run ./cmd/demo       # 端到端：并发下单/撤单 -> 重建 -> 状态比对
```

## 文档

- [docs/design.md](docs/design.md) — 撮合规则、事件模型与重放协议、风控配置项、本地验证方法

## 代码结构

```
internal/market  委托类型与合法性校验（可区分拒绝原因）
internal/match   价格优先 + FIFO 时间优先的订单簿撮合器
internal/event   有序事件模型与序号协议（重复/乱序/缺口必拒）
internal/risk    净持仓 / 名义敞口风控
internal/pubsub  非阻塞多订阅者分发（慢消费者丢旧事件并计数）
internal/engine  串行驱动、事件发布与确定性重放
cmd/demo         端到端示例
```
