# order-matching-engine-go

可确定性重放的限价订单簿撮合与执行服务（纯内存，无外部依赖）。

- 限价单 / 市价单（市价为 IOC，残余撤销），价格优先、时间优先；
- 部分成交、撤单；非法数量、价格越界、不可接受委托给出**可区分**拒绝原因；
- 全部状态变更记录为严格连续有序事件，可从事件流断点重建；
  序号重复 / 乱序 / 缺口明确拒绝，不静默跳过；
- 撮合结果向多订阅者非阻塞分发，慢订阅者被断开而不阻塞撮合、不丢事件（可重连追赶）；
- 账户净持仓、挂单笔数、名义敞口、单笔金额风控；触发限额即拒绝且状态不变；
- 一把锁串行化状态变更，并发下单 / 撤单结论确定，`-race` 干净；
- **盘口快照**：随时读取买卖各档价位 / 剩余总量 / 挂单笔数及明细，
  读取免锁不阻塞撮合，视图自洽（每档总量 = 明细之和）；
- **账实一致性审计**：盘口汇总、账户敞口、事件进度三方对账，
  不一致按类别报告并定位到大致事件序号；
- **检查点与增量重建**：事件流推进中随时落检查点，之后只重放剩余事件即可
  补齐状态。检查点**无论从正在运行的实时引擎落、还是从重放中间态落**，
  增量重建结果都与从零全量重建、与实时状态**逐字段一致**（每笔委托的最终状态 /
  成交量 / 受理序号、盘口档位、账户敞口）；并发下单 / 撤单期间落检查点同样成立。
  检查点缺失 / 损坏（校验值不匹配）/ LastSeq 矛盾 / 事件指纹对不上（放错位置）/
  尾部序号链对不上时，一律识别并安全退回全量重建，绝不顺着错误起点继续；
  若事件流本身畸形，全量重放明确报错且线上状态保持不变。老事件流零迁移。

详细设计见 [docs/design.md](docs/design.md)。

## 本地验证

```bash
go build ./...
go test ./... -v          # 日志含事件序号与判定依据
go test -race ./...       # 并发安全
go run ./cmd/matchingd    # 固定脚本演示：撮合 + 重建 + 盘口快照 + 审计 + 检查点

# 单独验证三项新能力
go test ./internal/engine/ -run 'TestDepthSnapshot' -v   # 盘口快照一致性与并发
go test ./internal/engine/ -run 'TestAudit' -v           # 审计（含注入不一致）
go test ./internal/engine/ -run 'TestCheckpoint' -v      # 检查点增量重建与性能

# 检查点一致性（bugfix-b）：实时落检查点增量重建 == 从零全量重建 == 实时，逐字段
go test ./internal/engine/ -run 'TestLiveCheckpointIncrementalEqualsFull|TestMixedScenarioFinalOrderStates' -v
go test ./internal/engine/ -run 'TestPostRebuildBehaviorMatchesLive' -v          # 重建后续行为一致
go test ./internal/engine/ -run 'TestConcurrentCheckpointRebuild' -race -v       # 并发期间落检查点
go test ./internal/engine/ -run 'TestCheckpointFallback|TestRebuildMalformed|TestCheckpointTailBroken' -v
```
