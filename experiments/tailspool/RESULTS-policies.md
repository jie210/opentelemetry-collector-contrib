# 策略自动投影的决策与导出对照

测试机 10.98.1.5。使用同一 v0.165.1 加 TakeMany 回移构建。对照组通过官方 processor factory、默认内存存储和未裁剪的完整 OTLP 输入运行。两个实验组分别运行正常的 compact 路径以及接收一部分输入后 SIGKILL 并重启的 compact 路径。三组读取相同的 `processors.tail_sampling.policies`，仅实验组按策略推导字段。独立的官方 v0.165.1 发布依赖也可运行同一测试。

| 策略配置 | 每组 trace | 官方 keep | 官方 drop | 每条路径导出 span | 正常 / 恢复决策差异 | 正常 / 恢复导出差异 |
|---|---:|---:|---:|---:|---:|---:|
| 仅保留 OK 状态 | 512 | 256 | 256 | 576 | 0 / 0 | 0 / 0 |
| resource 字符串正则、span 数值、状态、布尔、延迟、AND 和 drop 否决 | 512 | 200 | 312 | 450 | 0 / 0 | 0 / 0 |
| OTTL 要求同一个 span 同时满足路由与 ERROR 条件 | 512 | 128 | 384 | 288 | 0 / 0 | 0 / 0 |
| span 数量、trace flags、tracestate 和确定性概率策略 | 512 | 172 | 340 | 384 | 0 / 0 | 0 / 0 |

每份配置使用相同的 512 个 trace ID，共 1,152 个输入 span。先让 128 条 trace 完成决策，再接收其余 384 条 trace 的第一批 span。故障组在这一点被 SIGKILL，重启后补发第二批 span，并向先前 128 条 trace 发送迟到 span。逐 ID 比较官方 sampled 和 non-sampled hook 写出的决策清单，检查真实 drop、缺失或重复决策。随后以官方 keep 清单筛选原始 fixture，独立生成期望导出。三条路径均按 SHA-256 多重集合比对每个 span、Resource、Scope 和 schema URL。该 fixture 还包括与策略无关的 1 KiB 属性、事件和链接。字段单测另确认不匹配的值、类型和缺失字段的区别被保留。

对于状态策略，日志记录实际观测的状态枚举集合。对于可推导的其他策略，正常路径保留按 key 投影的小批次，恢复时按相同规则从原文重建。OTTL 及涉及字节数的策略保留原文引用，决策时读取完整输入。keep 之后，按 trace ID 从对应原始 OTLP 批次取出全部 span，传给下游 consumer。

原到期时刻冻结投影后，正常和恢复使用同一官方 processor 完成决策。额外的决策 tick、跨 trace 的 token bucket 和重启时的进程状态都会影响时间敏感策略。表中结果只对应列出的输入和配置。校验 sink 观测到 backend 导出边界，未连接 Kafka。

## 30 秒窗口的持续打流验证

在同一台机器使用 YAML 配置的 `status_code: [ERROR]` 策略、30 秒窗口、512 MiB cgroup 限额和 320 MiB GOMEMLIMIT。持续 155 秒调度 24,576 span/s，中途 SIGKILL。后端重启一次，恢复 192,576 条待决 trace，未发生 OOM。客户端 29,703 批成功、57 批未获 ACK，最长无成功 ACK 间隔为 305 ms。已 ACK 输入应保留的 38,024 个 span 全部匹配；多出、重复和来自未确认批次的导出均为零。恢复后第 35 至 65 秒的 ACK 吞吐为 24,542 span/s。恢复决策相对原截止时间的最大滞后为 4.885 秒，上轮 ERROR 专用实现为 1.218 秒。

`policy-projection-final` 为测试机 `/data/otel-tail-bench` 下本次运行目录。配置、三组对照测试日志、最终构建标识及故障注入计数见 [实测 JSON](benchmark-results-policies.json)。复现逐 trace 对照可运行 `go test -run 'TestPolicyParity|TestProjectionRetains' -count=1 -v`，使用 `-modfile=v165.mod` 可选官方 v0.165.1 发布依赖。
