# 实现计划：Collector backend 尾采样顺序盘缓存

目标链路为 **业务 → otelcol LB → OTLP/gRPC → otelcol backend → Kafka**。改动集中在 backend，keep 的数据交给下一个 consumer 即完成，Kafka 侧不在本次范围内。用户通过 **`processors.tail_sampling.policies`** 配置采样；backend 自动生成每个策略的折叠规则。目标是在大 span、长决策窗口下控制待决内存，同时让保留的 trace 以完整 OTLP 数据进入下游。

## 一、原型现状与实测

本节引用的原型源码、`RESULTS-*.md` 与 `benchmark-results-*.json` 已从目录中删除，可用 `git show 8ec24bf6ef9:experiments/tailspool/<文件名>` 查看。

### 磁盘格式与写入顺序

以下对应已验证的 `compact` 原型稳态路径。目录由 backend 指定；测试机在 `/data/otel-tail-bench` 下为每轮分配独立目录。两种文件各有一个用途。

| 文件 | 字节布局 | 用途 |
|---|---|---|
| `00000007.otlp` 等段文件 | `[请求 A 的 protobuf 原文][请求 B 的 protobuf 原文]…`，无分隔符、无每条长度头 | 一个请求占一个连续区间。原文是解压后的 OTLP 请求体，不含 gRPC 帧头；长度和位置由 WAL 提供。 |
| `records.wal` | 每帧为 `uint32 payload长度 + uint32 CRC32(IEEE) + payload`，整数为小端序 | 登记、决定、完成在同一日志中按顺序追加。CRC 覆盖元数据帧；原文段没有单独的 payload CRC。 |

**一步一步写。**

1. gRPC codec 将收到的请求体复制到独立的 `raw []byte`。handler 将 raw 解码一次，遍历 Resource → Scope → Span，按 trace ID 生成本请求的投影。大属性在这个阶段仍有瞬时内存开销。handler 返回后，待决表只持有投影或文件引用。
2. writer 在锁内取当前段的文件大小作为 offset。若追加整个请求会超过 16 MiB，compact 原型先 Sync 旧段，再打开下一段。生产默认值由阶段 A 的轮转 Sync 等载对照确定。单次请求始终写在同一段内；若某个请求自身超过目标段大小，整个请求独占一个超大段。执行一次 `file.Write(raw)` 并检查实际字节数。一般投影另有一次小对象序列化；完整请求无需为写盘重新编码。
3. 给本批次分配递增 seq，构造登记帧 T。其 payload 依次为 `T(1) | seq(8) | dueUnixNano(8) | inboxID(8) | segment(4) | offset(8) | length(4) | {traceID(16), flags(1)} × N`，括号内单位为字节。N 是本批不同 trace 数，相同 trace 的 100 个 span 只占一个登记项。稳态 inboxID 为 0。flags 可携带迟到缓存决定或状态摘要。WAL 中没有 span 属性正文。
4. 同一把锁保护内存登记和 WAL 追加，T 帧 Flush 成功、投影挂入待决表、handler 成功返回后，才能给 LB 成功响应。Flush 把用户态缓冲交给内核；每次 RPC 不执行 fsync。若进程在原文写完、登记前退出，该原文区间没有可回放的 T；若登记完成但 ACK 在网络上丢失，LB 重试仍可能造成重复。已验证的是同机进程退出后重启。

### 用两次 gRPC 请求走完整个采样窗口

以下字节长度、ID 和时刻为演算示例。策略只有 `status_code: ERROR`，窗口为 30 秒。两个请求均在同一段，初始无历史决定。

| 到达 | 收到的 span（trace / span / 状态） | 写入位置 | 登记后的内存 |
|---|---|---|---|
| t=0，A | X / x1 / OK；Y / y1 / OK | seq=101，段 7，offset=0，length=8192 | `index[X]=[101]`，`index[Y]=[101]`；101.pending=2。X、Y 的状态位都含 OK，到期均为 t=30。 |
| t=8，B | X / x2 / ERROR；Z / z1 / OK | seq=102，段 7，offset=8192，length=4096 | `index[X]=[101,102]`，`index[Z]=[102]`；102.pending=2。X 的状态位合并为 OK、ERROR，到期仍是 t=30；Z 到期为 t=38。 |

**采样器此时拿到什么？** 纯状态策略的投影是每条 trace 一个状态位集合。原型每 20 ms 查看截止时间最小堆，每组最多取 256 条到期 trace。t=30 时，对 X 临时构造两条只带 X 和状态码的 span（OK、ERROR），对 Y 构造一条（OK），再调用官方 `sampler.ConsumeTraces`。官方 processor 在自己的一秒窗口内按配置求值。X 命中 ERROR，Y 未命中。t=38 后同理对 Z 求值。这里的 t=30、38 是可提交求值的时刻，实际回调时间还包括调度和官方窗口。临时构造的 span 只进入 evaluator；原型为官方 processor 配置的 next consumer 直接返回，防止将投影误作业务数据导出。

若配置加了 `span_count`，上述“每种状态构造一条 span”的办法会算错数量。原型会切到逐 span 投影，保留每条输入的身份和所需字段，将各请求的小投影编码后放入 `parts`，到期再解码并拼入采样输入。若策略树需要完整上下文（例如 OTTL），待决表只挂 record 引用；到期时将同一组候选按 record 分组，按 seq 顺序读取，筛出候选 trace 后交给同一官方 processor。这条路径即使最终 drop 也有求值读盘开销。投影分支：`projection.go` · 到期组装：`pending.go`

### 从决定追到磁盘字节，再追到导出 span

1. **收到 X 的 keep。** 官方 processor 写入 sampled decision cache，原型的 `observedCache.Put` 收到 X。它追加 `K | X` 决定帧，找到 101、102，分别把 X 加进两条记录的 keep 集合，各自 pending 从 2 减至 1。此时两个批次都还含未决 trace，导出路径暂时没有读盘。
2. **收到 Y 的 drop。** 追加 `D | Y`。101.pending 变为 0，101.keep 为 {X}。调用段 7 的 `ReadAt(buf[:8192], 0)`，解码 A，遍历原始 span，保留 x1、删除 y1，删除由此变空的 Scope/Resource。输出中的 x1 带有原来的属性、事件、链接和所属 Resource/Scope。keep 的是 X 整条 trace，因此 x1 的状态为 OK 也会导出。X、Y 的回调顺序互换，101 的结果相同。
3. **收到 Z 的 drop。** 102.pending 变为 0，102.keep 为 {X}。调用 `ReadAt(buf[:4096], 8192)`，解码 B，只交付 x2。两个请求合起来输出 x1、x2 各一次，下游可能分两批收到 X。同一 record 含多个 keep trace 时，一次导出回读即可取齐；keep 集合为空则直接完成。需要完整输入求值的策略可能在求值和导出两个时点分别读取同一 record。
4. **确认与回收。** 原型调用本地校验 sink 成功后追加 `C | seq | 已提交的校验输出条数`，减少段的活跃 record 数；封闭段归零才可删除。重启时按完整 WAL 帧重建关联；尾部半帧截断，CRC 错误报错。如果已写 K/D、还没写 C，可以重做该 record；本地校验文件按最后一次 C 截断再回放。Kafka 无法用文件截断实现相同语义。决策与读取实现：`spool.go` · 恢复规则：`journal.go`

**读盘成本的计算单位是请求。** 在上述纯状态示例里，4 个 span 只保留 2 个，但两个原始请求都含 X，导出仍要逻辑读取 12,288 字节并解码两次。估算冷盘压力应统计“至少有一条 keep trace 的请求字节数”，另外计入完整输入策略的求值读取和重启重建读取；物理读量还受页缓存和预读影响。

### 原型恢复与限制

原型在 activate 接管全部 inbox 后启动 dispatch。待决窗口和 WAL 使用 UnixNano 墙钟，到期后再向官方 processor 的一秒窗口提交；attach 在原截止时间冻结投影。回读及校验 sink 同步运行于 Put 持有的 spool 锁内。状态位路径以外，逐 span 投影和完整上下文回读都会增加内存或磁盘压力。恢复入口：`online.go` · 调度：`pending.go` · 交付：`spool.go`

实验程序只接受 trace-complete。只有 `plan.full` 会把 record 持续加入 `v.raw`；相同 wait 下，求值只接收满足 `r.due - wait < v.due` 的片段。完整上下文和字节策略在原型里回读原文。启用 tracestate 预算且最终投影未携带 tracestate 时，rate_limiting 可能缺少随机值输入。K/D 及检查点缓存缺少 Threshold、PolicyName；轮转 OpenFile 失败可重试，短写、WAL、回读或 sink 错误可使 s.err 持续生效。实验 harness 为 sampled 和 non-sampled 两侧各注入容量 150,000 的 LRU，现有差分只覆盖启用缓存的配置。官方两个 cache_size 默认均为 0。历史验证也没有覆盖 span-ingest。实验缓存注入：`main.go` · [默认配置](https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/v0.165.1/processor/tailsamplingprocessor/factory.go) · 配置与投影：`projection.go` · 回放：`journal.go`

### 实测范围

- **存储路径。** 在 6,144 span/s 冷回读等载下，顺序盘稳态物理读/写约 8.9/24.5 MiB/s，TakeMany Pebble 约 51.2/75.2 MiB/s。对照使用 v0.165.1 加两项上游 TakeMany 回移。冷盘对照：`RESULTS-pressure.md`
- **决策一致性。** 四套配置各 512 条 trace。正常与 SIGKILL 恢复各对照官方 2,048 个逐 trace 决策、1,698 个完整导出 span，差异均为零。覆盖状态、类型化属性、延迟、组合否决、同一 span 的 OTTL 条件、计数、flags、tracestate 和概率。这四组未启用上述输出修改 gate，也未在投影冻结与官方发布决定之间注入 span。策略差分报告：`RESULTS-policies.md`
- **写入与恢复。** 仅配置 ERROR 状态位投影的版本在上述 24,576 span/s 故障档持续 155 秒，恢复 192,576 条待决 trace，无 OOM。最长无 ACK 间隔 305 ms，57 批未获 ACK；已 ACK 输入应保留的 38,024 个 span 均匹配。最大恢复决策滞后为 4.885 秒，前一版为 1.218 秒。本轮接管 143 个 inbox；192,576 是恢复时待决总量，其中已过期的条数未记录。现有指标无法把滞后退化归因于 activate，阶段 A 先采集串行基线。完整性已验证到 backend 校验 sink，LB 重发和 kill 后的重复上界由阶段 C 验收。故障实测：`benchmark-results-policies.json` · 前版恢复数据：`RESULTS-online.md`

## 二、生产设计与验收

### 数据在 tail_sampling 中的动作

| 时点 | 动作 | 数据如何变化 |
|---|---|---|
| 启动 | 读取实际 policy 树、processor 设置和 feature gate，为每个叶子策略生成折叠器，初始化官方 evaluator 的匹配逻辑。 | 得到整个 pipeline 的折叠规则。配置里的匹配值、正则、阈值、否决优先级沿用官方实现。 |
| 接收与登记 | 在 backend gRPC 解压后、接收缓冲区释放前，取得原始 OTLP protobuf，顺序追加一次。记录段号、偏移、长度、批次关联的 trace ID 和首次到达的到期时间。 | 一个批次含多条 trace，磁盘仍只写一份原文。达到约定的可恢复边界后才向 LB 返回成功。 |
| **进入 tail_sampling** | 按官方拆分顺序遍历每个片段，对每个叶子策略更新该 trace 的折叠状态：计数、状态位、时间边界、首个随机值，或“是否有 Resource/Span/Event 命中”。 | 例如策略要求 `price >= 10`，本次值为 3 时只记录“尚未命中”，不保存属性值。每条 trace 的内存只与叶子策略数有关，与 span 数和大小无关。原始属性、事件、链接仍在磁盘上。 |
| **接收时决定或继承** | 对每个 trace 片段先查已有决定；待决 trace 更新大小计数并检查超限，span-ingest 模式还可对当前片段提前求值。 | 首次终局与 tick 共用 K/D 发布；缓存或活动决定命中分别按下表处理新到片段。 |
| **等待 decision_wait** | 同一 trace 跨批次累计折叠状态；沿用首次登记的绝对到期时间。迟到片段先查决策缓存，未命中再判断活动 trace 状态。 | 大 payload 的体积与待决堆内存解耦。实际内存取决于活跃 trace 数、叶子策略数及每条 trace 关联的记录数。 |
| **到期求值** | 按官方策略顺序和组合语义，用各叶子的折叠结论及累计成本得出最终决定。求值期间不回读原文。 | 产出 trace ID 和最终 keep/drop；启用采样阈值或策略标记时，还要传递相应的决策元数据。 |
| **keep：回读并向下游交付** | 由 trace ID 查到一到多个批次位置，按位置读回原文，遍历原始 Resource、Scope、Span，筛出全部已 keep 的 span，交给下一个 `consumer.Traces`。 | 保留原始 span ID、属性、事件、链接、资源及 schema URL。若官方配置会更新 tracestate 或写入策略名，也须在恢复后的完整对象上应用相同变更。 |
| **drop：释放引用** | 在关联记录上完成该 trace 的决定；当一条记录中所有 trace 都有结论时统一处理。全 drop 跳过导出回读，混合批次一次筛出所有 keep span。 | 段内记录引用归零且相应提交完成后回收封闭段。混合批次需要等待最晚的一条 trace，容量估算应覆盖这段时间。 |

**跨批次示例。** 批次 A 含 T1、T2，批次 B 又含 T1。若 T1 keep、T2 drop，索引将 T1 定位到 A 和 B；从 A 只取 T1 的 span，从 B 取回 T1 的其余 span。这样无需逐 span 建磁盘 KV 索引，同时保留合法重复输入的重数。

### 接入与索引

在 backend 配置专用 OTLP/gRPC traces receiver，并让它和紧邻的 tail_sampling 实例共享一个 spool。普通 `consumer.Traces` 入口只收到 pdata，已经丢失原始请求字节。专用入口应在同一次调用中把原文、解码视图交给 spool，等落盘和登记成功再结束 RPC。阶段 A 在监听 gRPC 前检查这条 traces pipeline 的实际组件顺序。专用 receiver 与对应 tail_sampling 之间出现任何 processor 就拒绝启动，错误列出 pipeline 和中间组件名，以免原文回读重新引入已过滤的 span 或已脱敏的字段。采样后的 processor 接收回读的完整数据。原型接收入口：`main.go` · 接收与登记：`online.go`

**批次位置的传递。** 拟增加一个 backend 私有的登记入口，输入为批次位置、原始到达时间和按 trace 更新后的折叠状态。位置保存在 sampler 和 spool 共享的侧索引中，不写进 OTLP 属性，也不依赖跨异步边界传递 gRPC context。一条 record 持有段、offset、length、到期时间、待决 trace 数和 keep 集合。每个不同的 trace ID 在同一 record 只登记一次，`index[id]` 可引用多个 record。另用 `views[id]` 保存该 trace 的折叠状态。先追加原文，再写登记 WAL，最后发布内存索引并返回 ACK；短写或登记失败返回错误，恢复时只承认可完整回放的登记。此顺序的现有实测范围是进程 SIGKILL。原文和双向关联：`spool.go` · 登记日志：`journal.go`

### 接收与 tick 共用的终局决定

生产 fork 的默认模式为 `trace-complete`。即使使用该模式，接收时也要检查大小上限。规划支持 `span-ingest`，仅限目标官方版本允许的无状态策略组合；保留官方配置校验。该模式在阶段 B 的专项差分通过后开放，此前显式配置时在监听前报错。root span 触发的 decision_wait_after_root_received 只调整调度批次，由 tick 发布决定。[官方接收与释放](https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/v0.165.1/processor/tailsamplingprocessor/processor.go#L1104-L1199)

| 决定来源 | 求值输入与时机 | 结算动作 |
|---|---|---|
| 接收时大小超限 | 对未命中已有决定的待决 trace，按官方拆分片段先更新 SizeBytes。限额非零且严格越界时立即 NotSampled。 | 登记 D，移出调度，结算历史和触发越界的当前 record。 |
| span-ingest | 每个拆分片段依次求值。ReceivedBatches 只含当前片段，SpanCount 和启用时的 SizeBytes 是累计值。遇到尚未排除的 drop 策略按官方逻辑保持 Pending。 | Sampled 或 Dropped 提前发布 K/D；keep 交付历史及当前全部 span。记录触发片段，使首次策略标记及阈值按官方作用范围应用。仍 Pending 的 trace 在清理 tick 发布 D。 |
| trace-complete tick | 对冻结成员执行下节三阶段求值。 | 将最终 keep/drop 交给同一发布入口。 |
| 容量移除（block_on_overflow=false） | 接纳新 ID 时 idToTrace 已达 num_traces，官方移除 deleteTraceQueue 队首项，不求值、不发布决定、不写缓存。 | 写 V 帧结束该实例。待决 trace 的全部关联按 drop 结算，不导出、不触发 hook；已决但仍活动的项只结束活动决定。之后同 ID 按新实例登记。 |
| 迟到：决策缓存命中 | 先查 sampled / non-sampled LRU。sampled 命中时，开启 recordPolicy 则在 Scope 属性写入非空 PolicyName 对应的 `tailsampling.policy`，并写 `tailsampling.cached_decision=true`；开启 useTracestate 则应用缓存 Threshold。 | 只转发新 keep 片段。non-sampled 命中直接消费新片段，不导出、不触发 non-sampled hook。 |
| 迟到：活动决定命中 | 缓存未命中且 idToTrace 仍持有最终决定。对 Sampled，开启 useTracestate 才向新片段写 FinalThreshold；沿用原有 Scope 属性，不添加策略名或 cached_decision 标记。 | 只转发新 keep 片段。对 NotSampled，官方每个迟到拆分片段会再次调用 releaseNotSampledTrace，包括 non-sampled hook。新关联继承 drop，旧关联不再结算。 |

两个 LRU 容量独立，0 表示禁用相应缓存。终局写入相应缓存且可查回时，官方释放 idToTrace 项，后续由 LRU 的访问与淘汰顺序决定命中期。无缓存时活动决定只在 idToTrace 项仍存活时生效。默认 block_on_overflow=false，达到 num_traces 且需要接纳新 ID 时按队列移除最早项，按上表“容量移除”行处理；true 时等待空间，不能以容量压力模拟 LRU 淘汰。显式清理仍生效，例如 span-ingest 清理 tick 会无条件移除仍 Pending 后转为 NotSampled 的项。两处都查不到时按新实例登记。活动表不设与 decision_wait 相同的保留 TTL。[缓存命中](https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/v0.165.1/processor/tailsamplingprocessor/processor.go#L598-L627) · [容量处理](https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/v0.165.1/processor/tailsamplingprocessor/processor.go#L630-L652) · [活动决定命中](https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/v0.165.1/processor/tailsamplingprocessor/processor.go#L1193-L1207)

**容量口径与移除顺序。** fork 的容量计数与官方 idToTrace 同口径：待决 trace 和仍持有活动决定的已决 trace 都计入，不能只数待决索引。容量移除与决定发布走同一个串行状态更新入口，先写入者生效：已冻结但尚未发布的候选若先被移除，其后的发布按“实例已结束”跳过；已发布的决定不会被移除撤销。按官方语义，被移除的待决 trace 即使已 ACK 也不会导出，阶段 C 的“不缺失”核对排除这些实例并单独计数。

**block_on_overflow=true 与零缓存。** trace-complete 模式下，官方只在决定写入缓存且可查回时才释放已决项。若 block_on_overflow=true 且任一侧缓存容量为 0，该侧结论的已决项会一直留在 idToTrace，达到 num_traces 后 waitForSpace 永久阻塞接收。v0.165.1 本身就有这一行为。fork 在监听前拒绝该组合，错误中列出 block_on_overflow 与对应的 cache_size。实验 harness 改为显式配置这两项，不再固定注入 LRU。

**分片与运行时重载。** 官方 num_shards 大于 1 时按 trace ID 哈希把数据路由到多个独立分片，num_traces、两个缓存容量和策略中的每秒限额按分片均分，且官方本身不允许与 tail_storage 同时使用。首版 fork 只支持 num_shards=1，大于 1 时在监听前报错。官方 SetSamplingPolicy 与 SetMaximumTraceSizeBytes 会在下一轮循环换用新配置，在途 trace 用新策略求值；折叠状态按启动时的策略树生成，无法直接沿用。首版 fork 拒绝这两个运行时调用并记录错误，配置变更走 Collector 重启，按下文重启接续顺序处理在途版本。[分片](https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/v0.165.1/processor/tailsamplingprocessor/sharded_processor.go) · [运行时重载](https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/v0.165.1/processor/tailsamplingprocessor/processor.go#L224-L285)

**登记与结算顺序。** 在每个 trace 的串行事件顺序中，先追加请求原文，再登记 T（含该批所有 trace 的关联），之后才允许当前片段触发 K/D。发布入口以 trace ID 和本轮待决实例的标识（例如首次登记序号）识别一次终局。将 K/D、决定元数据和触发片段边界写入可恢复日志，再发布缓存和更新 record。每个 (record 序号, 待决实例) 的关联只允许由 pending 转为 keep 或 drop 一次；只有实际转换才扣减 pending。已继承终局的关联直接登记为已决。同一请求可能含该 trace 的多个 ResourceSpans：按官方拆分顺序处理，并记录触发片段及后续片段的位置及其命中来源。record 的待决数按 trace 结算一次，实际交付须等本请求各片段的输出元数据确定后才就绪。混合 record 等全部关联结算后进入锁外交付或回收。重复回调、晚到的 tick 和重放遇到已结算关联时跳过。T 已写而 K/D 未写时按原片段顺序接续；K/D 已写但索引尚未更新时从日志重建。检查点需区分缓存淘汰前后的待决实例，防止旧 D 覆盖相同 ID 的新一轮决定。

### fork 如何改造 tick 主循环

官方 v0.165.1 的 evaluator 只接收完整 `ReceivedBatches`，tick 先 Take 全部候选的 span 再求值。公开 decision hook 只能观察已产生的决定，无法替换求值输入。生产 fork 因此将叶子 evaluator 改为读取折叠状态，tick 改为基于折叠状态求值，通过 OCB 构建自定义 backend。tick 期间不读盘，不需要回读额度。[官方 tick](https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/v0.165.1/processor/tailsamplingprocessor/processor.go#L709-L841)

1. **冻结候选。** 固定本 tick 的候选顺序和接收成员边界；冻结后到达的片段不再并入参与求值的折叠状态，只登记到导出索引。
2. **预筛并计算整批阈值。** 先执行官方顶层 drop 前缀，再按各预算策略的组合规则确定合格集合。`and` 内非预算子策略的预筛直接读取折叠结论，保留官方调用顺序、次数和错误处理。为预算 evaluator 增加摘要入口，传递合格集合、trace ID、累计成本及随机值。span 成本取真实 SpanCount，字节成本按下文计算，随机值按官方顺序采用首个有效 OTel rv，缺失时从 trace ID 推导。启用批预算时复用官方预算、排序、相同随机值处理与令牌扣减逻辑；关闭 gate 的逐 trace 令牌桶保持原调用时机。
3. **逐 trace 求值并发布。** 按冻结顺序执行最终策略组合，发布决定和元数据。keep 交付独立使用完整导出索引，包含应继承决定的迟到 span。

**升级责任。** OCB 构建清单固定上游 tag 和 fork 提交。receiver、段文件、WAL 和 FD 管理放在独立组件。阶段 A 建立生产出包流程。

每个拟采用的上游 release 单开升级 PR，安全修复可加急。backend Collector 组件维护人负责移植补丁、审查 evaluator、tick、预算、迟到缓存和 gate 的上游变动，并运行阶段 B 的差分矩阵。任何叶子 evaluator 的匹配或组合逻辑变化都要同步到对应折叠器。以**目标 tag、默认内存 tail storage、未裁剪的官方路径**为基准，提交构建标识、策略与 gate、每个策略的资格集合、成本和阈值、逐 trace 决定、逐 span 内容和重数，以及恢复与迟到用例结果。backend 发布负责人复核结果，对无法解释的差异阻断出包。阶段 A 结束前由团队在仓库维护人名单中登记这两个角色的具体负责人。

### 接收时折叠与回读交付

官方叶子策略在 trace 级的结论都可以由逐片段累计得到：计数和集合直接累加；属性、trace_state、OTTL 的结论是“任意 Resource/Span/Event 命中”，`invert_match` 为“全部都不命中”；latency 按官方遍历顺序维护最早开始、最晚结束和“曾满足”标志。启动时为策略树中每个叶子生成折叠器，正常接收与恢复重建共用：

1. 接收时按官方 splitResourceSpansByTrace 的片段顺序逐片段调用折叠器。该顺序等于官方 evaluator 遍历 `ReceivedBatches` 的顺序，对依赖遍历顺序的 latency 上限和首个随机值同样成立。
2. 每个叶子保存与官方 evaluator 相同的结论值（含求值错误及其 error_mode 处理），不只保存布尔。存在型结论一旦命中即停止该叶子的后续匹配，以节省接收 CPU。
3. 单条匹配直接复用官方 matcher、正则缓存和 OTTL 表达式；折叠器只负责 trace 级组合。OTTL 在接收时以片段所属 Resource/Scope/Span 构造上下文求值；条件中使用依赖当前时间的函数（如 `Now()`）时，接收时与 tick 时的结果可能不同，启动时检出并拒绝。
4. 预算类策略只累计成本和随机值，令牌扣减仍在决策时进行。
5. 17 种内置策略都走折叠路径。无法折叠的策略类型在监听前拒绝启动，不提供原文回读求值的回退。

每条待决 trace 的内存为固定头部加各叶子的状态，与 span 数和属性大小无关。阶段 D 按策略树计量每条 trace 的状态字节数和接收侧折叠 CPU。

**求值与导出分别取哪些 record。** 参与求值的只有冻结前已折叠的片段；导出索引保留全部关联，包括继承决定的迟到 span。WAL/检查点保存可恢复的原始到达顺序、到期时刻和已冻结候选的成员边界，原子发布冻结结果，确保重启后得到同一成员集合。若接收与接管顺序不同，使用下文的接管进度确认输入齐全，再冻结。

**交付边界。** Kafka 不在本次改造范围内。keep 的数据交给该 pipeline 的下一个 `consumer.Traces`（即 Kafka exporter），`ConsumeTraces` 返回成功即视为完成，随后写 C 并释放段引用。exporter 内存队列中尚未发出的数据，其持久性由 exporter 配置负责（例如基于 file_storage 的持久队列），不由 spool 保证。锁外交付是阶段 A 的通过条件：锁内仅持久登记“待交付”并固定段引用，锁外由有界 worker 回读、解码和交付。以持久 record 序号作为交付身份；若需拆批，再附稳定子序号，重试沿用同一身份。待交付积压以磁盘日志为准，内存只取一页工作项。

**重复上界。** 进程被 kill 时，已交给 exporter 但尚未写 C 的数据会在重启后再交付一次，这部分少量重复可以接受，但必须有确定上界，不能出现大量重复：

1. 每次交接成功后立即写 C，合并写入时限定条数和等待时间；在途交付的 record 数和字节数设硬上限。单次 kill 的重复量不超过该上限。
2. 拆批交付的 record 每个子批单独记完成，中途 kill 只重放未完成的子批。
3. 接收端在写 T 之前检查 gRPC deadline，已过期的请求直接返回错误、不登记，避免 LB 超时重发造成同一请求登记两次。背压或恢复期间快速返回 `UNAVAILABLE`，不挂起等待；LB 超时应大于 backend 登记耗时上限。若压测仍出现重发，可选开启按请求内容哈希在短窗口内去重。
4. exporter 侧的超时重试由 exporter 配置约束（重试次数、Kafka producer 幂等），不在 spool 范围内。

### v0.165.1 全部 17 种内置策略的折叠状态

**每条 trace 都保存 trace ID**，用于聚合、原文关联和发布决定。表中列出每个叶子额外保存的状态，策略树中同类叶子各自独立保存。属性策略在接收时同时检查 Resource 和 Span，与官方一致。

| 官方 policy type | 每条 trace 的折叠状态 | 语义与实现要求 |
|---|---|---|
| `always_sample` | 无 | 此策略总是采样，最终决定仍受其他 drop 策略约束。 |
| `status_code` | 出现过的 UNSET、OK、ERROR 位集合 | 任一 span 状态在配置集合中即命中。 |
| `latency` | 最早开始、最晚结束、“曾满足”标志 | 官方逐 span 更新最早开始和最晚结束并立即判断，任一时刻满足即命中。存在上限时，最终时长可能已超过上限而结论仍为命中，所以必须按片段顺序在接收时判断，不能只比较最终时长。覆盖毫秒截断、开始时间为 0 的特殊处理和上下限。 |
| `probabilistic` | trace ID；启用 `usetracestate` 时加首个有效 OTel `rv`/`th` | `sampling_percentage`、`hash_salt` 是配置。随机值选择和回退哈希复用官方实现。 |
| `string_attribute` | 一个“已命中”位 | 复用官方 matcher，空串、缺失和正则规则不变。`invert_match` 时为“全部 Resource 和 Span 都未命中”，任一命中即翻转为 NotSampled。 |
| `numeric_attribute` | 一个“已命中”位 | 同上，保留零值、缺失和类型不匹配，使用官方区间规则。 |
| `boolean_attribute` | 一个“已命中”位 | 同上，区分 false 和缺失。 |
| `span_count` | 累计 span 数 | 跨批累计，保留重复输入重数，与官方 SpanCount 一致。 |
| `trace_state` | 一个“已命中”位 | 复用官方 tracestate 解析和取值匹配。 |
| `trace_flags` | sampled 位的 OR | 任一 span 设置 sampled 位即命中。 |
| `ottl_condition` | span 条件、span event 条件各一个“已命中”位，加错误标志 | 接收时按官方上下文逐 span、逐 event 求值，条件只在单个 span 或 event 内组合，不跨 span 拼接。错误按 error_mode 处理并记录。 |
| `rate_limiting` | 累计 span 数；tracestate 预算模式加首个有效随机值 | 令牌扣减在决策时进行。覆盖顶层及允许的 and、composite 等嵌套位置。阶段 B 固定令牌余额、墙钟和同 tick 候选集合做差分。 |
| `bytes_limiting` | 累计字节成本；tracestate 预算模式加首个有效随机值 | 成本按官方 ingest 分组在接收时精确累计，逐 trace 通过阶段 B 的整数差分后启用。令牌扣减在决策时进行。 |
| `and` | 子策略各自的折叠状态 | 决策时按官方 AND 语义组合子结论。 |
| `not` | 子策略的折叠状态 | 决策时对子结论取反，包括字段缺失语义。 |
| `drop` | 子策略各自的折叠状态 | 子结论均命中时显式 drop；保留其对普通 keep 策略的否决优先级。 |
| `composite` | 子策略各自的折叠状态，加累计 span 数 | 决策时按子策略顺序、每秒总额度、分配比例和当秒已消耗额度分配。开启策略标记时还需记录命中的子策略。 |

策略以外的 processor 设置也可能需要字段。root 提前调度需要 parent span ID。启用 maximum_trace_size_bytes 或官方字节统计 gate 时，在接收路径按官方拆分的完整片段累计 ResourceSpansSize(rss)。bytes_limiting 独立累计每个片段单独包装为 Traces 后的 TracesSize，含外层标签与长度前缀。两个逻辑计数按需启用，同一片段可复用一次尺寸遍历。发布前应对所启用的功能和 feature gate 联合验收。表中列的是折叠状态，限速、字节预算、组合预算和 gate 组合仍待专项验证。[官方策略定义](https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/v0.165.1/processor/tailsamplingprocessor/config.go) · [官方 evaluator](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/v0.165.1/processor/tailsamplingprocessor/internal/sampling)

### 自监控指标兼容

v0.165.1 的 16 个 `processor_tail_sampling_*` 指标保留原名、属性和单位，现有看板与告警不需要修改。按口径分组如下：

| 类别 | 指标 | 要求 |
|---|---|---|
| 口径不变 | `count_traces_sampled`、`global_count_traces_sampled`、`early_releases_from_cache_decision`、`new_trace_id_received`、`traces_dropped_too_large`、`sampling_late_span_age`、`sampling_trace_dropped_too_early`、`count_spans_with_unparseable_tracestate`、`sampling_decision_timer_latency` | 在与官方相同的事件点计数。 |
| 需按折叠重新取值 | `count_spans_sampled`、`count_bytes_sampled`（均受 gate 控制） | 取折叠累计的 SpanCount 与 SizeBytes，与官方整数一致。 |
| 需按 spool 状态取值 | `sampling_traces_on_memory`、`sampling_trace_removal_age` | 与官方 idToTrace 同口径，包括容量移除（V）。 |
| 求值拆成两段 | `sampling_policy_execution_count`、`sampling_policy_execution_time_sum`、`sampling_policy_evaluation_error` | 次数与错误按 tick 中每个策略的组合求值计，与官方 Evaluate 调用次数一致；接收时遇到的折叠错误在该叶子参与求值时计入。耗时只计 tick 段，接收折叠耗时另设新指标。 |

新增 spool 指标：段和 WAL 字节数、待交付积压、在途交付条数与字节、重启后重放交付条数（即重复上界的实际值）、容量移除次数、因 deadline 过期在登记前拒绝的请求数、恢复各阶段耗时、折叠状态字节数和接收折叠耗时。阶段 B 用相同输入逐项比对计数类指标与官方的取值，耗时类只核对是否上报。

### 写入、恢复和交付的提交边界

1. **接收 ACK 与故障范围。** 本轮承诺同机进程 SIGKILL 恢复。登记 WAL 每批 Flush，旧段轮转时 Sync，检查点文件 Sync 后 rename，父目录未 fsync。阶段 A 在同一负载下开关轮转 Sync，比较 ACK 尾延迟、脏页回写、盘写入和 kill 后完整性，再确定默认行为。若以后要求掉电恢复，另行实现组提交：先持久化原文及必要的目录项，再持久化对应 WAL 后 ACK；inbox 早期 ACK 同样需同步文件和改名目录。检查点替换和段回收也需保证目录持久化顺序。
2. **重启接续顺序。** 开放 gRPC → 保存新批次及到达信息到临时 inbox，完整写入并原子改名后 ACK → 从检查点和后续 WAL 重建存活段、登记与 K/D/V → 未决记录按登记顺序回读原文，用同版折叠器重建折叠状态和两种启用的计数（也可在检查点中保存折叠状态快照，只回读其后的记录），已决待交付记录继续交付，已完成记录跳过 → 按原始到达顺序接管 inbox，按缓存及活动决定处理迟到输入 → 截止前已 ACK 输入全部可见的候选即可调度，新接收继续进行。每条 record 对每条 trace 只折叠一次，重放不得重复累计。检查点持久化折叠规则与 gate 版本；配置升级时沿用在途版本，或先排空再切换。阶段 A 先保留串行行为，分别记录三段恢复耗时（recover、rebuildViews、activate）、inbox 文件数与字节数、各阶段已到期数量和最老截止时间，以及首次 dispatch 提交、首次官方决定的时刻。按同一批次或 trace 关联截止、提交和决定时间，分开报告排队和求值滞后。
3. **并行接管的前置条件。** 阶段 C 先独占重建历史索引，然后由同一状态更新入口串行修改 WAL、index、截止堆和段引用。接管前固定一批已发布 inbox 的到达顺序，按序发布已登记进度，与新 inbox 的登记原子衔接。只有确认某个候选冻结点之前所有应参与的输入都已可见，才允许调度该候选。压力测试需覆盖持续进流及接管中再次 kill，避免恢复依赖 inbox 整体清空。硬链接接管先提交来源 ID，再删除临时名字。运行 race 检查，并构造“截止前 ACK、截止后才接管、该 span 足以翻转决定”的用例。
   **接管文件的 FD 与 inode。** 阶段 C 保留硬链接接管，每个 inbox 自创建起占一个 inode。adopt 和再次启动的 recover 都只登记存活段的位置，读取时按需打开，共用有硬上限的只读 FD 缓存。上限取部署给定的缓存额度与“进程软 RLIMIT_NOFILE 减去 WAL、活动段、网络连接及其他 FD 预留”两者较小值。打开中的 FD 也占额度。借用期间固定 FD，释放后才可按 LRU 关闭；额度满且全部在用时排队。已交付、段内记录归零且所有读者释放后才删除段。FD 回收不会释放尚有记录的 inode。

4. **决策元数据与检查点。** 阶段 B 给决定帧和检查点加版本，保存有效阈值、命中策略及所需的子策略信息，并在每个 record 上按片段区分首次决定、LRU 命中及活动决定命中，保存实际使用的元数据。检查点和增量日志恢复活动决定、容量移除（V）及 deleteTraceQueue 顺序，以及两侧 LRU 成员。交付按已记录的片段来源重放；重启后新片段按恢复后的两层状态重新查找。阶段 C 对重启前后读回的完整对象应用相应修改。混合 trace 的同一 Scope 若需写不同策略属性，按决定拆分 Scope，保留原属性与 schema URL。升级前排空缺少必需元数据的旧在途记录，或验证可等价重建的迁移路径。

   **LRU 一致性分级。** 一级必须与官方完全一致：首次决定、导出内容与重数，以及不重启时的缓存命中与淘汰。二级允许近似：kill 恢复后 LRU 的内部访问顺序。为避免每次缓存命中都写日志，检查点只保存成员和写入顺序，恢复后按写入顺序重建访问顺序。差异只影响恰好处在淘汰边界的迟到片段：本应命中的变为未命中并作为新实例求值，或反之。阶段 B 单独统计这类差异条数，要求不超过恢复时 LRU 中“检查点后有读取”的成员数。
5. **交付与重复上界。** Kafka 不在本次范围内，交付终点是下一个 `consumer.Traces`。用稳定的 record / 子批次身份重试，`ConsumeTraces` 成功后立即写 C 并释放段引用。重复只来自“已交给 exporter、C 尚未落盘”的在途部分和 LB 重发，按上文“重复上界”的四条约束控制。独立核算 LB 返回失败及 ACK 不明的请求。
6. **故障状态与 LB。** 接收端在登记前检查 gRPC deadline，过期请求不登记；背压与恢复期间快速失败，不让请求挂到 LB 超时。生产版对未改变状态的打开失败允许重试；对暂时的交付失败保留 record，由 worker 退避重试；对短写、WAL 或索引一致性不明的故障停止成功 ACK，撤销 readiness，修复或回放校验后再接流量。记录故障类别和积压，实测 LB 摘除、恢复时的 trace 路由和在途归属。盘满或下游长期不可用时单独核算背压。

**截止时间和时钟跳变。** 在线调度使用 Linux CLOCK_BOOTTIME（含系统挂起时间）。WAL、检查点及恢复接入的 inbox 元数据记录 boot ID、同一启动周期可比较的到达/截止时间、墙钟截止时间和到达序号；inbox 文件身份独立于墙钟。同一 boot ID 的进程重启用原 BOOTTIME 截止时间直接计算剩余等待，过期立即具备调度资格。对比墙钟与 BOOTTIME 的增量，检测显著偏移时记录方向、幅度及受影响的待决数。已冻结候选始终按已提交成员边界恢复。

若机器重启导致 boot ID 改变，仅对磁盘上可完整回放的未冻结记录使用墙钟回退。剩余等待取 `clamp(墙钟截止时间 − 当前墙钟, 0, 原窗口长度)`，再换算到新 BOOTTIME，并持久化本次换算以便再次重启；等待取零仍须等截止前已 ACK 的输入接管完毕。此规则限定回拨后的最长额外等待，跨 boot 的时钟误差仍可改变实际等待时长。分别注入运行中和 SIGKILL 停机期间的墙钟前跳/后跳，独立推进 BOOTTIME，验证同 boot 到期点不变、跨 boot 回退与告警生效、已冻结成员及导出重数一致。

### 实施里程碑与验收

| 阶段 | 实现顺序 | 通过条件 |
|---|---|---|
| A．隔离接入与交付 | 接入专用 receiver，硬校验直连，拆除决策线程上的同步交付，接入 readiness 和恢复分段计时。 | 直连可启动，插入 transform 或 filter 时在监听前报错，采样后 processor 收到完整 span。阻塞假 sink 时，只要盘仍有空间且负载在可持续范围内，后续请求仍能 ACK、无关 trace 仍能完成决定；解阻后逐 span 核对。分别注入轮转失败、短写和 WAL 失败。测量锁等待、WAL 与检查点耗时，完成轮转 Sync 对照。 |
| B．对齐官方决策与输出 | A 通过后，接入 17 种策略的折叠器、基于折叠状态的 tick 主循环及预算摘要接口。覆盖 gate 和决策元数据持久化。 | 每个候选版本执行下述折叠、边界、字节成本及 gate 矩阵，按 fork 升级职责复核。逐 trace 比较 keep/drop，逐 span 比较官方实际输出及重数。预算策略固定时钟、候选顺序和初始预算。 |
| C．恢复与重复上界 | 在恢复顺序用例通过后比较串行与并行接管，完成下述 FD 和 inode 验收，再接入真实 exporter。 | 在 24,576 span/s、30 秒、512 MiB 下中途 kill，对比 ACK 空窗、分阶段恢复耗时、决策滞后分位数与最大值。分别在登记、决定、回读、交给 exporter 后、写 C 前、回收时 kill，并注入 LB 超时重发；在 tail_sampling 之后接捕获 sink，用客户端 ACK 清单和官方输出核对：已 ACK 应保留的 span 不缺失、字段一致，重复数不超过在途上限。 |
| D．容量与灰度 | 在折叠路径通过 B 后，与同版本 TakeMany Pebble 等载逐档加压，复测多 backend 和慢盘。 | 同一输入分别测纯状态策略、大字符串属性、正则与 OTTL 条件、单条超长 trace；执行下述折叠开销用例。改变混批和 keep 比例，记录可持续 span/s、ACK、接收侧 CPU、RSS/cgroup、折叠状态与接收解码峰值、积压及物理读写。回切前排空或移交在途 WAL。 |

**B 的折叠差分。** 对每种叶子策略，用随机生成与手写 fixture 构造多请求、多 ResourceSpans 的 trace，把同一组片段分别送入折叠器和官方 evaluator（输入为冻结时的完整 ReceivedBatches），逐 trace 要求结论值和错误完全相同。必须覆盖：latency 设上限时先满足后超出、开始时间为 0 的 span 和毫秒截断边界；三种属性策略及 trace_state 的 invert_match，Resource 命中与 Span 命中，空串、缺失和类型不匹配；OTTL 的 span 与 event 条件、各 error_mode 下的求值错误；首个有效 rv/th 出现在后续片段；and、not、drop、composite 的嵌套组合。任一叶子出现差异都阻止发布。

**B 的边界用例。** 用可控时钟或屏障分别在截止前、截止后且官方尚未取候选、提交后但决定尚未发布、发布后送入额外 span。选择可翻转 span_count、latency 或 rate_limiting 的输入，正常和恢复各跑一次，逐 trace 对照官方路径。

**B 的接收决定与恢复差分。** 固定目标 tag、默认内存存储、相同缓存和 gate。构造 SizeBytes 恰好等于上限、但 TracesSize 已大于该上限的 trace，要求仍 Pending；下一片段使 SizeBytes 越界时立即 D。越界后再发同 ID 迟到 span，同时在同一 record 中混入应 keep 的另一条 trace。分别在相等时、触发片段 T 后、D 后但 record 结算前及检查点后 kill。逐片段比较两个启用计数，检查只产生一次终局、每个关联只结算一次、没有悬挂 record，其他 trace 完整导出。复测限额为零、缓存命中、缓存淘汰后同 ID 再到达，以及 root 调整到期和普通 tick 相遇的场景。

对 span-ingest 单列四组：pending 后提前 keep、提前 drop、持续 pending 到 tick 清理、配置有状态策略被官方与 fork 同样拒绝。覆盖单请求内同一 trace 跨多个 ResourceSpans、跨请求以及接收时决定前后重启。逐片段核对决定时点、历史与触发片段的输出字段、缓存命中标记和重数。已发布决定的恢复以持久化 K/D 为准；对尚未发布的有状态批预算，冻结候选、阈值与令牌扣减进度须可恢复或等价重放。补测阈值已算但部分候选尚未发布时 kill，防止重复扣预算。

**B 的 bytes_limiting 精确计量差分。** 接收端按官方 splitResourceSpansByTrace 将每个源 ResourceSpans 拆成各 trace 的片段，保留 Resource/Scope 边界、属性与 schema URL。每个片段单独包成 Traces 取 TracesSize，再按 trace 在冻结边界内累计。这样计入每个外层 resource_spans 的字段标签和 varint 长度前缀。直接求和 ResourceSpansSize、使用可选 SizeBytes 指标、按请求长度分摊均不足以复现该口径。正常与 WAL 恢复分别以目标官方 tag 的默认内存存储在冻结时返回的完整 ReceivedBatches 的 TracesSize 为真值，逐 trace 要求整数相等。fixture 覆盖多请求混合 trace、重复 Resource/Scope、schema URL、127/128 与 16383/16384 字节长度前缀边界及冻结后的迟到批次。开关 gate，覆盖顶层 drop、AND 内 OTTL 预筛、同批多个预算策略，固定候选顺序、随机值和预算时钟，逐策略核对合格集合、成本、阈值（启用时）、keep/drop 和实际导出重数。任何一处不同都阻止启用摘要路径。[官方字节成本](https://github.com/open-telemetry/opentelemetry-collector-contrib/blob/v0.165.1/processor/tailsamplingprocessor/internal/sampling/bytes_limiting.go)

**B 的 gate 用例。** 单独配置 rate_limiting 并切换 usetracestate，再测试策略标记及 composite。取官方未裁剪 pipeline 真正导出的数据作为期望，覆盖首次 keep、迟到缓存命中、重启后的迟到命中和同 Scope 混合策略。核对有效 tracestate、`tailsampling.policy`、`tailsampling.cached_decision` 以及适用的 `tailsampling.composite_policy` 的值、所属层级和缺失状态。检查点恢复和 WAL 增量回放分别执行。

**B 的迟到来源差分。** 使用 trace-complete、官方默认内存存储，同时启用 recordPolicy 与 useTracestate。官方与 fork 使用相同的真实 cache_size 配置；默认组显式移除 harness 的 LRU 覆盖。以相同 trace 分别生成 keep 和 NotSampled，决定发布后再发送两个迟到片段，使用非空策略名、可辨认的阈值，并比较官方实际输出中的完整 Scope 属性及 span tracestate。

| 场景 | 核对项 |
|---|---|
| 两侧容量均为 0，活动项仍在 | keep 的迟到片段应用有效阈值，策略名与 cached_decision 标记不被新增；再用预置同名 Scope 属性验证原值保留。NotSampled 每个迟到片段再次触发 hook；分别统计 hook 调用和持久决定，要求本轮只有一条 D，每个关联只结算一次，非采样片段导出数为零。 |
| 两侧容量均大于 0 | keep 迟到片段写入策略名、cached_decision=true 和有效阈值。non-sampled 缓存命中不增加 hook 次数。补测只启用一侧缓存，两类结论各自走对应分支。 |
| 淘汰、清理与重启 | 小容量 LRU 制造访问与淘汰；零缓存且 block_on_overflow=false 时用小 num_traces 触发活动项移除。同 ID 淘汰前继承、两层均失效后重新求值。block_on_overflow=true 在两侧缓存非零时测试等待空间行为，并验证任一侧为零时监听前报错。检查点与增量 WAL 分别在决定后、迟到片段已登记但尚未交付时 kill，以相同输入的连续官方执行为 oracle，逐片段核对来源、输出重数、Scope 属性和阈值。kill 后的 LRU 访问顺序按上文二级标准核对。 |
| 容量移除 | block_on_overflow=false、小 num_traces，使待决 trace 和已决活动项分别被移除，包括已冻结但尚未发布时被移除，并分别在 V 写入前后 kill。被移除的待决实例不导出、不触发 hook、关联全部结算且段可回收；冻结后被移除的候选不再发布；同 ID 后续片段按新实例求值；移除次数与官方一致。 |

**C 的文件资源验收。** 按 24,576 span/s、每请求 128 个 span 打流，在降低 RLIMIT_NOFILE 后人为延长恢复并在接管中再次 kill。若持续积压 60 秒且没有回收，约积累 11,520 个请求文件。验收记录进程 FD 峰值、缓存已打开及在借 FD、借用等待、inbox 和接管段数量、可用 inode 最低值和排空耗时。要求两次恢复都能完成，缓存峰值守住上限，进程保有预留，无 EMFILE 或 inode 耗尽，排空后旧段释放，所有已 ACK 应保留 span 的内容与重数一致。上线前按峰值请求率、目标最长恢复时长和文件系统预留核算 inode 余量。若无法覆盖目标，恢复入口改为按段顺序追加多个请求，再复测。

**D 的折叠开销验收。** 按策略树计量每条待决 trace 的折叠状态字节数，与活跃 trace 数相乘核对 RSS。在接收路径分别测纯状态策略、多条正则属性策略和 OTTL 条件下的每 span CPU，确认折叠没有使接收成为瓶颈；存在型叶子命中后停止匹配的收益单独报告。构造大量候选同时到期、单条超长 trace、共享 record 和整 tick 的限速策略，记录截止到决定的分位数和最大值、ACK 吞吐及 cgroup 峰值，在 512 MiB 下逐档找出接入和求值都能持续排空的吞吐。

上线前由业务确定可接受的 ACK 空窗、决策滞后、持久化故障范围和幂等责任。第一章的实测数字用于回归比较。

## 三、实现状态与测试机实测

分支 `feat/tailspool-fold` 基于 v0.165.1，在官方 tail_sampling 内实现 spool 模式，不再旁挂原型 sampler。

### 已实现

| 模块 | 内容 |
|---|---|
| 折叠（`internal/sampling`） | 读取 span 的叶子策略按片段折叠，latency 显式折叠；`TraceData.Folded` 保存折叠状态，bytes_limiting 用累计的 `TracesSize`。tick 用折叠状态构造候选，决定后释放。 |
| spool（`internal/spool`） | 段文件 + WAL（`[len][crc32c][payload]`，帧 H/T/K/D/V/L/C/W/M）。T 帧按片段登记 `(traceID, ResourceSpans 序号)` 关联；K/D/V 携带水位，水位之后的记录用 L 帧逐关联结算。keep / drop 两侧决定备忘各自按缓存容量（为 0 时按 num_traces）保留，检查点超过阈值时重写。交付由 `max_inflight_records` 个 worker 在锁外回读、过滤、调用下一个 consumer，成功后写 C 并释放段。 |
| processor spool 模式 | 配置 `tail_sampling.spool`。接收时把 tail_sampling 收到的 `ptrace.Traces` 序列化写段、登记 WAL 后即 ACK，记录进入 spool 的待处理队列；主循环按 seq 取记录，逐片段走官方缓存命中、容量移除和折叠，到达时间取登记时刻。恢复在主循环内串行执行：恢复决定备忘，按原到达时间重放未结算片段，保持原截止时间；恢复期间照常接流，新记录排在重放之后。Shutdown 不对待决 trace 求值，留给下次启动恢复。前面可以放任意同步 processor，入口为标准 otlp receiver。 |
| 确定性 | `splitResourceSpansByTrace` 按首次出现顺序输出，tick 批次按 trace ID 排序，使官方与 spool 两种模式可逐 span 对比。 |

### 与第二章设计的差异

- 不存 receiver 收到的原文，改存 tail_sampling 收到的数据（见 `FIX-PLAN.md` 第 1 节），没有专用 receiver 和直连校验。
- 决策 hook 看到的 `ReceivedBatches` 为空；依赖它的扩展未接入。
- 恢复串行，没有 inbox 和 BOOTTIME 处理，墙钟回拨未覆盖。
- 启动时拒绝：recordPolicy / useTracestate gate、span-ingest、tail_storage、多 shard、任一缓存为 0 时的 block_on_overflow、含 `Now(` 的 OTTL 条件；运行时拒绝策略和大小上限的热更新。

### 进程内差分

`spool_e2e_test.go` 以同一输入分别运行官方内存路径和 spool 模式，逐 span 比较输出及重数，各 5 个随机种子：线上配置、容量移除、小缓存与字节上限、富策略（and / drop / not / OTTL / composite，含 first match）。不重启时全部完全一致；中途重启后，无容量压力的用例完全一致，容量压力下 LRU 访问顺序只能近似（即第二章的二级标准），测试只记录差异数。`internal/spool` 另测撕裂尾帧、段回收、提交后崩溃恢复，以及“记录处理中途被移除或因大小上限 drop”后重放与运行时状态一致。全部测试在 `-race` 下通过。

### 测试机实测

测试机 8 vCPU、15 GiB 内存，spool 目录在数据盘 `/data/tailspool-fold`。负载、collector 和计数 sink 同机运行。工具在 `experiments/tailspool/bench`：

- `tsbench load` 生成确定性负载，每请求 128 span（8 代 trace 各 16 条、每条 1 span；单条 trace 共 8 span，相邻 span 间隔 200 个请求），每 span 512 B 属性。约 2% trace 末 span 为 ERROR，约 2.5% trace 有 150 ms span。失败按退避重试到成功。
- `tsbench sink` 按 span 槽位计数；`tsbench report` 对比两次运行的每个 span 槽位。
- 策略为线上配置：`decision_wait: 30s`、`num_traces: 200000`、ERROR / latency 100 ms / 10% 概率。

官方路径在主循环里逐 trace 同步导出，关闭 exporter 队列时 200 req/s 就跟不上，客户端超时重试会污染基线，因此官方基线开启内存 `sending_queue`。spool 模式关闭队列，`ConsumeTraces` 返回即落到 sink，kill 验证的正是 spool 的交付边界。

**无 kill 对比（各 60 秒负载）**

| 负载 | 模式 | 达成 span/s | 峰值 RSS | CPU 秒 | spool 盘峰值 | 导出请求数 | 与官方逐 span 对比 |
|---|---|---|---|---|---|---|---|
| 200 req/s | 官方 | 24,108 | 1.86 GB | 32 | — | 26,602 | 基线，保留 203,302 span |
| 200 req/s | spool | 24,108 | 0.38 GB | 33 | 555 MB | 11,979 | 缺失 0、多出 0、重复 0 |
| 400 req/s | 官方 | 49,707 | 3.63–3.86 GB | 80 | — | 53,351 | 两次官方运行之间相差 3,424 span |
| 400 req/s | spool | 49,701 | 0.60–0.63 GB | 73 | 1,048 MB | 23,979 | 与同轮官方完全一致 |
| 800 req/s | 官方 | 100,905 | 4.00 GB | 117 | — | 27,605 | 基线，保留 211,502 span |
| 800 req/s | spool | 100,907 | 0.61 GB | 115 | 1,064 MB | 12,479 | 缺失 0、多出 0、重复 0 |

400 req/s 时活动 trace 恰在 num_traces 附近，移除哪些 trace 取决于到达与 tick 的相对时机，官方自身两次运行就差 3,424 span；第二轮官方与 spool 完全一致。800 req/s 时待决 trace 持续被容量移除，两种模式输出仍一致。spool 按记录导出，导出请求数约为官方逐 trace 导出的一半以下。

**kill -9 后立即重启（客户端重试未 ACK 的请求）**

| 负载 | kill | sink 延迟 | 缺失 | 多出 | 重复 span（记录） | 重启到可接收 | 恢复耗时（重放片段 / 待决 trace） |
|---|---|---|---|---|---|---|---|
| 200 req/s | 第 20、45 秒 | 0 | 0 | 0 | 0 | 0.12–0.55 s | 1.7 s（42 万 / 6.4 万）、3.1 s（67 万 / 14.4 万） |
| 200 req/s | 每 8 秒共 7 次 | 0 | 0 | 0 | 0 | 0.12–0.66 s | 0.6–3.5 s，最多 74 万片段 |
| 200 req/s | 每 8 秒共 7 次 | 50 ms | 0 | 0 | 844（48） | 同上 | 同上 |
| 800 req/s | 第 20、45 秒 | 0 | 0 | 0 | 7（1） | 0.87–1.30 s | 7.1 s、6.7 s（151 万 / 20 万） |

sink 延迟 50 ms 让交付几乎总在途，用来放大“已交给下游、C 未落盘”的窗口：7 次 kill 共重复 48 条记录，平均每次约 7 条，低于 `max_inflight_records`=16 的上限，每个重复 span 只多一份。被拒的客户端请求（连接拒绝）重试后没有产生重复。恢复期间 receiver 已在监听，新请求排在恢复之后处理。800 req/s 带容量压力的重启这次也逐 span 一致，但该场景只承诺二级近似。

以上为修复前的结果。修复后的复测见 `FIX-PLAN.md` 的“实施状态与复测”。

### 尚未覆盖

- 阶段 A 的轮转失败、短写、WAL 失败注入；锁等待、WAL 与检查点耗时分解；`sync_on_rotate` 开关对照；ACK 尾延迟和决策滞后分位数。
- 阶段 C 的定点 kill（登记、决定、回读、交给 exporter 后、写 C 前、回收时）目前只靠定时 kill 和慢 sink 随机命中；LB 超时重发只用客户端重试模拟；FD / inode 与降低 RLIMIT_NOFILE 的验收；512 MiB cgroup 限制。
- 阶段 D 与 TakeMany Pebble 的等载对照、多 backend、慢盘、大属性 / 正则 / OTTL 的接收侧 CPU。
- 被拒绝的 gate、span-ingest、多 shard 及决策 hook 的完整 `ReceivedBatches`。
