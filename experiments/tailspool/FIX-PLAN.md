# 修复计划：存储内容、登记 deadline 与恢复期接流、读盘失败

针对 `feat/tailspool-fold` 评审中发现的三个问题。三者都已由探针复现：

| 问题 | 复现 | 后果 |
|---|---|---|
| 存的是 receiver 收到的原文，求值用的是 tail_sampling 收到的数据 | 在同一 pdata 对象上原地改属性，processor 接受请求，sink 收到修改前的值 | 策略按修改后的数据求值，交付的却是原文，两者不一致 |
| deadline 在等锁前检查 | 请求在等登记锁时过期，WAL 仍登记成功；端口监听后 ACK 仍会停顿 | 客户端已超时的请求被登记，重试产生重复；恢复期间 ACK 停顿 |
| 读盘失败被当作交付完成 | 注入段文件读失败后 committed=1、`delivered_spans=0`，重启后无待重放记录 | 已登记、应保留的数据永久丢失 |

## 1. 改为存 tail_sampling 看到的数据

### 根因

spool 写盘的是 tailspool receiver 收到的原始请求字节，求值用的却是 tail_sampling 收到的 `td`，关联按“(trace ID, ResourceSpans 下标)”在两者之间对位。只要两者不同，结果就错：

- 原地修改（attributes、transform、resource）：求值看到修改后的值，交付回读的是原值，修改在输出中消失（探针复现的就是这种）。
- 删除 span（filter）：原文里仍有这些 span，交付时按 trace 过滤会把它们带回来。
- 删除整个 Resource：下标错位，交付错误的 span、漏掉应保留的。

`consumeSpooled` 只比较两者是否为同一对象（`spool_mode.go` 第 72–76 行），挡不住原地修改。之前考虑的“启动前校验 tail_sampling 必须是第一个 processor”只能靠新增 extension 读取配置实现，并且仍需专用 receiver。

### 方案

存、取都在 tail_sampling 内完成：登记时把收到的 `td` 序列化后写段，交付时回读的就是求值所用的同一份数据。

1. `consumeSpooled` 在拆分关联之后、登记之前，用 `ptrace.ProtoMarshaler.MarshalTraces(td)` 得到写盘字节（与 OTLP 请求体线格式兼容，回读仍用 `UnmarshalTraces`）。序列化在 `ConsumeTraces` 调用方的 goroutine、锁外完成，多个请求可并行。tail_sampling 声明 `MutatesData: true`，Collector 保证它拿到的 `td` 归自己所有，序列化期间不会被并发修改。
2. 删除 `rawrequest` 包、`tailspoolreceiver` 模块和对象一致性检查；spool 模式改用标准 `otlp` receiver，gRPC 与 HTTP 都可用。WAL 与段格式不变。
3. tail_sampling 前面可以放任意**同步**的 processor（attributes、transform、filter、resource、memory_limiter 等）；它们的结果会体现在求值和输出里，与官方内存路径一致。tail_sampling 之后不受限制。
4. 仍然不能放在前面的是**异步** processor（batch、groupbytrace 等）：它们先向上游返回成功，之后才把数据交给 tail_sampling，ACK 早于写盘，kill 时丢失缓冲中的数据。这与官方内存路径前面放 batch 的风险相同，不是 spool 引入的；在 README 和配置说明中写明，不做启动校验。
5. deadline：标准 otlp receiver 会把 gRPC deadline 放在 context 里向下传递，第二节的写盘点检查照常生效。processor 返回的 gRPC status 错误由 otlp receiver 原样返回，背压用 `codes.Unavailable`，客户端默认重试。
6. spool 目录加 `flock` 独占锁，防止两个实例（或两个进程）共用同一目录。

### 代价

每个请求多一次序列化，CPU 与反序列化同量级。在测试机按 PLAN.md 第三章 200 / 400 / 800 req/s 用例，对比原文方案的 CPU 秒、峰值 RSS 和 ACK 延迟，结果回写 PLAN.md。

### 测试

- e2e 差分改为不经 `rawrequest`，直接调用 `ConsumeTraces`，原有用例全部保持逐 span 一致。
- 新增：tail_sampling 前放 attributes processor（改属性）和 filter processor（删部分 span、删整个 Resource），要求 spool 模式输出与同配置官方内存路径逐 span 一致，sink 收到修改后的值、被删 span 不出现。
- bench 配置改用 otlp receiver，删去 `otelcol-tailspool` 中的 tailspool receiver。

## 2. 在真正登记前检查 deadline，并明确恢复期间的接流行为

### 根因

`consumeSpooled` 先检查 `ctx.Err()`，再取 `ingestMu`，`Register` 后在持锁状态下把批次发送到 `workChan`（`spool_mode.go` 第 77–107 行）。`workChan` 容量只有 CPU 核数（`processor.go` 第 172 行）。主循环在恢复重放（实测 10 万 span/s 下约 7 秒）或慢 tick 时不取通道，持锁请求阻塞在发送上，其余请求排队等 `ingestMu`；等待期间 deadline 过期，但检查早已做完，取得锁后照常登记。于是：

- 客户端已放弃的请求被登记，重试产生重复。测试机 kill 用例未暴露，是因为客户端超时为 30 秒，大于恢复时长。
- 端口在约 1 秒内监听，但恢复期间 ACK 全部停顿。

### 方案（恢复期间继续接流）

1. **ACK 只取决于写盘。** spool 内新增“待处理队列”：`Register` 在 `s.mu` 内分配 seq、写段、追加并刷新 T 帧，把记录追加到队列尾部后立即返回；不再使用 `ingestMu`，也不在 RPC 路径上发送到主循环。
   - 主循环按 seq 从队列取记录，`Begin` / `End` 水位语义不变。spool 模式下 `workChan` 改为容量 1 的唤醒信号。
   - 队列中的记录在内存预算 `max_unprocessed_bytes`（默认 64 MiB，按序列化字节计）内携带已解码的拆分结果；超出预算只保留磁盘引用，主循环取到时回读解码，与恢复重放共用 `ReadRecord` + 拆分 + 未结算关联的路径。
   - 主循环回读溢出记录失败时，按第三节“运行期间读失败从不丢数据”的原则原地退避重试，不跳过该记录（跳过会让 `End` 把水位推过仍未结算的关联）；其后的记录随之等待，积压由第 4 条的背压兜底。
   - Shutdown 时队列中未处理的记录留在盘上，下次启动按重放处理。
2. **deadline 检查移到写盘点。** 改为 `Register(ctx, data, assocs, arrival)`，在取得 `s.mu` 并完成必要的段轮转之后、写段之前检查 `ctx.Err()`。可选 `min_deadline_remaining`（默认 0）：剩余时间小于该值时拒绝，进一步缩小“刷盘后、响应到达前过期”的窗口；该窗口无法消除，计入“ACK 丢失”的重复上界。
   - 锁内只保留写段、刷 WAL 和切换段文件。`sync_on_rotate` 对旧段的 fsync 移到后台 goroutine：旧段切换后不再写入，回收前等待其 fsync 完成即可。
   - 检查点重写目前在 `s.mu` 内同步执行（`recover.go` 第 331–388 行），会写出全部活跃记录与决定备忘。800 req/s 时活跃记录约 2.4 万条、每条 T 帧约 2.6 KB，快照约 60 MB，锁内停顿可达百毫秒级。改为：锁内冻结快照内容并开始把新帧同时记入内存尾缓冲；锁外写快照文件；再回到锁内追加尾缓冲、改名并切换日志。
   - 触发阈值改为“日志超过 `max(checkpoint_size, 2 × 上次快照大小)`”。现在快照本身接近或超过 64 MiB 时，每次登记都会再次触发检查点，形成反复重写。
3. **到达时间用登记时刻。** 主循环处理新记录时传入 T 帧的 arrival，而不是处理时刻，使积压时截止时间不顺延，与恢复重放一致。官方路径在积压时以主循环处理时刻为到达时间，所以有积压时 spool 的决定时点等价于官方在“没有积压”时的时点，而不是官方在同样积压下的时点。
4. **显式背压。** 写盘前检查待处理积压（记录数、字节数）与 spool 目录所在盘的可用空间，超过 `max_unprocessed_records` / `max_unprocessed_bytes_on_disk` 或低于 `min_free_disk` 时返回 `UNAVAILABLE`（OTLP 客户端默认重试）。未写盘即拒绝，不产生重复。
5. **恢复期间的行为（定为：继续接流）。**
   - processor `Start` 完成 Open 后返回，otlp receiver 随后监听，ACK 延迟等于写盘延迟。
   - 主循环先处理重放记录，再按 seq 处理恢复期间新登记的记录，顺序与未 kill 时一致。
   - 新 trace 的截止时间按各自到达时间计算；重放结束时已过期的 trace 在下一个 tick 决定，决策滞后通过指标暴露。
   - 恢复期间新记录同样受背压上限约束；上限按“目标恢复时长 × 峰值请求率”配置。
6. **指标。** 登记耗时与等锁耗时、待处理积压（记录 / 字节 / 盘上回读次数）、因 deadline 拒绝次数、因背压拒绝次数、恢复各阶段耗时。

### 测试

- 单测：`Register` 在等 `s.mu` 或段轮转期间过期则不写段、不写 T；主循环阻塞时 `consumeSpooled` 仍在写盘延迟内返回；超出内存预算的记录从盘上回读，输出与内存路径一致；溢出记录回读失败时主循环重试且不推进水位；背压拒绝不产生 T；检查点在锁外写出期间登记的帧全部进入新日志，kill 后重放结果与运行时一致；快照大于阈值时不会反复重写。
- e2e 差分：spool 主循环被人为阻塞数秒（模拟恢复和慢 tick），与**未阻塞**的官方路径逐 span 一致（见第 3 条的到达时间语义）。
- 测试机：客户端超时设为 1 秒，在 800 req/s 下 kill 并恢复，要求 ACK 最长停顿接近写盘延迟、`registered after deadline` 为 0、重复不超过在途上限；记录恢复期间的积压峰值和决策滞后。

## 3. 读盘失败时保留可恢复记录

### 根因

`deliver` 回读或解码失败时立即返回 `true`（`deliver.go` 第 98–106 行），worker 随即写 C 并释放段引用（第 80–86 行），记录与段文件一起被回收。重试中再次回读失败同样返回 `true`（第 130–132 行）。恢复重放读失败时对全部关联写 drop 的 L 帧（`spool_mode.go` 第 253–259 行），同样是永久丢弃。另外 `Open` 发现记录越过段尾时直接返回 `errCorrupt`（`recover.go` 第 274 行），单条坏记录会让 Collector 无法启动。

### 方案

原则：**运行期间读失败从不丢数据；重启后再试一次，仍读不出的坏记录才释放。** 可靠性范围只承诺进程 kill，kill 不会损坏已写入内核的数据，读失败只可能来自磁盘或文件系统故障、外部改动文件，因此不加原文 CRC、不升级 WAL 版本，也不引入长期隔离状态。

1. **运行期间读失败不提交。** `deliver` 改为返回 `(done bool, err error)`。回读或解码失败时不写 C、不释放段引用，记录进入延迟重试队列（指数退避，上限 30 秒），不阻塞其他记录，`MaxInflightBytes` 预算在等待期间释放。本次运行内一直重试，不区分错误类型。
2. **重启后再试一次，仍失败就释放。** 上次运行留下的记录带“已跨重启”标记：
   - 待交付记录：交付 worker 回读失败时做有限次重试（3 次，间隔 1 秒，跨过短暂故障），仍失败则写 C、释放段引用。
   - 重放记录：`restoreSpool` 回读失败时同样有限次重试，仍失败则对该记录未结算的关联写 drop 的 L 帧并提交；同一 trace 的其他片段照常折叠和决定（读不出的片段无法参与求值）。
   - 重试预算按段计算：一个段的记录在有限次重试后仍失败，同段其余记录各只读一次，失败即释放，避免整段损坏时每条记录都等 3 秒、把串行的恢复重放拖长。交付 worker 的重试由定时器触发、不占用 worker，多条记录并行等待，总耗时约为一轮重试间隔，同段记录可能各自读满 3 次。
   - Open 时记录越过段尾或段文件缺失：不可能是临时错误，直接释放，Open 继续完成，不再因单条记录返回 `errCorrupt`。
   - 本次运行中新登记的记录不受此规则影响，读失败时仍按第 1 条一直重试，最迟在下次重启时释放，所以坏记录占用的段空间最多保留到下次重启。
3. **可观测。** 指标 `read_failures`（运行期间失败次数）、`read_retry_records`（等待重试的记录数）、`released_unreadable_records` / `released_unreadable_bytes`（重启后释放）。释放时按记录打 error 日志，带 seq、段号、偏移、长度、关联数和 keep 数，便于评估丢失范围；存在等待重试的记录时组件状态上报 `RecoverableError`。

### 测试

- 为段文件读取加入可注入的 `ReaderAt` 包装，单测覆盖：
  - 运行期间临时失败若干次后成功：不写 C、最终交付、无缺失。
  - 运行期间持续失败：不写 C、段文件保留；重启后读取恢复正常则交付，仍失败则在有限次重试后写 C、段被回收、释放计数加 1。
  - 重放时读失败：有限次重试后写 L 并提交，同一 trace 其他片段的输出与官方去掉该片段后的结果一致。
- `Open` 遇到被截断的段或缺失的段文件：启动成功、该记录被释放、其他记录正常交付。
- 测试机：在交付中途对一个段文件注入读错误（如 `chmod 000`，一段时间后恢复），核对缺失为 0；持续不恢复并重启，核对只释放该记录、其他数据完整。

## 实施顺序

每个问题一个 commit，按依赖排序：

1. 读盘失败（主要在 `internal/spool`，另改 `spool_mode.go` 的 `restoreSpool`，风险最低）。
2. 改为存 tail_sampling 看到的数据，删除 tailspool receiver 与 `rawrequest`（简化登记入口，为下一步铺路）。
3. 登记 deadline 与待处理队列（改动 processor 主循环的取数方式）。

每步完成后 `-race` 跑 processor、spool 全部测试；全部完成后在测试机复测 PLAN.md 第三章的用例，并加上序列化开销对比、1 秒客户端超时、读错误注入三组，结果回写 PLAN.md。

## 实施状态与复测

三步已按上述顺序提交：读盘失败 `e9b36427ea2`，存 tail_sampling 看到的数据 `0223f594381`，登记 deadline 与待处理队列 `eb8ed98cdca`。processor 模块全部测试在 `-race` 下通过。

### 与方案的差异

- 指标只进了 spool 的 `Stats`（`ReadFailures`、`RetryingRecords`、`ReleasedUnreadable*`、`RejectedExpired`、`RejectedBackpressure`、`Unprocessed*`），没有导出为 Collector 指标，也没有上报 `RecoverableError` 组件状态；登记等锁耗时没有统计。
- 配置名：内存预算为 `max_unprocessed_memory`（默认 64 MiB），盘上积压上限为 `max_unprocessed_records` / `max_unprocessed_bytes`，另有 `min_free_disk`、`min_deadline_remaining`；后四项默认关闭。背压返回 `spool.ErrBackpressure`，otlp receiver 映射为 `Unavailable`。
- 主循环的唤醒信号由 spool 提供（`Spool.Wake`），`workChan` 在 spool 模式下只用于通知关闭。溢出记录回读失败时主循环记下重试时间并用定时器唤醒，期间 tick 照常执行。
- 不允许放在前面的异步 processor 只写进了 `Config.Spool` 的说明；README 目前没有 spool 章节。
- 第 1 节的“前置 attributes / filter processor”用 processor 包内的单测代替：同一对象原地改属性并删掉一个 Resource 后交给 spool，sink 收到修改后的值、被删 span 不出现。第 2 节的“主循环阻塞数秒仍与未阻塞官方逐 span 一致”用单测代替：记录排队 2.1 秒后，其 trace 按登记时刻少等两个批次；手动 tick 的差分框架里批次按 tick 计，阻塞时长无法与官方对齐。
- 读错误注入只在单测中做（注入 `readAt` 与截断段文件），测试机未做。

### 测试机复测

测试机重置后数据盘 `vdb` 未挂载，本次挂在 `/mnt/otelbench`（与第三章同一块盘），负载与工具同第三章。同一轮里用修复前的二进制（存原文，`bin-raw`）做对照，tsbench 新增成功请求的 ACK 延迟分位数和按状态码统计的失败次数。基线为第三章保存的官方运行结果。

| 用例 | 版本 | CPU 秒 | 峰值 RSS | spool 盘峰值 | ACK p50 / p99 / p999 / 最大 | 与官方逐 span 对比 |
|---|---|---|---|---|---|---|
| 200 req/s | 修复后 | 48 | 372 MB | 558 MB | 0.9 / 12 / 250 / 336 ms | 完全一致 |
| 200 req/s | 修复前 | 48 | 366 MB | 543 MB | 0.8 / 50 / 107 / 141 ms | 完全一致 |
| 400 req/s | 修复后 | 91 | 629 MB | 1,051 MB | 0.8 / 11 / 121 / 386 ms | 见下文 |
| 800 req/s | 修复后 | 142 | 603 MB | 1,064 MB | 0.9 / 12 / 118 / 203 ms | 完全一致 |
| 800 req/s | 修复前 | 129 | 617 MB | 1,064 MB | 0.8 / 64 / 73 / 82 ms | 完全一致 |

- 序列化开销：800 req/s 时 CPU 多约 10%，200 req/s 时持平；RSS 与盘占用基本不变。本轮 CPU 秒整体高于第三章（200 req/s 修复前 48 对 34），只做同轮对比。
- ACK 延迟：开 `sync_on_rotate` 时，修复前每次轮转在锁内 fsync，p99 为 50–64 ms；修复后 fsync 移到后台，p99 降到 7–12 ms。100–300 ms 的尾部两版都有，且与 fsync 无关：200 req/s 下另跑三组，修复后开 / 关 fsync 的最大值为 225 / 263 ms，修复前关 fsync 为 224 ms。负载、collector、sink 同机运行，尾部主要来自环境，没有进一步分解。
- 400 req/s：活动 trace 处在 num_traces 附近，修复后比第二次官方运行少 696 span，比第一次官方运行多 3,424 span，两次官方运行之间本身差 3,424 span，属于第三章所述的容量边界不确定性。

**kill -9 后立即重启**

| 用例 | 版本 | 客户端失败（按状态码） | 缺失 | 多出 | 重复 span / 份数（记录） | 重启到可接收 |
|---|---|---|---|---|---|---|
| 800 req/s，客户端超时 1 s，第 20、45 秒 kill | 修复后 | Unavailable 144 | 0 | 0 | 0 | 0.89 / 1.44 s |
| 同上 | 修复前 | DeadlineExceeded 128、Unavailable 128 | 0 | 0 | 114 / 454（16） | 0.78 / 1.20 s |
| 200 req/s，sink 延迟 50 ms，每 8 秒共 7 次 | 修复后 | Unavailable 448 | 0 | 0 | 747 / 747（48） | 0.12–0.79 s |

- 1 秒超时下，修复前恢复期间 ACK 停顿，客户端超时 128 次，已登记的请求被重发，16 条记录重复、部分 span 送出 4 份；修复后恢复期间照常 ACK（全程最大 90 ms），没有超时，也没有重复。失败只发生在进程重启、端口未监听的窗口内。
- 修复后恢复期间新请求先落盘排队，800 req/s 的 kill 用例 spool 盘峰值为 1,914 MB（修复前 1,075 MB），峰值 RSS 820 MB（修复前 612 MB），差额为恢复期间积压的记录与其内存中的拆分结果（受 `max_unprocessed_memory` 约束）。
- 慢 sink 的 7 次 kill 共重复 48 条记录，与第三章相同，在每次 kill 不超过 `max_inflight_records`=16 的上限之内。
