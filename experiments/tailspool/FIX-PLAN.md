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

> 修订：“读失败只可能来自磁盘或外部改动”不成立，嵌套过深的请求能编码却不能解码，见文末“坏记录从哪里来”。解码错误现在三处都立即释放，待处理队首的 I/O 错误重试 5 次后释放；交付环节的 I/O 错误仍按本节处理。

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

## 已确认问题：待处理队首读失败可导致其他 trace 漏采样

**状态：** 已在 Linux 测试机 `10.98.1.5` 上对提交 `9b5b8ca1be5d` 复现。涉及 spool 模式下尚未折叠的请求，故障发生在采样决定之前的回读路径。现有的交付读失败重试发生在采样决定之后，无法补救此处的错误决定。修复方案第 1、2 项已实施，见本节末“实施与复测”。

**预期：** 只配置 `status_code: ERROR`，没有容量淘汰或大小限制时，在决策窗口内成功接收的 ERROR 片段应参与同一 trace 的求值。一旦命中，普通 span 和 ERROR span 都应被保留。普通 span 进入待决状态后，trace 继续保持 `Pending`，直到到期 tick 发布决定。

### 真实时钟复现

使用默认一秒 ticker、`decision_wait: 3s` 和 `max_unprocessed_memory: 1` 字节，强制待处理记录从磁盘回读。用三条独立请求构造固定顺序，A 的两条 span 有相同的 trace ID 和不同的 span ID。

| 登记顺序 | 输入 | 实际处理 |
|---|---|---|
| 1 | Trace A 的普通 span | 写盘后被主循环读取，纳入 A 的待决状态。A 没有发布决定。 |
| 2 | 无关的 Trace B | 用实际登记路径写入等长的非法 protobuf，制造可恢复的解码故障。B 留在待处理队列首位。 |
| 3 | Trace A 的 ERROR span | 在普通 span 到达后 6–19 ms 内写入完好的数据，processor 的 `ConsumeTraces` 返回成功。记录排在 B 后面，尚未纳入 A 的待决状态。 |
| 约 4 秒 | 到期 tick | B 仍在重试，A 的 ERROR span 仍在队列里。A 只用第 1 条记录求值，发布 drop。 |
| 决定发布后 | 修复 B 的数据 | 用原始的合法字节原位恢复 B。队列成功排空，A 的 ERROR span 继承已发布的 drop，A 的两条 span 均未导出。 |

故障组使用真实 ticker 连续执行 3 次，3 次均复现。对照组只把 B 换成正常可读的记录，3 次均各导出 A 的普通 span 和 ERROR span 一次。早期的手动 tick 探针有同样结果，真实时钟用例确认了这并非由手动推进 tick 造成。测试覆盖 processor 写盘返回、调度和下游捕获，未经过实际 OTLP 网络请求。日志和探针保留在测试机：

- `/data/tailspool-0863799-verify/run-9b5b8ca1/out/queue-wallclock-probe.log`
- `/data/tailspool-0863799-verify/run-9b5b8ca1/src/processor/tailsamplingprocessor/verification_wallclock_probe_test.go`

### 为什么一条记录会影响其他 trace

待处理队列按 **OTLP 请求记录的登记顺序** 共享，不按 trace ID 隔离。`Register` 在段文件写入并 Flush 登记 WAL 后即可成功返回，记录稍后才由主循环读入、拆分并折叠。队首读失败时，`processNextSpooled` 安排退避重试并返回，以免推进已经处理完的记录水位。队首留在原位，之后的记录无法开始处理。到期 tick 仍可独立运行，且只读取当前已折叠状态，没有确认窗口内已成功登记的片段都已处理。最终决定发布后，尚未折叠的片段走决定继承路径。

因此，B 的错误只损坏了 B 的测试记录，但阻塞了后续记录的处理。A 的 ERROR 记录本身可读，且在决策窗口内已经成功登记。A 漏采样是因为 tick 在队列处理完之前发布了终局决定。这个问题已在注入读故障时复现，健康对照没有出现。普通积压、恢复重放和实际磁盘故障下的发生频率尚未验证。

代码入口：[写盘后返回](../../processor/tailsamplingprocessor/spool_mode.go#L73) · [共享队首](../../processor/tailsamplingprocessor/internal/spool/spool.go#L347) · [读失败重试](../../processor/tailsamplingprocessor/spool_mode.go#L100) · [独立 tick](../../processor/tailsamplingprocessor/processor.go#L615) · [决定继承](../../processor/tailsamplingprocessor/spool_mode.go#L241)。

### OTLP 实测复核

测试机上用 `9b5b8ca1be5` 构建的 collector（otlp receiver → tail_sampling spool → otlp exporter），只配 `status_code: ERROR`，`decision_wait: 5s`，`max_unprocessed_memory: 1`，真实 ticker。探针 `bench/cmd/headprobe` 经 OTLP gRPC 依次发送 A 的普通 span、200 个填充请求（主循环积压约 0.4 s）、B、A 的 ERROR span，全部 ACK 后在 B 被读到之前清零段文件里 B 的 49 字节。每组 3 次，每次新目录：

| 组 | A 截止后（+8 s） | 最终 | collector 读失败告警 |
|---|---|---|---|
| 对照，B 完好 | 普通 1、ERROR 1 | 同左 | 0 |
| B 在 +8 s 恢复 | 0、0 | 0、0；恢复后下一次重试读通，队列排空，A 的 ERROR 继承 drop | 7 |
| B 不恢复，之后再发 50 条无关 ERROR trace | 0、0 | 50 条全部 ACK，10 s 内导出 0 条 | 8 |

### 坏记录从哪里来

本次运行登记的记录读失败只有三类来源：

1. **数据本身读不回来（确定性，可由客户端触发）。** pdata 的 protobuf 解码有递归上限（`RecursionLimit = 100`），编码没有。属性里 map 嵌套到 33 层时，`ProtoMarshaler` 正常编码，`ProtoUnmarshaler` 返回 `exceeded max recursion depth`；JSON 解码到 120 层都正常。OTLP gRPC 和 HTTP protobuf 在 receiver 解码时就拒绝这类请求，OTLP/HTTP JSON 会接受，之后 spool 把它编码写盘，再也读不回来。管道内的处理器或其他 receiver 构造的深嵌套属性同理。测试机实测（`bench/cmd/poisonprobe`，always_sample）：
   - `max_unprocessed_memory: 1`（相当于有积压）：一条 40 层嵌套的 JSON 请求返回 200，之后 20 条普通请求全部 200，8 s 内导出 0 条，整条管道停住。
   - 默认 64 MiB：坏记录的载荷在内存里，折叠正常，普通请求 20/20 导出；坏记录本身交付时读不回来，本次运行内一直重试，重启后重试 3 次释放，丢失。官方处理器不写盘，不会在这里失败，只是下游 OTLP 接收端同样会拒绝它。
2. **I/O 错误。** 段文件刚写入时通常还在页缓存里；积压大、内存紧张时被回收，回读才落到设备，设备或云盘故障会返回 EIO。频率低，无法预防。
3. **外部改动。** 其他进程或人工截断、改写段文件（文件被删除不影响，已打开的句柄仍可读）。目录锁是 flock，跨主机的网络文件系统上不能防止两个实例共用目录。

代码路径检查过：段文件只在持锁的 `Register` 中追加，偏移取自写入前的大小；排队记录持有段引用，引用归零才删除段；`Stop` 在主循环退出后才关闭文件。没有发现由 spool 自身逻辑写坏或提前删除记录的路径。

### 原版如何处理

- 默认内存存储不序列化，深嵌套 span 正常通过采样，到下一跳 OTLP protobuf 接收端才被拒绝。测试机同一请求：20 条普通请求全部导出，坏 span 导出失败（`Permanent error ... exceeded max recursion depth`），互不影响。
- Pebble 存储（实验特性）同样用 protobuf 编解码，写入成功、读不回来。`on_read_error` 默认 `drop_trace`，`Take` 返回错误，processor 记 Error 后丢掉这一个 trace；`return_partial` 只返回能读的部分。任何读错误都立即处置，不重试，不影响其他 trace。本地实测：深嵌套 trace 被丢，同库另一个 trace 正常读出。

spool 的问题不在于坏数据丢了，而在于对确定性失败无限重试，并让它挡住后续记录。

### 修复方案

参考 Pebble，读不回来的数据直接丢。丢弃粒度是记录：一条记录是一个请求，只丢其中的片段，相关 trace 在其他记录里的片段照常参与决定，相当于 Pebble 的 `return_partial`。

1. **解码错误立即释放。** 解码失败是确定性的。待处理队首、交付、重启后重放三处读到解码错误时，都立即调用 `releaseUnreadableLocked`：关联全部按 drop 结算并提交，记 Error 日志和 `ReleasedUnreadable`，然后继续处理。待处理队首释放时照常推进水位，WAL 语义不变。
2. **待处理队首的 I/O 错误短暂重试后释放。** 队首挡着后续全部记录，退避重试数次（共约 1–2 s）仍失败则同样释放。交付环节的 I/O 错误保持现状：本次运行内重试，重启后有限次重试再释放，一条记录卡住不影响其他记录。（已被文末“读失败统一为可配置重试”取代。）
3. **tick 等待已登记的请求：已实施。** 解码失败立即释放后，原复现场景通过。I/O 有限重试和无故障的回读积压都会让到期 tick 先于窗口内已登记的片段求值，两者都已实测复现。现在到期 trace 若仍有片段在待处理队列中，就扣住到这些片段处理完再求值，其余 trace 照常决策。实现与复测见下文。
4. **写盘前检查嵌套深度：可选，暂不做。** 原版同样接受这类请求，丢弃后已不会阻塞，此项只能让丢失对客户端可见。

### 修复验收

- 深嵌套 JSON 请求：积压和默认内存预算下，之后的普通请求全部正常导出；坏记录被释放一次，不反复重试；重启后不再出现。
- A/B/A 顺序，B 为解码失败：B 立即释放，A 的两条 span 各导出一次。
- 队首持续 I/O 错误：重试数次后释放，后续无关记录恢复正常采样；验证同机 SIGKILL 前后 WAL 水位不会跳过未结算的关联。
- 交付时解码失败立即释放并提交；I/O 错误仍按原逻辑重试。
- 验证 I/O 重试跨过 tick 时的决策一致性，并补测正常积压和重启恢复，验收条件见下文。

### 实施与复测

已实施方案第 1、2 项，第 3 项见下文“tick 等待已登记的请求”，第 4 项未做。

- `readRecord` 把解码失败包装为 `errUndecodable`。`readFailedLocked`（交付）和 `ReadRecovered`（重放）遇到它立即释放，不标记整段不可读。
- `ReadUnprocessed` 改为返回 `(td, released, err)`：解码失败，或累计 5 次读失败（处理器退避 0.1、0.2、0.4、0.8 s，共约 1.5 s），即把记录移出待处理队列、按 drop 结算并提交、推进水位；处理器随即处理下一条。
- 单测：交付、重放、待处理队首三处的解码失败各释放一次且不重试，重启后不再出现；队首连续 I/O 失败在第 5 次释放；processor 层用 40 层嵌套的请求覆盖回读与内存两种路径，A 的两条 span 各导出一次；段文件被截断的队首约 1.5 s 后释放，之后的请求正常采样。新用例在修复前的实现上均失败。

测试机复测（同一 OTLP 探针，修复后的 collector，每组 3 次）：

| 用例 | 修复前 | 修复后 |
|---|---|---|
| A/B/A，B 清零（解码失败），+8 s 恢复 | A 两条 span 均未导出，读失败告警 7 | A 普通 1、ERROR 1；B 释放 1 次，无重试 |
| A/B/A，B 不恢复，之后 50 条无关 ERROR trace | A 未导出，无关 trace 导出 0/50 | A 普通 1、ERROR 1；无关 trace 50/50 |
| 对照 | A 普通 1、ERROR 1 | 同左 |
| 40 层嵌套 JSON，回读路径 | 普通请求 0/20，管道停住 | 20/20，坏记录释放 1 次 |
| 40 层嵌套 JSON，默认内存预算 | 20/20，坏记录交付时反复重试，重启后才释放 | 20/20，交付时释放 1 次，重启后无残留 |

日志在测试机 `/mnt/otelbench/tailspool-fold/probe/{otlp,poison}`（修复前）和 `{otlp-fix,poison-fix}`（修复后）。

### 已确认的剩余问题：队首 I/O 重试跨过决策 tick

在测试机 `10.98.1.5` 上复测提交 `64da25450b4`。真实 OTLP/gRPC 的解码失败对照已通过，A 的普通 span 和 ERROR span 都各导出一次。另用 processor 探针注入段文件读取错误 `EIO`，验证 I/O 有限重试期间的决策行为。

**复现步骤**

1. 使用默认一秒 ticker，配置 `decision_wait: 3s`、`num_traces: 10`，只启用 `status_code: ERROR`。将 `max_unprocessed_memory` 和 `segment_size` 都设为 1 字节，让待处理请求走磁盘回读，并使三个非空请求位于各自的段文件。
2. 在 t=0 发送 Trace A 的普通 span，等它被读入待决状态。此时 A 尚未发布决定。
3. 约 t=2.8 s 时，先发送无关的 Trace B，再发送同一 Trace A 的 ERROR span。只对 B 所在段的读取注入连续 `EIO`。A 的 ERROR 数据保持完好，并在 3 秒窗口内从 `ConsumeTraces` 成功返回。探针直接调用 processor，使用真实时钟。
4. 让默认 tick 和队首重试继续运行，分别记录 A 发布决定、B 被释放、队列排空和 sink 收到的 span 数。

**实测结果（3 次结果一致）**

| 事件 | 相对 t=0 的时间或结果 |
|---|---|
| A 的 ERROR span 成功登记 | 2.801–2.802 s，早于 3 s 窗口 |
| A 发布 drop | 4.002–4.003 s，此时 B 仍在重试，A 的 ERROR span 仍在队列里 |
| B 第 5 次读取失败后释放，随后队列排空 | 4.305–4.306 s |
| A 最终导出 | 普通 span 0，ERROR span 0 |

I/O 错误触发约 1.5 秒的累计退避。有限重试保证队列最终推进，到期 tick 仍能在此期间凭 A 已折叠的普通 span 发布 drop。ERROR span 随后被处理时继承这个决定。相关代码：[队首回读、重试计数与释放](../../processor/tailsamplingprocessor/internal/spool/spool.go#L360) · [到期 tick](../../processor/tailsamplingprocessor/processor.go#L624)。

测试机证据目录为 `/data/tailspool-0863799-verify/run-64da2545`。日志为 `out/io-boundary.log`，隔离副本中的探针为 `src/processor/tailsamplingprocessor/verification_io_boundary_test.go`，注入钩子为同一模块下的 `internal/spool/verification_io_hook.go`。在该模块下运行 `go test -mod=readonly -race -tags verify_spool_io_boundary -count=3 -run '^TestVerificationDecisionDuringBoundedHeadIOFailure$' -v .` 可复现实测。现有探针断言的是缺陷行为，修复时需将断言改为以下预期。

**修复验收**

- 保持上述 A/B/A 时序，确认 ERROR 在窗口内成功登记，且首个到期 tick 发生时 B 仍在重试。A 应保持待决。B 在第 5 次失败后释放一次，A 的 ERROR 纳入求值后只发布一次 keep，A 的两条 span 各导出一次。
- 用同一时序让 B 在用尽重试前恢复，验证 B 可继续处理，A 仍只发布一次 keep、两条 span 各导出一次。加入一条在 tick 前已处理完所有窗口内片段的无关 trace，验证其决定不因 B 推迟。
- 补测无 I/O 故障的积压和重启后继续接流，分别验证窗口内已登记的片段纳入本轮求值，以及在决定发布后到达的片段沿用既有决定。覆盖 `block_on_overflow` 达到容量上限的情况，并在 B 释放前后分别注入同机 SIGKILL，核对 WAL 重放不跳过未结算的关联。

### tick 等待已登记的请求：实施与复测

**根因**：spool 写盘即 ACK，主循环随后才逐条处理，而定时 tick 是同一个 `select` 里的独立分支，只看已处理的折叠状态。原版在数据送入 `workChan` 后才返回，ACK 与决策之间的滞后有上限；spool 解耦后这个上限消失。队首 I/O 重试和普通积压都会让到期 tick 先于窗口内已 ACK 的片段求值，后续片段继承 drop。

**实现：只扣住有片段在排队的 trace**

第一版（提交 `6c22bb518c5`）让整个 tick 等队列：只要队首早于 tick，所有到期 trace 一起推迟，与积压无关的 trace 也被拖住。现改为按 trace 扣住：

- spool 在 `Register` 入队时记录每个 trace 最后一条排队记录的序号（`lastQueued`），记录出队（开始处理或释放）时，若仍是该 trace 的最后一条就删除。表里只有正在排队的 trace，没有积压时为空，`Queued` 直接返回。
- tick 取出到期批次后，用 `Spool.Queued` 找出仍有片段排队的 trace，从批次中移出，连同当时的最后序号放入按序号排序的小顶堆；其余照常决策。
- 每处理或释放一条记录后，弹出序号已被越过（`Spool.Begun`）的 trace 一起决策。等待点在到期时固定，之后新到的片段不会继续延长，持续接收片段的 trace 也不会被无限推迟。
- `waitForSpace` 中嵌套的 tick 不扣住任何 trace，并把已扣住的一并决策，否则主循环会等自己。手动 tick 与定时 tick 同样处理。
- 剩余代价只落在确有片段排队的 trace 上，它们本来就必须等这些片段，与原版让客户端等待处理的语义一致。

**单测**（真实 ticker，2 ms 一次，`decision_wait: 1s`，只采 ERROR；另有一条无关 ERROR trace U，在积压之前已处理完）

| 用例 | 不等待 | 全局等待（第一版） | 按 trace 扣住 |
|---|---|---|---|
| A/B/A，B 所在段截断，5 次读失败后释放 | A 漏采 | A 1/1，U 与 A 一起等 | 10/10：A 1/1，U 先于 A 导出 |
| 同上，B 在第 1 次读失败后恢复 | A 漏采 | 同上 | 10/10 同上 |
| A、20000 条无关请求（回读）、A 的 ERROR | A 漏采 | 同上 | 10/10 同上 |
| `block_on_overflow`、`num_traces: 1`，3 条请求排队 | — | 10/10 | 10/10 全部导出，嵌套 tick 不死锁 |

“U 先于 A 导出”这一断言在第一版上 9/9 失败。processor 模块全量测试与 `-race` 通过。

**测试机 OTLP 复测**（`decision_wait: 3s`，headprobe，每组 3 次；otelcol-fix 为 `64da25450b4`，otelcol-tick 为本修复）

| 用例 | 修复前 | 修复后 |
|---|---|---|
| io：B 所在段截断（`segment_size: 1`、`max_unprocessed_memory: 1`），A 的 ERROR 在 +2.56 s ACK | 3/3 A 普通 0、ERROR 0 | 3/3 A 普通 1、ERROR 1；读失败 4、释放 1 |
| io 对照：不截断 | A 1/1 | A 1/1 |
| 回读积压：`max_unprocessed_memory: 1`，3000 个请求 × 500 span，A 的 ERROR 在 +1.7 s ACK | 3/3 A 1/1 | 3/3 A 1/1 |
| 回读积压：6000 个请求 × 500 span，A 的 ERROR 在 +2.3 s ACK | 3/3 A 普通 0、ERROR 0 | 3/3 A 普通 1、ERROR 1；其中 2 次在 +6 s 尚未导出，+11 s 时已导出 |

无 I/O 故障、只靠回读积压就能稳定复现漏采，第一版全部纠正，但决策随积压整体延后。日志在测试机 `/mnt/otelbench/tailspool-fold/probe/tickfix/`。默认内存预算下的积压（`backlog/`，最多 3000 × 500 span）修复前也未复现，处理速度跟得上接收。

**三种实现对比**（同一 headprobe，加一条在 t=0 发送、之后再无片段的无关 ERROR trace U，记录 sink 首次收到的时间；每组 3 次，括号内为范围）

| 场景 | 实现 | A 导出 | U 导出 |
|---|---|---|---|
| io：B 截断，约 1.5 s 重试后释放 | 不等待（otelcol-fix） | 未导出 | +3.88–3.91 s |
| | 全局等待（otelcol-tick） | +4.89–4.91 s | +4.89–4.92 s |
| | 按 trace 扣住（otelcol-hold） | +4.13–4.16 s | +3.90–3.91 s |
| 回读积压 6000 × 500 span | 不等待 | 未导出 | +3.91–3.97 s |
| | 全局等待 | +6.03–6.36 s | +6.03–6.36 s |
| | 按 trace 扣住 | +6.14–6.24 s | +3.98–4.00 s |

按 trace 扣住后，无关 trace 的决策时间与不等待时相同；A 只等自己排队的片段。io 场景下 A 比全局等待早约 0.75 s，因为全局等待期间错过的 tick 被丢弃，批次整体后移。日志在测试机 `/mnt/otelbench/tailspool-fold/probe/hold/`。

尚未覆盖：重启后继续接流、`block_on_overflow` 达到容量上限时的 OTLP 实测，以及 B 释放前后的 SIGKILL。

### 读失败统一为可配置重试

读盘失败归为磁盘性能或故障问题，应扩容或修复磁盘，spool 不再为它做多层兼容。待处理队首、交付、重启后重放三处统一处理：

- 新增配置 `spool.read_retries`（默认 0，不能为负）。I/O 读失败记 Warn 日志，按 0.1 s 起翻倍、上限 30 s 退避重试，共 `read_retries` 次；用完或未配置时记 Error 日志，把记录的关联全部按 drop 结算并提交。解码失败不受此配置影响，始终立即释放。
- 删除“运行期间一直重试、重启后再试 3 次”的交付规则和“同段其余记录只试一次”的段标记，重启前后的记录不再区分。
- 默认配置下队首读失败立即释放，不会卡住队列；配置了重试时，重试期间到期的 trace 由上节的按 trace 扣住保证不漏采。
- 单测：三处在 `read_retries` 为 0 和非 0 时的读取次数与释放；解码失败在开启重试时也只读一次；processor 层截断段文件后，0 次立即释放、2 次在第 3 次失败后释放，开启重试期间恢复则正常处理。processor 模块全量测试与 `-race` 通过。
