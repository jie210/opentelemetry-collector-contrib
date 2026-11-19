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
   - 队列中的记录在内存预算 `max_unprocessed_bytes`（默认 64 MiB）内携带已解码的拆分结果；超出预算只保留磁盘引用，主循环取到时回读解码，与恢复重放共用 `ReadRecord` + 拆分 + 未结算关联的路径。
2. **deadline 检查移到写盘点。** 改为 `Register(ctx, data, assocs, arrival)`，在取得 `s.mu` 之后、写段之前检查 `ctx.Err()`。可选 `min_deadline_remaining`（默认 0）：剩余时间小于该值时拒绝，进一步缩小“刷盘后、响应到达前过期”的窗口；该窗口无法消除，计入“ACK 丢失”的重复上界。
   - 锁内只有写段、刷 WAL 和轮转；`sync_on_rotate` 的 fsync 也在拿锁后检查之前完成，不会漏判。
   - 检查点重写目前也在 `s.mu` 内，可能让等锁时间达到百毫秒级，一并移到锁外（先冻结快照，再在锁外写文件和改名）。
3. **到达时间用登记时刻。** 主循环处理新记录时传入 T 帧的 arrival，而不是处理时刻，使积压时截止时间不顺延，与恢复重放一致。
4. **显式背压。** 写盘前检查待处理积压（记录数、字节数）与 spool 目录所在盘的可用空间，超过 `max_unprocessed_records` / `max_unprocessed_bytes_on_disk` 或低于 `min_free_disk` 时返回 `UNAVAILABLE`（OTLP 客户端默认重试）。未写盘即拒绝，不产生重复。
5. **恢复期间的行为（定为：继续接流）。**
   - processor `Start` 完成 Open 后返回，otlp receiver 随后监听，ACK 延迟等于写盘延迟。
   - 主循环先处理重放记录，再按 seq 处理恢复期间新登记的记录，顺序与未 kill 时一致。
   - 新 trace 的截止时间按各自到达时间计算；重放结束时已过期的 trace 在下一个 tick 决定，决策滞后通过指标暴露。
   - 恢复期间新记录同样受背压上限约束；上限按“目标恢复时长 × 峰值请求率”配置。
6. **指标。** 登记耗时与等锁耗时、待处理积压（记录 / 字节 / 盘上回读次数）、因 deadline 拒绝次数、因背压拒绝次数、恢复各阶段耗时。

### 测试

- 单测：`Register` 在等 `s.mu` 期间过期则不写段、不写 T；主循环阻塞时 `consumeSpooled` 仍在写盘延迟内返回；超出内存预算的记录从盘上回读，输出与内存路径一致；背压拒绝不产生 T。
- e2e 差分：主循环被人为阻塞数秒（模拟恢复和慢 tick），逐 span 输出与官方一致。
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

1. 读盘失败（改动局限在 `internal/spool`，风险最低）。
2. 改为存 tail_sampling 看到的数据，删除 tailspool receiver 与 `rawrequest`（简化登记入口，为下一步铺路）。
3. 登记 deadline 与待处理队列（改动 processor 主循环的取数方式）。

每步完成后 `-race` 跑 processor、spool 全部测试；全部完成后在测试机复测 PLAN.md 第三章的用例，并加上序列化开销对比、1 秒客户端超时、读错误注入三组，结果回写 PLAN.md。
