# 修复计划：直连校验、登记 deadline 与恢复期接流、读盘失败

针对 `feat/tailspool-fold` 评审中发现的三个问题。三者都已由探针复现：

| 问题 | 复现 | 后果 |
|---|---|---|
| 直连校验只在运行时比较对象 | 在同一 pdata 对象上原地改属性，processor 接受请求，sink 收到修改前的值 | 策略按修改后的数据求值，交付的却是原文，两者不一致 |
| deadline 在等锁前检查 | 请求在等登记锁时过期，WAL 仍登记成功；端口监听后 ACK 仍会停顿 | 客户端已超时的请求被登记，重试产生重复；恢复期间 ACK 停顿 |
| 读盘失败被当作交付完成 | 注入段文件读失败后 committed=1、`delivered_spans=0`，重启后无待重放记录 | 已登记、应保留的数据永久丢失 |

## 1. 启动前校验直连

### 根因

`consumeSpooled` 只比较 context 中的 `rawrequest.Request.Traces` 与入参是否为同一对象（`spool_mode.go` 第 72–76 行）。原地修改的 processor（attributes、transform、resource 等）不换对象，校验通过；求值看到修改后的数据，交付回读的却是登记的原文。换对象或异步转发的 processor（batch、groupbytrace）要到第一条请求才暴露，做不到监听前报错。

### 为什么不用 `MutatesData`

receiver 的 next 是 pipeline 的 capabilities 节点，其 `MutatesData` 是所有 processor 与 exporter 的或（collector `service/internal/graph/graph.go` 第 346–353 行），没有位置信息。按它判断会误杀放在 tail_sampling 之后的改数据 processor，而这些 processor 无害：spool 每次交付都从段文件重新解码一份新对象，交付成功后不再持有；下游修改后返回失败，重试也会重新回读（`deliver.go` 第 128–133 行）。所以规则只约束 tail_sampling **之前**。

### 方案

v0.165 中，实现 `extensioncapabilities.ConfigSnapshotWatcher` 的 extension 会在 `Pipelines.StartAll` **之前**收到生效配置（collector `service/service.go` 第 242–249 行），可以按位置精确校验。

1. 新增 extension `tailspool_guard`（放在 `receiver/tailspoolreceiver` 模块内，避免新增模块）。`NotifyConfigSnapshot` 读取 `Effective()` 中的 `service.pipelines` 和 `processors`，对 traces pipeline 校验：
   - 每个配置了 `spool` 的 tail_sampling 实例：只出现在一条 pipeline；该 pipeline 的 `processors[0]` 就是它；该 pipeline 的 receiver 全部为 `tailspool`（排除 connector 和其他 receiver）。
   - 每个 `tailspool` receiver：只出现在一条 pipeline（多 pipeline 时 fanout 可能克隆数据给 tail_sampling）；该 pipeline 的第一个 processor 是配置了 spool 的 tail_sampling。
   - tail_sampling 之后的 processor 和 exporter 不受限制。
   - 校验结果按组件 ID 保存；校验失败直接返回错误，Collector 在启动 pipeline 前退出。
2. spool 模式的 tail_sampling 在 `Start` 中通过 `host.GetExtensions()` 找到 guard 并查询自身 ID 的校验结果；找不到 guard 或结果不是“通过”就返回错误。tailspool receiver 在 `Start` 中、`Listen` 之前做同样的查询。pipeline 按逆拓扑序启动，processor 先于 receiver，因此两边都在监听前失败。
3. 运行时保留对象一致性检查作为兜底；若收到没有原文的请求，除了拒绝本请求，还通过 `componentstatus` 上报致命错误，让 Collector 退出，而不是持续拒流。
4. spool 目录加 `flock` 独占锁，防止两个实例（或两个进程）共用同一目录。

需要用户在 `service.extensions` 中加入 `tailspool_guard`，README 与配置校验错误信息里写明。

### 测试

- guard 单测：tail_sampling 前放 attributes / transform / filter / batch / memory_limiter，tailspool receiver 进多条 pipeline，tail_sampling 不在首位，connector 作为 receiver，均报错；tail_sampling 后放 attributes、transform、batch 均通过。
- 组件测试：未配置 guard 时 processor `Start` 报错；构建完整 collector（复用 `bench/cmd/otelcol-tailspool`）验证错误配置在监听前退出、端口未被占用。
- 回归：原地修改探针在新校验下无法启动。

## 2. 在真正登记前检查 deadline，并明确恢复期间的接流行为

### 根因

`consumeSpooled` 先检查 `ctx.Err()`，再取 `ingestMu`，`Register` 后在持锁状态下把批次发送到 `workChan`（`spool_mode.go` 第 77–107 行）。`workChan` 容量只有 CPU 核数（`processor.go` 第 172 行）。主循环在恢复重放（实测 10 万 span/s 下约 7 秒）或慢 tick 时不取通道，持锁请求阻塞在发送上，其余请求排队等 `ingestMu`；等待期间 deadline 过期，但检查早已做完，取得锁后照常登记。于是：

- 客户端已放弃的请求被登记，重试产生重复。测试机 kill 用例未暴露，是因为客户端超时为 30 秒，大于恢复时长。
- 端口在约 1 秒内监听，但恢复期间 ACK 全部停顿。

### 方案（恢复期间继续接流）

1. **ACK 只取决于写盘。** spool 内新增“待处理队列”：`Register` 在 `s.mu` 内分配 seq、写段、追加并刷新 T 帧，把记录追加到队列尾部后立即返回；不再使用 `ingestMu`，也不在 RPC 路径上发送到主循环。
   - 主循环按 seq 从队列取记录，`Begin` / `End` 水位语义不变。spool 模式下 `workChan` 改为容量 1 的唤醒信号。
   - 队列中的记录在内存预算 `max_unprocessed_bytes`（默认 64 MiB）内携带已解码的拆分结果；超出预算只保留磁盘引用，主循环取到时回读解码，与恢复重放共用 `ReadRecord` + 拆分 + 未结算关联的路径。
2. **deadline 检查移到写盘点。** 改为 `Register(ctx, raw, assocs, arrival)`，在取得 `s.mu` 之后、写段之前检查 `ctx.Err()`。可选 `min_deadline_remaining`（默认 0）：剩余时间小于该值时拒绝，进一步缩小“刷盘后、响应到达前过期”的窗口；该窗口无法消除，计入“ACK 丢失”的重复上界。
   - 锁内只有写段、刷 WAL 和轮转；`sync_on_rotate` 的 fsync 也在拿锁后检查之前完成，不会漏判。
   - 检查点重写目前也在 `s.mu` 内，可能让等锁时间达到百毫秒级，一并移到锁外（先冻结快照，再在锁外写文件和改名）。
3. **到达时间用登记时刻。** 主循环处理新记录时传入 T 帧的 arrival，而不是处理时刻，使积压时截止时间不顺延，与恢复重放一致。
4. **显式背压。** 写盘前检查待处理积压（记录数、字节数）与 spool 目录所在盘的可用空间，超过 `max_unprocessed_records` / `max_unprocessed_bytes_on_disk` 或低于 `min_free_disk` 时返回 `UNAVAILABLE`（OTLP 客户端默认重试）。未写盘即拒绝，不产生重复。
5. **恢复期间的行为（定为：继续接流）。**
   - receiver 在 Open 完成后即监听，ACK 延迟等于写盘延迟。
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

`deliver` 回读或解码失败时返回 `true`（`deliver.go` 第 98–106 行），worker 随即写 C 并释放段引用（第 80–86 行），记录与段文件一起被回收。重试中再次回读失败同样返回 `true`（第 130–132 行）。恢复重放读失败时对全部关联写 drop 的 L 帧（`spool_mode.go` 第 253–259 行），同样是永久丢弃。另外 `Open` 发现记录越过段尾时直接返回 `errCorrupt`（`recover.go` 第 274 行），单条坏记录会让 Collector 无法启动。

### 方案

1. **读失败不提交。** `deliver` 改为返回 `(done bool, err error)`。读失败时不写 C、不释放段引用，记录进入延迟重试队列（指数退避，上限 30 秒），不阻塞其他记录；`MaxInflightBytes` 预算在等待期间释放。
2. **区分临时错误与损坏。**
   - IO 错误视为临时，持续重试。
   - 长度越界、解码失败、原文校验和不符视为损坏：记录转为隔离状态，保留段引用（段文件不删），不再自动重试；本次运行内不写任何帧，重启后会再尝试一次。
   - T 帧增加原文 `crc32c`（WAL 版本升到 3，仍可读取版本 2，版本 2 的记录不校验），用于明确识别损坏。
3. **恢复路径同样不丢弃。** 重放读失败时不调用 `Inherit(false)`：先重试，仍失败则隔离该记录，不写 L 帧；同一 trace 的其他片段照常折叠和决定。隔离的关联在 trace 决定后保持未结算，记录留在隔离状态。
4. **Open 不因单条记录失败。** 越过段尾或段文件缺失的记录转为隔离，Open 继续完成。
5. **可观测与处置。** 指标 `quarantined_records`、`quarantined_bytes`、`read_failures`；组件状态上报 `RecoverableError`。隔离记录会一直占用段空间，由运维确认后通过配置 `release_quarantined: true` 重启释放（写 C 并回收），默认关闭。

### 测试

- 为段文件读取加入可注入的 `ReaderAt` 包装，单测覆盖：临时失败若干次后成功，要求不写 C、最终交付、无缺失；持续失败，要求不写 C、段文件保留、重启后仍待交付；内容损坏（改一个字节），要求被 CRC 识别并隔离；重放时读失败，要求不写 L 帧、同 trace 其他片段的输出与官方一致。
- `Open` 遇到被截断的段：要求启动成功、该记录隔离、其他记录正常交付。
- 测试机：在交付中途对一个段文件注入读错误（如 `chmod 000` 后恢复），核对缺失为 0。

## 实施顺序

每个问题一个 commit，按依赖排序：

1. 读盘失败（改动局限在 `internal/spool`，风险最低）。
2. 登记 deadline 与待处理队列（改动 processor 主循环的取数方式，完成后重跑全部 e2e 差分与测试机 kill 用例）。
3. 启动前直连校验（新增 guard extension，更新 bench 配置与 README）。

每步完成后 `-race` 跑 processor、spool、receiver 全部测试；全部完成后在测试机复测 PLAN.md 第三章的用例，并加上 1 秒客户端超时与读错误注入两组，结果回写 PLAN.md。
