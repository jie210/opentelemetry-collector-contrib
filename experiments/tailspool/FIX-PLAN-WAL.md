# 修复计划：WAL 故障后终止运行与重复交付上限

状态：已在工作区实施。修复前基线为 `d16944501bc305e1fdf3da562ddb347e4079d70a`。第 1、4、5 节保留 2026-11-20 修复前的测试机记录；第 2、3 节规定运行和验收行为，第 6 节记录修复后的实测结果。

本计划修复 WAL 故障后继续交付，以及段文件错误导致健康 WAL 被静默跳写的问题。配置变更后按新策略处理未决数据属于预期行为；FD 和等待请求内存的探针结果保留为观察，不列入本次修复范围。

## 1. 修复前已复现的问题

WAL 写失败后，spool 只停止新登记，已有决定、关联结算和交付仍继续，内存状态与可恢复日志逐渐分离。`max_inflight_records` 限制了同时交付的记录数，却无法限制故障后累计的“下游已收到、完成标记未记录”的记录数。

探针先登记 24 条 keep 记录并 Flush T/K，再关闭 WAL 文件描述符，使后续完成标记 C 的 Flush 失败；段文件和下游保持正常。配置 `max_inflight_records=1`，在 `-race -count=3` 下三次均得到：

| 项目 | 实测 |
|---|---:|
| 成功登记记录 | 24 |
| 配置的在途交付记录上限 | 1 |
| WAL 故障后、重开前成功交付记录 | 24 |
| 重开 spool 后再次交付记录 | 24 |

探针分别读取首次 Stop 前和重新 Open 后的 Delivered 计数：首次 24 条中，第 1 条成功交付触发 C Flush 错误，其余 23 条在错误已被发现后仍继续交付。恢复后又交付 24 条。该探针通过关闭文件描述符注入写失败，并在同一测试进程中关闭、重开 spool；它不是物理磁盘故障或实际 SIGKILL 测试。

另在同一版本的测试机隔离源码上保持 WAL 健康，登记一条已决定 keep 和一条未决记录，各占一个段。第三次登记注入段 WriteAt 错误，然后启动 worker。三次 `-race` 均观察到 1 条已交付、重开前 WAL 中 0 条 C、已交付记录的段被删除。Open 将该记录计入 ReleasedUnreadable；仍有引用的未决段保留，其记录进入 Refeed。这个用例证明可产生悬挂登记。探针源码、overlay 及原始日志在测试机 `out/segment-recheck-ywE3R3/`。此前关闭 WAL 描述符的 24 条重放是另一种故障路径。

修复前的实现：

- [`spool.go`](../../processor/tailsamplingprocessor/internal/spool/spool.go)：`fail` 只保存首个 `s.err`；`Register` 拒绝后续登记；`appendLocked` 在错误状态下直接跳过，`Decide`、`Inherit`、`Evict` 等调用方仍继续修改内存。
- [`deliver.go`](../../processor/tailsamplingprocessor/internal/spool/deliver.go)：worker 只检查 `stopping`，不检查 `s.err`；C 写入或 Flush 失败后仍调用 `releaseLocked`，然后领取下一条记录。
- [`recover.go`](../../processor/tailsamplingprocessor/internal/spool/recover.go)：删段前再次 Flush；本探针中 Flush 持续失败，段保留，但缺少 C，重开后重新交付。

## 2. 修复方向与范围

采用与 Pebble 相同的致命持久化错误处理原则：**运行期间发现 WAL 追加或 Flush 失败，记录致命日志并终止 Collector 进程，不继续推进决定和交付。** 保留 spool 的既有重启恢复逻辑。

恢复使用重启后的当前配置：未决 trace 从磁盘原文重建折叠状态，按新策略求值；已经记录终局的关联沿用 WAL 中的 keep/drop 继续结算和交付。无需保存历史采样配置，也不因采样策略变更而拒绝启动。这是本计划的恢复契约，替代旧设计中“未决数据必须沿用历史策略”的要求；相关旧文档的整理不属于本次修改。

参考项目锁定的 [Pebble v2.1.7 写入和提交实现](https://github.com/cockroachdb/pebble/blob/v2.1.7/db.go)：WAL `WriteRecord` 错误触发 panic，提交管线错误调用 `Logger.Fatalf`，WAL 轮转关闭或创建失败也触发 panic。官方扩展使用 `NoSync`，后台错误的发现存在滞后，不能解读为磁盘写失败的瞬间立即退出。

[官方扩展](https://github.com/open-telemetry/opentelemetry-collector-contrib/tree/main/extension/tailstorage/pebbletailstorageextension) 对普通 Append 错误会记日志并继续，且启动时清空历史待决数据；本方案只采用其致命错误处理原则，不采用清空数据的恢复语义。

### 故障分类

| 场景 | 目标行为 |
|---|---|
| 运行时活 WAL | T 的 append/flush、`appendLocked` 的追加、`Spool.Flush`、worker 成功交付后的 C Flush、`releaseUnreadableLocked` 的 C Flush、删段前 Flush 出错，均在持有 `s.mu` 时同步非零退出 |
| 运行时 checkpoint | `writeSnapshot`（含临时文件创建、写、Flush）在锁外执行，出错后取得锁即终止；持锁的 `appendEncoded(tail)`（含 Flush）或 rename 出错就地终止。运行时 K/D/V 的 seq 携带水位，独立 W 帧仅由快照写入 |
| 启动时末尾半帧 | 保留 `readLog` 返回最后完整帧边界及 `Open` 截断尾部的行为，包括帧头或 payload 未写完 |
| 启动时其他错误 | WAL 打开失败、完整帧 CRC 错误、解码失败、未知版本及初始化失败均由 Open 返回错误。启动恢复和初始 checkpoint 共用部分运行时函数，必须区分启动调用路径 |
| 运行时段写入、轮转与同步错误 | 选择持锁同步退出。段 WriteAt 错误或短写、后台段 Sync 错误，均在锁外 I/O 返回并重新取得 s.mu 后就地终止；持锁的 rotate 返回错误时就地终止。后台 Sync 没有对应的 Register 可以接收错误，且该段可能已有成功登记。启动时打开段仍由 Open 处理 |
| 接收与下游错误 | 取消、deadline、队列背压、磁盘余量不足维持原处理；下游错误保留重试，段读取或解码失败维持现有读取与释放规则 |

### 实施要求

1. 运行时观察到上述致命 WAL、checkpoint 或段文件错误后，在持有 `s.mu` 时记录操作、路径、原始错误及适用的 seq，同步终止进程。生产路径直接调用 os.Exit(非零退出码)，不使用 panic 展开栈，以免 defer 解锁、执行 wroteLocked 或推进段回收。运行时不能留下 s.err 已设置、appendLocked 静默跳过而 worker 仍继续的状态。工作线程写 C 失败须发生在释放该记录之前；禁止错误后继续领取记录、删除段或推进 checkpoint 安装。rename 本身失败时在安装函数内终止。
2. 不委托 Collector Shutdown。Stop 要等待正在交付的 worker 完成，并执行最终 Flush；这与冻结故障现场的语义不同。正常 Stop 的最终 Flush 仍按关闭路径返回错误。
3. 全 drop 的 C 在 `finishLocked` 只追加。若追加失败，在调用 `releaseLocked` 前终止。若 C 已入缓冲，release 可能已更新引用，甚至已进入 `removeSegmentLocked` 才发现 Flush 失败；此时须在移除段映射、关闭和删除段文件前终止。后续 `Spool.Flush` 才发现失败的情形同样以实际回放前缀恢复。不得以 bufio 会重复返回旧错误作为保护条件。
4. checkpoint 活跃时，append 先复制整帧到 tail 再写活日志。修复后活日志写失败的线程持锁退出，等待安装的 checkpoint 无法取得锁。安装边界仍为同目录 rename，不增加目录 fsync。
5. 使用受控子进程验证生产退出路径，测试钩子仅控制时序和写入结果。以进程退出码、盘上文件和独立 sink 做断言。

本轮恢复承诺仍限定在原文和 WAL 可完整回放的故障范围。修复不增加掉电持久化保证，也不保证物理损坏的磁盘数据能够恢复。LB/客户端重发与下游失败重试可能造成的重复须独立统计。

## 3. 修复验收

以下为验收条件。每次故障退出后、再次 Open 前先保存活 WAL、临时文件和段清单。注入方式包含关闭描述符（零字节写入），以及底层写出指定帧的一部分后返回错误。1 MiB 缓冲可能把先前的完整帧与当前帧一起写出。按实际文件从头连续读取、校验并解码到最后一个完整帧，以该前缀的回放状态作为期望；完整帧校验错误另走启动失败用例。

每次退出按不同 record seq 计数。对已成功返回的 ConsumeTraces，统计其中按回放状态仍待交付的记录，数量应 ≤ max_inflight_records。在固定 T/K 且无 checkpoint 的用例里，这等价于成功返回且前缀中无 C。checkpoint 会省略已完成记录，不能在新日志中仅凭缺少字面 C 判定重复。另用独立 sink 按身份及重数核对实际重复；尚未返回且 sink 未收到的调用，恢复后首次出现算补齐。sink 已收到但响应尚未返回是另一种可能，单列核对；不能只用进程内 Delivered 计数推断外部副作用。

| 用例 | 注入与核对 | 通过条件 |
|---|---|---|
| 原 24 条记录、上限 1 | 预先 Flush T/K，第一条返回成功后让 C 零字节或半帧写失败 | 非零退出，所有已确定 keep 的身份最终齐全，重复记录 ≤ 1；明确第一条 C 不完整时该条重放 |
| 多 worker | 上限 1、2、16，预先固定 keep，用屏障区分成功返回、sink 尚未接收、已接收尚未返回 | 逐次核对成功返回且仍待交付的 seq 数和 sink 实际重复记录数，分别 ≤ 上限；首次补齐单列，客户端重发和下游错误重试隔离 |
| T 追加/Flush 失败 | 先成功登记对照请求，再写原文并在本次 T 写入时退出 | 本次 RPC 未返回，以完整可回放 T 判定登记，只有原文字节不构成承诺；此前已返回成功的请求均可按其登记及后续终局对账 |
| 段错误且活 WAL 健康 | 已持久登记的 sealed 段同时覆盖 ready、未决记录；分别注入 WriteAt 错误和短写、rotate 打开错误、后台 Sync 错误，设置屏障控制交付时序 | 子进程取得锁处理错误时非零退出。回放前缀仍需的段均可读，恢复不会因本次错误删段而新增 ReleasedUnreadable；核对既有决定、未决 refeed 和失败写入未产生 T。同步错误另核对该段此前成功的登记 |
| K/D/V/L 失败 | 分别注入决定、驱逐及关联结算失败，包含 K 与 C 同次 Flush | 按实际前缀逐关联核对。已确定 keep（含 L 的等价决定）按原决定恢复；缺失决定的关联按当前配置重新求值，概率策略允许结果变化 |
| 全 drop 与段回收 | 分开注入 C append 错误、最后引用触发的删段前 Flush 错误；另覆盖活段延后 Flush | 前者在释放前退出，后者在物理删段前退出；回放前缀引用的原文仍可读，记录按前缀结算 |
| checkpoint | 分别注入临时快照写/Flush（含 W）、tail 写/Flush、rename 失败；另让快照写毕等待锁，持锁方活 WAL append 零写或半帧后报错 | rename 前失败保留旧活日志；竞态用例断言旧文件身份及回放前缀，没有完整的故障目标帧，允许末尾半帧和遗留 .tmp。成功 rename 的对照用例按完整新快照与 tail 恢复 |
| 启动恢复 | 构造尾部半帧头、半 payload、完整帧坏 CRC、未知版本、无法打开 WAL | 前两项保留完整前缀并成功恢复；后三项启动失败，保留现场 |
| 普通可重试错误 | deadline、队列背压、磁盘余量阈值、下游暂时失败 | 不误触发进程退出；截止前未登记的请求不产生 T |
| 配置变更恢复回归 | 未决普通 span 在 ERROR 策略下登记后，改为 always_sample 重启；另测已记录决定、尚未交付的记录 | 启动成功，未决 span 按新配置采样；已记录决定的关联沿用原 keep/drop；不要求历史配置 |
| 正常回归 | 原有全量测试与 `-race`，加真实 OTLP、SIGKILL 和慢 sink 对照 | 输出身份、重数及字段符合基线；WAL 故障子进程用例真实退出，修复后的期望断言通过 |

测试机故障用例至少重复 3 次，保留每次 PID、退出码、故障点、ACK 清单、sink 记录、恢复输出和段文件清单。重复上限按每次故障核对，不能仅用多次 kill 的合计量证明每次都守住上限。

实施顺序：整理故障入口和分类 → 增加子进程回归测试 → 实现致命退出 → 模块测试与 race → 测试机定点故障和正常回归。提交实现之前保留修复前失败证据，不用修改断言掩盖差异。

## 4. 其他测试结果的定性与范围

以下结果保留原始测量事实，按实际部署条件判断，不列为已经确认的生产缺陷。

| 项目 | 实测或检查事实 | 当前结论 |
|---|---|---|
| 配置变更恢复 | ERROR 策略下的一条未决普通 span，在同目录改用 always_sample 重启后，启动被接受并导出。三次一致。 | 符合重启后使用新配置的预期，不是缺陷。保留为恢复回归用例；不增加历史配置存储、配置身份校验或策略不一致时拒绝启动的机制。 |
| 存活段 FD 占用 | `segment_size=1` 时，256 条记录使 FD 从 10 增到 265；`RLIMIT_NOFILE=96` 时，88 条登记成功，第 89 条报 `too many open files`。三次一致。正常 OTLP 压测的 FD 峰值为 19。 | 探针只证明每个存活段保持打开，以及人为低限制下可以耗尽 FD。用户给定生产 FD 下限为 65535，本轮没有在该条件和默认 64 MiB 段大小下复现故障。降为部署容量核算备注，不增加 FD 缓存或修改文件管理。容量核算同时考虑磁盘大小、正常段数量、重启产生的未满段及网络连接等 FD 占用，不把 96 当作生产基线。 |
| 等待登记的请求内存 | 阻塞主循环、队列上限为 8 时，额外 32 个各带 1 MiB 属性的请求进入等待，Go 堆增加 67,396,120–67,422,560 字节，约 64.3 MiB。序列化、拆分和预计算发生在等待名额之前。三次一致。 | 这是直接调用 processor 的瞬时内存观察。并发调用等待时持有 pdata 本身是正常开销，spool 还持有序列化副本。本轮没有官方内存路径的同条件对照，没有真实 OTLP 高并发或 OOM 证据。保留为可选性能评估，不修改准入顺序或增加字节预算。只有实际负载证明需要优化时，再评估提前等待能否减少副本；不能据此承诺总内存严格有界。 |
| 容量压力恢复测试的断言 | 静态检查发现 `spool_e2e_test.go` 在重启且有容量压力的组里只记录差异后返回，没有验证 PLAN 二级近似的上限，也不比较该分支的 span 内容。 | 属于已有验收覆盖不足，不是本轮新复现的数据缺陷，不扩大成本轮 WAL 修复任务。本次 WAL 验收用例需要独立、明确的退出和重复上限断言；后续若声称容量压力恢复达标，应另补差异分类、数量上限和内容断言。 |

本次交付范围为 WAL、checkpoint 和运行时段写入、轮转、同步的致命错误处理、相应故障回归及正常恢复回归。FD 缓存、内存准入优化、历史策略保存和容量压力差分改造均不在本次实现清单中。

## 5. 修复前的基线与证据

测试机为 8 CPU、Go 1.26.0。原 processor 模块完整 `go test -mod=readonly -race -count=1 -timeout=12m ./...` 在增加探针前通过；新增探针各运行 3 次。正常 OTLP 每组只运行 1 次，负载为 200 req/s、6000 请求、678,400 个输入 span、30 秒决策窗口。

基线是**当前分支同一二进制关闭 spool 的内存路径**，不是单独构建的未经修改的上游 release。基线 exporter 开启发送队列，spool 关闭发送队列。OTLP 对照比较 span ID 槽位计数和重数，不比较全部字段；已有进程内差分测试另有字段比较。

| OTLP 场景 | 输出 span | 缺失 | 多出身份 | 重复 span / 多出的副本 | 涉及重复的请求记录 |
|---|---:|---:|---:|---:|---:|
| 当前分支内存基线 | 94,171 | — | — | 0 / 0 | 0 |
| spool，无故障 | 94,171 | 0 | 0 | 0 / 0 | 0 |
| spool，第 10、22、34 秒 SIGKILL | 94,171 | 0 | 0 | 0 / 0 | 0 |
| spool，sink 延迟 50 ms，第 31、36、41 秒 SIGKILL | 94,649 | 0 | 0 | 478 / 478 | 48 |

SIGKILL 组有 32 个请求重试、128 次 `Unavailable`，无 `DeadlineExceeded`；慢 sink 组无客户端重试。慢 sink 的重复总量为 48 条记录，等于 `3 × 16`，但没有逐次归因，不能据此单独证明每次 kill 均不超过 16。

无故障时采样到的进程 VmHWM：内存路径 1268.180 MiB，spool 261.715 MiB；spool 目录峰值 416.134 MiB。采样周期 0.5 秒；单轮结果不能代表长期积压资源上限。

证据在测试机目录 `/mnt/otelbench/tailspool-verify-d169445-20261120/`：

- `REVISION`、`out/source-manifest.json`：提交和原始模块文件校验和；103 个原模块 Go/go.mod/go.sum 文件的汇总 SHA256 与本地一致，值为 `b6cb300f38741f693deef4e6bc3826fd5e87b5fa8406895045396f79d5437aa8`。
- `out/suite-race.log`、`out/suite-race.exit`：原有模块测试。
- `out/probes-spool.log`：WAL/FD 探针，包含失败断言；`out/probes-memory.log`、`out/probes-config.log`：行为探针。
- `out/summary.json`、`*.counts`、`*.attempts`、`*.collector.log`、`*.load.log`、`*.samples.jsonl`、`*.events.jsonl`：汇总及 OTLP 原始数据。
- `verify_e2e.py`、`verify_slow.py`、`summarize.py`、`REPRODUCE.txt`：测试驱动、统计脚本和复现说明；隔离源码中的 `verification_*_test.go` 为新增探针，实现未改。
- `evidence-sha256.json`：68 个证据文件的校验和，本地下载后逐个校验通过。

本地副本为 `/private/tmp/tailspool-verify-d169445-20261120-evidence/`；临时目录可能被清理，长期复现以测试机证据目录、提交身份和校验和为准。慢 sink 测试最初的脚本语法错误和端口检查失败分别保留在 `out/slow-driver-first-attempt.log`、`out/slow-driver-port-check.log`，这两次均未启动负载，不计为产品测试结果。

本轮尚未覆盖：512 MiB cgroup、真实文件系统 ENOSPC/EIO、多 backend/LB 路由、掉电、上游原始 release 对照、完整策略/gate 矩阵及长期下游阻塞。不得将这些项目计为通过。

## 6. 修复实施与验收记录

实现移除了运行时的 `s.err` 停写状态。WAL 追加或 Flush、运行时 checkpoint 及段写入、轮转或同步失败时，调用方在持有 `s.mu` 的状态下记录错误并执行 `os.Exit(1)`。启动时的初始 checkpoint 和不可读记录处理将存储错误返回给 Open。正常 Stop 仍等待 worker 并返回最终 Flush 的错误。

测试机为 `10.98.1.5`，证据目录为 `/mnt/otelbench/tailspool-walfix-5veK40/`。测试基于上面的修复前提交加工作区改动。测试机与工作区的 12 个 spool Go 文件按 SHA-256 逐一比对，清单在 `out/workspace-spool.sha256`。最终 Collector、负载工具及其校验和在 `final/bin/` 和 `final/out/bin.sha256`。

**真实 Collector 的 C 写入故障。** 每轮通过 OTLP/HTTP 发送 24 个单 span 请求并逐个收到成功响应。观察到磁盘 WAL 中 24 条 K 后，将该测试进程的 Linux 文件大小软限制设为当时 WAL 的长度，使下一次增长写入收到 EFBIG。下游是独立 OTLP sink，在故障和重启之间持续运行。重启前保存活 WAL 和段文件，校验帧的 CRC，重启后按 span ID 比对计数。

| 每轮，max_inflight_records=1 | 修复前关闭 WAL 描述符探针 | 修复后真实 Collector，三轮一致 |
|---|---:|---:|
| 首次运行已交付 | 24 | 1 |
| 重启后新增交付 | 24 | 24 |
| 重复的记录身份 | 24 | 1 |
| 实际退出方式 | 探针调用 Stop，再 Open | C Flush 报 EFBIG 后进程退出码 1 |

三轮退出时回放前缀均为 2390 字节，完整 C 为 0；重启后每轮共计 25 次交付、24 个不同身份、0 缺失。三个进程 PID、ACK 清单、sink 原始计数、Collector 日志和重开前文件分别保存在 `out/collector-c-{1,2,3}/`，驱动为 `verify_collector_c.py`。文件大小限制仅施加于测试进程。

**定点子进程回归。** `TestPersistenceFailure` 包含 29 组故障点及 worker 数组合，使用真实退出码、重开前读回的 WAL、段文件及跨进程保留的 sink 身份验收。每组启用 race 并重复 3 次，共 87 次预期的退出码 1。零字节与部分写、完整 T 随错误返回、同一次 Flush 中 K 完整或不完整而 C 不完整均有断言；覆盖上限 1、2、16、全 drop、不可读记录结算、健康 WAL 下的段错误、checkpoint 临时文件 Flush、tail 写与 Flush、rename 和等待安装时活日志失败。6 种启动边界单独运行。证据在 `out/faults-verified.log`。

**采样策略变更。** 未决普通 span 从 ERROR 策略改为 always_sample 后重启，三轮均导出。另用真实 Collector 重做上述三轮 EFBIG 测试，在重启前将 always_sample 改为仅保留 ERROR。24 个普通 span 都已在原 WAL 中记录 keep，三轮重启后仍补齐全部身份，每轮仅第 1 条重复。证据分别为 `out/config-race.log`、`verify_collector_policy.py` 和 `out/collector-policy-{1,2,3}/`。

`tailsamplingprocessor` 模块的 `go test -mod=readonly -race -count=1 -timeout=12m ./...` 及 `go vet ./...` 均成功。测试包含接收入队、deadline、磁盘余量和读取重试回归；新增的下游首次调用返回错误、第二次成功的用例三轮均只完成一次交付。日志为 `out/suite-verified.log`、`out/vet-verified.log` 和 `out/faults-verified.log`。

**最终二进制的 OTLP 对照。** 同一二进制、每组 6000 请求、200 请求/秒，两端按 span ID 和重数对账。内存基线输出 94,171 个 span。无故障 spool 和普通三次 SIGKILL 均输出 94,171，缺失、多出身份、重复均为 0；普通 SIGKILL 组有 32 个请求发生客户端重试。50 ms 慢 sink 加三次 SIGKILL 输出 94,652，缺失和多出身份均为 0，481 个额外 span 副本涉及 48 条请求记录，客户端重试为 0。此处 48 为三次合计；逐次上限由上述有屏障的故障用例独立核验。汇总、二进制哈希、原始槽位计数与重启事件位于测试机 `final/out/`，本地证据副本在 `/private/tmp/tailspool-walfix-5veK40-evidence/`。实测覆盖 Linux 进程写失败和进程重启；掉电及物理盘损坏仍在本轮验证范围之外。
