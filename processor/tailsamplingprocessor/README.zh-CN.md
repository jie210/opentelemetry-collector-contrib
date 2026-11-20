# 尾部采样处理器（Tail Sampling Processor）

[English](./README.md) · [配置定义](./config.go) · [指标说明](./documentation.md)

`tail_sampling` 按 trace ID 汇集 span，再根据策略决定保留或丢弃整条 trace。它可以直接使用，无需在前面增加 `groupbytrace`。本组件的 traces 支持级别为 beta，常规组件随 contrib 和 k8s 发行版提供；下文的磁盘 spool 是本分支新增的功能。

同一条 trace 的所有 span 必须到达同一个 Collector 实例。处理器会重新组装批次，原请求上下文会随之丢失，因此依赖请求上下文的处理器（如 `k8sattributes`）应放在它前面。

## 配置参数

在 `processors.tail_sampling` 下配置采样。必填项 `policies` 为策略列表，每条策略有名称 `name`、类型 `type` 和该类型的参数。

| 参数 | 默认值 | 用途 |
| --- | --- | --- |
| `sampling_strategy` | `trace-complete` | 选择定时决策或到达时求值，见下文“采样时机”。 |
| `decision_wait` | `30s` | 从首次收到 trace 到定时处理的等待时间。 |
| `decision_wait_after_root_received` | `0s` | 收到根 span 后可提前触发定时处理，零表示关闭。 |
| `num_traces` | 50000 | 同时跟踪的 trace 数量。 |
| `expected_new_traces_per_sec` | 0 | 预估每秒新增 trace 数，用于预分配。 |
| `decision_cache.sampled_cache_size`、`decision_cache.non_sampled_cache_size` | 各 0 | 分别设置保留、丢弃决定的 LRU 缓存容量，以 trace ID 数计。默认内存模式下零表示关闭。需要让决定比 trace 数据保留更久时，可将缓存设得大于 `num_traces`，按保留和丢弃的比例分别估算。 |
| `sample_on_first_match` | `false` | 首次命中即可作出决定。启用 `usetracestate` 时需要检查各策略的阈值，应关闭此选项。 |
| `drop_pending_traces_on_shutdown` | `false` | 普通内存模式关闭时丢弃待决 trace；默认用已收到的数据决策。spool 模式可在重启后恢复待决记录。 |
| `maximum_trace_size_bytes` | 0 | 单条 trace 的字节上限，超过即丢弃，零表示不设限。 |
| `block_on_overflow` | `false` | 达到容量上限时等待空位。 |
| `num_shards` | 1，最大 256 | 按 trace ID 哈希分配到并行事件循环。同一 trace 始终进入同一分片。 |
| `tail_storage` | 未设置 | 指定实验性存储扩展，需启用对应特性开关。 |
| `spool` | 未启用 | 将待决请求原文写入磁盘，参数见下一节。 |

多分片按比例拆分 `num_traces`、预分配量、决策缓存和各策略的每秒速率上限。每个分片独立执行限额，可能先于整体达到容量；很小的限额还会受到每片最小配额的影响。`burst_capacity` 保留每片的完整容量，以便一条 trace 在一个分片内完成限流判断。使用 `tail_storage` 或 `spool` 时只支持单分片。

## 磁盘 spool

配置 `processors.tail_sampling.spool` 后，采样状态保存在内存中，请求原文写入段文件，登记和决策等信息写入预写日志（WAL）。启用时只有 `directory` 必填。下面的例子保留包含错误 span 的 trace，并在换用新数据文件后，把旧文件里尚在系统缓存中的数据写入磁盘。

```yaml
processors:
  tail_sampling:
    policies:
      - name: keep-errors
        type: status_code
        status_code: {status_codes: [ERROR]}
    spool:
      directory: /var/lib/otelcol/tailspool
      sync_on_rotate: true
      max_inflight_records: 16
```

| spool 参数 | 默认值 | 用途 | 使用场景 |
| --- | --- | --- | --- |
| `directory` | 必填 | 段文件和 WAL 的目录。每个实例使用独立且可写的目录，重启时复用原目录才能恢复待决数据。 | 容器重建后仍需恢复待决请求时，将目录放在持久卷上，每个副本使用独立目录。 |
| `segment_size` | 67108864（64 MiB） | 段文件目标大小，单位字节。如果下一条请求会让非空段超过该值，则另开新段。 | 少量长期待决请求占住大段文件时，可调小，让其他段更早回收。高吞吐下若文件轮转过于频繁，可调大。 |
| `checkpoint_size` | 67108864（64 MiB） | 触发 WAL 压缩检查点的大小阈值，单位字节。实际触发还取决于日志相对上次快照的增长量。 | 重启时回放大 WAL 耗时过长，可调小阈值。若频繁写检查点拖慢登记，且能接受更长的回放时间，可调大。 |
| `sync_on_rotate` | `false` | 控制切换到新数据文件后，是否在后台把旧文件中仍在系统缓存里的数据写入磁盘。 | 只需在 Collector 进程崩溃后同机恢复时，可保留默认值。若还希望降低机器突然断电时旧文件丢数据的风险，可开启。客户端收到成功响应时，这次后台写盘可能还没完成。例如，请求 A 还在写旧文件，请求 B 因剩余空间不足而开始写新文件。开启此项后，B 可以继续写。等操作系统收齐 A 和其他已分到旧文件的请求数据，spool 再把旧文件刷到磁盘。 |
| `max_inflight_records` | 16 | 交付 worker 数，限制正在交给下游、等待提交的记录数。 | 待交付记录积压且下游仍有余量时，可调高并发。下游需要限制并发调用，或希望收紧进程崩溃后可能重复交付的记录数上限时，可调低。 |
| `max_inflight_bytes` | 67108864（64 MiB） | 按请求落盘大小计算的并发交付字节预算。单条超大记录可独占交付。 | 多个大请求同时交付造成内存压力时，可调低预算。若字节预算限制了 worker 并发，且内存有余量，可调高。 |
| `min_free_disk` | 0 | 可用空间低于此字节数时拒绝新登记并返回可重试错误。零关闭检查。 | spool 与其他服务共用磁盘时，可设正值，为其他写入留出空间。空间恢复后，需要客户端重发此前被拒绝的请求。 |
| `min_deadline_remaining` | `0s` | 带截止时间的请求在登记时至少应剩余的时长，例如 `100ms`。零允许所有尚未过期的截止时间。 | 客户端经常在收到 ACK 前超时并重发时，可预留一段响应时间。设得越大，越多短截止时间的请求会在登记前被拒绝。 |
| `read_retries` | 0 | 读取已存记录发生 I/O 错误时的额外尝试次数。耗尽后丢弃该记录；无法解码的记录直接丢弃。下游消费失败有独立的交付重试逻辑。 | 存储偶发暂时性读取错误时，可设少量重试。次数越多，留给存储恢复的时间越长，积压记录回放和停机等待也可能更久。 |

**使用条件。** 采用 `trace-complete`、单分片，省略 `tail_storage`。启用 `block_on_overflow` 时，两类决策缓存的容量都必须大于零。spool 与 `recordpolicy`、`usetracestate` 特性开关不兼容。启动时还会检查策略能否按到达的片段增量求值，例如使用 `Now()` 的 OTTL 条件可能在到达和决策两个时刻产生不同结果，因此会被拒绝。

**确认与恢复。** 要让上游在请求完成登记后才收到成功响应，spool 前面的处理器应同步传递请求；`batch` 等缓冲组件可能提前确认。交付时，下一个 consumer 返回成功，spool 就可以提交记录。下游是异步 exporter 队列时，成功入队即完成这次交接；下游同步发送时，永久拒绝会返回 spool，持续重试可能占满交付 worker。进程重启后会回放已记录的决定，尚未决策的 trace 按当前策略求值。这一恢复行为以本机文件在进程退出后完好为前提，逐请求 ACK 不提供掉电持久性保证。[spool 配置定义](./spool_config.go)

## 采样策略

| 策略 | 决策依据 |
| --- | --- |
| `always_sample` | 保留所有 trace。 |
| `latency` | 按最早开始到最晚结束的时间差判断。只设 `threshold_ms` 时，超过下限即可保留；也可设 `upper_threshold_ms`。 |
| `numeric_attribute`、`string_attribute`、`boolean_attribute` | 匹配资源或记录属性。数值支持上下界，字符串支持精确值或正则，布尔属性匹配指定值。 |
| `probabilistic` | 按配置比例保留 trace。默认对 trace ID 与 `hash_salt` 做 FNV-1a 哈希。 |
| `status_code` | 匹配 span 状态码 `OK`、`ERROR` 或 `UNSET`。 |
| `trace_state`、`trace_flags` | 前者匹配 TraceState 键值，后者检查 trace 中是否有 span 设置了 sampled 标志。 |
| `rate_limiting`、`bytes_limiting` | 使用令牌桶分别限制每秒保留的 span 数或字节数，支持突发容量。 |
| `span_count` | 按整条 trace 的 span 数判断，包含配置的最小和最大边界。 |
| `ottl_condition` | 对 span 或 span event 求布尔表达式。建议显式写上下文，例如 `resource.attributes["service.name"]`、`spanevent.name`。 |
| `and`、`not`、`drop` | 分别用于组合多个匹配条件、反转单个子策略的结果、按匹配条件显式丢弃。 |
| `composite` | 按顺序执行子策略并分配速率。例如总预算 100 span/s，可分配 50% 和 25% 给前两项，最后用 `always_sample` 消耗剩余容量。 |

### 采样时机

- **`trace-complete`** 在定时器触发时根据已积累的数据决策，可使用有状态策略。配置根 span 加速后，可以早于 `decision_wait` 完成决策。
- **`span-ingest`** 在每批数据到达时求值，只支持无状态策略。明确保留或丢弃时立即结束；其他 trace 保持待决，到清理时按未采样结束，期间不重新计算先前批次。

两种模式都可以用决策缓存处理晚到 span。

### 多策略决策顺序

依次检查以下条件，满足即得出最终结果：

1. 有显式丢弃决定，丢弃。
2. 有旧式 `InvertNotSampled` 决定，丢弃。
3. 有保留决定，保留。
4. 有旧式 `InvertSampled` 决定且没有普通的未采样决定，保留。
5. 其余情况均不采样。

旧式反转决定已弃用。在 `and` 和 `composite` 内，子策略最终给出普通的采样或未采样结果。新配置可用 `drop` 显式丢弃，或用 `not` 对单个策略取反。下面是各类策略的配置示例。[配置测试样例](./testdata/tail_sampling_config.yaml)

```yaml
processors:
  tail_sampling:
    decision_wait: 10s
    num_traces: 100
    expected_new_traces_per_sec: 10
    decision_cache:
      sampled_cache_size: 100_000
      non_sampled_cache_size: 100_000
    policies:
      [
          {
            name: test-policy-1,
            type: always_sample
          },
          {
            name: test-policy-2,
            type: latency,
            latency: {threshold_ms: 5000, upper_threshold_ms: 10000}
          },
          {
            name: test-policy-3,
            type: numeric_attribute,
            numeric_attribute: {key: key1, min_value: 50, max_value: 100}
          },
          {
            name: test-policy-4,
            type: probabilistic,
            probabilistic: {sampling_percentage: 10}
          },
          {
            name: test-policy-5,
            type: status_code,
            status_code: {status_codes: [ERROR, UNSET]}
          },
          {
            name: test-policy-6,
            type: string_attribute,
            string_attribute: {key: key2, values: [value1, value2]}
          },
          {
            name: test-policy-7,
            type: string_attribute,
            string_attribute: {key: key2, values: [value1, val*], enabled_regex_matching: true, cache_max_size: 10}
          },
          {
            name: test-policy-8,
            type: rate_limiting,
            rate_limiting: {spans_per_second: 35, burst_capacity: 70}
         },
         {
            name: test-policy-9,
            type: bytes_limiting,
            bytes_limiting: {bytes_per_second: 1024000, burst_capacity: 2048000}
         },
         {
            name: test-policy-10,
            type: span_count,
            span_count: {min_spans: 2, max_spans: 20}
         },
         {
             name: test-policy-11,
             type: trace_state,
             trace_state: { key: key3, values: [value1, value2] }
         },
         {
              name: test-policy-12,
              type: boolean_attribute,
              boolean_attribute: {key: key4, value: true}
         },
         {
              name: test-policy-13,
              type: ottl_condition,
              ottl_condition: {
                   error_mode: ignore,
                   span: [
                        "attributes[\"test_attr_key_1\"] == \"test_attr_val_1\"",
                        "attributes[\"test_attr_key_2\"] != \"test_attr_val_1\"",
                   ],
                   spanevent: [
                        "name != \"test_span_event_name\"",
                        "attributes[\"test_event_attr_key_2\"] != \"test_event_attr_val_1\"",
                   ]
              }
         },
         {
              name: test-policy-14,
              type: ottl_condition,
              ottl_condition: {
                   error_mode: ignore,
                   span: [
                        "resource.attributes[\"service.name\"] == \"checkout\"",
                        "span.attributes[\"http.status_code\"] >= 500",
                   ],
                   spanevent: [
                        "spanevent.name == \"exception\"",
                   ]
              }
         },
         {
            name: and-policy-1,
            type: and,
            and: {
              and_sub_policy:
              [
                {
                  name: test-and-policy-1,
                  type: numeric_attribute,
                  numeric_attribute: { key: key1, min_value: 50, max_value: 100 }
                },
                {
                    name: test-and-policy-2,
                    type: string_attribute,
                    string_attribute: { key: key2, values: [ value1, value2 ] }
                },
              ]
            }
         },
         {
            name: not-policy-1,
            type: not,
            not: {
              not_sub_policy: {
                name: test-not-policy-1,
                type: latency,
                latency: { threshold_ms: 1000 }
              }
            }
         },
         {
            name: drop-policy-1,
            type: drop,
            drop: {
              drop_sub_policy:
              [
                {
                    name: test-drop-policy-1,
                    type: string_attribute,
                    string_attribute: {key: url.path, values: [\/health, \/metrics], enabled_regex_matching: true}
                }
              ]
            }
         },
         {
            name: composite-policy-1,
            type: composite,
            composite:
              {
                max_total_spans_per_second: 1000,
                policy_order: [test-composite-policy-1, test-composite-policy-2, test-composite-policy-3],
                composite_sub_policy:
                  [
                    {
                      name: test-composite-policy-1,
                      type: numeric_attribute,
                      numeric_attribute: {key: key1, min_value: 50}
                    },
                    {
                      name: test-composite-policy-2,
                      type: string_attribute,
                      string_attribute: {key: key2, values: [value1, value2]}
                    },
                    {
                      name: test-composite-policy-3,
                      type: always_sample
                    }
                  ],
                rate_allocation:
                  [
                    {
                      policy: test-composite-policy-1,
                      percent: 50
                    },
                    {
                      policy: test-composite-policy-2,
                      percent: 25
                    }
                  ]
              }
          },
        ]
```

### 按字节限流

`bytes_limiting` 按序列化的 trace 字节大小扣减令牌。令牌以 `bytes_per_second` 的速度持续补充，桶容量由 `burst_capacity` 控制，默认是每秒速率的两倍。令牌不足时不采样该 trace。以下配置允许持续 1 MiB/s、最大突发 5 MiB。

```yaml
processors:
  tail_sampling:
    policies:
      - name: volume-control
        type: bytes_limiting
        bytes_limiting:
          bytes_per_second: 1048576    # 持续速率 1 MiB/s
          burst_capacity: 5242880      # 最大突发 5 MiB
```

### Tracestate 与概率阈值

默认关闭的 alpha 特性开关 `processor.tailsamplingprocessor.usetracestate` 可读取和更新 W3C `tracestate` 中 `ot` 的 `rv` 和 `th` 字段，以便上下游采样器一致地计算调整后的计数。

- `probabilistic` 优先使用显式随机值 `rv`，也可使用从 trace ID 派生的随机值，并与配置阈值比较。缺少概率采样信息时，回退到带 `hash_salt` 的 FNV 哈希。
- `rate_limiting` 和 `bytes_limiting` 会报告实际采样阈值，适用于顶层以及嵌套在 `and` 中的策略。预算充足时全部保留，预算不足时优先保留随机值最高的 trace。嵌套在 `composite` 中时仍采用原有的全采样阈值报告方式。
- 决定保留时，处理器取所有投票保留的策略中的最小有效阈值，更新出站 span 的 `th`。过滤型策略对应 `th=0`，已有更严格阈值的 span 保留原值。

## 实际配置示例

假设正在为 `team_a` 逐步启用尾部采样。其他团队继续全采样；就绪和存活探针采样 0.1%；`service-1` 的高流量接口 `/v1/name/{id}` 采样 1%，其他接口全采样。包含错误 span 或设置了 `app.force_sample=true` 的 trace 保留；`app.do_not_sample=true` 的显式丢弃优先。

```yaml
tail_sampling:
  decision_wait: 10s
  num_traces: 100
  expected_new_traces_per_sec: 10
  policies: [
      {
        # 规则 1：其他团队保持全采样
        name: backwards-compatibility-policy,
        type: and,
        and:
          {
            and_sub_policy:
              [
                {
                  name: services-using-tail_sampling-policy,
                  type: string_attribute,
                  string_attribute:
                    {
                      key: service.name,
                      values:
                        [
                          list,
                          of,
                          services,
                          using,
                          tail_sampling,
                        ],
                      invert_match: true,
                    },
                },
                { name: sample-all-policy, type: always_sample },
              ],
          },
      },
      # team_a 策略开始
      {
        # 规则 2：以低比例保留就绪与存活探针
        name: team_a-probe,
        type: and,
        and:
          {
            and_sub_policy:
              [
                {
                  # 筛选服务名
                  name: service-name-policy,
                  type: string_attribute,
                  string_attribute:
                    {
                      key: service.name,
                      values: [service-1, service-2, service-3],
                    },
                },
                {
                  # 筛选路由
                  name: route-live-ready-policy,
                  type: string_attribute,
                  string_attribute:
                    {
                      key: http.route,
                      values: [/live, /ready],
                      enabled_regex_matching: true,
                    },
                },
                {
                  # 按比例采样
                  name: probabilistic-policy,
                  type: probabilistic,
                  probabilistic: { sampling_percentage: 0.1 },
                },
              ],
          },
      },
      {
        # 规则 3：降低高流量接口的采样率
        name: team_a-noisy-endpoint-1,
        type: and,
        and:
          {
            and_sub_policy:
              [
                {
                  name: service-name-policy,
                  type: string_attribute,
                  string_attribute:
                    { key: service.name, values: [service-1] },
                },
                {
                  # 筛选路由
                  name: route-name-policy,
                  type: string_attribute,
                  string_attribute:
                    {
                      key: http.route,
                      values: [/v1/name/.+],
                      enabled_regex_matching: true,
                    },
                },
                {
                  # 按比例采样
                  name: probabilistic-policy,
                  type: probabilistic,
                  probabilistic: { sampling_percentage: 1 },
                },
              ],
          },
      },
      {
        # 规则 4：保留该服务其他接口
        name: team_a-service-1,
        type: and,
        and:
          {
            and_sub_policy:
              [
                {
                  name: service-name-policy,
                  type: string_attribute,
                  string_attribute:
                    { key: service.name, values: [service-1] },
                },
                {
                  # 匹配指定路由以外的请求
                  name: route-name-policy,
                  type: string_attribute,
                  string_attribute:
                    {
                      key: http.route,
                      values: [/v1/name/.+],
                      enabled_regex_matching: true,
                      invert_match: true,
                    },
                },
                {
                  # 按比例采样
                  name: probabilistic-policy,
                  type: probabilistic,
                  probabilistic: { sampling_percentage: 100 },
                },
              ],
          },
      },
      {
        # 规则 5：保留错误 trace
        name: team_a-status-policy,
        type: and,
        and:
          {
            and_sub_policy:
              [
                {
                  name: service-name-policy,
                  type: string_attribute,
                  string_attribute:
                    {
                      key: service.name,
                      values:
                        [
                          list,
                          of,
                          services,
                          using,
                          tail_sampling,
                        ],
                    },
                },
                {
                  name: trace-status-policy,
                  type: status_code,
                  status_code: { status_codes: [ERROR] },
                },
              ],
          },
      },
      {
        # 规则 6：按属性强制保留
        # 强制保留指定请求
        name: team_a-force-sample,
        type: boolean_attribute,
        boolean_attribute: { key: app.force_sample, value: true },
      },
      {
        # 规则 7：按属性显式丢弃
        # 显式丢弃指定请求
        name: do-not-sample,
        type: drop,
        drop: {
          drop_sub_policy:
            [
              {
                name: team_a-do-not-sample,
                type: boolean_attribute,
                boolean_attribute: { key: app.do_not_sample, value: true }
              }
            ]
        }
      },
      # team_a 策略结束
    ]
```

## 部署与扩容

同一 trace 的所有 span 需要一致地路由到同一实例。可以分两层部署：前层使用负载均衡 exporter 按 trace ID 分流，后层执行尾部采样。分层部署有利于故障隔离，也可以在单层实例上配置两条 pipeline。[负载均衡 exporter](../../exporter/loadbalancingexporter)

如果只需要按比例采样，独立的概率采样处理器可以直接根据 trace ID 决策，无需等待其他 span。如果已使用尾部采样，可直接增加 `probabilistic` 策略，让其他策略保留的 trace 也能正常交付。[概率采样处理器](../probabilisticsamplerprocessor)

## 常见问题与监控

**为什么 `sampling_trace_dropped_too_early` 很高？** 普通内存模式下，环形缓冲区达到 `num_traces` 容量时，新 trace 可能挤出最旧的待决 trace。可以提高容量（会增加内存使用），或缩短 `decision_wait`（会给晚到 span 留下更少时间）。结合下列指标判断瓶颈。[完整指标列表](./documentation.md)

| 指标 | 观察方法 |
| --- | --- |
| `otelcol_processor_tail_sampling_sampling_trace_dropped_too_early` | 查看决策前被移除的 trace 数。 |
| `otelcol_processor_tail_sampling_sampling_trace_removal_age` | 将移除年龄的低分位数（例如 p1）与 `decision_wait` 比较，接近等待时间说明流量增长时容易提前淘汰。 |
| `otelcol_processor_tail_sampling_sampling_decision_timer_latency` | 从一批 trace 决策到传给后续组件的耗时；超过一秒可能延迟其他决策。 |
| `otelcol_processor_tail_sampling_sampling_late_span_age` | 查看晚到 span 的延迟分布，评估是否调整等待窗口。 |
| `sampling_policy_evaluation_error` | 检查策略求值错误。 |

### 晚到的 span

trace 的决定仍在缓冲区时，晚到 span 继承原决定。缓冲区已淘汰该 trace 且没有对应缓存时，处理器会将其视为新 trace，重新等待并决策。启用决策缓存后，只要对应 trace ID 尚未被 LRU 淘汰，后续 span 就能继续沿用决定。

估算晚到 span 比例时，可使用 `otelcol_processor_tail_sampling_sampling_late_span_age{le="+Inf"} / otelcol_processor_tail_sampling_count_spans_sampled`。分母需要开启 `processor.tailsamplingprocessor.metricstatcountspanssampled`。

### 采样比例和策略命中率

整体 trace 采样比例：

```promql
otelcol_processor_tail_sampling_global_count_traces_sampled{sampled="true"} /
otelcol_processor_tail_sampling_global_count_traces_sampled
```

各策略的保留和丢弃投票比例：

```promql
sum (otelcol_processor_tail_sampling_count_traces_sampled{decision="sampled"}) by (policy) /
sum (otelcol_processor_tail_sampling_count_traces_sampled) by (policy)
```

```promql
sum (otelcol_processor_tail_sampling_count_traces_sampled{decision="dropped"}) by (policy) /
sum (otelcol_processor_tail_sampling_count_traces_sampled) by (policy)
```

策略投票保留后，最终仍可能因其他策略显式丢弃而不采样。

### 记录命中的采样策略

开启 `processor.tailsamplingprocessor.recordpolicy` 后，已采样 span 可包含以下属性：

| 属性 | 何时出现 |
| --- | --- |
| `tailsampling.policy` | 记录命中的策略名，通过决策缓存保留时省略。 |
| `tailsampling.composite_policy` | 组合策略命中时记录其子策略名。 |
| `tailsampling.cached_decision` | 使用缓存决定时标记。 |

### 存储扩展和旧式反转决定

配置 `tail_storage` 需要启用 `processor.tailsamplingprocessor.tailstorageextension`，并提供实现实验性 `TailStorage` 接口的扩展。开关默认关闭，此时设置 `tail_storage` 会导致配置校验失败。

开启 `processor.tailsamplingprocessor.disableinvertdecisions` 后，字符串、数值和布尔属性策略的 `invert_match` 产生普通的 `Sampled` 或 `NotSampled` 决定。需要显式否决时使用 `drop`，需要对单条策略取反时使用 `not`。
