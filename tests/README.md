# tests 测试项清单

黑盒集成测试。驱动入口只走公开 API：proxy.New / Server.ServeHTTP / config.Config / cache 包。禁 e2e(端到端测试)。全部上游为 httptest(本地假上游)。全部用例 in-process(进程内)运行，不发真实外网请求。

## 怎么跑

```bash
export PATH=$PATH:/usr/local/go/bin; go test ./tests/ -count=1
```

- 全量回归：`go test ./...`
- 竞态检测：`go test ./tests/ -count=1 -race`。merger_pump_race_test.go 以 -race(竞态检测器) 为主要断言手段。
- 同步方式全部为状态驱动：命中计数、轮询、channel 门控。无 sleep 赌时序。
- 整包耗时约 52 秒。flight 超限用例自身约 12 秒（flightWaitLimit 为包级常量 10 秒）。

## 清单口径

- 顶层测试函数 20 个。子测试展开 13 个：416(范围未满足) 矩阵 3、selfRef(自引用候选) 2、门控成对 2、pump(泵送循环) 压力 6 轮。合计 33 个可运行单元。
- helpers_test.go 是共享夹具：配置构造、假上游构造器、计数 Writer、goroutine(Go 协程) 收敛检查。它不产测试项。
- 分级对齐提交 b8ed8e9：P0 7 项、P1 10 项、P2 3 项。

## 测试项

### reconcile_crosscheck_test.go —— 多源拼接三道内容级对账

场景构造：胜者 200 慢速供数，判速升级(slow-download escalation) 拉入补充源 Range 续传，两路拼接同一 blob(镜像层数据对象)。

- `TestMisalignedSupplementPrunedBeforeFirstByte`（P0）
  - 易错点：206(部分内容) 响应头声明合法、载荷整体错位的坏镜像源混入客户端流。
  - 断言：错位载荷 0 字节进客户端流（逐字节比对）；剪枝留痕 verify-mismatch(校验不符) / cross-check-mismatch(交叉对账不符) 指向补充源且为 WARN 级；完成 sources=2 alive_sources=1；补充源恰 1 次请求（剪枝后不重试不重加）；胜者供出全量；缓存落盘内容正确。
- `TestCorrectSupplementNotPruned`（P0）
  - 易错点：正确的非胜者补充源被对账误剪。
  - 断言：全程 0 条剪枝日志；完成 alive_sources=2；客户端字节逐字节正确；各源恰 1 次请求；缓存落盘。
- `TestWinnerConflictWarnsOnceNotReversePruned`（P0）
  - 易错点：已确认输出被抢先出数污染后，胜者重复区间对账不符 → 误反向剪掉胜者；冲突 Warn 逐块刷屏。
  - 断言：对账冲突 Warn 恰好 1 次（防刷屏）；胜者不被剪（alive_sources=2）；被污染累积内容被写缓存前 sha256(内容摘要) 强校验拒绝入库；无正式缓存条目、无半截残留；留痕「拒绝写缓存」。
- `TestSelfRefCandidateSkippedNoUpstreamRequest`（P0，2 子测试）
  - 易错点：selfRef(自引用候选)（template: $1 展开指向本代理自身）发起上游请求 → 递归成环；按 item 重试原地空转。
  - 断言：仅自引用 → 客户端拿 502、哨兵上游(canary) 0 命中、跳过留痕恰 1 次（重试预算设 3 也不空转）；混合真实候选 → 真实候选照常服务、哨兵仍 0 命中。

### resume_scan_test.go —— 续传扫描取消权移交与 416 判完整矩阵

场景构造：预置半截文件，候选扫描毫秒级返回，活体流继续。历史 bug：扫描预算 deadline 挂进请求上下文，命中 206 后 body 一读即 context canceled，活体流约 2 分钟被杀、客户端响应截断。

- `TestResumeScanBudgetDoesNotKillLiveStream`（P0）
  - 易错点：扫描 cancel(取消权) 未随流移交，活体流被预算杀掉。
  - 断言：客户端收全量 512KB 且逐字节正确（被杀会截断在 64KB 前缀附近）；候选命中留痕恰 1；立即响应留痕恰 1；全程无 context canceled / deadline exceeded；上游恰 1 次 Range(区间) 请求；半截删除、缓存写入正确；goroutine 收敛。
- `TestResumeScanAllFailSafeAbortKeepsPart`（P1）
  - 易错点：扫描全败（网络级失败）降级全量回源 → 错位拼接。
  - 断言：客户端只收半截前缀 64KB（安全中断，无降级）；留痕「候选扫描失败」与「安全中断」；半截保留 64KB（进度不回退）；不写缓存；goroutine 收敛。
- `TestResumeScan416CompleteWritesCache`（P1）
  - 易错点：416 判完整正向通道失效，完整半截无法固化为缓存。
  - 断言：Content-Range(内容区间头) 总长等于半截长度 → 「发现半截已完整」留痕恰 1；整体回放写缓存；半截删除；客户端拿全量 200；上游恰 1 次（416 后不再读上游）。
- `TestResumeScan416UntrustedTotalMatrix`（P1，3 子测试）
  - 易错点：毒缓存——按不可信 416 把半截固化为正式缓存（对任意 Range 一律回 416 的镜像事故形态）。
  - 断言：总长多一字节、少一字节、缺 Content-Range 三形态均不可信；「发现半截已完整」0 行；留痕不可信原因（总长缺失或 ≠ 半截长度）；丢弃半截；已 commit 响应安全中断在前缀处；绝不写缓存；goroutine 收敛。

### flight_wait_limit_test.go —— flight(在途合流) 等待上限超限语义

场景构造：同 key 并发回源。flightWaitLimit(单飞等待上限) 为包级常量 10 秒，执行方上游按块慢速供数覆盖该窗口。

- `TestFlightWaitLimitTimeoutWaiterRefetches`（P0）
  - 易错点：等待方超限后挂死，或第二路下载与执行方互污染缓存。
  - 断言：等待方超限自行走候选回源，超限留痕恰 1；两路各自完整且互不污染；无在途流场景不出现搭车响应头 X-Cache: PREFETCH；上游恰 2 次（执行方一路 + 等待方一路）；缓存落盘正确、无半截；goroutine 收敛（slack 8：连接池驻留属常态）。
- `TestFlightWaitLimitRidealongPreventsSecondDownload`（P0）
  - 易错点：blob 半截续传在途时，并发 GET 进 flight 等待、超限后发起第二路完整下载（重复下载）。
  - 断言：并发 GET 搭车 X-Cache: PREFETCH，磁盘前缀 + 实时增量拼接出全量且正确；留搭车日志；0 条超限留痕；上游恰 1 路且带 Range 头；无第二路完整 GET（无 Range 请求计数 0）；半截删除、缓存正确。

### prefetch_budget_test.go —— 预取(prefetch) 串行推进不饥饿

- `TestPrefetchMaxLayerLastNotStarved`（P1）
  - 易错点：最大层排在预取队列最后，前层被客户端在途下载占用 → 预取耗尽在 flight 合并等待上，最大层一次都排不到（现场饥饿形态）。
  - 断言：layer1 被客户端慢速占用时，预取合并等待结束后继续串行推进；layer2 与最大层 layer3、config 全部写入缓存且内容正确；上游每 blob 只一路下载（layer1 由客户端供数，合并不重复拉）；留痕「预取合并客户端在途回源完成」与「预取 blob 写入缓存完成」。

### range_resume_disconnect_test.go —— 起点>0 Range 断开保留半截

- `TestRangeStartPositiveDisconnectKeepsPartial`（P1）
  - 易错点：断点重试 Range: bytes=N- 的客户端中途断开 → 半截白扔（现场 302MB 层已下 30MB 白扔）；续传误按客户端起点而非半截长度发起。
  - 断言：已 commit 的 206 截断响应（区间 Content-Range 按完整 blob 语义）；broken pipe(管道断裂) 精确触发（failAfterWriter 额度 160KB = 前缀切片 + 2 增量块）；上游续传 Range = bytes=262144-（按半截长度，绝不按客户端起点重拉保留区间）；上游恰 1 次；半截保留在确定小区间 [304K, 336K]（进度不回退）且内容为原文前缀；绝不入库；宽限终止留痕；goroutine 收敛。

### token_realm_isolation_test.go —— token dance 按 realm(鉴权域) 隔离

- `TestTokenDanceRealmIsolation`（P1）
  - 易错点：token 进程内缓存的 key 塌缩（缺 realm 维度）→ 两个镜像站串用同一 token，重试被困 401(未授权)（现场「A 站用了 B 站签发的 token」形态）。
  - 断言：两候选 realm 不同、scope 相同（最小隔离维度）；各 realm 的 token 端点恰 1 次请求（同 realm 第二次命中缓存、跨 realm 绝不复用）；token 请求的 service/scope 查询参数与各自挑战一致；各镜像站重试带自己 realm 签发的 token（A 拿 tok-a、B 拿 tok-b）；token 缓存命中留痕恰 1。

### gzip_replay_test.go —— gzip(压缩编码) 回放

- `TestGzipContentEncodingMissHitReplay`（P1）
  - 易错点：代理解压或转存压缩流，或 HIT 回放丢 Content-Encoding 头 → 客户端二次解压失败（「缓存文件乱码能不能复用」取证结论固化）。
  - 断言：MISS body 为原始压缩字节（逐字节同一份，不解压不转存）；Content-Encoding / Content-Type 原样透传；首次请求非 HIT；HIT 的头与体和 MISS 完全一致；Content-Length 正确；上游恰 1 次（第二次走本地缓存）。

### cache_janitor_tolerance_test.go —— janitor(清理器) 容错

- `TestCleanToleratesAbnormalFiles`（P1）
  - 易错点：janitor 把异常文件当损坏清掉 → 半截读回 short read(短读)（现场权限事故链）。
  - 断言：目录形态条目被跳过不删除；垃圾文件、截断条目、0 字节半截、.tmp- 残留逐类按文档语义清理；新鲜半截（活跃下载）保留；有效条目与永久条目（ttl_seconds=0）不误删；Clean 返回删除数；二次 Clean 幂等（删除 0）。

### merger_pump_race_test.go —— pump(泵送循环) 竞态压力

- `TestMergerPumpRaceStress`（P1，6 轮子测试交替）
  - 易错点：pump 协程启动即无锁读 src.res.body，take() / closeAll() 在锁外写 body=nil（历史 -race 复现竞态）。
  - 断言：正确与错位补充源交替 6 轮，逐轮客户端完整且内容正确；0 条对账冲突（补充源要么被剪要么全对）；胜者与补充源各恰 1 次请求；错位轮必有剪枝行、正确轮 0 剪枝。-race 下锁定竞态窗口；非 -race 运行退化为功能回归。

### docker_type_gating_test.go —— 规则缺 type: docker 的门控族

- `TestBlobConcurrencyGatedByRuleType`（P1，成对 2 子测试）
  - 易错点：特性静默失效不可观测；或误以为缺 type 也会补充。
  - 断言：正向（type: docker）→ 补充间隔到期后第二候选收到 Range 续传，留痕「补充多源并发下载」，无 not_docker_rule 留痕；负向（type 留空）→ 第二候选 0 请求，客户端仍收全量正确数据，留痕「未补充多源并发 reason=not_docker_rule」，无补充留痕。

### multisource_merge_test.go —— 多源合并共识（docker 可变目标）

- `TestMultiSourceTagsUnion`（P2）
  - 易错点：tags(标签列表) 合并取单源覆盖，慢源漏 tag。
  - 断言：快源 [v1,v2] + 慢源 [v2,v3] → 并集去重 [v1,v2,v3] 字典序；X-Cache: BYPASS(旁路，可变目标不缓存)；留痕 merged=3。
- `TestMultiSourceReferrersDedup`（P2）
  - 易错点：referrers(引用者) 描述符按单键去重，同 digest(内容摘要) 异 mediaType 被误删。
  - 断言：按 mediaType+digest 组合键去重；重复项只留一份、同 digest 异 mediaType 各自成条；恰 4 条；留痕 merged=4。
- `TestMultiSourceManifestTiePicksFastest`（P2）
  - 易错点：manifest(镜像清单) 摘要共识平票无裁决规则。
  - 断言：偶数源 1:1 平票取最快成功者；胜出 body 与 Docker-Content-Digest 均为快源；留痕 agree=1 disagree=1；不出现 agree=2。

## 跳过的子场景与原因

以下子场景经公开 API 实证后标注跳过。跳过决定记录在对应测试文件头部注释。

- 预取任务超时耗尽预算（「5min 预算耗尽放弃剩余 blob」）：prefetchTaskTimeout(预取任务超时) 是包级常量。它运行在独立 ctx(上下文)，与请求解耦。公开 API 无法缩短，确定性单测不可驱动。补偿覆盖：TestPrefetchMaxLayerLastNotStarved 锁定同一饥饿链路的可观察契约（串行推进到每一层、flight 合并等待有界且不阻塞后续层）。
- janitor 真实 EACCES：EACCES(权限拒绝) 子场景需要「无权限文件」。本环境以 root 运行，chmod(改权限位) 无法阻止 root 读取，读取失败不可复现。补偿覆盖：「root 属主有效条目不被误删」在本环境即默认形态，由 TestCleanToleratesAbnormalFiles 的有效条目断言直接覆盖。
- 门控族前提实证修正（三项不写负向断言）：
  - slowdownload(判速升级)：确受规则 type 门控。proxy 包已有负向用例 TestSlowDownloadRuleTypeWebNoEscalation。tests 包不重复登记。
  - partial-resume(半截续传) 与宽限终止：实证不受规则 type 门控。type 留空时半截续传完整接管，立即响应、候选命中、宽限终止日志齐全。「静默关闭」断言与现实现相悖（红测），不写。
  - prefetch(预取)：实证不受规则 type 门控，且触发链路不可达。manifest GET 按 docker-mutable(可变目标) 走 BYPASS，不经 finalizeFetchEntry。公开 API 无法构造可断言形态，不写。
