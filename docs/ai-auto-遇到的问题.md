# proxy-cache 历史问题清单

本清单整理自三类材料：本工作区会话目录（活跃区 + 搬移区，约 110 个，含子会话记录）、`git log` 全部提交记录、`tests/README.md` 的现有测试清单。统计口径**截至 2026-11-20**；本版新增「**原始错误文案**」维度，并按**错误族**重新分组：每条先给当时逐字出现的报错原文，再说明「这句原文在说什么、为什么会引出该问题」。原文**逐字摘自历史会话**，不改写、不美化：整行照抄；只有超长日志行按字段截断，并在条目里用「（截断：…）」标注截断起点。代码块内一律是**连续原文片段**，不用省略号拼接。会话中以 JSON 转义形式保存的日志（如 `\"`、`\n\t`）按原样保留其转义字符。每条给出 原文 / 含义 / 出处（会话 id + 记录序号 seq + 时间）；会话 id 与索引目录名一致（活跃区多为 `session-<uuid>`，搬移区早期会话为裸 uuid）。「出现 N 个会话」= 该条所引的文案（或其截断前缀）作为**原始日志行 / 客户端输出 / 用户原话**，出现在 `user/message` 或 `tool/result` 记录里、按会话去重；assistant 的转述、源码与文档里的字面引用、以及事后专门挖掘/整理历史对话的会话都不计。同一错误族与其中单个实例（某一行、某个 digest）不同时，逐处分别写成「该实例 N 个会话；族共 M 个会话」。「另见」只列该条错误真实再现（日志或用户粘贴输出）的额外出处。

## 分组总览

| 分组 | 错误族（组名用代表性原文） | 条数 | 序号 | 这一族在说什么（一句话） |
| ---- | -------------------------- | ---- | ---- | ------------------------ |
| 1 | `blob 回源内容摘要不符，拒绝写缓存` | 2 | 1、22 | 上游回的字节与请求 digest 不符，写缓存前被拦下，但已直传给客户端的段收不回。 |
| 2 | `502 Bad Gateway` | 3 | 3、5、10 | 候选源不可用或被预算提前判死，对外只剩 502 或让客户端干等到超时。 |
| 3 | `write: broken pipe` | 2 | 4、9 | 连接中途断掉：客户端断开还继续读上游，上游断流又留下半截文件。 |
| 4 | `x509: certificate signed by unknown authority` | 1 | 20 | 上游证书链不受信，候选探测把每轮重试都烧在证书错误上。 |
| 5 | `permission denied` | 1 | 21 | 9p 盘上缓存文件属主/权限与运行用户不匹配，读写都被拒（另附 #15 一条同根因原文）。 |
| 6 | `cannot allocate memory` | 1 | 16 | 整块读缓存体打爆内存，读失败又被当成「文件损坏」直接删缓存。 |
| 7 | `必须以 http:// 或 https:// 开头` | 2 | 6、7 | 把配置翻译成上游 URL 这一步出错：模板校验、裸路径无 scheme、Host 不参与匹配。 |
| 8 | `descriptor is neither a manifest or index` | 3 | 2、12、19 | 该不该缓存判错：Range 判旁路、304 判失败、可变目标被缓存。 |
| 9 | `is not in std` | 3 | 11、14、15 | 构建与代码本身不成立：源码包被排除、vet 不过、竞态、续传读半截被拒（客户端 0 字节短读）。 |
| 10 | `aname=drvfs` | 2 | 17、18 | 运行环境坏在文件系统：9p 挂载不稳、`/dev/null` 被换成普通文件。 |
| 11 | `（无报错文案）` | 2 | 8、13 | 没有报错文案：纯性能损失与用户口述的「看起来坏了」。 |

## 组 1｜内容校验失败：`blob 回源内容摘要不符，拒绝写缓存`

**这一族在说什么**：上游某个候选源回的字节与请求的 digest 不符（坏字节 / 错误内容）。写缓存前的 sha256 强校验能把这种响应拦在库外，但**已经直传给 docker 客户端的那一段收不回来**——客户端自己按 digest 校验时才发现不符，于是 commit 失败、整次 pull 断掉；`window size exceeded` 是客户端 copy 阶段窗口越界，等价于数据流在中途被错位字节打断，是同一件事在客户端侧最先炸出来的症状。

**涉及问题**

| 序号 | 问题（简单标题） | 是否已解决 | 总结（30 字以内） |
| ---- | ---------------- | ---------- | ----------------- |
| 1 | 多源拼接坏字节 | 已解决（8fab96b） | 坏源错位数据被拼进客户端流；现写缓存前强校验并交叉对账 |
| 22 | 上游源回错内容 | 部分解决 | 个别镜像站回错字节，落盘前有校验，直传仍无校验 |

**原始错误文案（逐字）**

- **#1 多源拼接坏字节** · 客户端 docker pull 输出 · session-2ee568c5-6e4e-44ce-a573-df0bcd0b4da7 seq=9（2026-11-14T17:04:48+08:00）

  ```text
  failed to copy: window size exceeded
  ```

  含义：客户端在 copy 阶段按窗口校验收到的字节，窗口对不上（多源拼接把坏源的错位数据拼进了流）就整次 pull 中止。这句是用户最先看到的失败，本身不含原因，原因在下一条服务端日志里。出现 1 个会话。

- **#1 多源拼接坏字节** · 服务端日志（截断：该行 msg 字段起，保留 actual_digest 与 body_bytes） · session-2ee568c5-6e4e-44ce-a573-df0bcd0b4da7 seq=106（2026-11-14T17:08:35+08:00）

  ```text
  msg(消息)="blob 回源内容摘要不符，拒绝写缓存（防毒缓存：上游返回了错误内容）" source(来源)=prefetch image(镜像)=adockero/webai-2api digest(摘要)=sha256:a0838251947604c749da9678ec604482b10f0f3dcd4246eda5cb9121db4060c5 rule(规则)=docker digest(摘要)=sha256:a0838251947604c749da9678ec604482b10f0f3dcd4246eda5cb9121db4060c5 actual_digest(实际摘要)=sha256:d8808a1afde098b3c05100b914f765280d78e33377abe572de8254744703e7f0 body_bytes(响应体字节)=694 target(目标)=v2/adockero/webai-2api/blobs/sha256:a0838251947604c749da9678ec604482b10f0f3dcd4246eda5cb9121db4060c5
  ```

  含义：prefetch 路径回源同一个 blob，算出的 `actual_digest` 与请求的 digest 不符、`body_bytes=694`，写缓存前的强校验因此拒绝入库（防投毒）。也就是说上游确实回了错误载荷——这就是拼进客户端流的坏字节的内容级证据。`blob 回源内容摘要不符` 族 3 个会话；同一文案另见 session-944d8ef7-30e7-4c54-a56c-102be3893bad seq=308（2026-11-13T19:08:34+08:00）。

- **#22 上游源回错内容** · 客户端 docker pull 输出 · session-f2a800c1-cc37-4c0d-bf53-d0ba5004b082 seq=681（2026-11-07T15:12:52+08:00）

  ```text
  failed commit on ref "layer-sha256:2f1083db9fd6034465f38075ee9819d791b39750b28b1a291825e1eafb79ec04": commit failed: unexpected commit digest sha256:057194ca6d0ea458b32a86474ed12f15c7c77bcdf3bf2df2d129c03e229e1319, expected sha256:2f1083db9fd6034465f38075ee9819d791b39750b28b1a291825e1eafb79ec04: failed precondition
  ```

  含义：docker 收完一层后先算 digest 再 commit，算出来的 `057194ca…` 与期望的 `2f1083db…` 不符 → 这一层 commit 失败（`failed precondition`），整次 pull 报错。日志里连试 10 次都是同一个错哈希，说明坏内容已经被拼成了固定结果。`unexpected commit digest` 族 3 个会话。

- **#22 上游源回错内容** · 客户端 docker pull 输出 · session-6f00530e-cbb4-458d-8664-d518b3c203b3 seq=675（2026-11-20T18:31:41+08:00）

  ```text
  failed commit on ref "config-sha256:e2ac70e7319a02c5a477f5825259bd118b94e8b02c279c67afa63adab6d8685b": commit failed: unexpected commit digest sha256:05360c9edc1d0e5f6af0729a1b6db1e9554d0a0333c494b4fbd2588fb708cac5, expected sha256:e2ac70e7319a02c5a477f5825259bd118b94e8b02c279c67afa63adab6d8685b: failed precondition
  ```

  含义：同一个症状这次落在 **config blob** 上（拉 hello-world 首拉即中），说明不是某一层内容特殊，而是上游回错字节、直传段没有任何校验；重试一次就恢复正常，也印证了是偶发的错误载荷而不是缓存里固化的坏条目。该 config 实例 1 个会话；`unexpected commit digest` 族共 3 个会话。

## 组 2｜上游不可用：`502 Bad Gateway`

**这一族在说什么**：候选源要么真的不可用（匿名 401、404、反爬 HTML），要么**被预算提前判死**（当时探测 5s、扫描 20s 到点即取消；HEAD 对 docker blob 的扫描总预算已改为 60s 下限并按候选数放大）。对外只剩两种表现：代理直接回 `502 Bad Gateway`，或者迟迟不回首字节、让 docker 客户端等到超时。

**涉及问题**

| 序号 | 问题（简单标题） | 是否已解决 | 总结（30 字以内） |
| ---- | ---------------- | ---------- | ----------------- |
| 3 | 探测与扫描预算失准 | 已解决（e84d860） | 探测预算吃掉blob预算并误杀活体流；现只约束选源 |
| 5 | 上游 401 与 HTML 毒化 | 已解决（7627c61） | 401全败502、HTML页被缓存；现换证重试并拒判 |
| 10 | 客户端零进度 | 已解决（f208959） | 响应头被候选扫描拖过30s；现先commit再回放前缀 |

**原始错误文案（逐字）**

- **#3 探测与扫描预算失准** · 服务端日志（截断：该行 err 字段起） · session-24db0992-1a92-48da-9570-50883f37f98f seq=50（2026-11-06T14:06:42+08:00）

  ```text
  err="probe-timeout（候选探测预算 5s 内响应头/首块未就绪）: 读取上游响应失败（https://docker.1panel.live/v2/adockero/opencode-to-openai/blobs/sha256:98cdbf92430c3e82913a543f0a8ce202459dada6048bd9d6e02989894e6a3a26）: context canceled"
  ```

  含义：候选 `docker.1panel.live` 其实已经回了 206，但探测预算把「发请求 + token dance + 首块就绪」一起计时，5 秒到点就 `context canceled` 判硬失败，后面的重试也一并跳过。`probe-timeout（候选探测预算 5s 内响应头/首块未就绪）` 族 3 个会话；另见 session-683da33e-3637-4d76-ab24-252f4d45a148 seq=4（2026-11-06T14:07:46+08:00）。

- **#3 探测与扫描预算失准** · 服务端日志 · 683da33e-3637-4d76-ab24-252f4d45a148 seq=289（2026-11-06T14:25:30+08:00）

  ```text
  time=2026-11-06T06:25:17.814Z level=ERROR msg="url-redirect 候选扫描总预算耗尽，提前终止候选扫描" budget=20s round=2 round_total=2 last_error="probe-timeout（候选探测预算 5s 内响应头/首块未就绪）: 请求上游失败（https://docker.m.daocloud.io/v2/adockero/openco
  ```

  含义：第二次复现：一整轮候选扫描的 20 秒总预算先耗尽，扫描被提前终止，`last_error` 仍是上一条 probe-timeout——预算层层叠加，blob 自己的 30 秒探测时间根本没轮到。出现 1 个会话。

- **#3 探测与扫描预算失准** · 服务端日志（同一会话里一次 502 的收尾行） · 683da33e-3637-4d76-ab24-252f4d45a148 seq=206（2026-11-06T14:22:35+08:00）

  ```text
  time=2026-11-06T06:08:12.676Z level=ERROR msg=请求处理失败 status=502 error="全部 url-redirect 候选失败，路径目标无直连回退（无完整原始 URL 可直连）: probe-timeout（候选探测预算 5s 内响应头/首块未就绪）: 读取上游响应失败（https://docker.1panel.live/v2/adockero/opencode-to-openai/blobs/sha256:98cdbf92430c3e82913a543f0a8ce202459dada6048bd9d6e02989894e6a3a26）: context canceled"
  ```

  含义：预算耗尽后的对外结果：路径目标没有可直连的完整原始 URL，没有直连回退，于是整条请求以 502 收尾。客户端看到的 `502 Bad Gateway` 就是这么来的。

- **#5 上游 401 与 HTML 毒化** · 客户端 docker pull 输出 · session-e0df265d-62e8-4c84-9599-2ef86b76ba81 seq=8（2026-10-10T16:34:10+08:00）

  ```text
  error from registry: failed to copy: httpReadSeeker: failed open: unexpected status from GET request to http://registry-proxy-cache.linkease.net:5480/v2/adockero/nohost/blobs/sha256:1f27999aa90ab64a87bed10fe9268e7cd15ec6479631196c49529aa4cd4a41cd: 502 Bad Gateway
  ```

  含义：blob GET 直接拿到 502。当时上游对匿名请求回 401，代理不会做 token dance，所有候选都失败后 502 原样透给客户端，于是「拉一个公开镜像」也拉不动。出现 11 个会话（`502 Bad Gateway` 族）；另见 session-44822c78-6ca0-4ddf-bb9b-a400605f03df seq=15（2026-11-07T09:39:31+08:00，用户贴的同一类 pull 输出，状态码处被 `…` 截断，看不到 `502 Bad Gateway` 字样）。

- **#5 上游 401 与 HTML 毒化** · 客户端 docker pull 输出 · session-a09849e3-2cb3-485f-9107-a13f9f5b17eb seq=10（2026-11-07T09:39:31+08:00）

  ```text
  error from registry: unexpected status from GET request to http://registry-proxy-cache.linkease.net:5480/v2/adockero/opencode-to-openai/referrers/sha256:d257b2849e4ff0e7f8ae1406f49c79e98289007854f1473ee5e43a88f6b938dc: 502 Bad Gateway
  ```

  含义：层都拉完了，最后问 `referrers/<digest>` 时上游回 404，代理把它放大成 502，docker 因此把整次 pull 判失败——一个本可容忍的「没有 referrers」变成了致命错误。该 `referrers` 行 3 个会话；`502 Bad Gateway` 族共 11 个会话。

- **#5 上游 401 与 HTML 毒化** · 客户端 docker pull 输出 · session-a09849e3-2cb3-485f-9107-a13f9f5b17eb seq=255（2026-11-07T10:34:52+08:00）

  ```text
  failed to unpack image on snapshotter overlayfs: unexpected media type text/html for sha256:e0085ae8abbbbdc5d3508184dfdbf08e117de93e9aea286a289944606e3d9481: not found
  ```

  含义：上游镜像站对 registry API 回了 200 + 中文反爬 HTML 页，代理只看了状态码就当成功响应写进缓存；docker 解包时发现该 digest 的内容是 `text/html`，直接 `not found`。这就是「HTML 毒化」。`unexpected media type text/html` 族 5 个会话；另见 session-4e028ec7-3e35-4cda-892a-74da2fd008b7 seq=15（2026-11-07T15:12:02+08:00）。

- **#10 客户端零进度** · 客户端 docker pull 输出 · session-27fe905b-fa22-4fb7-bd92-94d0b78f4e9c seq=8（2026-10-12T15:08:28+08:00）

  ```text
  failed to copy: httpReadSeeker: failed open: failed to do request: Get "https://registry-proxy-cache.linkease.net:5480/v2/adockero/claude-code-ui/blobs/sha256:78129a0976a23edec1b2a88a8f9f839a79efe33d956f7fef3b202e968ab2b48f": net/http: timeout awaiting response headers
  ```

  含义：docker 卡在「等响应头」这一步超时。那时代理还在做候选扫描、一个字节都没回，客户端只能断开重试，用户永远看不到进度行（零进度）。该 digest 实例 2 个会话；同一句 `net/http: timeout awaiting response headers` 在 README / 测试代码里还被反复引用，那些不算「出现」。

- **#10 客户端零进度** · 用户口述 · session-06d26e50-e9b0-4f62-b844-1386d98465d4 seq=10（2026-11-14T15:15:03+08:00）

  ```text
  代理已经在下载，docker客户端这边应该给点进度，而不是一点进度都没
  ```

  含义：用户原话（不是报错）：服务端日志在刷「blob 半截续传进度」，客户端 pull 却一行进度都没有。说明服务端在干活、但响应头还没 commit 给客户端，客户端连进度条都起不来——这是 #10 的用户侧症状描述。该原话 2 个会话。

## 组 3｜流中断与客户端断开：`write: broken pipe`

**这一族在说什么**：这一族都是「连接中途没了」：客户端先断开（`write: broken pipe`），或上游 HTTP/2 把流掐断（`PROTOCOL_ERROR`）。前者暴露的是「断开了还继续读上游」，后者暴露的是「200 没有 Content-Length 时不升级多源」，两者都会留下半截文件或让客户端拿到不完整数据。

**涉及问题**

| 序号 | 问题（简单标题） | 是否已解决 | 总结（30 字以内） |
| ---- | ---------------- | ---------- | ----------------- |
| 4 | 客户端断开语义 | 已解决（b251149） | 断开后仍读上游1.5小时；现断开即停并保留半截 |
| 9 | 单层大 blob 无并发 | 已解决（a28a014） | 1.32GiB层单路53KB/s；现按间隔补源并发 |

**原始错误文案（逐字）**

- **#4 客户端断开语义** · 服务端日志 · session-124a15cf-d426-4e98-8195-f9dfba582e7a seq=264（2026-11-06T09:59:47+08:00）

  ```text
  time=2026-11-06T01:31:26.815Z level=WARN msg=写响应体失败（客户端已断开，继续读上游仅填缓存） target=/v2/adockero/deepseek-harness/blobs/sha256:4a266315f936ba13b9ef0e11cb158cf4d6e51288dddf8231ad4a9ef722a4f340 err="write tcp 172.19.0.4:8080->10.8.8.32:44576: write: broken pipe"
  ```

  含义：往客户端写响应体时报 broken pipe：客户端已经断开。旧语义是「继续读上游仅填缓存」，于是这路回源没有停：按当时速度（60–90KB/s）估算读完 500MB 要约 1.5 小时；断开时这路已跑 `elapsed=993.4s`（约 16.5 分钟）、只到 19%，白占上游连接。`write: broken pipe` 族 10 个会话。

- **#4 客户端断开语义** · 服务端日志（紧邻的进度行） · session-124a15cf-d426-4e98-8195-f9dfba582e7a seq=264（2026-11-06T09:59:47+08:00）

  ```text
  time=2026-11-06T01:31:26.815Z level=INFO msg=流式透传进度 rule=docker candidate=https://docker.1panel.live/v2/adockero/deepseek-harness/blobs/sha256:4a266315f936ba13b9ef0e11cb158cf4d6e51288dddf8231ad4a9ef722a4f340 status=206 bytes=96964608 content_length=500096322 percent=19 speed=48742B/s elapsed=993.4s client=disconnected
  ```

  含义：同一条连接上的进度行：`status=206`、已传 96,964,608 字节、`content_length=500096322`、`speed=48742B/s`、`elapsed=993.4s`，末尾 `client=disconnected`。它证明断开之后这路确实还在跑，且速度只有 48KB/s——「断开即停」就是针对这条日志改的。另见 session-6f00530e-cbb4-458d-8664-d518b3c203b3 seq=290（2026-11-20T16:34:09+08:00，复盘回放导出，引用行在 `msg=流式透传进度` 处被截断，只能看到开头）；完整行另见 18a7a004-2c2b-43bd-80ec-d32cc8b99e4b seq=96（2026-11-16T09:27:19+08:00，另一份导出里保留了整行）。

- **#9 单层大 blob 无并发** · 客户端 docker pull 输出 · session-944d8ef7-30e7-4c54-a56c-102be3893bad seq=10（2026-11-13T17:26:30+08:00）

  ```text
  9382755a046f: Downloading [=================================>                 ]  947.9MB/1.419GB
  ```

  含义：1.32GiB 的单层只有一路连接，进度条 19 分钟基本停在 947.9MB/1.419GB（约 53KB/s）。客户端没有报错，只是「永远下不完」——这是无并发补源时的典型表现。出现 1 个会话。

- **#9 单层大 blob 无并发** · 服务端日志 · 6b88d798-862c-42c6-bd75-87668c108de0 seq=77（2026-11-13T17:28:40+08:00）

  ```text
  time(时间)=2026-11-13T09:03:45.637Z ERROR msg(消息)=流式回源读上游失败，终止本次响应且不写缓存（半截数据绝不入库） source(来源)=client image(镜像)=adockero/camoufox digest(摘要)=sha256:9382755a046f591d82990a377ec3539289c45cbd72f21afd46d8aec3f4d34c81 rule(规则)=docker candidate(候选源)=https://docker.1ms.run/v2/adockero/camoufox/blobs/sha256:9382755a046f591d82990a377ec3539289c45cbd72f21afd46d8aec3f4d34c81 status(状态码)=200 bytes(字节数)=60427286 err(错误)="stream error: stream ID 7; PROTOCOL_ERROR; received from peer"
  ```

  含义：上游 HTTP/2 直接把流掐了（`stream ID 7; PROTOCOL_ERROR`），只收到 60,427,286 字节；后续那行「blob 半截续传开始」显示总长 1,418,684,569 字节。当时 slow-download 只认 Content-Length：这条响应是 200 且没有 Content-Length，判不出总长、升级多源的条件不成立而静默跳过（HEAD 已加 manifest 里的 layer size 兜底），所以整层只能一路硬撑，最后留下 `.part` 半截。出现 1 个会话。

## 组 4｜TLS 证书不受信：`x509: certificate signed by unknown authority`

**这一族在说什么**：候选源的证书链不受信（自签 / 链不全 / 主机名不匹配）。tls 握手阶段就失败，跟镜像内容无关；但候选轮询会把每轮 4 次重试全部烧在同一个证书错误上，看起来像「上游全挂了」。

**涉及问题**

| 序号 | 问题（简单标题） | 是否已解决 | 总结（30 字以内） |
| ---- | ---------------- | ---------- | ----------------- |
| 20 | 上游证书不受信 | 已解决（5b13a52） | x509不受信每轮空烧4次重试；现三级忽略开关 |

**原始错误文案（逐字）**

- **#20 上游证书不受信** · 服务端日志（截断：该行 err 字段起） · session-5c178c52-4042-44bd-9026-0a3534393e0a seq=3（2026-10-12T16:56:15+08:00）

  ```text
  err="请求上游失败（https://docker.actima.top/v2/adockero/claude-code-ui/blobs/sha256:968455f5deaf4b4d2914eb8b9bdb85f10a56363c28b134f7ede31dd9ab56fcb7）: Get \"https://docker.actima.top/v2/adockero/claude-code-ui/blobs/sha256:968455f5deaf4b4d2914eb8b9bdb85f10a56363c28b134f7ede31dd9ab56fcb7\": tls: failed to verify certificate: x509: certificate signed by unknown authority"
  ```

  含义：`docker.actima.top` 的证书链不受信（unknown authority），候选探测每试一次就失败一次；一轮候选 4 次重试全烧在这条错误上，最后整次 pull 失败。该 `actima.top` 实例 1 个会话；`x509: certificate signed by unknown authority` 族 7 个会话。

- **#20 上游证书不受信** · 服务端日志（截断：该行 err 字段起的 Head 部分） · session-847454be-43d3-49a1-aa2c-37bb439b043b seq=147（2026-11-06T10:40:21+08:00）

  ```text
  Head \"https://kspeeder.common-all:5443/v2/foxhui/webai-2api/manifests/latest?ns=docker.io\": tls: failed to verify certificate: x509: certificate is valid for *.linkease.net, linkease.net, not kspeeder.common-all"
  ```

  含义：另一种形态：内网 `kspeeder.common-all:5443` 的证书只对 `*.linkease.net` 有效，用主机名访问必然主机名不匹配；同样是每轮 4 次空转。这两种形态促成了后来的三级 `insecure` 开关（全局 / 规则级 / 候选级）。`certificate is valid for *.linkease.net` 该族 2 个会话；另见 session-1631cfb7-a508-44ad-bc6b-465d35d7e99d seq=96（2026-11-06T10:42:01+08:00）。

## 组 5｜权限与属主：`permission denied`

**这一族在说什么**：缓存目录挂在 9p（drvfs）上，服务写出来的文件属主是 root、权限 0600；容器一旦以镜像内的 app 用户（uid 1000 / 10001）运行，读缓存和写缓存都会 `permission denied`。旧代码把「读失败」当「文件损坏」删掉，于是每个请求都在删缓存再回源。补充：镜像默认仍是 `USER app`（uid 1000），本次部署是用 `docker run --user 0:0` 覆盖运行，所以下面表里「现以root运行」说的是容器启动参数，不是镜像默认用户。

**涉及问题**

| 序号 | 问题（简单标题） | 是否已解决 | 总结（30 字以内） |
| ---- | ---------------- | ---------- | ----------------- |
| 21 | 缓存文件属主错乱 | 已解决 | 9p盘上缓存文件属主为root，app用户读不了；现以root运行 |

**原始错误文案（逐字）**

- **#21 缓存文件属主错乱** · 服务端日志 · session-6f00530e-cbb4-458d-8664-d518b3c203b3 seq=641（2026-11-20T18:24:52+08:00）

  ```text
  time(时间)=2026-11-20T10:23:07.488Z level(级别)=WARN msg(消息)="读取缓存文件遇到临时 IO 错误（已保留缓存文件），本次按未命中回源" file(文件)=/app/cache-data/cache-docker/edff7f45184add72ac4069055dbc3a5cded86c89f876376779766d85baeaf6b2 err(错误)="打开缓存文件失败: open /app/cache-data/cache-docker/edff7f45184add72ac4069055dbc3a5cded86c89f876376779766d85baeaf6b2: permission denied"
  ```

  含义：命中缓存后打开文件被拒：9p 上该文件属主 root、权限 0600，而进程是 app 用户。同一行前半句是「保留缓存文件，本次按未命中回源」——新代码已经不删了，但文件仍然读不到，等于缓存永远不生效。出现 1 个会话。

- **#15 构建与部署事故** · 附带原文（写入侧的同一类属主问题） · session-24db0992-1a92-48da-9570-50883f37f98f seq=784（2026-11-06T15:13:57+08:00）

  ```text
  time=2026-11-06T07:13:17.683Z level=WARN msg=写入缓存失败 source=client image=library/node digest=sha256:14f8540414db8ad842e3ed959a3c7e3e25a13e5981a5b3c2a1443a336907ce94 key=be5ba4efa70b1d2bf3c4413a538905fbd0dff4f92674d3ec9e359975187cc1c7 target=v2/library/node/blobs/sha256:14f8540414db8ad842e3ed959a3c7e3e25a13e5981a5b3c2a1443a336907ce94 err="创建缓存临时文件失败: open /app/cache-data/cache-docker/.tmp-151736862: permission denied"
  ```

  含义：新镜像以 app（uid 10001）运行、缓存目录仍是 root 属主：连临时文件都建不出来，docker 的层永远写不进缓存（每条 blob 都是「写入缓存失败」）。这条属于 #15，原文列在这里是因为它与 #21 是同一个根因的写入侧表现。

## 组 6｜内存不足与缓存误删：`cannot allocate memory`

**这一族在说什么**：读取侧用 `io.ReadAll` 把整个缓存体读进内存，767MB 的层约需 1GB 峰值内存；申请失败时报 `cannot allocate memory`。真正致命的是下一句：读取失败被**一律当成「文件损坏」**，于是正常缓存被删掉、请求转成未命中，回源到慢速镜像站。

**涉及问题**

| 序号 | 问题（简单标题） | 是否已解决 | 总结（30 字以内） |
| ---- | ---------------- | ---------- | ----------------- |
| 16 | 大缓存整块读内存 | 已解决（45a5b9e） | 767MB整块读入内存爆掉后删缓存；现改流式读写 |

**原始错误文案（逐字）**

- **#16 大缓存整块读内存** · 服务端日志 · session-6f00530e-cbb4-458d-8664-d518b3c203b3 seq=80（2026-11-20T16:11:55+08:00）

  ```text
  time(时间)=2026-11-20T08:03:40.112Z level(级别)=WARN msg(消息)=缓存文件损坏，删除后按未命中处理 file(文件)=/app/cache-data/cache-docker/d8d313ac3cd9e1ab4152b5b80f20648da451ccee1066c1c40987412d3d95d83a err(错误)="读取缓存体失败: read /app/cache-data/cache-docker/d8d313ac3cd9e1ab4152b5b80f20648da451ccee1066c1c40987412d3d95d83a: cannot allocate memory"
  ```

  含义：一行日志里同时有「判决」和「证据」：判决是「缓存文件损坏，删除后按未命中处理」，证据却是 `读取缓存体失败: read …: cannot allocate memory`——文件本身没坏，是内存申请失败。删掉之后同一请求转未命中，回源到约 40KB/s 的慢源，5 分 25 秒只取到 11,060,758 字节就被客户端断开。该缓存文件实例 1 个会话；`cannot allocate memory` 族 2 个会话。

## 组 7｜配置与 URL 解析：`必须以 http:// 或 https:// 开头`

**这一族在说什么**：两类都出在「把配置翻译成上游 URL」这一步：当时的启动校验要求候选模板字面以 `http(s)://` 开头，于是 `template: $1`（表示原始目标 URL）被拒（bbf0b2c 之后已放行）；而即使放行，路径目标下 `$1` 展开成的是裸路径 `v2/...`，没有 scheme，`url.Parse` 之后请求直接报 `unsupported protocol scheme ""`。入站 Host 早期不参与匹配，是同一族的另一半。

**涉及问题**

| 序号 | 问题（简单标题） | 是否已解决 | 总结（30 字以内） |
| ---- | ---------------- | ---------- | ----------------- |
| 6 | 配置语义陷阱 | 部分解决（bbf0b2c） | ttl:0与规则级缓存已修；缺type:docker仍静默 |
| 7 | 入站 Host 未参与匹配 | 已解决（eb1229f） | Host不参与匹配、$1展开成裸路径；现均已修正 |

**原始错误文案（逐字）**

- **#6 配置语义陷阱** · 用户口述 · session-ba123bff-bd63-4953-8f2c-a5ea6e00ac9e seq=8（2026-11-05T16:12:16+08:00）

  ```text
  怎么配置原url就提示必须http开头的，处理下希望原url也参与候选，只是走的是代理的方式
  ```

  含义：用户原话（不是日志）：他按「让原始目标 URL 也参与候选」的思路配了模板，启动时被拒，只记得提示里要求 http 开头。当时没有把报错原文贴回来，下面是会话里逐字引用到的校验文案出处。同一句 `必须以 http:// 或 https:// 开头` 在 2 个会话的原始输出里出现；用户原话本身 1 个会话。

- **#6 配置语义陷阱** · 服务端启动校验的错误文案（会话内逐字引用的源码行） · session-ba123bff-bd63-4953-8f2c-a5ea6e00ac9e seq=22（2026-11-05T16:14:50+08:00）

  ```text
  Line 1456: 			return fmt.Errorf("%s 第 %d 个候选模板 %q 必须以 http:// 或 https:// 开头", where, i, t)
  ```

  含义：这句就是启动校验真正返回的文案模板（`config` 的 url-redirect 候选模板校验）：先要求模板 trim 后以 `http://` / `https://` 开头，再要求含已知占位符。当时 `template: $1` 不以 http 字面开头，所以被它拦下——报错语义与用户记忆一致；bbf0b2c 之后 HEAD 已对 `$1` / `${1}` 显式放行（`config/config.go` 的 `validateRedirectItems`），现在只有其他非 http(s) 模板才会报这句。

- **#7 入站 Host 未参与匹配** · 服务端日志 · session-24db0992-1a92-48da-9570-50883f37f98f seq=50（2026-11-06T14:06:42+08:00）

  ```text
  time=2026-11-06T05:41:33.522Z level=WARN msg="url-redirect 候选请求失败" round=1 round_total=2 attempt=1 attempt_total=4 index=6 template=$1 candidate=v2/adockero/opencode-to-openai/blobs/sha256:98cdbf92430c3e82913a543f0a8ce202459dada6048bd9d6e02989894e6a3a26 cost=0s err="请求上游失败（v2/adockero/opencode-to-openai/blobs/sha256:98cdbf92430c3e82913a543f0a8ce202459dada6048bd9d6e02989894e6a3a26）: Get \"v2/adockero/opencode-to-openai/blobs/sha256:98cdbf92430c3e82913a543f0a8ce202459dada6048bd9d6e02989894e6a3a26\": unsupported protocol scheme \"\""
  ```

  含义：请求的 URL 是 `v2/adockero/...` 这种裸路径（`template: $1` 在路径目标下的展开结果），`Get` 阶段没有 scheme 可用，直接报 `unsupported protocol scheme ""`；每轮固定空转 4 次重试之后再换下一个候选。`unsupported protocol scheme` 族 7 个会话。

## 组 8｜缓存判定与可变目标：`descriptor is neither a manifest or index`

**这一族在说什么**：这一族是「该不该缓存 / 该不该走缓存」判错了：带 Range 就被判旁路，大层永远下不完；304 空体被判成候选失败，HIT 永远命中不了；tag / manifest 这类可变目标被当成不可变内容缓存，于是 docker 拿到旧内容或 HTML 垃圾。

**涉及问题**

| 序号 | 问题（简单标题） | 是否已解决 | 总结（30 字以内） |
| ---- | ---------------- | ---------- | ----------------- |
| 2 | Range 起点被判旁路 | 已解决（bae1c14） | 带Range即旁路致大层下不完；现按起点本地切片续传 |
| 12 | 弱 ETag 与 304 | 已解决（e026e95） | 弱ETag未剥W/前缀、条件头透传；现均已修正 |
| 19 | 可变目标被缓存 | 已解决（97fef24） | tag与manifest旧内容被缓存；现合并并跳过缓存 |

**原始错误文案（逐字）**

- **#2 Range 起点被判旁路** · 服务端日志 · session-24db0992-1a92-48da-9570-50883f37f98f seq=50（2026-11-06T14:06:42+08:00）

  ```text
  time=2026-11-06T05:41:31.421Z level=INFO msg="GET 请求绕过缓存（BYPASS）" source=client image=adockero/opencode-to-openai digest=sha256:98cdbf92430c3e82913a543f0a8ce202459dada6048bd9d6e02989894e6a3a26 rule=docker reason=request-range target=v2/adockero/opencode-to-openai/blobs/sha256:98cdbf92430c3e82913a543f0a8ce202459dada6048bd9d6e02989894e6a3a26
  ```

  含义：这个 blob 的 GET 带了 Range，`reason=request-range` 把它判成 BYPASS：不读缓存、不写缓存、也不留半截，纯透传上游。docker 拉大层时首次 `GET blob` 就带 Range（断点续传重试更是从已有偏移开始），于是缓存对这类请求完全失效，链路一断就只能从头再来。`reason=request-range` 族 3 个会话；另见 session-847454be-43d3-49a1-aa2c-37bb439b043b seq=167（2026-11-06T10:41:28+08:00）。

- **#12 弱 ETag 与 304** · 服务端日志 · session-cbe6d976-b21c-4e4d-a8b8-84da4eb64a80 seq=8（2026-10-10T09:31:35+08:00）

  ```text
  time=2026-10-10T01:26:10.607Z level=WARN msg="url-redirect 候选未通过成功判据" attempt=0 index=0 template=https://hk.gh-proxy.org/$1 candidate=https://hk.gh-proxy.org/https://raw.githubusercontent.com/dongchengjie/airport/main/subs/merged/tested_within_vless.yaml check=content status=304 body_bytes=0
  ```

  含义：客户端带条件头、上游回 `304` 空体；代理把 304 当成「这个候选没通过成功判据」（`check=content`、`body_bytes=0`），于是弃掉这个源、换下一个源重试——HIT 侧永远回不出 304。存储侧另有弱 ETag 没剥 `W/` 前缀，导致 HIT 根本无法产生 304，两处叠加。`status=304 body_bytes=0` 该行 1 个会话；另见 session-a09849e3-2cb3-485f-9107-a13f9f5b17eb seq=57（2026-11-07T09:41:30+08:00，同一 `url-redirect 候选未通过成功判据` msg，这次是 referrers 的 `status=404`，不是 304）。

- **#19 可变目标被缓存** · 用户口述 · session-4e028ec7-3e35-4cda-892a-74da2fd008b7 seq=58（2026-11-07T15:25:25+08:00）

  ```text
  有多个源返回的不缓存的路径的接口，如果是  /v2/[p#1 镜像]/tags/list应该并发请求多个轮询然后合并tags然后返回。。避免有的源没刷新镜像列表，其他不缓存的路径也是这样，规划好。。还要规划好，返回的镜像tags和镜像层包hash，应该在docker的接口流程内保持一致，规划好。。还要看下另一个当前工作区的会话看下会不会冲突
  ```

  含义：用户原话（需求，不是报错）：`/v2/<镜像>/tags/list` 这类本来就不缓存的接口，要并发问多个源、合并 tags 再返回，动机是「避免有的源没刷新镜像列表」。换句话说 tag / manifest 这类可变目标一旦被缓存（或只问一个源），就会返回旧内容。出现 1 个会话。

- **#19 可变目标被缓存** · 客户端 docker tag 输出（可变目标被缓存的直接后果） · session-a09849e3-2cb3-485f-9107-a13f9f5b17eb seq=255（2026-11-07T10:34:52+08:00）

  ```text
  Error response from daemon: error walking manifest for registry-proxy-cache.linkease.net:5480/adockero/deepseek-harness:my: descriptor is neither a manifest or index
  ```

  含义：tag 命中的是已被 HTML 毒化写入缓存的内容，docker 解析 manifest 时发现这段字节既不是 manifest 也不是 index，`docker tag` 直接失败。可变目标不该缓存、缓存了又没校验内容类型，就是这个后果。出现 1 个会话。

## 组 9｜构建与代码质量：`is not in std`

**这一族在说什么**：这一族不是线上流量问题，而是「代码/构建本身不成立」：`.dockerignore` 把源码包一起排除导致 `go build` 找不到包；`slog` 参数少写 `...` 展开，`go vet` 直接编译失败、运行时打出 `!BADKEY`；`slowMerger` 的并发竞态被 `-race` 抓到；以及续传时 `.part` 打不开（`permission denied`）让客户端只读到 0 字节、报 `unexpected EOF`。

**涉及问题**

| 序号 | 问题（简单标题） | 是否已解决 | 总结（30 字以内） |
| ---- | ---------------- | ---------- | ----------------- |
| 11 | slowMerger 并发竞态 | 已解决（b8ed8e9） | pump启动即无锁读body，被并发置空；现已提前捕获 |
| 14 | 日志规范 | 已解决（2681804） | 缺参数展开致BADKEY、键名不双语；现登记校验 |
| 15 | 构建与部署事故 | 已解决（db8fef7） | 误排源码包、uid致权限失败；现排除项与uid已修 |

**原始错误文案（逐字）**

- **#11 slowMerger 并发竞态** · go test -race 报告 · 8b4b3a11-c8dc-4289-8af0-9dbf5707cbd3 seq=293（2026-10-12T18:55:42+08:00）

  ```text
  WARNING: DATA RACE
  Write at 0x00c0000ce2f8 by goroutine 16:
    proxy-cache/proxy.(*slowMerger).take.(*probeResult).Close.func1()
        /code/www/my/github/proxy-cache/proxy/probe.go:64 +0x6f
    sync.(*Once).doSlow()
        /usr/local/go/src/sync/once.go:74 +0xf0
    sync.(*Once).Do()
        /usr/local/go/src/sync/once.go:65 +0x44
    proxy-cache/proxy.(*probeResult).Close()
        /code/www/my/github/proxy-cache/proxy/probe.go:61 +0xe97
    proxy-cache/proxy.(*slowMerger).take()
        /code/www/my/github/proxy-cache/proxy/slowdownload.go:376 +0xe0e
    proxy-cache/proxy.(*slowMerger).run()
        /code/www/my/github/proxy-cache/proxy/slowdownload.go:428 +0x2c4
    proxy-cache/proxy.TestSlowMergerMaxBehindPrune()
        /code/www/my/github/proxy-cache/proxy/slowdownload_unit_test.go:400 +0xa50
    testing.tRunner()
        /usr/local/go/src/testing/testing.go:1689 +0x21e
    testing.(*T).Run.gowrap1()
        /usr/local/go/src/testing/testing.go:1742 +0x44
  
  Previous read at 0x00c0000ce2f8 by goroutine 18:
    proxy-cache/proxy.(*slowSource).pump()
        /code/www/my/github/proxy-cache/proxy/slowdownload.go:248 +0xef
  ```

  含义：写方在 `slowMerger.take()` / `closeAll()` 里于锁外调用 `probeResult.Close` 写 body（probe.go:64），读方是刚启动的 `slowSource.pump()` 无锁读 body（slowdownload.go:248）。真跑起来时就是「pump 刚启动 body 就被置空」，读出来的是空或半截——客户端看到的就是流中断。`WARNING: DATA RACE` 族 2 个会话。

- **#14 日志规范** · go vet / go test 输出 · 6b20ca66-7230-4d63-b925-3e27f126aee0 seq=249（2026-11-06T10:39:50+08:00）

  ```text
  proxy/proxy.go:353:36: slog.Logger.Debug arg "labels.attrs(dockerLogSourceClient, \"method\", r.Method, \"remote\", r.RemoteAddr,\n\t\"target\", target, \"target_kind\", targetKind(fullURL))" should be a string or a slog.Attr (possible missing key or value)
  ```

  含义：`labels.attrs(...)` 返回 `[]any`，被当成**单个**参数传给 `slog.Debug`，少了 `...` 展开：go vet 在编译期就报错、整包 `[build failed]`；同一处 bug 运行时会把整段参数打成 `!BADKEY` 畸形字段。三处（353 / 549 / 710）是同一个错误。该 `proxy/proxy.go:353:36` 实例 1 个会话；含 `slog.Logger.Debug arg` / `!BADKEY` 的 BADKEY/vet 族 2 个会话；另见 session-124a15cf-d426-4e98-8195-f9dfba582e7a seq=285（2026-11-06T10:50:46+08:00）。

- **#14 日志规范** · 用户口述 · session-c3fe027d-59f9-4684-a63e-007d5a5d6d4d seq=28（2026-11-06T18:00:38+08:00）

  ```text
  日志输出格式不对，好设计那不是要求key要中文+英文吗
  ```

  含义：用户原话（不是报错）：日志键名要求「英文(中文)」双写（如 `msg(消息)=`），旧代码只写英文键。这是规范纠错，与上面的 vet 错误同属「日志规范」这一条。该原话 1 个会话。

- **#15 构建与部署事故** · docker build 输出 · session-ff879f33-52d2-47f0-bf10-c896b6478c40 seq=114（2026-10-09T09:06:35+08:00）

  ```text
  #19 0.243 main.go:22:2: package proxy-cache/cache is not in std (/usr/local/go/src/proxy-cache/cache)
  ```

  含义：builder 阶段 `go build` 找不到本地包 `proxy-cache/cache`，报「不在标准库」——`.dockerignore` 里写了 `cache/`（本意是排除运行时缓存目录 `cache-data/`），把源码包 `cache/` 一起排除了，COPY 进镜像的源码缺一块。`is not in std` 族 2 个会话；另见 session-f92e42d0-186e-459d-8e67-91585fcf31c5 seq=3（2026-10-09T09:07:18+08:00）。

- **#15 构建与部署事故** · 客户端 docker pull 输出 · session-944d8ef7-30e7-4c54-a56c-102be3893bad seq=313（2026-11-13T19:10:04+08:00）

  ```text
  short read: expected 129306884 bytes but got 0: unexpected EOF
  ```

  含义：docker 期望从某个 blob 读到 129,306,884 字节，结果一个字节都没拿到就 EOF。同一会话的日志显示该层 `.part` 实际已有 109,948,500 字节（`part_bytes=109948500 total=129306884`，session-944d8ef7 seq=318）；续传时打开这个 `.part` 被 `permission denied`（seq=331 的 `读半截文件失败: open …/.part-8c84…: permission denied`），所以本次一个字节都没写出去——不是「缓存里留下 0 字节的半截文件」，而是「半截在、但读不了」（属主/权限链的下游后果）。客户端 11:08:41 开始、11:10:04 失败，1 分 23 秒。该 129MB 实例 1 个会话；`short read: expected` 族 4 个会话。

## 组 10｜运行环境与文件系统：`aname=drvfs`

**这一族在说什么**：这一族没有业务报错，坏在环境：缓存目录是 Windows D 盘的 9p（drvfs）挂载，大文件读写不稳、属主也不受控；同一天 gs-mac-dind 上的 `/dev/null` 还被换成了普通文件，任何写 `/dev/null` 的工具都会真的写盘。

**涉及问题**

| 序号 | 问题（简单标题） | 是否已解决 | 总结（30 字以内） |
| ---- | ---------------- | ---------- | ----------------- |
| 17 | 缓存目录在 9p 盘 | 部分解决 | 缓存目录在9p挂载上，大文件读写不稳；迁移未做 |
| 18 | /dev/null 被换成文件 | 已解决 | 被换成普通文件致工具写失败；已用mknod恢复 |

**原始错误文案（逐字）**

- **#17 缓存目录在 9p 盘** · 无文本型报错

  > 无文本型报错：缓存目录 `/app/cache-data` 是 Windows D 盘的 9p（drvfs）挂载，当时的表现只是「大文件读写不稳」，并被同一天 767MB 层的事故放大；没有独立的报错文案。真实证据是容器内的 `mounts` 行（下一条）。迁移到 ext4（`/data-linux`）这件事至今未做。

- **#17 缓存目录在 9p 盘** · 容器内 mounts 输出（现场证据） · session-6f00530e-cbb4-458d-8664-d518b3c203b3 seq=178（2026-11-20T16:18:36+08:00）

  ```text
  D:\134 /app/cache-data 9p rw,noatime,aname=drvfs;path=D:\;uid=0;gid=0;metadata;symlinkroot=/mnt/host/,cache=0x5,access=client,msize=65536,trans=fd,rfd=5,wfd=5 0 0
  ```

  含义：这一行说明 `/app/cache-data` 的来源是 `D:\`（`aname=drvfs`），参数 `msize=65536`、`cache=0x5`、`access=client` 都是 9p 的典型取值；9p 本身不会报错，但大块读写不稳、延迟高——767MB 层的「整块读」正是在这套挂载上失败、又被判成损坏的。

- **#18 /dev/null 被换成文件** · ls -l 输出 · session-6f00530e-cbb4-458d-8664-d518b3c203b3 seq=226（2026-11-20T16:21:15+08:00）

  ```text
  -rw-rw-rw-    1 root     root           167 Nov 20 16:17 /dev/null
  ```

  含义：`/dev/null` 变成了 167 字节的普通文件，而不再是字符设备（正常应是 `crw-rw-rw- … 1, 3`）。于是任何「把输出丢进 /dev/null」的工具都真的在写盘，把 `/dev` 的 64MB tmpfs 写满；同批输出里 `/dev/random`、`/dev/zero` 仍是字符设备，只有 `/dev/null` 坏了。出现 1 个会话。

## 组 11｜无文本型报错：`（无报错文案）`

**这一族在说什么**：这两条没有报错文案：一条是纯性能损失（同一层被下载两遍），一条是用户对「缓存文件看着像乱码」的口述质疑。对这类问题只能靠现场表现和日志里的 Info/Warn 行描述。

**涉及问题**

| 序号 | 问题（简单标题） | 是否已解决 | 总结（30 字以内） |
| ---- | ---------------- | ---------- | ----------------- |
| 8 | 预取搭车重复下载 | 已解决（1c5f29f） | 等待上限10s后另起第二路完整下载；现搭车共享 |
| 13 | TTL 与 gzip 回放 | 已解决（1d6ed46） | ttl:0曾被改30m、亚秒取整；gzip原样回放已固化 |

**原始错误文案（逐字）**

- **#8 预取搭车重复下载** · 无文本型报错

  > 无文本型报错：预取在途时客户端只等 `flightWaitLimit`（10s），超时后自行回源，同一个 blob 出现第二路完整下载；日志里没有错误行，用户侧表现是同一层下载两遍、更慢。修法是让客户端请求搭上预取/在途回源（搭车共享，`1c5f29f`）。

- **#8 预取搭车重复下载** · 附带原文（WARN 级，不是错误） · session-847454be-43d3-49a1-aa2c-37bb439b043b seq=152（2026-11-06T10:40:34+08:00）

  ```text
  time=2026-11-06T02:30:04.367Z level=WARN msg="预取 blob 回源失败（不影响客户端，客户端请求照走原路径）" rule=docker name=adockero/opencode-to-openai digest=sha256:dd051e1b8c92ff101fa3071ab97ea4924885942f15644e01c3dc095764e863e3 target=v2/adockero/opencode-to-openai/blobs/sha256:dd051e1b8c92ff101fa3071ab97ea4924885942f15644e01c3dc095764e863e3 err="预取流式回源读上游失败（https://docker.1panel.live/v2/adockero/opencode-to-openai/blobs/sha256:dd051e1b8c92ff101fa3071ab97ea4924885942f15644e01c3dc095764e863e3）: context deadline exceeded"
  ```

  含义：预取失败时的留痕：明确写了「不影响客户端，客户端请求照走原路径」——预取自己没成，客户端会另开一路，于是同一 blob 两路下载。这条是 WARN，不是错误。

- **#13 TTL 与 gzip 回放** · 用户口述 · session-cbe6d976-b21c-4e4d-a8b8-84da4eb64a80 seq=87（2026-10-10T11:59:07+08:00）

  ```text
  为什么缓存的文件，查看是乱码的，这样你确定能用于下次吗
  ```

  含义：用户原话（不是报错）：直接打开缓存文件看到的是 gzip 压缩字节，所以像乱码。实际行为是对的——`Content-Encoding` 原样落盘、回放时按头还原；问题在于「直接看文件」这个观察方式。出现 1 个会话。

- **#13 TTL 与 gzip 回放** · 用户口述（同条另一个症状：ttl:0 被改） · session-c8db3ea0-cdb5-44a7-b039-b7882e56d983 seq=171（2026-10-09T11:31:08+08:00）

  ```text
  ttl 0 就是无过期时间,，改下
  ```

  含义：用户原话：`ttl: 0` 的语义是「永不过期」，旧代码把它当成没配、静默改成 30m。这条与 gzip 回放同属 #13（存储语义 / 回放语义），同样没有报错文案可用。

## 附录：2026-11-20 线上事故现场记录

- 复现时补充发现：容器以镜像内 app（uid 1000）运行时，缓存目录 /app/cache-data 是 Windows D 盘的 9p（drvfs）挂载，服务写入的缓存文件属主为 root 且权限 0600，app 用户读不了 → 每次读取都 permission denied（旧代码会当成损坏删掉，新代码保留但仍然读不到）。按历史线上行为（旧容器实际以 root 运行）改为 `--user 0:0` 重新创建容器后恢复可读。

- 当前容器的创建参数（重建时照抄，注意 `--user 0:0` 与两个 bind 挂载）：`docker run -d --name proxy-cache --network common-all -p 5480:8080 --user 0:0 -v /data-linux/data/docker/sync/proxy-cache/config.yaml:/app/config.yaml:ro -v /data/docker/proxy-cache/cache-data:/app/cache-data --restart always adockero/proxy-cache:latest`

- 复现时还观察到：个别上游镜像站（如某次 hub.rat.dev 链路）会返回与请求摘要不符的字节，写缓存前的 sha256 校验能拦住入库，但已直传给客户端的那一段无法回收，客户端表现为 `failed commit ... unexpected commit digest`；重试一次即正常。

- 现场：2026-11-20 16:03:40，gs-mac-dind（10.8.7.22）经 registry-proxy-cache 拉取镜像，767MB 那一层的缓存文件被本服务自己删除，日志为 `缓存文件损坏，删除后按未命中处理`，错误是 `read: cannot allocate memory`。

- 直接原因（读取侧）：旧版生产读取路径 `cache/cache.go` 的 `readEntry` 用 `io.ReadAll` 把整个缓存文件读进内存，767MB 的层按此计算约需 1GB 峰值内存；读取失败后该错误被一律当成「文件损坏」，直接删除缓存文件。HEAD 里 `readEntry` 只保留为测试与极小文件的兼容入口（注释已写明），生产读取改走 `Open` 流式。

- 直接原因（写入与回源侧）：旧版 `encodeEntry` 先用整块缓冲区拼出「元信息 + 完整 body」再落盘，767MB 会再复制一份；旧版 `proxy/proxy.go` 的 MISS 主路径同样用 `io.ReadAll` 收完整 body，旧版 `proxy/partial_resume.go` 的 `accum` 用 `bytes.Buffer` 累积整层（HEAD 这三处都已改 `bodyBucket` 流式）。三处叠加，容器内存峰值达 5,481,320,448 字节（约 5.48GB）。

- 后果链：缓存被误删 → 同一请求转为未命中 → 回源到慢速镜像站，实测约 40KB/s → 5 分 25 秒只取到 11,060,758 字节即被客户端断开，留下 `.part` 半截文件，镜像实际未拉取完成。

- 放大因素：容器内缓存目录 `/app/cache-data` 是 Windows D 盘的 9p（drvfs）挂载，宿主机当时剩余内存约 2GB；大块内存申请一旦失败即报 `cannot allocate memory`，正常文件因此被误判为损坏。

- 处置方向：缓存读取改为文件句柄加分块回放（`io.SectionReader`/`io.Copy`），Range 切片语义不变；写入改为同目录临时文件边收边写后原子替换，保留写前 sha256 强校验；读取失败区分真正损坏（魔数、元信息长度异常）与临时 IO 错误，临时错误重试并保留文件，不再误删；磁盘格式保持向后兼容。

- 同日附带修复：gs-mac-dind 的 `/dev/null` 被替换成了普通文件（167 字节），导致工具写入失败并写满 `/dev` 的临时盘，已用 `mknod /dev/null c 1 3` 加 `chmod 666` 恢复。

本文件由历史会话与提交记录自动整理；原始错误文案逐字摘自历史会话，新增问题请追加到对应错误族下（新错误族请新建一节）。
