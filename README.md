# proxy-cache

GitHub 反向代理 + 本地缓存服务（Go 实现），用法与 [gh-proxy](https://gh-proxy.com/) / [hk.gh-proxy.org](https://hk.gh-proxy.org) 一致：把请求路径中 `/` 后面的整段当作目标 URL，服务端回源获取内容并把 **状态码、响应头、Body 原样透传** 给客户端，同时按配置缓存。

```
curl -i "http://127.0.0.1:8080/https://raw.githubusercontent.com/dongchengjie/airport/main/subs/merged/tested_within_vless.yaml"
```

## 功能特性

- **通用反代**：支持任意 `http://` / `https://` 目标 URL（不限于 GitHub），状态码/响应头/Body 透传
- **路径目标请求（docker registry 等）**：请求路径不是完整 URL（如 `/v2/<name>/manifests/<ref>`）时识别为「路径目标」，由命中的域名规则 + url-redirect 模板拼出回源地址（详见下文「[Docker Registry 镜像加速](#docker-registry-镜像加速)」）；仅支持 GET/HEAD、必须命中规则且配置 url-redirect、无直连回退，缓存 key 含入站 Host
- **本地文件缓存**：目标 URL 的 sha256 作为缓存键（路径目标以「`http://<入站Host>/<路径目标>`」为 key 输入），命中返回 `X-Cache: HIT`，未命中回源后写缓存返回 `X-Cache: MISS`；TTL 过期自动失效（惰性删除 + 后台扫描），写入原子替换、并发安全，内置 singleflight 防缓存击穿
- **GET 路径目标流式回源（docker registry 必备）**：缓存 MISS / BYPASS 的路径目标 GET 在候选竞速选定后**立即向客户端写出响应头（commit）**，随后边读上游边写给客户端与缓存——docker 客户端等响应头约 30s 就超时断开（`net/http: timeout awaiting response headers`），流式透传把首字节时间压缩为「选定候选」的耗时，不再被大 blob 的完整下载拖超时；EOF 后按既有条件原子写缓存；commit 后客户端断开（broken pipe）则**断开即停**——立即终止上游读取与多源合并流（不再「继续读完只填缓存」长时间占用上游连接与带宽），blob 缓存可读主路径保留半截（进度不回退，下次无 Range 请求从断点无感续传），客户端将以断点续传或缓存重试接续；commit 后上游读取失败则终止响应且不写缓存（半截数据绝不入库）；带 Range 的断点续传：**docker blob 目标的单区间 bytes Range 全部本地接管**——起点 0（`bytes=0-`，docker/containerd 首次拉层就带它）等价于完整 GET（缓存 HIT / 半截无感续传 / 预取搭车照常，对客户端回 206 + Content-Range）；起点 >0（断点续传重试，docker 无论从哪个偏移都带 Range）则「缓存里有完整条目就直接从条目 body 切片回 206、发都不发上游；只有半截前缀且起点不超过半截长度时从半截的起点位置回放 + 上游剩余区间续传拼接」，无半截 / 起点超出半截（有缺口）才维持旧版 206 透传；非 blob 目标与后缀区间/多区间同样维持旧版 206 透传（完整 URL 目标（web）与 HEAD 保持整包缓冲回放不变）
- **规则级独立缓存目录**：每条 domain-rule 的 `cache` 可独立配置 `enabled`/`ttl`/`path`/`clean-interval`，支持「全局 `cache.enabled: false` 关闭缓存、命中规则的流量按规则开启并写入各自独立目录」（启动时预热创建全部缓存目录，按目录复用缓存实例与后台清理协程）
- **url-redirect 多上游轮询回源**：模板展开为多个候选代理 URL，round-robin **起始下标逐请求轮换**（第 1 个请求从第 1 条开始、第 2 个从第 2 条开始……），默认**阶梯并发竞速**（hedge：首个候选立即发出，每隔 `url-redirect-hedge-delay` 放行下一个并发探测，第一个通过成功判据的候选胜出、其余取消；`0` 退化为纯串行）；单候选探测预算 `url-redirect-probe-timeout`（响应头/判据未就绪即硬失败快切）；硬失败（403 / token dance 后仍 401 / 探测预算耗尽）跳过原地重试直接切下一候选；全部失败可回退直连原始 URL（路径目标无直连回退），极坏情况由内置扫描总预算兜底提前 502（manifest 等 20s、docker blob 60s 起）
- **url-redirect 模板变量**：除 `$1`（目标串，兼容保留）外支持请求级预定义变量 `$http.server.header.full_url`（入站请求完整 URL）与 `$http.server.header.full_url_no_server`（入站请求去掉 `scheme://host` 的 `/path?query`），均支持 `${...}` 大括号写法；按变量名长度降序替换（避免前缀遮蔽），未知 `$` 序列原样保留，详见下文「[url-redirect 模板变量](#url-redirect-模板变量)」
- **url-redirect 候选对象与候选级覆盖**：`url-redirect` 为**对象数组**（破坏性变更：旧版纯字符串写法解析期报错并给出迁移示例），候选 = `template`（原模板字符串，占位符语义不变）+ 可选 `insecure-skip-verify` / `http-proxy` / `connect-timeout` / `http-timeout` / `read-timeout` / `probe-timeout`；这些可选项均为三级优先级**候选 > 规则 > 全局**（未配置逐级继承；候选显式空串 `http-proxy` = 强制直连、显式 `0` 超时 = 不限制），适合「个别镜像站证书链不全」「单个候选必须走慢代理并放宽超时」的场景，详见下文「[url-redirect 候选对象与候选级覆盖](#url-redirect-候选对象与候选级覆盖)」
- **HEAD 走候选轮询管线**：HEAD 与 GET 一致先查缓存（命中回放响应头，无 body），未命中按候选轮询回源（方法保持 HEAD，docker registry 的 HEAD 探测同样打到镜像站）；HEAD 响应无 body 不写缓存；未配置 url-redirect 时回退直连（与旧版一致）
- **docker 镜像 layer 异步预取（manifest 驱动）**：识别出 `GET /v2/<name>/manifests/<ref>` 路径目标回源成功且响应为单平台 image manifest 后，异步预取该镜像其余 layer blob 写入缓存（`docker-prefetch-enabled` 默认开，并发 `docker-prefetch-concurrency` 默认 4）——docker daemon 默认 `max-concurrent-downloads=3` 逐层下载慢，预取后客户端后续 blob 请求直接 `X-Cache: HIT`；预取复用与客户端完全相同的回源路径（候选轮询/超时覆盖/http-proxy/insecure TLS/token dance/缓存写入），只写缓存不产生客户端响应；同一 blob 在途集合去重 + singleflight 与客户端并发请求合并；**预取在途中的层被客户端请求时直接搭车**（拼接已落盘前缀与实时增量，`X-Cache: PREFETCH`，上游仅预取一路下载、已下载部分直接复用；响应头快照未就绪时立即合成响应头 commit，不再阻塞等候选竞速，详见下文「[在途预取层搭车](#在途预取层搭车)」）；失败只记日志绝不影响客户端；blob 按 digest 不可变、不存在刷新问题，详见下文「[manifest 驱动的镜像 layer 异步预取](#manifest-驱动的镜像-layer-异步预取)」
- **docker blob 半截保留与无感续传**：blob（`/v2/<name>/blobs/<digest>`）流式下载（客户端主路径与预取路径）中断/失败时，把已下载前缀保留为缓存目录内的半截文件（`.part-<缓存key>`，大小即已下载字节数，`docker-partial-resume-enabled` 默认开）——重 pull 时 docker 客户端是无 Range 的完整 GET（从 0 开始），proxy 撞上「缓存 MISS + 半截文件」时从断点 Range 续传并对客户端回**一次完整 200**（本地前缀回放 + 上游剩余实时拼接，docker 无需支持 Range、完全无感知，`X-Cache: RESUME`）；**响应头立即合成 commit、本地前缀立即回放（不再等待候选扫描命中——2026-11 现场「docker pull 一行进度都没有」的根因修复），上游增量仍须等 206 且 Content-Range 起点匹配的安全校验通过后才拼接**；并发请求经既有搭车机制共享同一路续传流，上游只一路下载；续传中途失败保留更新后的半截（进度不回退）；manifest 仅 blob 启用；`Range` 起点 0 的 `bytes=0-` 等价完整 GET（照常续传），**起点 >0 且不超过半截长度时同样复用续传管线**（从半截的起点回放 + 上游剩余拼接，对客户端回 206），起点超出半截（有缺口）才维持旧版 206 透传；上游不回 206 时安全降级，详见下文「[blob 半截保留与无感续传](#blob-半截保留与无感续传)」
- **docker 可变目标多源并发回源与合并**：docker 可变目标（tag manifest / referrers / tags list，不缓存每次回源）BYPASS 时并发请求全部候选源并按端点类型合并——tags/list 合并各源 tags（并集去重、字典序排序），referrers/<digest> 合并 descriptors（按 mediaType+digest 去重），manifests/<tag> 按各成功源 `Docker-Content-Digest` 取多数共识（平票取最快成功者，HEAD 与 GET 同策略）；解决镜像站之间同步快慢不一导致先命中源漏 tag / 给旧 digest 的问题（`docker-multi-source-enabled` 默认开），详见下文「[docker 可变目标多源并发回源与合并](#docker-可变目标多源并发回源与合并)」
- **可配置成功判据**：`success-check: status`（HTTP 200 即成功）或 `content`（200 且 body 非空，默认，防止镜像返回空内容/软错误）；HEAD 响应天然无 body，content 判据对 HEAD 自动按 status 语义；HTTP 304（内容未变更）在两种模式下均视为成功候选，304 原样透传给客户端、不写缓存
- **条件请求头不透传**：回源（候选与直连）按请求头白名单透传（含 `Range`），但 `If-Match` / `If-Modified-Since` / `If-None-Match` / `If-Range` / `If-Unmodified-Since` 不透传——回源始终为无条件 GET/HEAD，保证上游返回 200 + 完整 body 以过判据、写缓存；客户端的 304 语义由代理自身缓存 ETag 逻辑（HIT 命中时返回 304）负责
- **域名规则（domain-rules）**：正则匹配「`http://<入站Host>/<目标URL>`」整串（入站 Host 归一化小写后参与匹配，非锚定；目标 URL 正则写法仍兼容），命中后覆盖 `cache.enabled`、`cache.ttl`、`cache.path`、`cache.clean-interval`、`http-proxy`、`url-redirect`、`success-check`、`fallback-direct`、`insecure-skip-verify`、`timeout`（read-timeout 退化别名）、`http-timeout`、`read-timeout`、`connect-timeout`、`url-redirect-item-error-retry`、`url-redirect-all-error-retry` 等配置；每条规则还可配置 `exclude` 排除正则列表（支持多个），match 命中后任一 exclude 命中即排除该规则、继续向下尝试后续规则
- **上游代理支持**：全局与规则级 `http-proxy`（http/https/socks5），所有回源请求（含 url-redirect 候选请求）均可走代理
- **allow-list / deny-list**：正则白/黑名单访问控制（deny 优先，allow 非空即白名单模式）
- **可观测性**：`X-Cache`（HIT/MISS/BYPASS/PREFETCH）与 `X-Proxy-Upstream`（本次内容来源）响应头、结构化日志、`/healthz` 健康检查
- **工程化**：YAML 配置（`-c` 参数或 `PROXY_CACHE_CONFIG` 环境变量指定路径）、Docker 严格二阶段构建、优雅退出

## 目录结构

```
proxy-cache/
├── main.go                 # 入口：参数解析、配置加载、服务启动与优雅退出
├── go.mod / go.sum         # 模块定义（唯一依赖 gopkg.in/yaml.v3）
├── config/
│   └── config.go           # 配置加载/校验/默认值；域名规则合并为生效配置 Options
├── cache/
│   └── cache.go
│   └── manager.go          # 缓存管理器：按目录复用 Cache 实例（规则级独立目录）            # 文件缓存：sha256 键、原子写、过期判定、后台清理
├── proxy/
│   ├── proxy.go            # 核心反代：目标提取（完整 URL / 路径目标）、ACL、回源分发、成功判据、透传
│   ├── client_range.go     # 客户端 Range 语义：blob 起点 0 = 完整 GET；任意起点本地切片 / 半截对齐续传
│   ├── candidates.go       # 候选探测提速管线：probe 预算 + 硬失败快切 + 阶梯竞速（hedge）+ 扫描总预算
│   ├── probe.go            # 探测式上游尝试：响应头可用 + 首块判据即返回（body 保持活体）、探测预算
│   ├── stream.go           # GET 路径目标流式透传：commit 后边读上游边写客户端与缓存
│   ├── vars.go             # url-redirect 模板变量上下文与占位符展开（$1 / $http.server.header.*）
│   └── flight.go           # singleflight：同 URL 并发回源只执行一次（等待上限 10s，超限自拔回源）
├── config.example.yaml     # 配置示例（含完整注释）
├── Dockerfile              # 二阶段构建（golang builder -> alpine runtime）
└── README.md
```

## 快速开始

### 本地运行

要求 Go >= 1.21。

```bash
# 1. 准备配置
cp config.example.yaml config.yaml

# 2. 直接运行（配置路径也可用环境变量 PROXY_CACHE_CONFIG 指定）
go run . -c config.yaml

# 或编译后运行
go build -o proxy-cache .
./proxy-cache -c config.yaml
```

> 国内网络环境若 `go mod download` 失败，设置模块代理：
> `export GOPROXY=https://goproxy.cn,direct`

### Docker 构建（严格二阶段）

```bash
docker build -t proxy-cache .
# 国内网络加速：
# docker build --build-arg GOPROXY=https://goproxy.cn,direct -t proxy-cache .
```

二阶段说明：

| 阶段 | 基础镜像 | 内容 |
| --- | --- | --- |
| Stage 1 `builder` | `golang:1.23-alpine` | `go mod download` + `go build`（CGO 禁用，静态编译，`-ldflags="-s -w"` 去符号） |
| Stage 2 `runtime` | `alpine:3.20` | 仅含：编译产物二进制、`/app/config.yaml`（默认配置）、`ca-certificates`、非 root 用户 `app(uid=1000)`；不含任何构建工具与源码 |

### docker run（挂载配置与缓存目录）

```bash
mkdir -p ./cache-data

docker run -d --name proxy-cache \
  -p 8080:8080 \
  -v $(pwd)/config.yaml:/app/config.yaml:ro \
  -v $(pwd)/cache-data:/app/cache-data \
  proxy-cache
```

说明：

- 配置挂载到 `/app/config.yaml`（容器入口 `proxy-cache -c /app/config.yaml`，环境变量 `PROXY_CACHE_CONFIG` 亦可）；不挂载时使用镜像内置的默认配置
- 缓存挂载到 `/app/cache-data` 建议持久化，避免容器重建后缓存丢失；宿主机目录需要对容器内 uid=1000 可写（如 `chmod 777 ./cache-data` 或 `chown 1000 ./cache-data`）；使用规则级 `cache.path` 独立目录时，对应目录同样需要挂载并保证可写
- 服务监听地址由配置文件 `listen` 决定（默认 `0.0.0.0:8080`），容器默认暴露 8080

## 配置项说明

### 全局配置

| 配置项 | 类型 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `listen` | string | `0.0.0.0:8080` | HTTP 服务监听地址 |
| `http-timeout` | duration | 不限制（0） | 单次上游尝试的**总超时**：覆盖「建连 + 响应头 + 完整读 body + 401 后 token dance 及其带 token 重试」，按每次尝试独立计时（候选重试/整轮重试时每次重新计时） |
| `read-timeout` | duration | 不限制（0） | **无新增数据超时（idle）**：以本次尝试累计收到的字节数为进度标志（响应头到达也算一次进度），窗口内有进度就重置计时器，窗口内无任何新数据才触发超时并取消该次尝试（错误信息含 `read-timeout` 便于辨认）——慢速但持续传输的大文件不会被误砍 |
| `connect-timeout` | duration | 不限制（0） | 建连超时（TCP 拨号 + TLS 握手共用） |
| `timeout` | duration | 不限制（0） | **旧字段，语义已变更**：仅为 `read-timeout` 的退化别名，不再是回源总超时；且不再有旧版的 `60s` 默认兜底（未配置 = 不限制，有意的行为变更） |
| `url-redirect-item-error-retry` | int | `0`（不重试） | 单个候选的重试次数：某候选一次尝试失败（网络错误/超时/未过 success-check）后原地立即重试 N 次（总尝试 = 1+N，无退避），全部失败才轮到下一候选。硬失败（上游 403 / token dance 后仍 401 / 探测预算耗尽）跳过原地重试直接切下一候选 |
| `url-redirect-all-error-retry` | int | `0`（不重试） | 整轮全错后的重试轮数：一轮 = 所有候选（含各自 item 重试）都失败；全轮失败后整体重来最多 N 轮（总轮数 = 1+N），重试轮沿用同一候选顺序 |
| `url-redirect-probe-timeout` | duration | `5s` | **单次候选尝试的探测预算**：从发出请求到「**最终响应头到达**（含 401 token dance 的第二次响应）」的时限，覆盖建连后的等待与 token dance 全程；到期取消该次尝试并硬失败快切下一候选。**响应头到手即算探测成功**——「首块 body 到达」不算进预算（现场 1ms.run 已回 206、dance 花 2.9s，却因为还在等首块被判 `probe-timeout` 硬失败丢弃）；停表后只等首块/body，由 `http-timeout`（整次尝试总超时）与 `read-timeout`（无新增数据）负责；建连本身由 `connect-timeout` 负责。显式 `0` = 不限制。**docker blob 目标（`/v2/<name>/blobs/<digest>`）在候选级/规则级/全局都未显式配置时改用更大的缺省 `30s`**（大 layer 的上游响应头经常 5s 内到不了，5s 预算会把候选全判硬失败直接 502）；内置候选扫描总预算同步按目标放宽（manifest 等 `20s`、blob `60s` 起，见 `url-redirect-hedge-delay` 行）。命中候选仍立即 commit——commit 时刻与预算大小无关 |
| `url-redirect-hedge-delay` | duration | `2s` | **候选阶梯并发竞速（hedge）的放行间隔**：候选按 round-robin 顺序起步，首个候选立即发出，之后每隔该间隔放行下一个候选并发探测（前一个不取消）；某候选先失败时立即放行下一候选（不等间隔）；第一个通过成功判据的候选胜出，其余全部取消（context 取消，连接及时释放）。`0` = 纯串行禁用竞速（上一候选出结果后才放行下一个）。最坏「选定候选」耗时 ≈ (候选数-1)×该间隔 + 胜者探测耗时；全部候选都失败的极坏情况由内置扫描总预算兜底，提前终止并 502（docker 客户端会自行重试该层）：manifest 等小目标仍为 `20s`，**docker blob 目标 60s 起**（取 `max(60s, (候选数-1)×hedge-delay + 最大候选探测预算 + 20s)`——否则 20s 会在 30s 探测预算用完前先掐死扫描，`url-redirect-all-error-retry` 也形同虚设）。预算只是「放弃阈值」，不影响 commit 时刻 |
| `slow-download-speed` | bytes | `1MB` | **慢速升级多源并发续传的判慢阈值**（B/s，字节写法见下方说明）：type=docker 规则的流式回源按 `slow-download-window` 滑动窗口计平均送出速率，低于它判「过慢」，对（候选数>1 的）下载向其余全部候选并发发起 Range 续传并有序合并提速；`<=0`（或 `0`）= **关闭整个特性**。仅 type=docker 规则生效——**规则没写 `type: docker` 时本项与其余三个 `slow-download-*` 静默不生效**（不报错），详见下文「[慢速自动升级多源并发续传](#慢速自动升级多源并发续传)」 |
| `slow-download-window` | duration | `10s` | 判速滑动窗口时长：窗口满按窗口平均速率判慢（启用时 Validate 钳制最小 `1s`，过短窗口会把网络抖动误判为持续过慢）；不满足触发条件零开销（除窗口计时本身） |
| `slow-download-min-remaining` | bytes | `64MB` | 触发升级所需最小剩余字节数：剩余（已知总长 - 已传）不足不升级（尾巴太小没必要并发拉满）；`0` = 不设门槛；上游总长不可知（无 `Content-Length`/`Content-Range`）则永不触发 |
| `slow-download-max-behind` | bytes | `8MB` | 多源合并的剪枝阈值：合并中某源落后领先源超过该字节数即被关闭移除（收敛多源并发的带宽放大）；启用时（`slow-download-speed > 0`）必须 `>0`，Validate 拒绝 |
| `blob-concurrency-interval` | duration | `5m` | **时间驱动的单层 blob 多源并发补充间隔**（仅 type=docker 规则，与 `slow-download-*` 同一门控）：单层 blob 下载（客户端主路径与半截续传执行方）**每满该间隔且该层尚未下载完成**，就向**尚未加入**合并的候选源并发发起 Range 续传（复用同一 `slowMerger` 有序合并），客户端全程无感、只做加法（不切换/不中断已在下载的源，下完前一直保持并发）。首次触发 = 该层下载开始后满该间隔仍未完成，之后每满该间隔再检查一次；全部候选都加入后保持现状直到完成。`0` = 关闭时间驱动补充（判速触发仍独立生效）；负数启动报错。动机见下文「[时间驱动的多源并发补充](#时间驱动的多源并发补充blob-concurrency-interval)」（1.32GiB 单层、上游 200 无 `Content-Length`、19 分钟只下 57.6MiB 的现场故障） |
| `docker-prefetch-enabled` | bool | `true` | **docker 镜像 layer 异步预取开关**：`GET /v2/<name>/manifests/<ref>` 路径目标回源成功（HTTP 200 已写入缓存）且响应为单平台 image manifest 时，异步预取该镜像其余 layer blob 写入缓存，客户端后续 blob 请求直接命中；**预取在途中的层被客户端请求时直接搭车**（`X-Cache: PREFETCH`，上游仅预取一路下载）；HEAD / 206(Range) / 多平台 manifest list 与 OCI index 不触发；预取失败只记日志、绝不影响客户端请求。详见下文「[manifest 驱动的镜像 layer 异步预取](#manifest-驱动的镜像-layer-异步预取)」与「[在途预取层搭车](#在途预取层搭车)」 |
| `docker-prefetch-concurrency` | int | `4` | docker 层预取的并发数上限（全部预取共享一个 Server 级信号量）；必须为正整数（显式 `<=0` 启动报错——关闭预取请用 `docker-prefetch-enabled: false`，两者语义分离） |
| `docker-partial-resume-enabled` | bool | `true` | **docker blob 半截保留与无感续传开关**：blob（`/v2/<name>/blobs/<digest>`）流式下载（客户端主路径与预取路径）中断/失败时把已下载前缀保留为缓存目录半截文件（`.part-<缓存key>`）；客户端无 Range 完整 GET 撞上「缓存 MISS + 半截文件」时从断点 Range 续传，对客户端回一次完整 200（前缀回放 + 剩余实时拼接，docker 无需支持 Range，`X-Cache: RESUME`）；**响应头立即合成 commit + 本地前缀立即回放（不再等待候选扫描命中）**，上游增量仍须等 206 且 Content-Range 起点匹配的安全校验；manifest 会变仅 blob 启用；`Range` 起点 0 的 `bytes=0-` 等价完整 GET（照常缓存/续传）；**起点 >0 且不超过半截长度时复用本续传管线**（从半截的起点位置回放 + 上游剩余区间拼接，对客户端回 206，内部照常写完整缓存），起点超出半截（有缺口）才维持旧版 206 透传并打 Warn；上游不回 206 安全降级全量回源。详见下文「[blob 半截保留与无感续传](#blob-半截保留与无感续传)」 |
| `docker-resume-client-gone-grace` | duration | `2s` | **docker blob 半截续传「客户端断开宽限终止」的宽限时长**（仅续传执行方，`source=resume`）：续传中向客户端写响应体失败（docker 已断开）且在途条目无搭车订阅者时启动宽限计时——到期仍无订阅者则终止上游读取、保留半截（进度不回退）、移除在途条目，不再「继续读完只填缓存」长时间占用上游带宽；宽限期内新订阅者搭车则放弃终止继续拉，断开时已有订阅者直接维持现状（不宽限）；语义分工：客户端主路径是断开即停、异步预取（`source=prefetch`）保持「继续填缓存」语义不变，均不受本项影响；显式 `<=0` 关闭宽限终止（完全恢复旧「继续读完填缓存」语义）。详见下文「[blob 半截保留与无感续传](#blob-半截保留与无感续传)」 |
| `docker-multi-source-enabled` | bool | `true` | **docker 可变目标多源并发回源与合并开关**：docker 可变目标（tag manifest / referrers / tags list，不缓存每次回源）BYPASS 时并发请求全部 url-redirect 候选源（成功判据与顺序管线相同，HTML 反爬页等未过判据的源拒之于合并之外），tags/list 合并各源 tags（并集去重、字典序排序），referrers/<digest> 合并 descriptors（按 mediaType+digest 去重），manifests/<tag> 按各成功源 `Docker-Content-Digest` 取多数共识（平票取最快成功者，HEAD 与 GET 同策略）；边界：tags/list 带分页参数（`?n=` / `?last=`）不做合并、候选数 <2 不启用、其余 BYPASS（request-range / request-no-cache / cache-disabled 等）维持既有顺序管线、全部源失败退回既有顺序管线（继承其 4xx 透传 / 502 语义）；并发上限 8、每候选超时复用现有候选级配置。详见下文「[docker 可变目标多源并发回源与合并](#docker-可变目标多源并发回源与合并)」 |
| `http-proxy` | string | 空（直连） | 全局上游代理，支持 `http://`、`https://`、`socks5://`。三级覆盖（高→低）：**url-redirect 候选对象 `http-proxy`（显式空串 = 该候选强制直连）> 域名规则 > 本全局值**，详见「[url-redirect 候选对象与候选级覆盖](#url-redirect-候选对象与候选级覆盖)」 |
| `success-check` | string | `content` | 候选成功判据：`status`（200 即成功）/ `content`（200 且 body 非空） |
| `fallback-direct` | bool | `true` | url-redirect 候选全部失败后是否回退直连原始 URL |
| `insecure-skip-verify` | bool | `false` | 全局「忽略 TLS 证书校验」：开启后该路径所有上游连接（候选 / fallback 直连 / 30x 重定向目标 / token dance 两次请求）都跳过证书校验。三级优先级（高→低）：**候选对象 > 域名规则 > 全局**，未配置逐级继承，详见下文「[url-redirect 候选对象与候选级覆盖](#url-redirect-候选对象与候选级覆盖)」；安全代价：等于关闭该路径的身份与窃听防护，仅建议对证书链不全的镜像站按候选级精确开启 |
| `url-redirect` | object[] | 空 | 回源候选**对象列表**（破坏性变更：不再支持旧版纯字符串写法，解析期报错并给出迁移示例）。每个对象：`template`（必填非空，原模板字符串，占位符 `$1`/`${1}` 或 `$http.server.header.full_url(_no_server)`，恰为 `$1`/`${1}` 时 = 原始 URL 独立候选，见下文「url-redirect 模板变量」）+ 可选 `insecure-skip-verify` / `http-proxy` / `connect-timeout` / `http-timeout` / `read-timeout` / `probe-timeout`（候选级三级覆盖，见「[url-redirect 候选对象与候选级覆盖](#url-redirect-候选对象与候选级覆盖)」）；空列表 = 全部直连回源；其余字段（含预留字段 `enabled`/`weight`/`hedge-delay`）一律解析报错，详见下文「[url-redirect 候选对象与候选级覆盖](#url-redirect-候选对象与候选级覆盖)」 |
| `cache.*` | object | 见下 | 全局缓存配置 |
| `log.level` | string | `info` | 日志级别：`debug` / `info` / `warn` / `error`（大小写不敏感，非法值启动时直接报错），详见下文「日志」 |
| `log.progress-interval` | duration | `10s` | 流式透传进度日志的输出间隔：GET 路径目标流式回源（docker pull 大 blob）期间每隔该间隔输出一条「流式透传进度」日志（已传字节数/上游声明大小与百分比/区间速率/自 commit 起耗时/客户端是否仍在接收），上游停滞无新数据期间自然不输出；整条流在该间隔内完成则一条进度日志都不出（最终日志已有）。`<=0` 关闭进度日志 |
| `log.progress-summary-enabled` | bool | `true` | **活跃下载周期进度汇总开关**：开启时每 `log.progress-summary-interval` 输出一轮全局汇总（客户端流式回源 / 缓存回放 / 搭车订阅 / 半截续传 / 后台预取的进度集中输出），本轮没有任何下载登记时静默。显式 `false` 关闭 |
| `log.progress-summary-interval` | duration | `3m` | **活跃下载周期进度汇总的输出间隔**：与 `log.progress-interval`（单条流的 10s 节拍）分工不同，本项是全局活跃下载视图（含异步预取下载），取 3 分钟避免汇总日志淹没单流进度。`<=0` 关闭周期汇总。详见下文「[活跃下载周期汇总](#活跃下载周期汇总)」 |
| `domain-rules` | list | 空 | 域名/URL 规则列表，自上而下第一条命中生效 |
| `allow-list` | string[] | 空 | 允许列表（正则）。非空时为白名单模式 |
| `deny-list` | string[] | 空 | 拒绝列表（正则）。命中即返回 403 |

> 时长格式统一支持：Go duration（`30s`/`30m`/`1h30m`）、纯数字（按秒）、`2d`（天）、`1w`（周）。

> 字节（bytes）格式统一支持：纯整数（按字节，`1048576`）或 1024 进制的单位字符串 `"512KB"` / `"1MB"` / `"1.5MB"` / `"64MB"` / `"2GB"`（单位 `B`/`KB`/`MB`/`GB`/`TB` 大小写不敏感、支持小数）。`slow-download-*` 四键的负数为配置错误（启动快速失败）。

> **超时模型（与旧版差异）**：三个超时（`http-timeout` / `read-timeout` / `connect-timeout`）相互独立，全局与规则级都可配——同一字段规则没配就用全局，全局也没有 = 该项不限制（0）。`read-timeout` 的生效优先级：`rule.read-timeout` > `rule.timeout` > `global.read-timeout` > `global.timeout` > 不限制。与旧版行为差异：① 旧 `timeout` 是「回源总超时，未配置兜底 `60s`」，现仅为 `read-timeout` 别名且**无任何默认兜底**（有意的行为变更）；② 旧版 TCP 拨号/TLS 握手固定 `10s` 超时，现由 `connect-timeout` 控制、未配置不限制；③ 大 blob 在旧版可能因「总超时内没读完」被整体砍掉（数据在稳定传输也照砍），现 `read-timeout` 按数据进度判定，慢速但持续传输不会被砍；候选级/整轮重试为新增能力，未配置时不重试（行为与旧版一致）。

### cache 配置

| 配置项 | 类型 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `cache.enabled` | bool | `true` | 是否启用缓存（全局关闭后仍可在域名规则内按规则开启，见下文「全局关闭缓存」场景） |
| `cache.path` | string | `./cache-data` | 缓存目录（命名避免与源码 `cache/` 包目录混用） |
| `cache.ttl` | duration | `30m` | 默认缓存时长（可被域名规则覆盖）；`0` = 永不过期（未配置默认 `30m`；负数非法） |
| `cache.clean-interval` | duration | `10m` | 后台过期扫描间隔，`<=0` 关闭（读取命中过期条目时也会惰性删除） |

全局 `cache.path` / `cache.clean-interval` 同样可被域名规则覆盖（`cache.path` / `cache.clean-interval`），规则未配置时沿用全局值。

### domain-rules 配置

| 配置项 | 类型 | 说明 |
| --- | --- | --- |
| `name` | string | 规则名（可选，用于日志/`X-Cache` 观测） |
| `match` | string | **必填**，正则表达式，对「**`http://<入站Host>/<目标URL>`**」整串做非锚定匹配（入站 Host 归一化为小写）。既可按入站域名写（如 `'http://github\.path\..+'`，Host 形如 `github.path.xxx` 时命中），旧的纯目标 URL 写法也兼容（如 `raw\.githubusercontent\.com`、`.*https://[^.]+\.githubusercontent\.com`） |
| `exclude` | string[] | 排除正则列表（可选，支持多项）。`match` 命中后再逐个检查（同样对整串非锚定匹配），**任一命中即排除该规则**（视为不匹配，继续向下尝试后续规则）；全部不命中才应用本规则 |
| `type` | string | 请求类型过滤（可选，发生在 `match`/`exclude` 匹配之前）：留空 = 不过滤请求类型（默认，完全兼容旧配置）；`docker` = 仅路径目标请求可命中本规则（如 docker registry 的 `/v2/xxx/manifests/latest`）；`web` = 仅完整 URL 请求可命中本规则（如 `/https://raw.githubusercontent.com/...`）；其他值启动时直接报错。配置自动做 trim + 小写归一化（`"Docker "` 视为 `"docker"`）。**⚠ 做 docker 镜像加速的规则务必写 `type: docker`**：规则级/全局的 `slow-download-*` 四个参数只在 `type=docker` 时生效，缺 type 时它们**静默不生效**（不报错、不打日志，线上真实踩过） |
| `cache.enabled` | bool | 覆盖该规则的缓存开关 |
| `cache.ttl` | duration | 覆盖该规则的缓存时长；未配置沿用全局 `cache.ttl`；`0` = 永不过期；负数非法 |
| `cache.path` | string | 该规则的**独立缓存目录**（trim 后为空 = 未配置，沿用全局 `cache.path`；同一 URL 永远落同一规则/目录） |
| `cache.clean-interval` | duration | 该规则缓存目录的后台过期扫描间隔（必须 >=0，`0` 关闭；未配置沿用全局 `cache.clean-interval`） |
| `http-proxy` | string | 该规则回源走指定代理（覆盖全局）；url-redirect 候选对象内的显式 `http-proxy` 优先级更高（显式空串 = 该候选强制直连），三级优先级见「[url-redirect 候选对象与候选级覆盖](#url-redirect-候选对象与候选级覆盖)」 |
| `url-redirect` | object[] | 该规则专用候选对象列表（覆盖全局；`[]` 表示该规则直连回源；对象字段同全局 `url-redirect`：`template` 必填 + `insecure-skip-verify`/`http-proxy`/`connect-timeout`/`http-timeout`/`read-timeout`/`probe-timeout` 可选，候选级三级覆盖优先级最高，见「[url-redirect 候选对象与候选级覆盖](#url-redirect-候选对象与候选级覆盖)」） |
| `success-check` | string | 该规则的成功判据 |
| `fallback-direct` | bool | 该规则是否允许回退直连 |
| `insecure-skip-verify` | bool | 覆盖该规则「忽略 TLS 证书校验」（未配置沿用全局；url-redirect 候选对象内的显式值优先级更高，三级优先级见「[url-redirect 候选对象与候选级覆盖](#url-redirect-候选对象与候选级覆盖)」） |
| `http-timeout` | duration | 覆盖该规则单次上游尝试的总超时；未配置沿用全局；url-redirect 候选对象可用 `http-timeout` 按候选覆盖（候选 > 规则 > 全局） |
| `read-timeout` | duration | 覆盖该规则的无新增数据超时；未配置时回落到该规则 `timeout`，仍未配置才沿用全局（`global.read-timeout` > `global.timeout`）；url-redirect 候选对象可用 `read-timeout` 按候选覆盖（候选 > 规则回落链） |
| `connect-timeout` | duration | 覆盖该规则建连超时（TCP 拨号 + TLS 握手）；未配置沿用全局；url-redirect 候选对象可用 `connect-timeout` 按候选覆盖（候选 > 规则 > 全局） |
| `timeout` | duration | 旧字段：仅当该规则未配置 `read-timeout` 时作为该规则 `read-timeout` 生效（退化别名，不再是总超时） |
| `url-redirect-item-error-retry` | int | 覆盖该规则单个候选失败后的原地重试次数；未配置沿用全局 |
| `url-redirect-all-error-retry` | int | 覆盖该规则整轮全败后的重试轮数；未配置沿用全局 |
| `url-redirect-probe-timeout` | duration | 覆盖该规则单次候选尝试的探测预算（默认 5s，`0` = 不限制）；未配置沿用全局。**最终响应头到达即算探测成功**（不把首块 body 算进预算）。规则显式配置即对全部目标（含 blob）生效；未配置时 blob 目标用 30s 缺省、其它目标 5s |
| `url-redirect-hedge-delay` | duration | 覆盖该规则候选阶梯竞速的放行间隔（默认 2s，`0` = 纯串行禁用竞速）；未配置沿用全局 |
| `slow-download-speed` | bytes | 覆盖该规则慢速升级多源并发续传的判慢阈值（B/s，`<=0` 关闭特性）；未配置沿用全局。**仅 type=docker 规则生效**，详见下文「[慢速自动升级多源并发续传](#慢速自动升级多源并发续传)」 |
| `slow-download-window` | duration | 覆盖该规则判速滑动窗口时长（启用时最小 `1s`）；未配置沿用全局 |
| `slow-download-min-remaining` | bytes | 覆盖该规则触发升级所需最小剩余字节数（`0` = 不设门槛）；未配置沿用全局 |
| `slow-download-max-behind` | bytes | 覆盖该规则多源合并的剪枝阈值（启用时必须 `>0`）；未配置沿用全局 |
| `blob-concurrency-interval` | duration | 覆盖该规则时间驱动的单层 blob 多源并发补充间隔（`0` = 关闭该规则的时间驱动补充，判速触发仍独立生效）；未配置沿用全局。**仅 type=docker 规则生效**，详见下文「[时间驱动的多源并发补充](#时间驱动的多源并发补充blob-concurrency-interval)」 |

规则覆盖项均为「可选」：不写则沿用全局配置；**第一条命中的规则生效**（不做多规则叠加）。

> `type` 请求类型分流说明：同一入站域名/同一段 `match` 下可配置多条规则，用 `type` 把 docker 流量与 web 流量分流到两条不同规则（各自的 cache/超时/url-redirect 候选互不影响）。`type` 过滤发生在 `match`/`exclude` 匹配**之前**——类型不符的规则直接跳过（debug 日志输出 `rule_type` 与 `target_kind`），继续向下尝试后续规则；启动日志「域名规则已加载」的 `type` 字段输出归一化生效值（不过滤时为 `any`）。

> 匹配范围说明：域名规则的 `match` / `exclude` 对「`http://<入站Host>/<目标URL>`」整串匹配（入站 Host 参与匹配）；**完整 URL 请求**的缓存 key（目标 URL 的 sha256）与 allow-list / deny-list 仍只对目标 URL 匹配，与入站 Host 无关；**路径目标请求**（目标不是完整 URL，如 docker registry 的 `/v2/xxx/manifests/latest`）的 ACL 匹配与缓存 key 输入则使用「`http://<入站Host>/<路径目标>`」整串（入站 Host 参与，保持拦截能力并避免跨入站域名缓存污染）。

**全局关闭缓存、命中规则才开启独立缓存**：把全局 `cache.enabled` 设为 `false` 后，在需要的规则内显式配置 `cache.enabled: true`（并可用 `cache.path` 指定独立目录）——未命中任何规则的流量一律 `X-Cache: BYPASS` 不缓存，命中规则的流量才缓存且写入规则自己的目录，与全局目录互不影响、各自独立清理。服务启动时会按配置预热创建全部可能被写入的缓存目录并启动各自的后台清理（同一路径首次出现的清理间隔生效）。

`exclude` 链式落位语义：某规则 match 命中但被 exclude 排除时，该规则视为不匹配，匹配流程**继续向下**尝试后续规则；后续规则也不命中（或同样被排除）则走全局默认配置。例如：githubusercontent 规则缓存 6h，exclude 掉 `dongchengjie/airport` 仓库后，该仓库的请求不再套用 6h 缓存，而是落到下一条规则或全局默认 `cache.ttl: 30m`。

## 配置示例

```yaml
listen: "0.0.0.0:8080"
http-timeout: 300s            # 单次上游尝试总超时（未配置 = 不限制）
read-timeout: 60s             # 无新增数据超时（未配置 = 不限制；60s 内无任何新数据才判超时）
connect-timeout: 10s          # 建连超时（未配置 = 不限制）
# timeout: 60s                # 旧字段：仅当 read-timeout 未配置时作为 read-timeout 生效
http-proxy: ""                # 或 http://127.0.0.1:7890
success-check: content        # status / content
fallback-direct: true

url-redirect:                 # 对象数组：$1 = 原始目标 URL，按顺序轮询（默认阶梯竞速：首个立即发出，
                              # 之后每隔 url-redirect-hedge-delay 放行下一个并发探测）
  - template: https://hk.gh-proxy.org/$1
  - template: https://gh-proxy.com/$1
  - template: https://mirror.ghproxy.com/$1
  # - template: https://docker.actima.top/$1
  #   insecure-skip-verify: true   # 仅这个候选证书不受信，候选级精确开启（优先级最高）

# insecure-skip-verify: false  # 全局忽略 TLS 证书校验（默认 false；规则级可覆盖、候选级优先，见 README 专节）

url-redirect-probe-timeout: 5s     # 单候选探测预算：响应头/判据 5s 未就绪即快切下一候选（0=不限制）
url-redirect-hedge-delay: 2s       # 候选阶梯竞速放行间隔（0=纯串行禁用竞速）
url-redirect-item-error-retry: 1   # 单候选失败后原地重试（总尝试 = 1+N；0 = 不重试；硬失败跳过）
url-redirect-all-error-retry: 1    # 整轮全败后整体重试（总轮数 = 1+N；0 = 不重试）

# docker-multi-source-enabled: true   # docker 可变目标多源并发回源与合并（默认开，见下文专节）
# blob-concurrency-interval: 5m        # 时间驱动单层 blob 多源并发补充（默认 5m；0 = 关闭；仅 type=docker 规则）

cache:
  enabled: true
  path: ./cache-data
  ttl: 30m                    # 0=永不过期（未配置默认 30m；负数非法）
  clean-interval: 10m

domain-rules:                 # 自上而下第一条命中生效；规则可配 type 做请求类型过滤（docker=仅路径目标 / web=仅完整 URL / 留空=不过滤）
  - name: githubusercontent
    match: '.*https://[^.]+\.githubusercontent\.com'
    exclude:                     # match 命中后任一 exclude 命中即排除本规则，
      - 'dongchengjie/airport'   # 该仓库不套用 6h 缓存，落到下一条规则或全局默认
    cache:
      enabled: true
      ttl: 6h                # raw 内容缓存 6 小时（0=永不过期）
      # path: ./cache-raw          # 规则独立缓存目录（不写沿用全局 cache.path）
      # clean-interval: 30m        # 该目录后台清理间隔（不写沿用全局 cache.clean-interval）
    # http-proxy: "http://127.0.0.1:7890"
    # success-check: status
    # fallback-direct: false

  # ---- 场景示例：全局 cache.enabled: false 时，仅命中规则的流量开启独立缓存 ----
  # - name: raw-only
  #   match: '.*https://[^.]+\.githubusercontent\.com'
  #   cache:
  #     enabled: true          # 规则内显式开启（优先于全局开关）
  #     path: ./cache-raw      # 写入规则自己的独立目录
  #     ttl: 6h
  #     clean-interval: 30m

  - name: github-release
    match: 'github\.com/[^/]+/[^/]+/releases/download/'
    cache:
      ttl: 1h

allow-list: []                # 非空时仅放行命中项
deny-list: []                 # 命中即 403
```

完整注释版见 [config.example.yaml](config.example.yaml)。

## curl 测试示例

```bash
# 首次请求：回源，观察响应头 X-Cache: MISS 与 X-Proxy-Upstream（本次命中的候选/直连）
curl -i "http://127.0.0.1:8080/https://raw.githubusercontent.com/dongchengjie/airport/main/subs/merged/tested_within_vless.yaml"
# HTTP/1.1 200 OK
# X-Cache: MISS
# X-Proxy-Upstream: https://hk.gh-proxy.org/https://raw.githubusercontent.com/...
# ...

# 再次请求：缓存命中，X-Cache: HIT，无回源（日志中无上游请求）
curl -sI "http://127.0.0.1:8080/https://raw.githubusercontent.com/dongchengjie/airport/main/subs/merged/tested_within_vless.yaml" | grep -i x-cache
# x-cache: HIT

# 强制跳过缓存重新回源（no-cache），结果仍会刷新缓存
curl -i -H "Cache-Control: no-cache" "http://127.0.0.1:8080/https://raw.githubusercontent.com/xxx"

# 任意 http/https 目标均可代理（不限于 GitHub）
curl -i "http://127.0.0.1:8080/https://example.com/index.html"

# 路径目标未命中规则（默认配置不承接路径目标）/ 命中 deny-list 时分别返回 400 / 403（JSON 错误体）
curl -i "http://127.0.0.1:8080/not-an-url"

# 健康检查
curl "http://127.0.0.1:8080/healthz"
# {"status":"ok"}
```

## url-redirect 轮询机制说明

以 3 个模板为例（`$1` 替换为原始目标 URL 后得到候选列表 `[C0, C1, C2]`）：

| 请求序号 | 起始下标（round-robin 轮换） | 尝试顺序 |
| --- | --- | --- |
| 第 1 个请求 | 0 | C0 → C1 → C2 |
| 第 2 个请求 | 1 | C1 → C2 → C0 |
| 第 3 个请求 | 2 | C2 → C0 → C1 |
| 第 4 个请求 | 0（回到起点） | C0 → C1 → C2 |

- **阶梯并发竞速（hedge，默认开启）**：候选按轮换后的顺序起步——首个候选立即发出，之后每隔 `url-redirect-hedge-delay`（默认 2s）放行下一个候选并发探测（**前一个不取消**）；某个候选先失败时立即放行下一候选（不等间隔，`hedge-delay: 0` 则退化为纯串行逐个尝试）；**第一个通过 success-check 判据的候选胜出**，其余全部取消（context 取消、连接及时释放）。5 候选最坏情况下「选定候选」耗时 ≈ (5-1)×2s + 胜者探测耗时（探测预算默认 5s），约 10s 量级，远低于 docker 客户端约 30s 的响应头超时；全部候选都失败的极坏情况由内置扫描总预算兜底——提前终止扫描并返回 502，docker 客户端会自行重试该层（manifest 等 20s、docker blob 60s 起，blob 按「候选数×hedge + 探测预算」动态放大）
- **命中即返回**：胜出候选的响应即最终返回（并按条件写缓存），响应头 `X-Proxy-Upstream` 标明来源
- **硬失败快切**：token dance 后仍 401、上游 403、探测预算（`url-redirect-probe-timeout`）耗尽这三类失败，跳过 `url-redirect-item-error-retry` 原地重试直接切下一候选（这类失败重试无意义却各烧数秒，日志会注明 `hard_fail` 原因）
- **失败切换**：其余候选失败（请求报错、非 200、或 content 模式下 GET 的 body 为空；HEAD 天然无 body 自动按 status 语义）都视为失败并尝试/放行下一个候选，同时记录 warn 日志；HTTP 304（内容未变更）视为成功候选——条件请求头不透传，上游对无条件请求仍回 304 属异常/缓存副本有效语义，此时 304 原样透传给客户端（不写缓存），不再判为失败；带 Range 的请求上游回 206 视为成功（流式透传、不写缓存）
- **上游明确 4xx 透传**：全部候选失败且最后失败是「拿到了明确上游 4xx 响应」（401 除外——由既有 token dance 语义处理）时，把该 4xx 状态码 + 响应头 + body（探测量级）原样透传给客户端，不再一律 502——典型如镜像站对 OCI referrers 端点（`/v2/<name>/referrers/<digest>`）回 404：docker 客户端对该端点语义自带 fallback，透传 404 后 pull 正常完成，而一律 502 会让 docker 判整层失败中断 pull；同理适用于未知端点 405 等客户端能理解的 registry 语义错误。换下一候选重试的既有语义不变（透传只在全部候选失败后发生）；网络/超时类失败（含候选扫描总预算耗尽）不透传，仍返回 502
- **全部失败**：`fallback-direct: true`（默认）回退直连原始 URL，直连结果原样透传（404/5xx 也如实返回）；`fallback-direct: false` 则直接返回 502 JSON 错误
- 轮询计数器为全局原子计数，命中不同域名规则使用各自候选列表时同样共享该计数（起始下标依旧逐请求轮换）
- 未配置 `url-redirect`（或规则覆盖为 `[]`）时，所有请求直接回源原始 URL，不套用成功判据

## 流式回源与 docker ~30s 响应头超时

docker 客户端拉取镜像时，对每个请求只等约 30s 的响应头（`net/http: timeout awaiting response headers`），超时即断开重试。旧版实现把上游 body **整包读进内存**后才 `WriteHeader`，首字节时间 = 候选轮询全部耗时 + 上游完整下载时间——一个 1.7MB 的 blob 在慢镜像站上就能把首字节拖过 30s，docker 断开后就出现 broken pipe。

现版对 **GET 路径目标（docker registry 回源）的缓存 MISS / BYPASS** 改为流式透传：

1. **探测阶段**（可自由切换候选）：候选按 hedge 竞速探测，只等「响应头到达 + 成功判据可判定」（content 判据读首块 ≤64KB 判非空），第一个通过判据的候选胜出；
2. **commit**：胜出后立即向客户端 `WriteHeader` 并透传响应头（沿用逐跳头过滤与 `X-Cache` / `X-Proxy-Upstream` 观测头；`Content-Length` 用上游声明值，上游没有就不设），随后把已缓冲首块写给客户端——**commit 之前绝不给客户端输出任何字节，commit 之后绝不再切换候选或改变状态码**；
3. **边读边写**：把上游 body 同时写给客户端与内存累积缓冲；客户端写失败（docker 已断开）即**断开即停**——立即终止上游读取（多源合并场景同时逐路关闭全部上游源），blob 缓存可读主路径保留半截（进度不回退）并输出 Warn 级「客户端已断开，终止上游读取」运维事件日志，客户端将以断点续传或缓存重试接续（旧版「继续读完上游只填缓存」语义已废止：现场 500MB 层断开后仍以几十 KB/s 拖约 1.5h 占住上游连接，其缓存价值已被「终止 + 半截保留 + 断点续传」取代）；期间按 `log.progress-interval`（默认 10s，`<=0` 关闭）周期输出「流式透传进度」日志（已传字节数/百分比/速率/耗时），长耗时大 blob 不再只有首尾两条日志（断开终止后进度日志自然不再输出）；
4. **EOF**：仅 200 且过判据时用 `cache.Set` 原子写缓存（临时文件 + rename，并发写不互相污染），并把完整 Entry 返回给单飞等待方；commit 后上游读取失败则记日志、终止本次响应、**不写缓存**（半截数据绝不入库）。

配套保障：

- **单飞等待上限**：大 blob 流式回源可持续几十秒到几分钟，同一缓存 key 的并发 MISS 等待 singleflight 超过 10s（内置常量）就自行走候选回源（不再排队），避免被首个执行方吊死到超过 docker 响应头超时；
- **HEAD 与完整 URL 目标（web）不变**：HEAD 探测沿用候选竞速管线但不写缓存、整包缓冲回放；web 请求仍整包缓冲后回放（行为不变）；
- **Range 断点续传**：带 `Range` 的请求绕过缓存读取，上游回 206 视为成功并流式透传、不写缓存（旧版 206 会被成功判据拒掉导致续传全挂）。

## 慢速自动升级多源并发续传

竞速胜出的镜像站有时「能连上但下载极慢」（首字节快、速率只有几十 KB/s），docker pull 一个大 blob 会拖几分钟。为此，**type=docker 规则**的 GET 路径目标流式回源默认开启慢速升级：下载过慢时自动向其余全部候选**并发发起 Range 续传**，与胜者既有连接按偏移有序合并，把速率拉到多镜像站的并集。

> **⚠ 前置条件：规则的 `type` 必须是 `docker`**。`slow-download-speed` / `-window` / `-min-remaining` / `-max-behind` 与 `blob-concurrency-interval` 五个参数（全局与规则级）只在 `type: docker` 规则的流式回源上生效；规则留空 type（默认 any）时这五个参数**静默不生效**——请求照样命中规则、缓存与候选照常工作，只是慢速升级与时间驱动补充永远不会触发，日志里那条 blob 单层下载开始会带 `reason(原因)=not_docker_rule` 说明原因。

### 「过慢」的定义与触发条件

commit 后的读循环里按 `slow-download-window`（默认 `10s`）滑动窗口计**平均送出速率**，**窗口满且窗口平均速率低于 `slow-download-speed`（默认 `1MB/s`）** 即判「过慢」。判慢后还需同时满足才真正升级：

1. **剩余量足够**：剩余字节（上游声明总长 - 已传）≥ `slow-download-min-remaining`（默认 `64MB`）——尾巴太小没必要并发拉满；上游总长不可知（无 `Content-Length`/`Content-Range`，且 manifest 里也没有该 digest 的 size——见下节「[时间驱动的多源并发补充](#时间驱动的多源并发补充blob-concurrency-interval)」）则不会触发；
2. **候选数 > 1**：只有当前胜者一个候选时无从并发；
3. **判速窗口独立**：判速触发按 `slow-download-window` 每窗口判一次，允许**多次补充**——后续窗口仍可把尚未加入的候选源补进同一合并器（「只做加法」，绝不切换/中断已在下载的源）。

全部条件不满足时保持现状零改动（除窗口计时外零开销）；客户端已断开时本流已在断开即停出口终止（见「[流式回源与 docker ~30s 响应头超时](#流式回源与-docker-30s-响应头超时)」），不会再走到升级判定——升级窗口只存在于客户端仍在线的下载过程中。触发时输出 Info 日志「下载过慢，升级多源并发续传」（含 `rule` / `candidate` / `bytes` / `content_length` / `window` / `window_speed` / `sources`）；升级后客户端断开则由 `abortAll` 逐路关闭全部上游源（含尚未定局的续传源），合并循环随即退出，同样按断开即停收尾。

### Range 续传与有序合并

- **续传区间**：胜者 200 + `Content-Length` 时向其余候选发 `Range: bytes={已传}-{总长-1}`；胜者 206（客户端断点续传）时从其 `Content-Range` 解析终点，续传偏移 = 起始偏移 + 已传（206 的终点解析不出来则该流不支持升级，静默跳过）；
- **复用同一套上游机制**：续传请求与候选探测共用同一个底层尝试函数（`startUpstream`）——请求头白名单、docker 401 匿名 token dance、read-timeout 看门狗、`http-timeout` 尝试总超时、候选级 `insecure-skip-verify` 全部一致；
- **响应校验**：续传响应必须 206 且 `Content-Range` 起始等于请求偏移（上游不支持 Range 回 200 → 丢弃该源，debug 留痕）；此前竞速中硬失败的候选也照常尝试，失败即剪枝（debug 留痕）；
- **有序合并**：胜者既有连接**不中断**，作为源之一并入（天然从当前已传偏移继续）；每源一个 4MB FIFO 缓冲（**内存上界 = 源数×4MB**，缓冲满暂停读该源）+ 绝对游标；客户端严格按偏移有序写——首块先到者为 leader，其后每次取「持有下一期望偏移」的就绪数据写给客户端与累积缓冲（缓存写入语义不变，仍仅 200 且过判据落盘）；
- **剪枝**：源出错、EOF 已尽且游标落后（流未完成却再无数据）、或落后领先源超过 `slow-download-max-behind`（默认 `8MB`）即关闭移除（日志含 reason）；
- **收尾**：游标推进到已知总长 → 走既有 EOF 收尾（finalize 写缓存 / 206 不写缓存、成功返回与完成日志，字段兼容）；所有源全部耗尽且未到总长 → 沿用「commit 后读上游失败」语义：Error 日志、终止响应、不写缓存，等待方收到错误（502）。

### 时间驱动的多源并发补充（blob-concurrency-interval）

判速触发（上一节）有个致命前提：**必须知道总长**（否则算不出「剩余量」，判定直接跳过）。线上真实故障正是踩在这里——用户 docker pull 一个 **1.32GiB 的单层 blob**：

1. 目标镜像站的 200 响应**没有 `Content-Length`**（chunked 上游）→ 总长不可知 → 判速升级被静默跳过；
2. 预取队列还没轮到该层（或预取已结束）；
3. 客户端只有一路连接约 **53KB/s**，19 分钟只下了 **57.6MiB**，随后被上游 `PROTOCOL_ERROR` 掐断；
4. 断流后走「[blob 半截保留与无感续传](#blob-半截保留与无感续传)」路径，而该路径当时**不叠加**多源升级 → 续传仍然只有一路。

`blob-concurrency-interval`（默认 `5m`，`0` = 关闭，全局与规则级都可配，语义与 `slow-download-*` 完全一致的三级优先级/继承）就是这条故障的正面对策：**不看速率、只看时间**——只要单层 blob 还没下完，就按时间补充更多源并发下载。

- **适用范围**：**客户端主路径**（`source=client` 的流式回源）与**半截续传执行方**（`source=resume`，即上面故障的第 4 步）两个执行方，且仅针对 **blob 目标**（`/v2/<name>/blobs/<digest>`，即「单层」——manifest 等小目标不适用）。预取（`source=prefetch`）与搭车（`source=ridealong`）保持既有取舍不变——前者不是客户端请求、后者没有自己的上游连接。
- **首次触发**：该层下载开始（commit）后满 `blob-concurrency-interval` 仍未完成 → 向**其余全部候选源**并发发起 Range 续传，复用既有 `slowMerger` 有序合并（与判速触发同一个合并器、同一套源校验与剪枝），客户端仍是一条连续的流——**不切状态码、不回退已发字节、不改响应头**。
- **循环**：之后每满该间隔再检查一次；有尚未加入的候选源就继续补充（`joinSlowSources` 增量加源），全部候选都加入后保持现有并发直到下载完成。全程**只做加法**：不切换、不中断已在下载的源。
- **总长兜底（关键）**：解析单平台 image manifest（无论 `docker-prefetch-enabled` 开不开）时，把每个 layer/config 的 `digest → size` 记进 **Server 级进程内索引**（有界 FIFO，见 `proxy/blob_size_index.go`）；blob 流式回源响应没有 `Content-Length` 时用 manifest size 兜底 `knownTotal`，日志字段 `total_source(总长来源)=upstream|manifest|unknown` 标注来源。blob 按 digest 寻址、内容不可变，manifest 的 size 与 digest 一样稳定；写缓存前仍由既有 sha256 强校验做最终把关（size 被上游伪造时拼出的内容摘要必然不符，绝不会入库）。
- **续传路径同样生效**：续传执行方每满间隔也会向尚未加入的候选源补充并发 Range 续传（总长优先取上游 206 的 `Content-Range`，拿不到再用 manifest size 兜底），日志 `source(来源)=resume`，与客户端主路径共用同一套判定/合并/日志（`timeBlobSupplement`）。
- **判定与动作日志**（全部 Info 级，键序遵循 `proxy/log_attrs.go`）：
  - 单层下载开始：`docker blob 单层下载开始（客户端主路径）`（续传执行方复用既有的「`blob 半截续传开始`」），带 `candidate(候选源)` / `total(总长)` / `total_source(总长来源)` / `candidates(候选数)` / `sources(源数量)=1`；
  - 每满间隔：`blob 下载 N 分钟未完成，评估多源并发补充`，带 `round(当前轮)` / `elapsed(已耗时)` / `bytes(字节数)` / `total(总长)` / `total_source(总长来源)` / `rate(平均速率)` / `joined_sources(已并入源数)` / `pending_sources(待并入源数)`；
  - 补充动作：`补充多源并发下载`，`added(本次加入源)` 列出本次加入的源（去重、排除当前胜者与已在合并中的源）+ `sources(源数量)` / `joined_sources(已并入源数)` / `alive_sources(存活源数)` / `offset(偏移)` / `total(总长)`；
  - 无法补充：`未补充多源并发`，`reason(原因)` 枚举 = `disabled`（interval<=0 且 slow-download-speed<=0）/ `not_docker_rule` / `content_length_unknown`（无 `Content-Length` 且 manifest 无 size）/ `candidates_lt_2` / `all_joined` / `client_gone` / `upstream_no_range`（该源对 Range 回 200 全量，跳过并记该源）/ `remaining_too_small`（复用 `slow-download-min-remaining` 语义）；
  - 剪枝 / 源失败 / 完成等既有日志全部保留，并补充 `sources(源数量)` / `alive_sources(存活源数)` 字段。
- **向后兼容**：不配新键时默认行为 = 5m 时间驱动（**行为变化**：以前总长不可知的慢速单层只有一路，现在 5 分钟后会补源）；显式 `blob-concurrency-interval: 0` 可关闭时间驱动、只保留判速触发（`slow-download-speed: 0` 反之）。
- **现场排障**：`grep '评估多源并发补充' proxy-cache.log` 看每层的定时判定，`grep '补充多源并发下载' proxy-cache.log` 看实际加了几路源，`grep '未补充多源并发' proxy-cache.log` + `reason(原因)=` 看为什么没加。

### 代价与边界（读配置前必看）

- **带宽放大**：升级后**每个源都会拉满剩余区间自己的流**（慢源在被剪枝前会白拉一段），靠 `slow-download-max-behind` 剪枝收敛——阈值越小收敛越快、越大越容忍慢源后程发力；时间驱动的补充源按合并游标处的偏移起拉，绝不重复拉取已写给客户端的区间；
- **仅 docker type 生效**：非 docker 规则 / 未配 `url-redirect` 的直连路径 / HEAD 管线完全不生效；候选并发数上限 = 候选列表长度（一般 ≤5），不做额外并发池；
- **连续性保证不变**：commit 后不再切换状态码/响应头，升级只增开续传源，绝不动客户端已见字节；时间补充按 `blob-concurrency-interval`（默认 5m）发生，不会在下载刚开始就并发拉满（要更快收敛就把该间隔调小，如 `1m`）；
- **关闭方式**：`slow-download-speed: 0`（或 `<=0`）关闭判速触发、`blob-concurrency-interval: 0` 关闭时间驱动补充——两者都关即完全恢复旧「单路下载」行为；也可按规则级覆盖只对个别 docker 规则开启/关闭。

## 单飞（singleflight）与等待上限

同一缓存 key 的并发未命中通过 singleflight 只回源一次，其余请求等待共享结果。大 blob 的流式回源可能持续几分钟，等待方最多等待 **10s**（内置常量）：超限的等待方自行走候选回源并同样参与写缓存——`cache.Set` 的临时文件 + rename 原子替换保证并发写缓存互不污染，读取方要么看到旧条目、要么看到完整新条目。完整 URL 目标（web）保持无限期等待（行为不变）。

## url-redirect 模板变量

模板配置在候选对象的 `template` 字段（`url-redirect` 现为对象数组，见下文「[url-redirect 候选对象与候选级覆盖](#url-redirect-候选对象与候选级覆盖)」，`template` 即旧版的模板字符串，占位符语义不变）。模板里可使用的占位符（每个变量均支持 `$name` 与 `${name}` 两种写法；展开按**变量名长度降序**替换——`full_url_no_server` 先于 `full_url`，避免同名前缀遮蔽；长变量先替换也规避了递归展开；未知的 `$` 序列原样保留为字面量；**斜杠接缝去重**：`full_url_no_server` 的值以 `/` 开头，模板里占位符前紧邻 `/` 时（如 `https://docker.1panel.live/$http...`）两个 `/` 合并为一个，拼出 `.../v2/...` 而非 `...//v2/...`）：

| 占位符 | 含义 |
| --- | --- |
| `$1` / `${1}` | **目标串（兼容保留）**：完整 URL 请求 = 目标 URL 原文；路径目标请求 = 路径目标本身（不带 scheme，如 `v2/xxx/manifests/latest`） |
| `$http.server.header.full_url` / `${http.server.header.full_url}` | 本次入站请求的完整 URL：`scheme://host/path?query`。scheme 优先取请求头 `X-Forwarded-Proto` 的首个值（小写），否则 `r.TLS != nil` 用 `https`、否则 `http`；host 为入站 `Host`（原样含端口）；path 用 `EscapedPath()`，`RawQuery` 非空时拼 `?` + `RawQuery` |
| `$http.server.header.full_url_no_server` / `${http.server.header.full_url_no_server}` | 入站请求 URL 去掉 `scheme://host` 的部分：`/path?query`（以 `/` 开头、含查询串） |

两个真实示例（变量取值与展开结果均按入站请求原样计算）：

```text
# docker registry（路径目标请求）
入站: http://registry-proxy-cache.linkease.net:5480/v2/adockero/proxy-cache/manifests/latest
$http.server.header.full_url           = http://registry-proxy-cache.linkease.net:5480/v2/adockero/proxy-cache/manifests/latest
$http.server.header.full_url_no_server = /v2/adockero/proxy-cache/manifests/latest
模板: https://docker.1panel.live/$http.server.header.full_url_no_server
展开: https://docker.1panel.live/v2/adockero/proxy-cache/manifests/latest

# github（完整 URL 请求）
入站: http://github.path.proxy-cache.port.8080.proxy_sslip.192.168.137.2.sslip.io/https://raw.githubusercontent.com/dongchengjie/airport/main/subs/merged/tested_within_vless.yaml
$http.server.header.full_url_no_server = /https://raw.githubusercontent.com/dongchengjie/airport/main/subs/merged/tested_within_vless.yaml
模板: https://hk.gh-proxy.org/$http.server.header.full_url_no_server
展开: https://hk.gh-proxy.org/https://raw.githubusercontent.com/dongchengjie/airport/main/subs/merged/tested_within_vless.yaml
```

**原始 URL 独立候选**：候选的 `template` 写成**恰好** `$1`（或 `${1}`）——即模板只有这一个占位符、不拼任何前缀——表示原始目标 URL 本身也作为一条独立候选参与候选轮询，配合候选级 `http-proxy` 可让原始 URL 走代理回源（见下文「[候选级 http-proxy 与超时覆盖（慢代理场景）](#候选级-http-proxy-与超时覆盖慢代理场景)」）。适用范围：**仅完整 URL 目标规则**（web 场景）；路径目标请求（docker 规则）下 `$1` 展开为不带 scheme 的路径串，该候选运行期会失败，不要在 docker 规则这样配。与 `fallback-direct` 的关系：原 URL 候选是**显式参与轮询**的普通候选（可按候选配 `http-proxy`/超时），`fallback-direct` 是候选全败后的兜底直连——不想候选全败后再多一次直连，可配 `fallback-direct: false`。

模板在启动时校验（`config.Validate`）：候选必须是键值对象（旧版纯字符串写法解析期报错并给出迁移示例），`template` 必须以 `http://` 或 `https://` 开头且至少包含一个已知占位符（`$1`/`${1}`，或任一已知 `$http.server.header.*` 变量的 `$` / `${}` 形式），否则启动报错并列出全部合法占位符写法；唯一例外：`template` trim 后**恰好**为 `$1`/`${1}` 视为合法的「原始 URL 独立候选」（见上文），跳过前缀检查。独立写 `template: $http.server.header.full_url`（展开含入站 Host）或 `template: $http.server.header.full_url_no_server`（展开仅为 `/path?query`）不是合法上游地址，仍会启动报错——错误信息会说明各自的展开语义，并指引原始 URL 请写 `template: $1`；候选对象内的未知字段（含预留未实现字段）同样解析期报错。

## url-redirect 候选对象与候选级覆盖

### 候选对象

`url-redirect`（全局与规则级同构）为**对象数组**（破坏性变更：旧版纯字符串写法不再兼容，解析期报错并给出迁移示例）：

```yaml
url-redirect:
  - template: https://docker.1panel.live/$http.server.header.full_url_no_server
  - template: https://docker.actima.top/$http.server.header.full_url_no_server
    insecure-skip-verify: true   # 仅这个镜像站证书不受信，候选级精确开启
  - template: https://docker.slow-mirror.example/$http.server.header.full_url_no_server
    http-proxy: socks5://127.0.0.1:1080   # 仅这个候选走指定代理（socks5/http/https）
    connect-timeout: 30s                  # 该候选建连放宽到 30s（其余候选不受影响）
    http-timeout: 20m                     # 该候选单次尝试总超时放宽到 20m
    probe-timeout: 20s                    # 该候选探测预算放宽到 20s（全局缺省 5s 会在慢代理上超时）
```

| 字段 | 类型 | 说明 |
| --- | --- | --- |
| `template` | string | **必填**（trim 后非空）：即旧版的模板字符串，占位符语义不变（见上文「[url-redirect 模板变量](#url-redirect-模板变量)」）；启动时校验 http(s):// 前缀与至少一个占位符；例外：恰好为 `$1`/`${1}` = 原始 URL 独立候选（原始目标 URL 显式参与轮询，仅完整 URL 目标规则适用），跳过前缀检查 |
| `insecure-skip-verify` | bool | 可选：候选级「忽略 TLS 证书校验」。省略（nil）= 继承规则级，再继承全局 |
| `http-proxy` | string | 可选：候选级回源代理（`http://`/`https://`/`socks5://`）。省略（nil）= 继承规则级 `http-proxy`，再继承全局；**显式空串（或仅空白）= 该候选强制直连**（覆盖上层已配置的代理）；非空值启动时校验地址格式 |
| `connect-timeout` | duration | 可选：候选级建连超时（TCP 拨号 + TLS 握手共用）。nil = 逐级继承；显式 `0` = 不限制；负数为配置错误 |
| `http-timeout` | duration | 可选：候选级单次尝试总超时（覆盖建连 + 响应头 + 完整读 body + token dance，按每次尝试独立计时）。nil = 逐级继承；显式 `0` = 不限制；负数为配置错误 |
| `read-timeout` | duration | 可选：候选级「无新增数据超时」（idle，有进度自动续期）。nil = 逐级继承（规则级含旧 `timeout` 退化别名回落链）；显式 `0` = 不限制；负数为配置错误 |
| `probe-timeout` | duration | 可选：候选级探测预算（规则/全局 `url-redirect-probe-timeout` 的候选级短名）——响应头/判据未就绪即硬失败快切下一候选，仅约束探测阶段。nil = 逐级继承（全局未配置时：blob 目标缺省 `30s`、其它目标缺省 `5s`）；显式 `0` = 不限制；负数为配置错误 |

候选对象做严格字段检查：配置了任何其他字段都会在解析期报错并列出支持字段（绝不静默忽略拼写错误）。

**预留字段（已规划、本期未实现，配置即报错）**：

| 预留字段 | 规划语义 |
| --- | --- |
| `enabled` | 临时下线某个候选（不用注释整行） |
| `weight` | 加权轮询（替代纯 round-robin 起始下标轮换） |
| `hedge-delay` | 候选级覆盖竞速放行间隔（作用于候选序列之间的调度，非单个候选的独立属性，故不按候选对象配置） |

### 「忽略 TLS 证书校验」三级优先级

从高到低（某候选最终生效值 = **候选显式值 ?? 规则显式值 ?? 全局值**）：

1. **候选对象内** `insecure-skip-verify`：对单个镜像站精确开启/关闭；
2. **域名规则级** `insecure-skip-verify`：对整条规则的上游覆盖全局；
3. **全局** `insecure-skip-verify`（默认 `false`）。

开启后**该路径上的所有上游连接都跳过 TLS 证书校验**，作用范围包括：

- url-redirect 候选上游与 fallback 直连 / 完整 URL 直连透传；
- 响应中的 30x 重定向目标（如 1ms.run → cloudfront）；
- Docker token dance 的两次请求（去 realm 取 token + 带 token 重试）。

**安全代价**：跳过证书校验等于关闭该路径的服务端身份验证与中间人/窃听防护（连接仍加密，但无法确认对端身份）。典型场景：内网自签证书上游，或镜像站证书链不全——候选探测报 `tls: failed to verify certificate: x509: certificate signed by unknown authority`，每轮候选探测的重试全部烧在同一个证书错误上。**建议**：全局保持默认 `false`，仅对证书链不全的镜像站在候选级精确开启；整条规则的上游都属内网自签时再用规则级。

### 候选级 http-proxy 与超时覆盖（慢代理场景）

`http-proxy` 与四个超时（`connect-timeout` / `http-timeout` / `read-timeout` / `probe-timeout`）和 `insecure-skip-verify` 同构，均为**三级优先级**（某候选最终生效值 = **候选显式值 ?? 规则显式值 ?? 全局值**，未配置逐级继承）：

1. **候选对象内**：仅该候选生效——显式空串 `http-proxy` = 该候选强制直连（覆盖上层代理）、显式 `0` 超时 = 该候选不限制（覆盖上层预算）；
2. **域名规则级**：`http-proxy` / `connect-timeout` / `http-timeout` / `read-timeout` / `url-redirect-probe-timeout` 的规则显式值（见「[domain-rules 配置](#domain-rules-配置)」）；
3. **全局**：`http-proxy` / `connect-timeout` / `http-timeout` / `read-timeout` / `url-redirect-probe-timeout` 全局值（见「[全局配置](#全局配置)」）。

每个候选的生效值在候选展开（ExpandCandidates）时逐候选独立解析，作用于该候选的整条尝试路径：上游客户端按「(代理, connect-timeout, insecure)」组合预构建复用（连接复用不受影响）；候选 `http-timeout` / `read-timeout` 是该候选每次尝试的总预算与无新增数据看门狗；候选 `probe-timeout` 是该候选探测阶段的硬失败快切预算；候选内的 30x 重定向跟随与 token dance 沿用同一候选客户端，天然继承候选级代理/建连/insecure 配置。fallback 直连回退不受候选级配置影响（仍按规则/全局值）。

**典型场景：某个镜像站必须走慢代理，其余直连**——全局/规则不配代理（其余候选直连），只给慢代理候选配 `http-proxy`，并按代理实际质量放宽该候选的各类超时（全局 5s 探测预算会烧在代理握手上，该候选会被预算误判硬失败快切）：

```yaml
domain-rules:
  - name: docker-registry
    type: docker
    match: '.+registry-proxy-cache\.linkease\...+'
    fallback-direct: false
    url-redirect-probe-timeout: 5s      # 直连候选保持 5s 快速失败
    url-redirect:
      - template: https://docker.1panel.live/$http.server.header.full_url_no_server
      - template: https://docker.1ms.run/$http.server.header.full_url_no_server
      - template: https://slow.via-proxy.example/$http.server.header.full_url_no_server
        http-proxy: socks5://127.0.0.1:1080   # 仅该候选走 socks5 代理
        connect-timeout: 30s                  # 代理建连慢：放宽到 30s
        http-timeout: 20m                     # 经代理的整次尝试放宽到 20m
        probe-timeout: 20s                    # 经代理的探测放宽到 20s（5s 预算内响应头经代理到不了）
```

反向同样成立：全局/规则已配代理时，对个别可达候选写 `http-proxy: ""` 强制直连、`probe-timeout: 0` 不限预算等，按候选精确覆盖上层值。

**典型场景：原 URL 也作为候选走代理回源**——`template` 恰为 `$1` 即原始目标 URL 本身参与候选轮询（与镜像站候选按 round-robin 一起轮），配候选级 `http-proxy` 让它走代理回源；它是显式参与轮询的普通候选，与 `fallback-direct`（全败后的兜底直连）语义不同，若不想候选全败后再多一次直连，配 `fallback-direct: false`。注意仅完整 URL 目标规则适用（路径目标/docker 规则下 `$1` 不带 scheme，该候选运行期会失败）：

```yaml
domain-rules:
  - name: github-raw
    match: '.*https://raw\.githubusercontent\.com.*'
    fallback-direct: false               # 原 URL 已显式参与轮询，全败后不再兜底直连
    url-redirect:
      - template: https://hk.gh-proxy.org/$http.server.header.full_url_no_server
      - template: $1                           # 原始 URL 也参与候选
        http-proxy: http://127.0.0.1:7890      # 仅该候选（原 URL）走代理回源
```

## Docker Registry 镜像加速

docker daemon 可以把本服务当作 registry 访问：请求路径不是完整 URL，而是 registry 协议路径（如 `GET/HEAD /v2/<name>/manifests/<ref>`、`GET /v2/<name>/blobs/<digest>`）。本服务将其识别为「**路径目标**」，由命中的域名规则 url-redirect 模板拼出镜像站回源地址。

### 配置示例

```yaml
domain-rules:
  - name: docker-registry
    # 请求类型过滤：仅路径目标请求命中本规则，web 流量不受影响
    #（留空 = 不过滤；同一入站域名下可另配 type: web 规则分流 web 流量）
    # ⚠ 必须写 type: docker —— 缺 type 时规则级/全局的 slow-download-* 四个参数静默不生效
    type: docker
    # 入站域名（解析到本服务）形如 registry-proxy-cache.linkease.net:5480 时命中；
    # 换成自己部署的域名（match 对「http://<入站Host>/<路径目标>」整串非锚定匹配）
    match: '.+registry-proxy-cache\.linkease\...+'
    cache:
      enabled: true
      ttl: 666h                # 镜像 manifest/layer 内容不可变，缓存 666 小时（0=永不过期）
      path: ./cache-docker     # 规则独立缓存目录，与其他场景隔离（Docker 挂载时记得带上并保证可写）
      clean-interval: 30m
    success-check: status      # registry /v2/ ping 等响应可能空 body，content 判据会误拒
    fallback-direct: false     # 路径目标无完整原始 URL 可直连，候选全败直接报错
    read-timeout: 120s         # 大镜像 blob 慢速但持续传输不被砍：120s 内无任何新增数据才判超时
    http-timeout: 600s         # 单次尝试总超时兜底（含 token dance 与读 body 全程）
    url-redirect-probe-timeout: 5s   # 单候选探测预算：响应头 5s 未就绪即硬失败快切下一候选
    url-redirect-hedge-delay: 2s     # 候选阶梯竞速：每 2s 放行下一个候选并发探测，先过判据者胜出
    url-redirect-item-error-retry: 1   # 镜像站偶发抖动：候选失败后原地重试一次（硬失败跳过）
    url-redirect-all-error-retry: 1    # 整轮候选全败后再整体重试一轮
    url-redirect:              # 多镜像站候选轮询（对象数组），均用 full_url_no_server 拼回源路径；
                               # 候选级 insecure-skip-verify 仅对该候选生效（三级优先级最高）
      - template: https://docker.1panel.live/$http.server.header.full_url_no_server
      - template: https://hub.rat.dev/$http.server.header.full_url_no_server
      - template: https://docker.m.daocloud.io/$http.server.header.full_url_no_server
      - template: https://docker.1ms.run/$http.server.header.full_url_no_server
      - template: https://docker.actima.top/$http.server.header.full_url_no_server
        insecure-skip-verify: true   # 仅这个镜像站证书不受信（x509: unknown authority），候选级精确开启
      # 内网 kspeeder：地址里的主机名不在它证书的 SAN 里（证书只对 *.linkease.net 有效），
      # 必须给该候选显式 insecure-skip-verify: true —— 否则每次 TLS 握手都失败、还会按
      # url-redirect-item-error-retry 反复原地重试（纯噪音，该候选永远不可能胜出）。
      # 若该服务另有证书匹配的域名，用域名比关校验更安全：
      #   - template: https://kspeeder.linkease.net:5443/$http.server.header.full_url_no_server
      - template: https://kspeeder.common-all:5443/$http.server.header.full_url_no_server
        insecure-skip-verify: true   # 地址主机名与证书不匹配（不是证书链不全），必需
      # 想让 Docker Hub 官方 registry 也作为候选兜底：写完整模板 + 给该候选配代理
      - template: https://registry-1.docker.io/$http.server.header.full_url_no_server
        http-proxy: socks5://127.0.0.1:1080
```

> **docker 规则（路径目标）里不要写 `template: $1`**：`$1` 是「不带 scheme 的目标串」，路径目标下它会展开成 `v2/xxx/blobs/sha256:...` 这样的路径串，回源必然报 `unsupported protocol scheme ""` 失败（配合 `url-redirect-item-error-retry` 还会原地反复重试），属于永远失败的噪音候选。`$1` 只在完整 URL 目标（web 规则）下有意义——见上文「[url-redirect 模板变量](#url-redirect-模板变量)」的「原始 URL 独立候选」。

> 候选探测报 `tls: failed to verify certificate: x509: certificate signed by unknown authority`（证书链不全的镜像站，每轮候选的重试会全部烧在同一个证书错误上）时，按上例对该候选配置 `insecure-skip-verify: true` 精确开启；优先级与安全代价见上文「[url-redirect 候选对象与候选级覆盖](#url-redirect-候选对象与候选级覆盖)」。

### 流式回源（docker pull 必读）

路径目标 GET 的缓存 MISS / BYPASS 回源为**流式透传**（详见「[流式回源与 docker ~30s 响应头超时](#流式回源与-docker-30s-响应头超时)」）：候选竞速选定后立即向 docker 写响应头（commit），再边读上游边写给 docker 与缓存。docker 客户端等响应头约 30s 就超时断开，流式透传保证首字节时间只有「选定候选」的耗时（hedge 竞速下 5 候选最坏约 10s 量级），大 blob 的完整下载时间不再计入首字节。commit 后 docker 断开（broken pipe）时**断开即停**：立即终止上游读取与多源合并流（不再继续读完只填缓存、不再长时间占用上游连接与带宽），blob 缓存可读主路径保留半截（进度不回退），docker 重试该层时从断点无感续传（半截已完整则整体回放写缓存）或直接缓存命中（HIT）。带 `Range` 的断点续传（docker 的 `httpReadSeeker` 无论从哪个偏移都带 Range）：起点 0（`bytes=0-`）等价于完整 GET（见下文「[blob 起点 0 的 Range = 完整 GET](#blob-起点-0-的-range--完整-get)」），缓存 HIT / 半截无感续传 / 预取搭车全部生效；**起点 >0（真·断点续传，现场起点 ≈ 82.8MB）时本地有完整条目就直接切片回 206、只有半截前缀时从起点回放 + 上游续传拼接**（见下文「[起点 >0 的 Range：本地切片秒回 / 半截对齐续传](#起点-0-的-range本地切片秒回--半截对齐续传)」）；非 blob 目标与后缀区间/多区间仍把上游 206 透传、不写缓存。

### blob 起点 0 的 Range = 完整 GET

docker/containerd 的 `httpReadSeeker` **无论从哪个偏移开始都会带** `Range: bytes=<offset>-`：首次拉某个 layer 时 offset=0，即 `Range: bytes=0-`。这条链路（客户端 → 本服务 → 镜像站）不稳时，几百 MB 的单层必然中途断开，能不能拉完完全取决于「断点续传」是否真的生效。

处理规则（只对**路径目标且能解析为 `/v2/<name>/blobs/<digest>`** 的请求生效）：

| 客户端 Range | 本服务的处理 | 客户端看到的 |
| --- | --- | --- |
| `bytes=0-` / `bytes=0-<end>`（**起点 0**） | 剥离 Range 后按**完整 GET** 处理：缓存 HIT / 半截无感续传（`X-Cache: RESUME`）/ 在途预取搭车（`X-Cache: PREFETCH`）/ 单飞合并全部生效 | `206` + `Content-Range: bytes 0-<total-1>/<total>` + `Accept-Ranges: bytes` |
| `bytes=<offset>-`（**起点 >0**） | 本地有完整缓存条目 → 从条目 body 切片回 206（**不发上游**）；无完整条目但有半截前缀且起点 ≤ 半截长度 → 从半截起点回放 + 上游剩余区间续传拼接（见下节） | `206` + `Content-Range: bytes <offset>-<last>/<total>` + `Accept-Ranges: bytes` |
| 起点 > 半截长度（有缺口）/ 无半截 / `bytes=-N` / 多区间 / 非 blob 目标 | 保持旧版：判 BYPASS，Range 原样透传上游（不写缓存、不做本地拼接）；有缺口时另打一条 Warn 说明缺口大小 | 上游的 `206` 原样透传 |

为什么「起点 0」可以按完整 GET 处理：`bytes=0-` 要的就是整个对象，与无 Range 的完整 GET 语义完全等价；blob 按 digest 寻址、内容不可变，缓存条目与半截文件都是「同一个对象的前缀」，复用与拼接都安全——这也是 docker 完全兼容 206 的语义（它自己发的 Range，本就期望 206）。总长未知（上游没声明 `Content-Length`、走 chunked）时无法构造合法的 `Content-Range`：开区间退回 `200` 全量响应（offset=0 的客户端可接受），带区间终点时按 RFC 7233 用总长未知写法 `bytes 0-<end>/*`。

配套的两点行为：

- **客户端中途断开时同样保留半截**：此前「带 Range 的请求一律不保留半截」，于是 302MB 的层断开一次就白扔已下载的几十 MB；现在起点 0 的 blob 请求断在半路，已下载前缀照样落盘，下次重试从更长前缀接着续（进度不回退）。
- **`probe-timeout` 对 blob 目标用 `30s` 缺省**（候选级/规则级/全局都未显式配置时；显式配置优先）：大 layer 的上游响应头经常 5s 内到不了，旧版 5s 预算会把候选全判硬失败直接 502。**探测预算的计时终点是「最终响应头到达」**（含 token dance 的第二次响应）：响应头到手即停表，不把首块 body 算进预算——现场 1ms.run 已回 206（dance 花 2.9s），却因为还在等首块被判 `probe-timeout` 硬失败丢弃。
- **内置候选扫描总预算按目标放宽**：manifest 等仍是 `20s`，**docker blob 目标 `60s` 起**（`max(60s, (候选数-1)×hedge-delay + 最大候选探测预算 + 20s)`）——否则 20s 会在 30s 探测预算用完之前先把扫描掐死（现场日志：探测已拿到 206 却因总预算耗尽收 502），配了 `url-redirect-all-error-retry` 也会形同虚设。预算只是「放弃阈值」，命中候选仍立即 commit，不会把客户端拖过 docker 约 30s 的响应头超时。

### 起点 >0 的 Range：本地切片秒回 / 半截对齐续传

现场真正卡住的不是「首次从 0 开始」，而是**断点续传的重试**：docker 的 `httpReadSeeker` 从已有偏移继续时同样带 `Range: bytes=<offset>-`（现场 content store 里该 layer 已落 311MB，重试请求的起点 ≈ 82.8MB）。旧版把所有 Range 一律判 BYPASS，于是这类重试既不复用本地缓存、也不复用本服务已经下到的半截前缀，每次都重新向上游要一遍（现场上游 13KB/s，慢而不死），每轮重试还要再烧一遍候选探测。

现在的分流（只对 `/v2/<name>/blobs/<digest>` 的单区间 `bytes` Range 生效）：

1. **缓存里有完整 blob 条目 → 直接本地切片回 206，一个字节都不发上游**：`Content-Range: bytes <start>-<last>/<total>`、`Accept-Ranges: bytes`、`Content-Length` 按区间长度算；终点越界按 RFC 收窄到 `total-1`，**起点越界回 416 + `Content-Range: bytes */<total>`**。这是「一次落盘、后续任意起点全秒回」的根治方向（docker 因任何原因重试同一层都不再打镜像站）；
2. **缓存 MISS 但有半截前缀，且请求起点 ≤ 半截长度** → 复用既有续传管线（不新造下载器）：半截的起点位置直接回放（`part[start:partLen]`）+ 上游从 `partLen` 继续 Range 续传拼接，对客户端回 206；内部仍按**完整 blob** 拼接并写缓存（缓存条目必须完整，后续任意起点才能走第 1 条）；
3. **有半截但请求起点 > 半截长度（中间缺一段）** → 不能凭空补（拼上去就是损坏内容）：维持旧版 206 透传，并打一条 Warn（`range_start` / `part_bytes` / `gap_bytes`），便于运维看出「半截为什么没帮上忙」；
4. **无半截 / 半截为空 / 后缀区间 / 多区间 / 非 blob 目标** → 维持旧版 206 透传（语义与旧版完全一致）。

边界与降级：区间请求拿不到对象总长（上游 206 没有可解析的 `Content-Range`）时绝不 commit——半截前缀长度不等于总长，谎报会让客户端从错误偏移继续；此时直接退回旧版 206 透传。区间结束早于总长时仍会继续读完上游填缓存（一次落盘后续全秒回；docker 实际发的基本都是开区间）。

### 镜像站匿名 401 挑战与自动 token 获取

daocloud / 1ms.run / hub.rat.dev 等镜像站对匿名 GET/HEAD 的 blob/manifest 请求常直接返回 **401**，响应带 `WWW-Authenticate: Bearer realm="https://auth.docker.io/token",service="registry.docker.io",scope="repository:<name>:pull"` 挑战。docker 客户端直连时会自动「按 challenge 去 realm 匿名取 token → 带 `Authorization: Bearer <token>` 重试」，所以直连能成功；反代场景若只透传裸请求，所有 url-redirect 候选都会被 401 拒绝、最终 502。

本代理已在回源链路自动完成这套**匿名 token dance**（与 docker 客户端直连行为等价）：GET/HEAD 回源收到 401 且挑战为 Bearer（含 realm）时，自动向 realm 匿名换取 token（仅附带挑战里的 service/scope 查询参数，绝不携带客户端的 Authorization）并带 `Authorization: Bearer <token>` 重试一次（仍 401 则按该响应原样返回，不循环）；token 按 `realm|service|scope` 进程内缓存（TTL 取响应 `expires_in`，缺省 60s），避免每个 blob 层都打一次 auth 端点。取 token 或重试失败时按原始 401 响应返回，继续走既有的候选轮询与成功判据（不影响缓存写盘语义）。token 获取、带 token 重试与读 body 全程都计入该规则的 `http-timeout`（单次尝试总超时），且 dance 各阶段的响应头/数据到达都会重置 `read-timeout` 看门狗。大镜像 blob 拉取耗时较长，建议为该规则配置 `read-timeout`（如示例中的 `read-timeout: 120s`——数据持续传输就不会被砍，仅当窗口内无任何新增数据才判超时）与 `http-timeout`（如 `600s` 兜底）；旧版「放宽 `timeout` 总超时」的写法已不适用（`timeout` 现仅为 read-timeout 的退化别名）。

### manifest 驱动的镜像 layer 异步预取

docker 客户端并发下载 layer 受限（daemon 默认 `max-concurrent-downloads=3`），manifest 拿到后逐层请求，拉镜像慢。本代理在识别出某次请求是 docker manifest 请求且回源成功后，**异步预取该镜像其余 layer blob 写入缓存**——客户端后续 blob 请求直接命中（`X-Cache: HIT`），不用等客户端逐个请求才下载。

**触发条件**（全部满足才预取；任一不满足静默返回，绝不影响客户端主请求）：

- **GET 路径目标回源成功**：HTTP 200 且已过成功判据写入缓存；`206`/`Range` 断点续传、`no-cache`、关缓存等 BYPASS 请求到不了「写入缓存」这一步，天然不触发；**HEAD 不触发**（HEAD 无 body 不写缓存）
- **URL 形如 `/v2/<name>/manifests/<ref>`**：`<name>` 支持多级仓库，`<ref>` 为 tag 或 digest（digest 形式的 manifest 请求同样触发）；blob 请求（`/v2/<name>/blobs/<digest>`）与完整 URL 目标（web）不触发
- **响应 Content-Type 为单平台 image manifest**：`application/vnd.docker.distribution.manifest.v2+json` 或 `application/vnd.oci.image.manifest.v1+json`（容忍 `;charset=...` 等参数后缀）。**manifest list / OCI index（多平台）不触发**——客户端拿到 index 后会立刻再请求具体平台的 child manifest，届时自然触发，代理无需解析 index 替客户端挑平台

预取内容 = manifest 的 `config.digest` + 全部 `layers[].digest`，按 `/v2/<name>/blobs/<digest>` 构造 blob 目标（`name` 取 manifest URL 中的 name；blob 缓存键与客户端 blob 请求同构、含入站 Host）。

**并发与去重**：

- **Server 级有界并发**：全部预取共享一个信号量（`docker-prefetch-concurrency`，默认 4），预取在异步 goroutine 中执行、绝不阻塞 manifest 响应
- **未过期缓存命中 → 跳过**：已有缓存的 blob 不重复下载
- **预取在途集合去重**：同一 blob 的预取只排队一次（多个客户端同时拉同一镜像 / 预取被重复触发时防重复排队）
- **singleflight 合并**：预取下载与客户端 blob 请求共用同一缓存键的 singleflight——客户端正在下载某 blob 时预取不另起第二路下载（等待并共享其结果）；客户端在预取登记在途流共享（hub）之前的极小窗口期撞上预取，同样经 singleflight 合并
- **在途流共享（hub）**：预取执行方为流式下载——边读上游边写缓存临时文件并逐块广播增量；客户端请求撞上还在下载中的层时直接**搭车**（见下节「[在途预取层搭车](#在途预取层搭车)」），上游对该 blob 始终只有一路下载

**回源路径复用**：预取 blob 的下载与客户端请求**完全相同的回源链路**——候选轮询（round-robin 起始下标共用）、候选级超时覆盖、`http-proxy`、insecure TLS、docker 401 匿名 token dance、成功判据与缓存写入全复用；**只写缓存、不产生客户端响应**。预取那一路不叠加 slowdownload 慢速升级与时间驱动的多源并发补充（上游速率由预取一路决定，过慢时靠搭车订阅超时降级 + 客户端 Range 重试兜底）——但解析 manifest 时**与预取开关无关地**把每个 layer 的 `digest → size` 记进进程内索引，供「blob 流式回源无 `Content-Length`」时的 `knownTotal` 兜底（见「[时间驱动的多源并发补充](#时间驱动的多源并发补充blob-concurrency-interval)」）。

**失败降级**：预取失败只记日志（Warn/Debug），不重试（候选管线自身已有单候选/整轮重试），绝不影响客户端主请求；正在搭车的客户端按 failed 广播降级（未 commit 改走原回源路径 / 已 commit 终止本次响应，docker 以 Range 续传重试），未搭车的客户端后续请求照走原路径。

**刷新语义**：blob 按 digest 寻址、**内容不可变，不存在刷新问题**，过期由现有 `cache.ttl` / `clean-interval` 与后台 Janitor 管理；预取**不改变 manifest 的 TTL/ETag/缓存行为**；tag（如 `:latest`）更新后新 manifest 引用相同 digest 的 layer 直接命中缓存，digest 变了才回源下载。预取任务自带独立超时兜底（5 分钟/任务，不使用请求 context），进程退出不等待预取任务（绝不悬挂）。

### 在途预取层搭车

上一节预取解决了「客户端没请求的层提前下载」，但有个缺口：某层（blob）**还在预取下载中**时客户端请求到它，旧版要进 singleflight 等待——等待上限只有 10s（大 layer 下载几十秒到几分钟），超限后客户端**自行回源 = 第二路完整下载**，既重复下载、又从 0 开始。**在途流共享（hub）+ 磁盘前缀拼接搭车**补上这个缺口：

- **搭车机制**：预取/续传开始流式下载时在 Server 级注册表登记（blob 缓存键 → 在途条目）；客户端 blob GET 缓存 MISS 后、进 singleflight 等待之前先查该注册表——命中在途条目即搭车：响应头照抄执行方那一路的响应头快照（`Content-Length` / `Docker-Content-Digest` / `Content-Type` / `X-Proxy-Upstream`），`X-Cache: PREFETCH`，body = 已落盘磁盘前缀（从 0 读临时/半截文件）+ 订阅增量 channel 的实时后续。已下载的部分直接复用，客户端不从 0 重新发起网络下载，上游对该 blob 始终**只有执行方一路下载**。
- **立即响应（首字节时序）**：执行方的响应头快照在候选扫描/竞速完成前**不会就绪**（blob 扫描预算 60s 起步，而 docker 客户端等响应头约 30s 就超时断开），因此搭车对「快照未就绪」的在途条目**立即合成响应头 commit，不再阻塞等快照**（2026-11 现场「docker pull 一行进度都没有」的直接卡点之一）——合成头与续传执行方同一构造（`Content-Type: application/octet-stream` + `Docker-Content-Digest`，总长取 manifest 索引、未知则 chunked 不谎报），commit 后照常回放本地前缀并随广播收增量；本地前缀内容永远可信，先发给客户端是安全的。
- **多客户端同时搭车**：各自独立订阅（独立增量 channel、逐订阅者独立拷贝广播块），互不阻塞；客户端读速快于预取下载速度时 reader 天然阻塞等增量（背压）。
- **边界**：仅无 Range 的完整 GET 搭车——HEAD 走原路径；带 Range（206 断点续传）/ `no-cache` / 关缓存等 BYPASS 请求不搭车。docker 客户端搭车被断后重试会用 Range 断点续传，既有「206 透传不写缓存」机制自然兜底「不从 0 下载」（重试命中已写盘的前缀缓存则直接 HIT）。
- **活性与降级**：搭车**不再有「等响应头快照」的阻塞窗口**（快照未就绪即合成头先 commit，见上）；订阅等待下一段增量仍有活性超时（常规 30s，与 docker 客户端等响应头的约 30s 超时同量级；快照未就绪的早响应阶段取与执行方同源的候选扫描总预算——执行方扫描期间不广播增量，不能因「扫描还没结束」掐断已 commit 的搭车响应）——已 commit 的搭车在增量停滞超时后终止本次响应并记日志（docker 对该层以 Range 续传重试）；订阅时尚未 commit（极小竞态窗口）则维持旧降级（放弃搭车走既有原回源路径，singleflight、流式透传、慢速升级等特性全保留）。搭车路径不叠加 slowdownload 慢速升级多源并发续传：上游速率由执行方那一路决定，过慢时靠订阅超时降级 + 客户端 Range 重试兜底。
- **预取任务超时兜底**：预取任务 5 分钟独立超时到期（经候选 `http-timeout` 预算传导）时该路下载失败，广播 failed 给全部搭车订阅者后移除条目（订阅者同上降级/终止）。
- **开关**：随 `docker-prefetch-enabled` 一同生效——关闭预取即无在途条目，搭车自然不生效，无新增配置项。

### blob 半截保留与无感续传

用户场景：镜像层的 digest（唯一值）不变，之前下载到一部分（如 248.9MB/773.3MB）时中断，**重 pull 时 docker 客户端是无 Range 的完整 GET（从 0 开始）**，但 proxy 这边已经下载过的一部分应该保留——客户端再请求该层时把已下载部分直接给客户端，proxy 同时从断掉的地方继续向远端下载。

- **半截保留**：blob（`/v2/<name>/blobs/<digest>`）的流式下载（客户端主路径与预取路径都算）中断/失败时，把已下载前缀保留为缓存目录内的半截文件 `.part-<缓存key>`（文件大小即已下载字节数，内容为 blob 原文前缀、无缓存文件头）。任何保留动作仅在「新前缀比现有半截更长」时落盘——**进度绝不回退**；进程崩溃残留由 `cache.Clean` 后台清理兜底（空半截立即清理；`mtime` 超过 24h——量级同缓存条目过期 TTL——的孤儿残留一并清理，活跃下载/续传中的半截持续追加、`mtime` 新鲜不受影响）。
- **无感续传（主场景）**：客户端无 Range 完整 GET 撞上「正式缓存 MISS + 半截文件存在」时走**续传执行方**——按既有候选轮询（round-robin 起始下标共用）逐候选发 `Range: bytes=<已存字节>-`（既有候选级超时 / `http-proxy` / insecure TLS / 401 匿名 token dance 全复用，整体 blob 放大扫描预算兜底），从上游 `206` 响应的 `Content-Range` 取总长，对客户端回**一次完整 200**：`Content-Length=总长`（可得时，拿不到则流式不设），body = 本地半截前缀回放 + 上游剩余区间实时拼接——**docker 客户端无需支持 Range、完全无感知**（`X-Cache: RESUME`）。拼接期间上游剩余数据**边转发边追加写半截文件**；EOF 后半截内容 = 完整 blob，按既有 `cache.Set` 语义原子转为正式缓存条目并立即删除半截文件。
- **立即响应（首字节时序优化）**：客户端响应头**不再等待候选扫描命中**——2026-11 现场「docker pull 输出停在 `Pulling from` 长时间零进度、代理侧续传日志却正常推进」的根因即此前的响应头被扫描窗口拖住（逐候选探测 + 401 匿名 token dance，预算 60s 起步，docker 约 30s 就断）。现在登记续传在途条目后**立即合成响应头 commit 并回放本地半截前缀**（总长取 manifest 索引 `blob_size_index.go`，未知则不设 `Content-Length` 走 chunked——docker 完全兼容，至少立即看到字节进度），docker 毫秒级看到响应头与进度条；扫描命中后经进度汇总 `setTotal` 按上游 `Content-Range` 校正总长。**上游增量仍必须等既有安全校验（206 且 `Content-Range` 起点匹配）通过后才拼接，绝不放宽**；扫描失败（含上游明确拒绝 Range）时响应已 commit，按「安全中断」截断本次响应（半截保留/丢弃规则不变，docker 以 Range/重试接续——绝不在已回放前缀的流上降级全量回源，从 0 的全量与已发前缀拼接必然错位）。
- **并发合并**：续传执行方登记与预取同一套在途流共享注册表（resume 条目），其余客户端请求/等待超限的重试请求经既有搭车机制订阅「半截前缀 + 实时增量」（`X-Cache: PREFETCH`，上游只一路下载）；同 blob 的预取任务经既有 singleflight（同缓存键）合并到同一路续传流。半截文件的全部写侧操作按缓存键互斥（逐块追加 / 整体保存 / 预取改名），且续传执行方逐块追加前校验文件大小——半截被并发保存方更新为更长前缀时本流立即收尾（更长前缀已保留，进度不回退），绝不产生错位拼接。
- **安全依据**：blob 按 digest 寻址、**内容不可变**——任意候选源的同 digest 内容一致，半截可从任意候选发 `Range` 续传拼接（续传可跨候选）；客户端/上游侧 `Docker-Content-Digest` 不做本地全量校验（GB 级 blob 成本过高），信任 digest 寻址语义。**上游字节写给客户端的前置校验绝不放宽：206 且 `Content-Range` 起点匹配半截长度确认前，绝不把上游字节写给客户端**——若上游对 Range 回 200 全量响应，把本地前缀与全量响应拼在一起会数据错位，绝不允许。立即响应只把**本地半截前缀**（同 digest 内容的合法前缀，内容永远可信）先发给客户端；扫描失败/非 206 时响应已 commit 的按「安全中断」截断（docker 重试接续，绝不在已回放前缀的流上拼全量），未 commit 的（区间请求且总长未知的旧时序）维持既有降级全量回源不变。
- **边界与降级**：manifest 会变，**仅 blob 启用**；客户端带 Range 的请求按起点分流——起点 0 的 `bytes=0-` 等价完整 GET（照常续传），起点 >0 且不超过半截长度时复用本管线（从半截的起点回放 + 上游剩余拼接、对客户端回 206，内部仍写完整缓存），起点超出半截（有缺口）/ 后缀区间 / 多区间 / 非 blob 目标维持旧版 206 透传；半截为 0 字节直接丢弃；上游对 Range 回非 206（不支持 / 200 全量回）→ 立即响应路径（响应头已 commit、前缀已回放）按「安全中断」截断本次响应并丢弃半截（已发的前缀字节是合法内容前缀、绝不错位，docker 重试走干净全量回源自愈），未 commit 的旧时序路径维持既有降级全量回源；纯网络/超时类失败保留半截继续全量回源（全量再失败时失败捕获会把更长前缀存回半截，进度不回退）；上游 416（Range 起点 ≥ 总长，而半截是合法前缀）证明**半截已完整**，直接读回写缓存回放，不再全量下载；续传中途读上游失败 / 长度不符 → 保留更新后的半截、本次响应被截断（docker 重试接着更长前缀续传——进度不回退）；EOF 但累积长度 ≠ 总长 → 不写缓存（绝不缓存短 body）。续传路径**接入**时间驱动的多源并发补充（`blob-concurrency-interval`，日志 `source(来源)=resume`）：每满间隔且该层未完成，就向尚未加入的候选源补充并发 Range 续传（只做加法、客户端无感、不切状态码），判速触发同样生效——见「[时间驱动的多源并发补充](#时间驱动的多源并发补充blob-concurrency-interval)」。
- **宽限终止（客户端断开宽限终止，仅续传执行方）**：续传执行方向客户端写响应体失败（docker 已断开）**且**在途条目（hub resume 条目）无搭车订阅者时，按 `docker-resume-client-gone-grace`（默认 `2s`）启动宽限计时——到期仍无订阅者则**终止上游读取、保留半截（进度不回退）、移除在途条目**，不再「继续读完只填缓存」长时间占用上游带宽；宽限期内有订阅者搭车（subscribe）则放弃终止、继续拉（订阅者还要收增量）；断开时已有订阅者则不宽限、直接维持现状。动机：旧语义「客户端断开后继续读完只填缓存」在现场表现为没人下载还长时间占上游带宽（261MB blob 断开后按旧语义又拉了 3m44s 才写完）。语义分工（用户明确约定）：docker 客户端驱动的链路（`client` 主路径 / `ridealong` 搭车读侧 / `resume` 续传执行方）客户端没了就该停——客户端主路径是**断开即停**（b251149，立即终止），续传执行方是**宽限终止**（给搭车订阅者留上车窗口），搭车读侧断开仅停读取订阅；**异步预取（`source=prefetch`）没有客户端概念，保持「继续填缓存」语义不变，绝不受本项影响**。日志：宽限终止为 Info 级运维事件（消息含「宽限终止」：`blob 半截续传客户端断开且无搭车订阅者，宽限终止续传（半截保留，下次从断点再续）`），触发判定时另有 Warn「`写响应体失败（客户端已断开，续传按宽限语义处置）`」（含宽限时长 `grace(宽限)` 与搭车订阅数 `subscribers(搭车订阅数)` 键）——均为运维事件而非故障，**绝不产生 Error**。配置：`docker-resume-client-gone-grace` 默认 `2s`；显式 `<=0` = 关闭宽限终止（完全恢复旧「继续读完填缓存」语义）。
- **开关**：`docker-partial-resume-enabled`（全局，默认开启；显式 `false` 关闭后全部路径行为与旧版完全一致——无半截文件的产生、保留与续传）。

### docker 可变目标多源并发回源与合并

用户场景：docker 可变目标（`GET/HEAD /v2/<name>/manifests/<tag>`、`/v2/<name>/referrers/<digest>`、`/v2/<name>/tags/list`，因内容随 push / 签名追加而变化**不缓存、每次回源**，见「docker 可变目标」判定）此前走单候选顺序轮询——先命中的源直接把响应透传给 docker。镜像站之间同步有快慢：先命中的源可能 `tags/list` 还没刷新（docker 拿不到最新 tag）、manifest 还是旧 digest（`Image is up to date` 假象）。`docker-multi-source-enabled`（全局，默认开启）把这类 BYPASS 回源改为**并发请求全部候选源 + 按端点类型合并/共识**：

- **tags/list 合并**：各源 tags 取**并集、去重、按字典序排序**后返回（顶层其余字段如 `name` 取最快成功源原文），任何慢源/快源都漏不了 tag。
- **referrers 合并**：各源 descriptors 数组按 **mediaType+digest 去重**合并（保留首次出现顺序与描述符未知字段），cosign 签名 / buildx attestation 在任一源上先出现都能及时返回。
- **manifests 摘要共识（GET 与 HEAD 同策略）**：收集各成功源响应头的 `Docker-Content-Digest` 按**票数取多数**，平票取最快成功者，用该源的 body+响应头整体应答——保证 docker pull 流程内 HEAD/GET 判定一致（不会 HEAD 探到新 digest、GET 又拿到旧 body）。
- **为什么共识后无需再做源 pin**：tags 与 manifest 由同一 push 事务产生，多数源给出的 `Docker-Content-Digest` 即最新内容寻址结果；**blob 按 digest 寻址、内容不可变**——共识摘要确定后，任意源取同 digest 的 manifest 与 blob 内容都一致，不存在「后续请求漂移到旧源拿旧内容」的问题，因此不需要把共识源固定下来（与半截续传「任意候选同 digest 内容一致可续」是同一条安全依据）。
- **成功判据复用**：与顺序管线完全相同（`success-check` + HTML 反爬页拒判 `html-error-page`），未过判据的源（如镜像站反爬页）被**拒之于合并之外**；tags/referrers 合并还要求源响应为 200 且 body 可解析为目标端点 JSON，解析失败视同该源失败。
- **失败语义**：**全部源失败 → 退回现有顺序候选管线**（继承其「上游明确 4xx 透传 / 其余 502」语义，如 referrers 404 透传——docker 对 registry 语义错误自带容错）；**仅 1 源成功 → 直接用该源**（等价旧结果但并发下更快）。
- **不合并的边界（刻意收窄）**：
  - 仅 `bypassReason == "docker-mutable"` 的 GET/HEAD 启用多源；其余 BYPASS（`request-range` / `request-no-cache` / `cache-disabled` / `negative-ttl` 等）**维持现有顺序管线不变**；
  - `tags/list` 带分页参数（`?n=` / `?last=`）**不做多源合并**——各源 tag 集合不同，同一 `(n,last)` 在不同源指向不同窗口，合并会产生错页/丢页，退回顺序管线（单源分页语义自洽）；
  - 候选数 <2（单候选）无合并/共识意义，维持顺序管线；
  - GET 可变目标请求带 Range 时（可变判定在 bypass 分流中先于 Range）同样进入多源路径：tags/referrers 合并要求 200，上游 206 全部判为不可合并、退回顺序管线维持既有 206 透传；manifest 共识语义不受 Range 影响。
- **应答形态与断开**：合并/共识必须收齐各源完整响应，无法边收边发，故本路径为**缓冲应答**（`X-Cache: BYPASS`，经 serveEntry 回放，GET 全量 / HEAD 仅响应头）；可变目标（manifest JSON / tags 列表 / referrers 列表）响应体量级小，可接受。客户端断开（`r.Context()` 取消）时**立即取消全部在途上游（断开即停）**——可变目标不写缓存、没有「继续读完填缓存」的价值可挽救——错误按既有断开语义低调处理（不 502 不刷日志）。
- **并发与超时**：候选全并发但设上限 **8**（超出排队放行）；每候选超时复用现有候选级配置（`probe-timeout` / `http-timeout` / `read-timeout` / `connect-timeout`，候选 > 规则 > 全局三级覆盖不变）——慢源不会拖过它自己的每源超时。
- **日志**：每次接管输出一条 Info 汇总（`docker 可变目标多源并发回源完成（tags/list 多源合并返回 | referrers 多源合并返回 | manifest 摘要共识返回）`），键为 `sources(源数量)` / `success(成功源数)` / `merged(合并条数)` / `agree(共识源数)` / `disagree(分歧源数)` / `digest(摘要)` / `upstream(上游)` / `cost(耗时)`（合并类端点的共识键恒 0、共识端点的 merged 恒 0，三键统一输出便于聚合检索）；单源失败为 Debug 级（含 `candidate(候选源)` / `err(错误)` / `hard_fail(硬失败原因)`）；全败退回顺序管线与分页跳过合并各有一条 Debug 留痕。按特性 grep：`grep 'docker 可变目标多源并发回源完成' proxy-cache.log`。
- **开关**：`docker-multi-source-enabled`（全局，默认开启；显式 `false` 关闭后全部路径行为与旧版顺序管线完全一致）。

### 用法

```bash
# 入站域名换成自己部署的（解析到本服务，端口对应 listen）
docker pull registry-proxy-cache.linkease.net:5480/adockero/proxy-cache

# 手动验证（HEAD 探测 manifest；Host 换成自己配置的入站域名）
curl -I -H "Host: registry-proxy-cache.linkease.net:5480" \
  "http://127.0.0.1:8080/v2/adockero/proxy-cache/manifests/latest"
```

### 路径目标请求的限制与行为说明

- **仅支持 GET / HEAD**：docker registry 拉取只用这两个方法；其他方法（POST/PUT 等）对路径目标返回 400（`路径目标仅支持 GET/HEAD 请求`）。POST/PUT 等对**完整 URL 目标**仍保持直接透传（现状不变）
- **建议配置 `type: docker`（做镜像加速时是必须）**：请求类型过滤（可选）——`type: docker` 仅承接路径目标请求、`type: web` 仅承接完整 URL 请求、留空不过滤（默认，兼容旧配置）。同一入站域名既做 docker 加速又做 web 加速时，可用两条同 `match` 规则按 `type` 分流，各自的 cache/超时/候选互不影响；过滤发生在 `match`/`exclude` 之前，类型不符的规则直接跳过。**⚠ 缺 `type: docker` 时，规则级与全局的 `slow-download-speed` / `-window` / `-min-remaining` / `-max-behind` 四个参数静默不生效**（请求照样命中、缓存照样工作，只是慢速升级多源并发续传永远不触发，日志里没有任何报错——线上真实踩过这个坑）
- **必须命中规则**：路径目标请求必须命中某条 domain-rules 规则且该规则生效 `url-redirect` 非空才能回源，否则 400（错误信息保留「目标 URL 必须是完整的 http/https 地址」语义并说明原因）；全局默认配置（未命中规则）不承接路径目标
- **HEAD/GET 行为一致**：docker 用 HEAD 探测 manifest/blob——HEAD 与 GET 统一走候选轮询管线：先查缓存（命中回放响应头，无 body，`X-Cache: HIT`）；未命中按候选轮询回源（方法保持 HEAD，success-check 对 HEAD 自动按 status 语义：200 即成功，304 仍视为成功）；HEAD 响应无 body，**不写缓存**；未配置 url-redirect 时回退直连（与旧版 HEAD 行为一致）
- **缓存 key 含入站 Host**：路径目标的缓存 key 输入为「`http://<入站Host>/<路径目标>`」（再取 sha256），不同入站域名的同路径缓存互不污染；完整 URL 请求的缓存 key 输入仍为目标 URL 原文（兼容已有缓存）
- **无直连回退**：路径目标没有完整原始 URL 可直连，url-redirect 候选全部失败时直接报错（`fallback-direct` 不适用，错误信息注明路径目标无直连回退）
- **ACL 拦截能力保留**：allow-list / deny-list 对「`http://<入站Host>/<路径目标>`」整串做 allow/deny 匹配（完整 URL 请求仍只对目标 URL 匹配，行为不变）

## http-proxy 说明

- 全局 `http-proxy` 对**所有回源请求**生效，包括 url-redirect 候选请求与 fallback 直连请求；支持 `http://`、`https://`、`socks5://` 代理地址
- 域名规则 `http-proxy` 覆盖全局：例如 GitHub 走本地代理、其他目标直连
- url-redirect 候选对象 `http-proxy` 优先级最高（三级覆盖：**候选 > 规则 > 全局**）：可为单个镜像站指定代理（如慢代理候选走 `socks5://`），或写显式空串让该候选强制直连（覆盖上层已配置的代理）；候选级代理与候选级超时（`connect-timeout` 等）组合使用，详见「[候选级 http-proxy 与超时覆盖（慢代理场景）](#候选级-http-proxy-与超时覆盖慢代理场景)」
- 仅「服务端 → 上游」的回源流量走该代理；客户端 → 本服务的流量不受影响；候选全部失败后的 fallback 直连回退不受候选级代理影响（仍按规则/全局值）

## 日志

日志为 slog 结构化文本输出到 stderr，级别由配置文件 `log.level` 控制（大小写不敏感；留空默认 `info`；非法值启动时直接报错）。四级从多到少：`debug` > `info` > `warn` > `error`，高级别包含低级别的全部日志。

```yaml
log:
  level: info   # debug / info / warn / error
```

所有日志的键名按「英文(中文)」双写输出（日志规范：字段名称必须英文 + 中文含义一起写；对照表见 `logging/keys.go`，新增键未登记会被 `go test ./logging` 的 `TestLogKeyCoverage` 拦下）。双写由 `main.go` 里的 `ReplaceAttr`（`logging.ReplaceAttr`）统一完成，调用点仍只写英文键，键序与取值完全不变；slog 内置的 `time`/`level`/`msg` 同样双写，一条日志里不会出现纯英文键：

```text
time(时间)=2026-11-06T18:18:41.188+08:00 level(级别)=INFO msg(消息)=缓存命中（HIT） source(来源)=client image(镜像)=foxhui/webai-2api status(状态码)=200 bytes(字节数)=52428800 speed(速率)=13.2MB/s elapsed(已耗时)=45.3s
```

grep 时按键名原文匹配即可（括号在 grep 基本正则里不是特殊字符），如 `grep 'source(来源)=prefetch'`；只按键名过滤可写 `grep 'source('`。

| 级别 | 能看到什么 |
| --- | --- |
| `info` | 每次决策结果全留痕：规则匹配结果（`规则匹配结果`，含入站 `host` 字段）、GET 绕过缓存原因（`GET 请求绕过缓存（BYPASS）`，`reason(原因)=cache-disabled/request-range/request-no-cache/...`）、url-redirect 轮询展开（`url-redirect 轮询展开`：本轮起始下标 `start_index(起始下标)` + 按尝试顺序的 `模板[下标] 模板 -> 占位符替换后最终URL` 明细）、候选命中并作为最终返回（含模板下标/模板/最终 URL/状态码/耗时）、候选硬失败快切（`url-redirect 候选硬失败，跳过原地重试直接下一候选`，`hard_fail(硬失败原因)=upstream-403/dance-still-401/probe-timeout`）、流式透传开始（`流式透传开始（已向客户端写出响应头）`：commit 的候选/状态码/Content-Length）、流式透传进度（`流式透传进度`：按 `log.progress-interval` 周期输出，含已传字节数/上游声明大小与百分比/区间速率/自 commit 起耗时/客户端是否仍在接收；间隔 `<=0` 关闭）、docker blob 单层下载开始（`docker blob 单层下载开始（客户端主路径）`：`total(总长)` 与 `total_source(总长来源)=upstream|manifest|unknown`、`candidates(候选数)`）、时间驱动的多源并发补充（`blob 下载 N 分钟未完成，评估多源并发补充` / `补充多源并发下载` / `未补充多源并发`（带 `reason(原因)` 枚举），见「[时间驱动的多源并发补充](#时间驱动的多源并发补充blob-concurrency-interval)」）、活跃下载周期汇总（`活跃下载进度汇总` 一条 + 每个活跃下载一条 `活跃下载进度`：按 `log.progress-summary-interval` 周期输出，覆盖 client（含缓存/回放）/ridealong/resume/prefetch，本轮无任何下载登记时静默，见下文「[活跃下载周期汇总](#活跃下载周期汇总)」）、全败回退直连、缓存命中（`缓存命中（HIT）`：缓存 key 与实际命中的文件路径 `file(文件)`）、写入缓存成功（`写入缓存成功`：规则名/缓存 key/落盘文件路径/body 大小/ttl）、成功返回内容（`成功返回内容`：200/206 时输出来源/状态码/字节数/缓存状态；多源并发补充过的流追加 `sources(源数量)`/`alive_sources(存活源数)`）、内容未变更（`内容未变更（304），客户端缓存副本有效`，最终 304 时输出）、blob 半截续传宽限终止（`blob 半截续传客户端断开且无搭车订阅者，宽限终止续传（半截保留，下次从断点再续）`：续传执行方客户端断开且宽限期内无搭车订阅者的运维事件而非故障，绝不 Error）、每条访问完成日志（`代理请求完成`） |
| `debug` | `info` 全部内容，再加：代理请求开始、逐条域名规则匹配过程（每条规则的 `matched(是否命中)` / `excluded(是否被排除)` 明细）、每次候选尝试开始、候选竞速放行（`url-redirect 候选竞速放行`）、候选被取消（其他候选胜出/扫描预算到期）、缓存查询 MISS 细节 |
| `warn` | 候选请求失败 / 未过成功判据 / 回退直连、等待单飞超限自拔回源（`等待同 key 单飞回源超限，自行走候选回源`）、commit 后客户端断开（`写响应体失败（客户端已断开，终止上游读取）` 与断开终止事件 `客户端已断开，终止上游读取（不再继续读完填缓存，客户端将以断点续传或缓存重试）`——运维事件而非故障，含已接收字节数 `bytes(字节数)` 与半截保留状态 `partial(半截保留)`）、续传执行方客户端断开按宽限语义处置（`写响应体失败（客户端已断开，续传按宽限语义处置）`——运维事件而非故障，含宽限时长 `grace(宽限)` 与搭车订阅数 `subscribers(搭车订阅数)`）、commit 后读上游失败（`流式回源读上游失败...不写缓存`）、写缓存失败、ACL 拦截等警告（含 `error` 级日志） |
| `error` | 仅错误：回源彻底失败（502）、候选扫描总预算耗尽（`url-redirect 候选扫描总预算耗尽，提前终止候选扫描`）、候选尝试 panic 等 |

### 日志字段说明（docker 链路统一标签）

docker 加速链路（`/v2/` 路径目标请求）的全部日志统一附带以下键，用于区分「异步下载」与「客户端请求下载」、并按镜像 grep（键名与附加顺序全仓库统一，由 `proxy/log_attrs.go` 的 helper 拼装；`source` 恒在首位、`image` 次之、`digest`/`ref` 再次，既有键 `rule`/`candidate`/`key`/`target`/`err` 等原样保留在其后；下表的「键」列即实际输出形态，如 `source(来源)=client`）：

| 键（输出形态） | 含义 |
| --- | --- |
| `source(来源)` | 本次日志行为的来源，四值枚举：`client`（客户端请求触发的回源：客户端流式主路径、HIT/MISS/BYPASS 回放与等待单飞超限后自行回源）、`ridealong`（搭车订阅在途流的客户端请求，即撞上预取/续传在途层拼接回放的请求，`X-Cache: PREFETCH`）、`resume`（blob 半截续传执行方——候选扫描/拼接/写缓存，`X-Cache: RESUME`）、`prefetch`（后台异步预取，manifest 驱动的 layer 异步下载） |
| `image(镜像)` | 镜像仓库名（`/v2/<name>/...` 的 `<name>`，多级原样保留，如 `foxhui/webai-2api`、`library/nvidia/cuda`）；解析不出（异常路径）时省略该键，不填空串 |
| `digest(摘要)` | blob digest（blob 目标 `/v2/<name>/blobs/<digest>` 的日志附带，如 `sha256:...`） |
| `ref(引用)` | manifest 的 tag 或 digest 引用（manifest 目标 `/v2/<name>/manifests/<ref>` 的日志附带，如 `latest`） |

判定规则：只有能判定为 `/v2/` 请求的日志才附加这些键；web 完整 URL 目标等非 docker 请求的日志不加，行为与旧版零变化。同一 blob 的完整生命周期串起来看就是：`source(来源)=client`（首个请求回源写缓存）→ `source(来源)=prefetch`（后台预取其余层）→ `source(来源)=ridealong`（后续请求搭上在途预取层）→ 中断时 `source(来源)=client`（半截保留）与 `source(来源)=resume`（断点续传执行方）。

**客户端断开终止事件（断开即停）**：commit 后写响应体失败（docker 已断开）时，流式回源不再「继续读完只填缓存」，而是立即终止上游读取（多源合并场景逐路关闭全部上游源）并留两条 Warn 级运维事件日志（`source(来源)=client`，属正常运维而非故障，不会出现 Error 级）：先是「`写响应体失败（客户端已断开，终止上游读取）`」（含 broken pipe 原始错误 `err(错误)`），随后是断开终止事件「`客户端已断开，终止上游读取（不再继续读完填缓存，客户端将以断点续传或缓存重试）`」——含统一标签 `image(镜像)`/`digest(摘要)`、已接收字节数 `bytes(字节数)`、半截保留状态 `partial(半截保留)`（`true` = 半截已保留可断点续传 / `false` = 不适用，如非 blob 目标或 `docker-partial-resume-enabled` 关闭或 BYPASS）；半截保留成功时另有既有「`blob 半截前缀已保留（下载中断，下次无 Range 请求将从断点无感续传）`」Info 日志。调用方对断开错误绝不再向客户端写任何字节（不 502），仅补一条 Debug「客户端已断开，本次响应终止」简记。按镜像 + 断开事件 grep：`grep '客户端已断开，终止上游读取' proxy-cache.log | grep 'image(镜像)=<镜像名>'`。

grep 示例（TextHandler 输出键值按 `k=v` 空格分隔、键序固定 `source` 在前，跨键过滤用管道、与键序无关）：

```bash
# 某镜像的全部链路日志（预取 + 客户端 + 搭车 + 续传一网打尽）
grep 'image(镜像)=foxhui/webai-2api' proxy-cache.log

# 只看某镜像的后台异步预取（等价于 image=xxx 与 source=prefetch 的组合过滤）
grep 'image(镜像)=foxhui/webai-2api' proxy-cache.log | grep 'source(来源)=prefetch'

# 只看客户端请求触发的下载 / 搭车 / 断点续传
grep 'source(来源)=client' proxy-cache.log
grep 'source(来源)=ridealong' proxy-cache.log
grep 'source(来源)=resume' proxy-cache.log

# 时间驱动的多源并发补充：定时判定 / 实际加了几路源 / 为什么没加（reason 枚举）
grep '评估多源并发补充' proxy-cache.log
grep '补充多源并发下载' proxy-cache.log
grep '未补充多源并发' proxy-cache.log
```

### 活跃下载周期汇总

除单条流自己的 10s 进度节拍（`log.progress-interval`，见上文「流式透传进度」）外，服务还维护一份 **Server 级活跃下载注册表**，按 `log.progress-summary-interval`（默认 **3m**；开关 `log.progress-summary-enabled` 默认开）把该周期内的下载集中输出一次——docker 场景下可直接看到「正在下载哪些镜像层、各到几成」，无需逐条翻单流进度日志。

- **覆盖范围（按 `source` 区分）**：`client`（客户端流式回源 `stream.go`，以及缓存命中 / 单飞等待 / BYPASS / 区间回放这类**本地回放** `serveEntry`、`serveEntryBlobRange`）、`ridealong`（客户端搭车在途预取/续传层，`X-Cache: PREFETCH`）、`resume`（blob 半截续传执行方与续传完成后的整体/区间回放，`X-Cache: RESUME`）、`prefetch`（后台异步预取，manifest 驱动的 layer 下载）。前三类是**客户端下载**（由 docker 请求驱动），`prefetch` 是**异步下载**——两者在 `source` 上天然区分，这也是「docker 客户端下载进度」与「异步下载进度」各自 grep 的依据。
- **可见性（本轮有访问就一定有输出）**：汇总头每轮带 `active`（tick 时仍在进行的下载数）与 `started`（自上次 tick 以来新登记的下载数）。只要 `active>0` 或 `started>0` 就输出一轮：活跃下载逐条给进度；即使在 3 分钟窗口内已经结束的短下载（含缓存命中回放），也会通过 `started` 体现在汇总头里，不会出现「确实在下载、但周期日志里什么都没有」。只有本轮既没有活跃下载、也没有任何新登记（没有被访问）时才整轮静默。
- **无幽灵条目**：每个下载在结束、断开、失败时都立即注销条目（`defer`），`started` 只是窗口内登记计数，不保留已完成条目，长期运行不增长内存。
- **输出格式**：一轮汇总 = 一条汇总头 + 每个活跃下载一条进度（`info` 级，与单流进度日志同级）：

```
time(时间)=... level(级别)=INFO msg(消息)=活跃下载进度汇总 active(活跃下载数)=3 started(新登记数)=5 client(客户端)=1 ridealong(搭车数)=0 resume(续传数)=0 prefetch(预取数)=2 interval(汇总间隔)=3m0s
time(时间)=... level(级别)=INFO msg(消息)=活跃下载进度 source(来源)=client image(镜像)=foxhui/webai-2api digest(摘要)=sha256:... method(请求方法)=GET bytes(字节数)=125829120 total(总长)=524288000 percent(进度百分比)=24 speed(速率)=13.2MB/s elapsed(已耗时)=45.3s
time(时间)=... level(级别)=INFO msg(消息)=活跃下载进度 source(来源)=prefetch image(镜像)=foxhui/webai-2api digest(摘要)=sha256:... method(请求方法)=GET bytes(字节数)=83886080 total(总长)=209715200 percent(进度百分比)=40 speed(速率)=8.4MB/s elapsed(已耗时)=21.0s
```

字段语义：`active(活跃下载数)` 为 tick 时活跃下载总数（`client(客户端)`/`ridealong(搭车数)`/`resume(续传数)`/`prefetch(预取数)` 为各来源的活跃数量）；`started(新登记数)` 为自上次汇总以来新登记的下载数（含周期内已结束的）；`bytes(字节数)` 已下载字节数（原始字节）；`total(总长)` 为总长、`percent(进度百分比)` 为 0-100 进度（总长从响应头 `Content-Length` 取得时才输出这两个字段；区间回放的 `total(总长)` 为本次区间长度）；`speed(速率)` 是**与上次汇总之间的区间速率**（不是整条流的平均速率），格式沿用既有 `humanBytesPerSec` 习惯；`elapsed(已耗时)` 为自本次下载开始写的耗时（秒，一位小数）。`image(镜像)`/`digest(摘要)` 仅 docker 目标附带（非 docker 下载省略该键，不填空串）。

- **两类 docker 进度分开看**（TextHandler 输出键序固定 `source` 在首位）：

```bash
# 只看周期汇总（客户端下载 + 异步下载一次性拿到）
grep 'msg(消息)=活跃下载进度' proxy-cache.log

# 只看客户端下载进度（client / ridealong / resume）
grep 'msg(消息)=活跃下载进度' proxy-cache.log | grep -Ev 'source\(来源\)=prefetch'

# 只看异步预取下载进度
grep 'msg(消息)=活跃下载进度' proxy-cache.log | grep 'source(来源)=prefetch'

# 某镜像的全部活跃下载进度
grep 'msg(消息)=活跃下载进度' proxy-cache.log | grep 'image(镜像)=foxhui/webai-2api'
```

- **与单流进度的分工**：`log.progress-interval`（默认 10s）是**每条流自己的节拍**，只覆盖该请求自身的回源/续传流；`log.progress-summary-interval`（默认 3m）是**全局活跃视图**，把客户端下载与异步预取下载放在一起看，两者互不影响、可分别关闭。
- **关闭方式**：`log.progress-summary-enabled: false` 或 `log.progress-summary-interval: 0`（任一即不启动汇总 goroutine；单流 `log.progress-interval` 进度日志不受影响）。

### 排障指引

- **规则缓存目录没有生成缓存文件**，按顺序看三处日志：
  1. 启动日志 `域名规则已加载`：核对每条规则的 `match(匹配表达式)`、`cache_enabled(缓存开关)`、`cache_path(缓存路径)`、`cache_ttl(缓存有效期)`、`url_redirect(重定向模板数)` 是否符合预期（规则未配置项显示沿用全局后的生效值）；
  2. 请求日志 `规则匹配结果`：`rule(规则)` 是命中规则名，`global-default` 表示没命中任何规则——match 里写了入站 Host 段（如 `http://github\.path\..+`）时，请求的 Host 必须以该段开头（如 `github.path.xxx`），直接用 IP:端口访问不会命中，会落全局默认（没有规则的 url-redirect、缓存写进全局 `cache.path` 目录）；
  3. `GET 请求绕过缓存（BYPASS）` / `写入缓存成功` / `缓存命中（HIT）`：分别说明这次为什么不缓存、实际写到了哪个文件（`file(文件)` 字段）、命中了哪个文件。
- **域名规则没按预期命中**：把 `log.level` 设为 `debug`，观察每条规则的「域名规则匹配」日志（`matched(是否命中)` 表示 match 是否命中、`excluded(是否被排除)` 表示是否被 exclude 排除）与最终的「规则匹配结果」日志（`rule(规则)` 为命中的规则名，未命中为 `global-default`），即可区分是 match 没命中还是被 exclude 排除。
- **url-redirect 候选行为异常**：`info` 级别即可看到「url-redirect 轮询展开」（候选数量、轮询起始下标、按尝试顺序的模板与替换后 URL）与每条候选的结果日志（命中为 `url-redirect 候选命中并作为最终返回`，失败在 `warn` 级别），无需开 debug 就能还原整轮轮询；`debug` 额外提供每次尝试开始的明细。

## 缓存实现说明

- **缓存键**：完整 URL 请求 = 目标 URL（含查询串）的 sha256 十六进制（64 字符），直接作为 `cache.path` 下的文件名；路径目标请求 = 「`http://<入站Host>/<路径目标>`」的 sha256（入站 Host 参与，避免不同入站域名的同路径缓存互相污染）
- **文件格式**：8 字节文件头（4 字节魔数 + 4 字节元信息长度）+ JSON 元信息（状态码、响应头、写入时间、TTL）+ 响应体原文；单文件自包含，便于回放完整状态码与响应头
- **并发安全**：写入先落同目录临时文件再 `rename` 原子替换；读取要么看到旧条目、要么看到完整新条目
- **防击穿**：同一 URL 并发未命中时通过 singleflight 只回源一次，其余请求共享结果
- **TTL 生效**：按「全局默认或域名规则覆盖」的 TTL 在**写入时固化**到条目；修改配置中的 TTL 只影响新写入条目（旧条目仍按写入时的 TTL 过期，或清空 cache 目录立即生效）。`cache.ttl: 0` 表示**永不过期**（条目内 `ttl_seconds=0` 标记，读取与后台清理均按此判定）。**注意**：永不过期条目后台清理不会删除，缓存目录会持续增长；仅 `Cache-Control: no-cache` 强制刷新可更新该条目内容，手动清空缓存目录可移除
- **过期清理**：读取命中过期条目时惰性删除 + 按生效目录每 `clean-interval`（全局默认或规则覆盖）后台全量扫描；TTL<=0 的**永不过期条目会被跳过**（仍清理损坏文件与残留临时文件）；blob 半截文件（`.part-<缓存key>`，见「[blob 半截保留与无感续传](#blob-半截保留与无感续传)）不按条目解析——空文件（无续传价值）或 `mtime` 超过 24h 的孤儿残留一并清理，活跃下载/续传中的半截持续追加、`mtime` 新鲜不受影响
- **规则级独立目录**：按目录复用缓存实例（同目录同一实例、同一后台清理协程，首次出现的 `clean-interval` 生效）；启动时按配置预热创建全部可能被写入的缓存目录，运行期按需获取实例失败时该请求降级为直接回源（BYPASS），不中断请求
- **缓存条件**：仅 GET 且响应 200（content 模式还要求 body 非空）才写缓存；HEAD 只读缓存不写（无 body）；上游 304（内容未变更）视为成功但不写缓存，304 原样透传给客户端；`Range` 请求、`Cache-Control: no-cache` 请求绕过缓存读取（no-cache 仍会刷新缓存）

## 其他行为说明

| 场景 | 行为 |
| --- | --- |
| `GET /` | 返回用法说明文本（含 Docker Registry 加速用法提示） |
| `GET /healthz` | 健康检查，返回 `{"status":"ok"}` |
| HEAD（完整 URL 或路径目标） | 与 GET 一致走候选竞速管线：先查缓存，命中回放响应头（无 body，`X-Cache: HIT`）；未命中按候选竞速回源（方法保持 HEAD，成功判据按 status 语义），**不写缓存**；未配置 url-redirect 时直连回源（`X-Cache: MISS` 或不可读缓存时 `BYPASS`） |
| POST / PUT 等其他方法（完整 URL 目标） | 直接透传到原始 URL（不走缓存、不走 url-redirect 轮询），`X-Cache: BYPASS`（现状不变） |
| POST / PUT 等其他方法（路径目标） | 400：路径目标仅支持 GET/HEAD（JSON 错误体） |
| 路径目标未命中任何规则，或命中规则但生效 url-redirect 为空 | 400：路径目标必须有命中的域名规则提供 url-redirect 才能回源（全局默认配置不承接路径目标；错误信息保留「目标 URL 必须是完整的 http/https 地址」语义） |
| 路径目标 url-redirect 候选全部失败 | 502：无直连回退（没有完整原始 URL 可直连，fallback-direct 不适用）；**例外**：最后失败为上游明确 4xx（401 除外）时透传该 4xx 状态码与响应，如 referrers 端点 404 / 未知端点 405——docker 对 registry 语义错误自带容错；网络/超时类失败（含扫描总预算耗尽）仍 502 |
| 目标 URL 非 http/https（如 `/not-an-url`） | 识别为路径目标：未命中规则 / 非 GET/HEAD 时 400（JSON 错误体）；ACL 对「`http://<Host>/<路径>`」整串匹配 |
| 缓存未启用 / Range / no-cache | 直接回源（仍走 url-redirect 候选竞速），`X-Cache: BYPASS`；路径目标 GET 同样流式透传 |
| 带 Range 的请求，上游回 206 | 视为成功并流式透传、**不写缓存**（docker 断点续传必需；旧版 206 会被成功判据拒掉导致续传全挂） |
| 流式回源 commit 后客户端断开 | **断开即停**：立即终止上游读取（多源合并场景逐路关闭全部上游源），不再继续读完只填缓存（旧语义废止——避免断开后长时间占用上游连接与带宽）；blob 缓存可读主路径保留半截（`.part-<缓存key>`，进度不回退），docker 重试该层时无感断点续传（半截已完整则整体回放写缓存）或直接 HIT；日志为 Warn 级运维事件（`客户端已断开，终止上游读取`），不写缓存 |
| 流式回源 commit 后上游读取失败 | 记日志、终止本次响应、**不写缓存**（半截数据绝不入库）；**blob 例外**：开启 `docker-partial-resume-enabled`（默认开）时已下载前缀保留为半截文件（`.part-<缓存key>`，进度不回退），单飞等待方得到 502 |
| blob 请求缓存 MISS 且半截文件存在（无 Range 完整 GET） | 走续传执行方：候选轮询从断点 `Range: bytes=<已存字节>-` 续传，对客户端回一次完整 200（前缀回放 + 剩余实时拼接，`X-Cache: RESUME`，docker 无需支持 Range）；并发请求经搭车机制共享同一路续传流；EOF 后写缓存并删半截；上游不回 206 → 丢弃半截安全降级全量回源；上游 416 → 半截已完整，整体回放写缓存；续传中途失败保留更新后的半截（详见「[blob 半截保留与无感续传](#blob-半截保留与无感续传)」） |
| 同 key 单飞回源等待超过 10s | 等待方自行走候选回源（不排队），`cache.Set` 原子替换保证并发写缓存互不污染 |
| 缓存命中且 `If-None-Match` 匹配 ETag（或上游对无条件请求回 304） | 返回 304（不带 `Content-Length` 实体头），客户端使用本地缓存副本，日志记为成功（`内容未变更（304）`） |
| 客户端条件请求头（`If-Match` 等 5 个） | 不透传给上游：回源始终为无条件 GET/HEAD（其余请求头按白名单透传，含 `Range`） |
| 命中 deny-list 或未命中非空 allow-list | 403 |
| 上游回 401 + `WWW-Authenticate: Bearer` 挑战（GET/HEAD 回源） | 自动匿名取 token 并带 `Authorization: Bearer` 重试一次（token 进程内缓存，详见「Docker Registry 镜像加速」）；取 token / 重试失败按原始 401 走既有判据 |
| 上游 TLS 证书校验失败（`x509: certificate signed by unknown authority` 等）且未开启忽略证书校验 | 该次尝试按软失败处理（可按 `url-redirect-item-error-retry` 原地重试、失败后轮换下一候选）；对候选/规则/全局开启 `insecure-skip-verify` 后跳过校验继续回源（见「[url-redirect 候选对象与候选级覆盖](#url-redirect-候选对象与候选级覆盖)」） |
| 回源彻底失败（候选全败且无/失败直连） | 502（JSON 错误体） |
| 配置文件不存在 | 使用内置默认配置启动并输出告警 |

## License

MIT
