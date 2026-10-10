# proxy-cache

GitHub 反向代理 + 本地缓存服务（Go 实现），用法与 [gh-proxy](https://gh-proxy.com/) / [hk.gh-proxy.org](https://hk.gh-proxy.org) 一致：把请求路径中 `/` 后面的整段当作目标 URL，服务端回源获取内容并把 **状态码、响应头、Body 原样透传** 给客户端，同时按配置缓存。

```
curl -i "http://127.0.0.1:8080/https://raw.githubusercontent.com/dongchengjie/airport/main/subs/merged/tested_within_vless.yaml"
```

## 功能特性

- **通用反代**：支持任意 `http://` / `https://` 目标 URL（不限于 GitHub），状态码/响应头/Body 透传
- **路径目标请求（docker registry 等）**：请求路径不是完整 URL（如 `/v2/<name>/manifests/<ref>`）时识别为「路径目标」，由命中的域名规则 + url-redirect 模板拼出回源地址（详见下文「[Docker Registry 镜像加速](#docker-registry-镜像加速)」）；仅支持 GET/HEAD、必须命中规则且配置 url-redirect、无直连回退，缓存 key 含入站 Host
- **本地文件缓存**：目标 URL 的 sha256 作为缓存键（路径目标以「`http://<入站Host>/<路径目标>`」为 key 输入），命中返回 `X-Cache: HIT`，未命中回源后写缓存返回 `X-Cache: MISS`；TTL 过期自动失效（惰性删除 + 后台扫描），写入原子替换、并发安全，内置 singleflight 防缓存击穿
- **规则级独立缓存目录**：每条 domain-rule 的 `cache` 可独立配置 `enabled`/`ttl`/`path`/`clean-interval`，支持「全局 `cache.enabled: false` 关闭缓存、命中规则的流量按规则开启并写入各自独立目录」（启动时预热创建全部缓存目录，按目录复用缓存实例与后台清理协程）
- **url-redirect 多上游轮询回源**：模板展开为多个候选代理 URL，round-robin **起始下标逐请求轮换**（第 1 个请求从第 1 条开始、第 2 个从第 2 条开始……），逐个尝试直到通过成功判据；全部失败可回退直连原始 URL（路径目标无直连回退）
- **url-redirect 模板变量**：除 `$1`（目标串，兼容保留）外支持请求级预定义变量 `$http.server.header.full_url`（入站请求完整 URL）与 `$http.server.header.full_url_no_server`（入站请求去掉 `scheme://host` 的 `/path?query`），均支持 `${...}` 大括号写法；按变量名长度降序替换（避免前缀遮蔽），未知 `$` 序列原样保留，详见下文「[url-redirect 模板变量](#url-redirect-模板变量)」
- **HEAD 走候选轮询管线**：HEAD 与 GET 一致先查缓存（命中回放响应头，无 body），未命中按候选轮询回源（方法保持 HEAD，docker registry 的 HEAD 探测同样打到镜像站）；HEAD 响应无 body 不写缓存；未配置 url-redirect 时回退直连（与旧版一致）
- **可配置成功判据**：`success-check: status`（HTTP 200 即成功）或 `content`（200 且 body 非空，默认，防止镜像返回空内容/软错误）；HEAD 响应天然无 body，content 判据对 HEAD 自动按 status 语义；HTTP 304（内容未变更）在两种模式下均视为成功候选，304 原样透传给客户端、不写缓存
- **条件请求头不透传**：回源（候选与直连）按请求头白名单透传（含 `Range`），但 `If-Match` / `If-Modified-Since` / `If-None-Match` / `If-Range` / `If-Unmodified-Since` 不透传——回源始终为无条件 GET/HEAD，保证上游返回 200 + 完整 body 以过判据、写缓存；客户端的 304 语义由代理自身缓存 ETag 逻辑（HIT 命中时返回 304）负责
- **域名规则（domain-rules）**：正则匹配「`http://<入站Host>/<目标URL>`」整串（入站 Host 归一化小写后参与匹配，非锚定；目标 URL 正则写法仍兼容），命中后覆盖 `cache.enabled`、`cache.ttl`、`cache.path`、`cache.clean-interval`、`http-proxy`、`url-redirect`、`success-check`、`fallback-direct`、`timeout` 等配置；每条规则还可配置 `exclude` 排除正则列表（支持多个），match 命中后任一 exclude 命中即排除该规则、继续向下尝试后续规则
- **上游代理支持**：全局与规则级 `http-proxy`（http/https/socks5），所有回源请求（含 url-redirect 候选请求）均可走代理
- **allow-list / deny-list**：正则白/黑名单访问控制（deny 优先，allow 非空即白名单模式）
- **可观测性**：`X-Cache`（HIT/MISS/BYPASS）与 `X-Proxy-Upstream`（本次内容来源）响应头、结构化日志、`/healthz` 健康检查
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
│   ├── proxy.go            # 核心反代：目标提取（完整 URL / 路径目标）、ACL、轮询回源、成功判据、透传
│   ├── vars.go             # url-redirect 模板变量上下文与占位符展开（$1 / $http.server.header.*）
│   └── flight.go           # singleflight：同 URL 并发回源只执行一次
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
| Stage 2 `runtime` | `alpine:3.20` | 仅含：编译产物二进制、`/app/config.yaml`（默认配置）、`ca-certificates`、非 root 用户 `app(uid=10001)`；不含任何构建工具与源码 |

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
- 缓存挂载到 `/app/cache-data` 建议持久化，避免容器重建后缓存丢失；宿主机目录需要对容器内 uid=10001 可写（如 `chmod 777 ./cache-data` 或 `chown 10001 ./cache-data`）；使用规则级 `cache.path` 独立目录时，对应目录同样需要挂载并保证可写
- 服务监听地址由配置文件 `listen` 决定（默认 `0.0.0.0:8080`），容器默认暴露 8080

## 配置项说明

### 全局配置

| 配置项 | 类型 | 默认值 | 说明 |
| --- | --- | --- | --- |
| `listen` | string | `0.0.0.0:8080` | HTTP 服务监听地址 |
| `timeout` | duration | `60s` | 回源请求（含候选请求）总超时 |
| `http-proxy` | string | 空（直连） | 全局上游代理，支持 `http://`、`https://`、`socks5://` |
| `success-check` | string | `content` | 候选成功判据：`status`（200 即成功）/ `content`（200 且 body 非空） |
| `fallback-direct` | bool | `true` | url-redirect 候选全部失败后是否回退直连原始 URL |
| `url-redirect` | string[] | 空 | 回源候选模板列表，占位符 `$1`/`${1}`（目标串）或 `$http.server.header.full_url(_no_server)`（入站请求 URL 变量，见下文「url-redirect 模板变量」）替换后得到候选 URL；空列表 = 全部直连回源 |
| `cache.*` | object | 见下 | 全局缓存配置 |
| `log.level` | string | `info` | 日志级别：`debug` / `info` / `warn` / `error`（大小写不敏感，非法值启动时直接报错），详见下文「日志」 |
| `domain-rules` | list | 空 | 域名/URL 规则列表，自上而下第一条命中生效 |
| `allow-list` | string[] | 空 | 允许列表（正则）。非空时为白名单模式 |
| `deny-list` | string[] | 空 | 拒绝列表（正则）。命中即返回 403 |

> 时长格式统一支持：Go duration（`30s`/`30m`/`1h30m`）、纯数字（按秒）、`2d`（天）、`1w`（周）。

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
| `cache.enabled` | bool | 覆盖该规则的缓存开关 |
| `cache.ttl` | duration | 覆盖该规则的缓存时长；未配置沿用全局 `cache.ttl`；`0` = 永不过期；负数非法 |
| `cache.path` | string | 该规则的**独立缓存目录**（trim 后为空 = 未配置，沿用全局 `cache.path`；同一 URL 永远落同一规则/目录） |
| `cache.clean-interval` | duration | 该规则缓存目录的后台过期扫描间隔（必须 >=0，`0` 关闭；未配置沿用全局 `cache.clean-interval`） |
| `http-proxy` | string | 该规则回源走指定代理（覆盖全局） |
| `url-redirect` | string[] | 该规则专用候选列表（覆盖全局；`[]` 表示该规则直连回源） |
| `success-check` | string | 该规则的成功判据 |
| `fallback-direct` | bool | 该规则是否允许回退直连 |
| `timeout` | duration | 该规则回源超时 |

规则覆盖项均为「可选」：不写则沿用全局配置；**第一条命中的规则生效**（不做多规则叠加）。

> 匹配范围说明：域名规则的 `match` / `exclude` 对「`http://<入站Host>/<目标URL>`」整串匹配（入站 Host 参与匹配）；**完整 URL 请求**的缓存 key（目标 URL 的 sha256）与 allow-list / deny-list 仍只对目标 URL 匹配，与入站 Host 无关；**路径目标请求**（目标不是完整 URL，如 docker registry 的 `/v2/xxx/manifests/latest`）的 ACL 匹配与缓存 key 输入则使用「`http://<入站Host>/<路径目标>`」整串（入站 Host 参与，保持拦截能力并避免跨入站域名缓存污染）。

**全局关闭缓存、命中规则才开启独立缓存**：把全局 `cache.enabled` 设为 `false` 后，在需要的规则内显式配置 `cache.enabled: true`（并可用 `cache.path` 指定独立目录）——未命中任何规则的流量一律 `X-Cache: BYPASS` 不缓存，命中规则的流量才缓存且写入规则自己的目录，与全局目录互不影响、各自独立清理。服务启动时会按配置预热创建全部可能被写入的缓存目录并启动各自的后台清理（同一路径首次出现的清理间隔生效）。

`exclude` 链式落位语义：某规则 match 命中但被 exclude 排除时，该规则视为不匹配，匹配流程**继续向下**尝试后续规则；后续规则也不命中（或同样被排除）则走全局默认配置。例如：githubusercontent 规则缓存 6h，exclude 掉 `dongchengjie/airport` 仓库后，该仓库的请求不再套用 6h 缓存，而是落到下一条规则或全局默认 `cache.ttl: 30m`。

## 配置示例

```yaml
listen: "0.0.0.0:8080"
timeout: 60s
http-proxy: ""                # 或 http://127.0.0.1:7890
success-check: content        # status / content
fallback-direct: true

url-redirect:                 # $1 = 原始目标 URL，按顺序轮询
  - https://hk.gh-proxy.org/$1
  - https://gh-proxy.com/$1
  - https://mirror.ghproxy.com/$1

cache:
  enabled: true
  path: ./cache-data
  ttl: 30m                    # 0=永不过期（未配置默认 30m；负数非法）
  clean-interval: 10m

domain-rules:
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

- **逐个尝试**：按轮换后的顺序依次请求候选，每个候选用 `success-check` 判据判定成功；**任一候选成功即返回**该响应（并写缓存），响应头 `X-Proxy-Upstream` 标明来源
- **失败切换**：候选请求报错、非 200、或（content 模式下 GET 的 body 为空；HEAD 天然无 body，自动按 status 语义），都视为失败并尝试下一个候选，同时记录 warn 日志；HTTP 304（内容未变更）视为成功候选——条件请求头不透传，上游对无条件请求仍回 304 属异常/缓存副本有效语义，此时 304 原样透传给客户端（不写缓存），不再判为失败
- **全部失败**：`fallback-direct: true`（默认）回退直连原始 URL，直连结果原样透传（404/5xx 也如实返回）；`fallback-direct: false` 则直接返回 502 JSON 错误
- 轮询计数器为全局原子计数，命中不同域名规则使用各自候选列表时同样共享该计数（起始下标依旧逐请求轮换）
- 未配置 `url-redirect`（或规则覆盖为 `[]`）时，所有请求直接回源原始 URL，不套用成功判据

## url-redirect 模板变量

模板里可使用的占位符（每个变量均支持 `$name` 与 `${name}` 两种写法；展开按**变量名长度降序**替换——`full_url_no_server` 先于 `full_url`，避免同名前缀遮蔽；长变量先替换也规避了递归展开；未知的 `$` 序列原样保留为字面量；**斜杠接缝去重**：`full_url_no_server` 的值以 `/` 开头，模板里占位符前紧邻 `/` 时（如 `https://docker.1panel.live/$http...`）两个 `/` 合并为一个，拼出 `.../v2/...` 而非 `...//v2/...`）：

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

模板在启动时校验（`config.Validate`）：每个模板必须以 `http://` 或 `https://` 开头，且至少包含一个已知占位符（`$1`/`${1}`，或任一已知 `$http.server.header.*` 变量的 `$` / `${}` 形式），否则启动报错并列出全部合法占位符写法。

## Docker Registry 镜像加速

docker daemon 可以把本服务当作 registry 访问：请求路径不是完整 URL，而是 registry 协议路径（如 `GET/HEAD /v2/<name>/manifests/<ref>`、`GET /v2/<name>/blobs/<digest>`）。本服务将其识别为「**路径目标**」，由命中的域名规则 url-redirect 模板拼出镜像站回源地址。

### 配置示例

```yaml
domain-rules:
  - name: docker-registry
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
    timeout: 300s              # 大镜像 blob 拉取耗时较长，放宽回源总超时
    url-redirect:              # 多镜像站候选轮询，均用 full_url_no_server 拼回源路径
      - https://docker.1panel.live/$http.server.header.full_url_no_server
      - https://hub.rat.dev/$http.server.header.full_url_no_server
      - https://docker.m.daocloud.io/$http.server.header.full_url_no_server
      - https://docker.1ms.run/$http.server.header.full_url_no_server
```

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
- **必须命中规则**：路径目标请求必须命中某条 domain-rules 规则且该规则生效 `url-redirect` 非空才能回源，否则 400（错误信息保留「目标 URL 必须是完整的 http/https 地址」语义并说明原因）；全局默认配置（未命中规则）不承接路径目标
- **HEAD/GET 行为一致**：docker 用 HEAD 探测 manifest/blob——HEAD 与 GET 统一走候选轮询管线：先查缓存（命中回放响应头，无 body，`X-Cache: HIT`）；未命中按候选轮询回源（方法保持 HEAD，success-check 对 HEAD 自动按 status 语义：200 即成功，304 仍视为成功）；HEAD 响应无 body，**不写缓存**；未配置 url-redirect 时回退直连（与旧版 HEAD 行为一致）
- **缓存 key 含入站 Host**：路径目标的缓存 key 输入为「`http://<入站Host>/<路径目标>`」（再取 sha256），不同入站域名的同路径缓存互不污染；完整 URL 请求的缓存 key 输入仍为目标 URL 原文（兼容已有缓存）
- **无直连回退**：路径目标没有完整原始 URL 可直连，url-redirect 候选全部失败时直接报错（`fallback-direct` 不适用，错误信息注明路径目标无直连回退）
- **ACL 拦截能力保留**：allow-list / deny-list 对「`http://<入站Host>/<路径目标>`」整串做 allow/deny 匹配（完整 URL 请求仍只对目标 URL 匹配，行为不变）

## http-proxy 说明

- 全局 `http-proxy` 对**所有回源请求**生效，包括 url-redirect 候选请求与 fallback 直连请求；支持 `http://`、`https://`、`socks5://` 代理地址
- 域名规则 `http-proxy` 覆盖全局：例如 GitHub 走本地代理、其他目标直连
- 仅「服务端 → 上游」的回源流量走该代理；客户端 → 本服务的流量不受影响

## 日志

日志为 slog 结构化文本输出到 stderr，级别由配置文件 `log.level` 控制（大小写不敏感；留空默认 `info`；非法值启动时直接报错）。四级从多到少：`debug` > `info` > `warn` > `error`，高级别包含低级别的全部日志。

```yaml
log:
  level: info   # debug / info / warn / error
```

| 级别 | 能看到什么 |
| --- | --- |
| `info` | 每次决策结果全留痕：规则匹配结果（`规则匹配结果`，含入站 `host` 字段）、GET 绕过缓存原因（`GET 请求绕过缓存（BYPASS）`，`reason=cache-disabled/request-range/request-no-cache/...`）、url-redirect 轮询展开（`url-redirect 轮询展开`：本轮起始下标 `start_index` + 按尝试顺序的 `模板[下标] 模板 -> 占位符替换后最终URL` 明细）、候选命中并作为最终返回（含模板下标/模板/最终 URL/状态码/耗时）、全败回退直连、缓存命中（`缓存命中（HIT）`：缓存 key 与实际命中的文件路径 `file`）、写入缓存成功（`写入缓存成功`：规则名/缓存 key/落盘文件路径/body 大小/ttl）、成功返回内容（`成功返回内容`：200 且 body 非空时输出来源/状态码/字节数/缓存状态）、内容未变更（`内容未变更（304），客户端缓存副本有效`，最终 304 时输出）、每条访问完成日志（`代理请求完成`） |
| `debug` | `info` 全部内容，再加：代理请求开始、逐条域名规则匹配过程（每条规则的 `matched` / `excluded` 明细）、每次候选尝试开始、缓存查询 MISS 细节 |
| `warn` | 候选请求失败 / 未过成功判据 / 回退直连、写缓存失败、ACL 拦截等警告（含 `error` 级日志） |
| `error` | 仅错误：回源彻底失败（502）等 |

### 排障指引

- **规则缓存目录没有生成缓存文件**，按顺序看三处日志：
  1. 启动日志 `域名规则已加载`：核对每条规则的 `match`、`cache_enabled`、`cache_path`、`cache_ttl`、`url_redirect` 是否符合预期（规则未配置项显示沿用全局后的生效值）；
  2. 请求日志 `规则匹配结果`：`rule` 是命中规则名，`global-default` 表示没命中任何规则——match 里写了入站 Host 段（如 `http://github\.path\..+`）时，请求的 Host 必须以该段开头（如 `github.path.xxx`），直接用 IP:端口访问不会命中，会落全局默认（没有规则的 url-redirect、缓存写进全局 `cache.path` 目录）；
  3. `GET 请求绕过缓存（BYPASS）` / `写入缓存成功` / `缓存命中（HIT）`：分别说明这次为什么不缓存、实际写到了哪个文件（`file` 字段）、命中了哪个文件。
- **域名规则没按预期命中**：把 `log.level` 设为 `debug`，观察每条规则的「域名规则匹配」日志（`matched` 表示 match 是否命中、`excluded` 表示是否被 exclude 排除）与最终的「规则匹配结果」日志（`rule` 为命中的规则名，未命中为 `global-default`），即可区分是 match 没命中还是被 exclude 排除。
- **url-redirect 候选行为异常**：`info` 级别即可看到「url-redirect 轮询展开」（候选数量、轮询起始下标、按尝试顺序的模板与替换后 URL）与每条候选的结果日志（命中为 `url-redirect 候选命中并作为最终返回`，失败在 `warn` 级别），无需开 debug 就能还原整轮轮询；`debug` 额外提供每次尝试开始的明细。

## 缓存实现说明

- **缓存键**：完整 URL 请求 = 目标 URL（含查询串）的 sha256 十六进制（64 字符），直接作为 `cache.path` 下的文件名；路径目标请求 = 「`http://<入站Host>/<路径目标>`」的 sha256（入站 Host 参与，避免不同入站域名的同路径缓存互相污染）
- **文件格式**：8 字节文件头（4 字节魔数 + 4 字节元信息长度）+ JSON 元信息（状态码、响应头、写入时间、TTL）+ 响应体原文；单文件自包含，便于回放完整状态码与响应头
- **并发安全**：写入先落同目录临时文件再 `rename` 原子替换；读取要么看到旧条目、要么看到完整新条目
- **防击穿**：同一 URL 并发未命中时通过 singleflight 只回源一次，其余请求共享结果
- **TTL 生效**：按「全局默认或域名规则覆盖」的 TTL 在**写入时固化**到条目；修改配置中的 TTL 只影响新写入条目（旧条目仍按写入时的 TTL 过期，或清空 cache 目录立即生效）。`cache.ttl: 0` 表示**永不过期**（条目内 `ttl_seconds=0` 标记，读取与后台清理均按此判定）。**注意**：永不过期条目后台清理不会删除，缓存目录会持续增长；仅 `Cache-Control: no-cache` 强制刷新可更新该条目内容，手动清空缓存目录可移除
- **过期清理**：读取命中过期条目时惰性删除 + 按生效目录每 `clean-interval`（全局默认或规则覆盖）后台全量扫描；TTL<=0 的**永不过期条目会被跳过**（仍清理损坏文件与残留临时文件）
- **规则级独立目录**：按目录复用缓存实例（同目录同一实例、同一后台清理协程，首次出现的 `clean-interval` 生效）；启动时按配置预热创建全部可能被写入的缓存目录，运行期按需获取实例失败时该请求降级为直接回源（BYPASS），不中断请求
- **缓存条件**：仅 GET 且响应 200（content 模式还要求 body 非空）才写缓存；HEAD 只读缓存不写（无 body）；上游 304（内容未变更）视为成功但不写缓存，304 原样透传给客户端；`Range` 请求、`Cache-Control: no-cache` 请求绕过缓存读取（no-cache 仍会刷新缓存）

## 其他行为说明

| 场景 | 行为 |
| --- | --- |
| `GET /` | 返回用法说明文本（含 Docker Registry 加速用法提示） |
| `GET /healthz` | 健康检查，返回 `{"status":"ok"}` |
| HEAD（完整 URL 或路径目标） | 与 GET 一致走候选轮询管线：先查缓存，命中回放响应头（无 body，`X-Cache: HIT`）；未命中按候选轮询回源（方法保持 HEAD，成功判据按 status 语义），**不写缓存**；未配置 url-redirect 时直连回源（`X-Cache: MISS` 或不可读缓存时 `BYPASS`） |
| POST / PUT 等其他方法（完整 URL 目标） | 直接透传到原始 URL（不走缓存、不走 url-redirect 轮询），`X-Cache: BYPASS`（现状不变） |
| POST / PUT 等其他方法（路径目标） | 400：路径目标仅支持 GET/HEAD（JSON 错误体） |
| 路径目标未命中任何规则，或命中规则但生效 url-redirect 为空 | 400：路径目标必须有命中的域名规则提供 url-redirect 才能回源（全局默认配置不承接路径目标；错误信息保留「目标 URL 必须是完整的 http/https 地址」语义） |
| 路径目标 url-redirect 候选全部失败 | 502：无直连回退（没有完整原始 URL 可直连，fallback-direct 不适用） |
| 目标 URL 非 http/https（如 `/not-an-url`） | 识别为路径目标：未命中规则 / 非 GET/HEAD 时 400（JSON 错误体）；ACL 对「`http://<Host>/<路径>`」整串匹配 |
| 缓存未启用 / Range / no-cache | 直接回源（仍走 url-redirect 轮询），`X-Cache: BYPASS` |
| 缓存命中且 `If-None-Match` 匹配 ETag（或上游对无条件请求回 304） | 返回 304（不带 `Content-Length` 实体头），客户端使用本地缓存副本，日志记为成功（`内容未变更（304）`） |
| 客户端条件请求头（`If-Match` 等 5 个） | 不透传给上游：回源始终为无条件 GET/HEAD（其余请求头按白名单透传，含 `Range`） |
| 命中 deny-list 或未命中非空 allow-list | 403 |
| 回源彻底失败（候选全败且无/失败直连） | 502（JSON 错误体） |
| 配置文件不存在 | 使用内置默认配置启动并输出告警 |

## License

MIT
