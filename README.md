# proxy-cache

GitHub 反向代理 + 本地缓存服务（Go 实现），用法与 [gh-proxy](https://gh-proxy.com/) / [hk.gh-proxy.org](https://hk.gh-proxy.org) 一致：把请求路径中 `/` 后面的整段当作目标 URL，服务端回源获取内容并把 **状态码、响应头、Body 原样透传** 给客户端，同时按配置缓存。

```
curl -i "http://127.0.0.1:8080/https://raw.githubusercontent.com/dongchengjie/airport/main/subs/merged/tested_within_vless.yaml"
```

## 功能特性

- **通用反代**：支持任意 `http://` / `https://` 目标 URL（不限于 GitHub），状态码/响应头/Body 透传
- **本地文件缓存**：目标 URL 的 sha256 作为缓存键，命中返回 `X-Cache: HIT`，未命中回源后写缓存返回 `X-Cache: MISS`；TTL 过期自动失效（惰性删除 + 后台扫描），写入原子替换、并发安全，内置 singleflight 防缓存击穿
- **规则级独立缓存目录**：每条 domain-rule 的 `cache` 可独立配置 `enabled`/`ttl`/`path`/`clean-interval`，支持「全局 `cache.enabled: false` 关闭缓存、命中规则的流量按规则开启并写入各自独立目录」（启动时预热创建全部缓存目录，按目录复用缓存实例与后台清理协程）
- **url-redirect 多上游轮询回源**：`$1` 模板展开为多个候选代理 URL，round-robin **起始下标逐请求轮换**（第 1 个请求从第 1 条开始、第 2 个从第 2 条开始……），逐个尝试直到通过成功判据；全部失败可回退直连原始 URL
- **可配置成功判据**：`success-check: status`（HTTP 200 即成功）或 `content`（200 且 body 非空，默认，防止镜像返回空内容/软错误）
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
│   ├── proxy.go            # 核心反代：目标提取、ACL、轮询回源、成功判据、透传
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
| `url-redirect` | string[] | 空 | 回源候选模板列表，`$1`/`${1}` 占位符替换为原始目标 URL；空列表 = 全部直连回源 |
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

> 匹配范围说明：域名规则的 `match` / `exclude` 对「`http://<入站Host>/<目标URL>`」整串匹配（入站 Host 参与匹配）；而**缓存 key（目标 URL 的 sha256）与 allow-list / deny-list 仍只对目标 URL 匹配**，与入站 Host 无关。

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

# 非法路径 / 命中 deny-list 时分别返回 400 / 403（JSON 错误体）
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
- **失败切换**：候选请求报错、非 200、或（content 模式下）body 为空，都视为失败并尝试下一个候选，同时记录 warn 日志
- **全部失败**：`fallback-direct: true`（默认）回退直连原始 URL，直连结果原样透传（404/5xx 也如实返回）；`fallback-direct: false` 则直接返回 502 JSON 错误
- 轮询计数器为全局原子计数，命中不同域名规则使用各自候选列表时同样共享该计数（起始下标依旧逐请求轮换）
- 未配置 `url-redirect`（或规则覆盖为 `[]`）时，所有请求直接回源原始 URL，不套用成功判据

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
| `debug` | `info` 全部内容，再加：代理请求开始、逐条域名规则匹配过程（每条规则的 `matched` / `excluded` 明细）、url-redirect 轮询起始下标与每次候选尝试、缓存查询 HIT/MISS 与写缓存细节 |
| `info` | 规则匹配结果（`规则匹配结果`）、url-redirect 候选命中（`url-redirect 候选命中`）、全败回退直连、每条访问完成日志（`代理请求完成`）、异常恢复提示 |
| `warn` | 候选请求失败 / 未过成功判据 / 回退直连、写缓存失败、ACL 拦截等警告（含 `error` 级日志） |
| `error` | 仅错误：回源彻底失败（502）等 |

### 排障指引

- **域名规则没按预期命中**：把 `log.level` 设为 `debug`，观察每条规则的「域名规则匹配」日志（`matched` 表示 match 是否命中、`excluded` 表示是否被 exclude 排除）与最终的「规则匹配结果」日志（`rule` 为命中的规则名，未命中为 `global-default`），即可区分是 match 没命中还是被 exclude 排除。
- **url-redirect 候选行为异常**：`debug` 级别下可看到「url-redirect 候选展开」（候选数量、轮询起始下标、尝试顺序）与每次「尝试 url-redirect 候选」日志，配合 `warn` 级别的失败原因定位问题候选。

## 缓存实现说明

- **缓存键**：目标 URL（含查询串）的 sha256 十六进制（64 字符），直接作为 `cache.path` 下的文件名
- **文件格式**：8 字节文件头（4 字节魔数 + 4 字节元信息长度）+ JSON 元信息（状态码、响应头、写入时间、TTL）+ 响应体原文；单文件自包含，便于回放完整状态码与响应头
- **并发安全**：写入先落同目录临时文件再 `rename` 原子替换；读取要么看到旧条目、要么看到完整新条目
- **防击穿**：同一 URL 并发未命中时通过 singleflight 只回源一次，其余请求共享结果
- **TTL 生效**：按「全局默认或域名规则覆盖」的 TTL 在**写入时固化**到条目；修改配置中的 TTL 只影响新写入条目（旧条目仍按写入时的 TTL 过期，或清空 cache 目录立即生效）。`cache.ttl: 0` 表示**永不过期**（条目内 `ttl_seconds=0` 标记，读取与后台清理均按此判定）。**注意**：永不过期条目后台清理不会删除，缓存目录会持续增长；仅 `Cache-Control: no-cache` 强制刷新可更新该条目内容，手动清空缓存目录可移除
- **过期清理**：读取命中过期条目时惰性删除 + 按生效目录每 `clean-interval`（全局默认或规则覆盖）后台全量扫描；TTL<=0 的**永不过期条目会被跳过**（仍清理损坏文件与残留临时文件）
- **规则级独立目录**：按目录复用缓存实例（同目录同一实例、同一后台清理协程，首次出现的 `clean-interval` 生效）；启动时按配置预热创建全部可能被写入的缓存目录，运行期按需获取实例失败时该请求降级为直接回源（BYPASS），不中断请求
- **缓存条件**：仅 GET 且响应 200（content 模式还要求 body 非空）才写缓存；`Range` 请求、`Cache-Control: no-cache` 请求绕过缓存读取（no-cache 仍会刷新缓存）

## 其他行为说明

| 场景 | 行为 |
| --- | --- |
| `GET /` | 返回用法说明文本 |
| `GET /healthz` | 健康检查，返回 `{"status":"ok"}` |
| HEAD / POST 等其他方法 | 直接透传到原始 URL（不走缓存、不走 url-redirect 轮询），`X-Cache: BYPASS` |
| 缓存未启用 / Range / no-cache | 直接回源（仍走 url-redirect 轮询），`X-Cache: BYPASS` |
| 缓存命中且 `If-None-Match` 匹配 ETag | 返回 304 |
| 目标 URL 非 http/https | 400（JSON 错误体） |
| 命中 deny-list 或未命中非空 allow-list | 403 |
| 回源彻底失败（候选全败且无/失败直连） | 502（JSON 错误体） |
| 配置文件不存在 | 使用内置默认配置启动并输出告警 |

## License

MIT
