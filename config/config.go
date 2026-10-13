// Package config 负责 proxy-cache 配置文件的加载、解析、默认值补全与校验，
// 并提供把「全局默认配置 + 命中的域名规则覆盖项」合并为单次请求最终生效配置（Options）的能力。
package config

import (
	"fmt"
	"log/slog"
	"net/url"
	"os"
	"regexp"
	"strconv"
	"strings"
	"time"

	"gopkg.in/yaml.v3"
)

// success-check 的两个合法取值。
const (
	// SuccessCheckStatus 仅要求 HTTP 200 即算回源成功。
	SuccessCheckStatus = "status"
	// SuccessCheckContent 要求 HTTP 200 且 body 非空才算回源成功（默认）。
	SuccessCheckContent = "content"
)

// domain-rules 规则 type 字段的合法取值（请求类型过滤；留空 = 不过滤请求类型）。
const (
	// RuleTypeDocker 仅承接路径目标请求（fullURL=false，如 docker registry 的 /v2/xxx/manifests/latest）。
	RuleTypeDocker = "docker"
	// RuleTypeWeb 仅承接完整 URL 请求（fullURL=true，如 /https://raw.githubusercontent.com/...）。
	RuleTypeWeb = "web"
)

// DefaultCacheTTL 全局 cache.ttl 未配置时的默认缓存时长。
const DefaultCacheTTL = 30 * time.Minute

// DefaultProgressLogInterval 全局 log.progress-interval 未配置时的默认流式透传进度日志间隔。
const DefaultProgressLogInterval = 10 * time.Second

// 慢速升级多源并发续传（slow-download escalation，仅 type=docker 规则的 GET 路径目标
// 流式回源生效）的缺省值。
const (
	// DefaultSlowDownloadSpeed 判慢阈值缺省值：1MB/s。生效窗口平均送出速率低于它判
	// 「过慢」；显式 <=0（或 0）= 关闭整个特性。
	DefaultSlowDownloadSpeed = 1 << 20
	// DefaultSlowDownloadWindow 判速滑动窗口时长缺省值：10s（Validate 钳制最小 1s）。
	DefaultSlowDownloadWindow = 10 * time.Second
	// DefaultSlowDownloadMinRemaining 触发所需最小剩余字节数缺省值：64MB
	//（已知总长不可知则永不触发；0 = 允许剩余 0 字节时也触发，负数 Validate 拒绝）。
	DefaultSlowDownloadMinRemaining = 64 << 20
	// DefaultSlowDownloadMaxBehind 多源合并的剪枝阈值缺省值：8MB（某源落后领先源超过
	// 该字节数即被剪枝关闭，收敛多源并发带来的带宽放大）。
	DefaultSlowDownloadMaxBehind = 8 << 20
)

// 候选探测提速（作用于 url-redirect 候选管线：流式 GET 与 HEAD 共用）的缺省值。
const (
	// DefaultURLRedirectProbeTimeout 单次候选尝试的探测预算缺省值：5s。
	// docker 客户端等响应头约 30s 就超时断开，探测预算默认压到 5s 量级，
	// 配合 hedge 放行间隔可把「选定候选」耗时控制在 10s 量级内。
	// 显式配置 0 = 不限制（区分「未配置走缺省」与「显式关闭预算」）。
	DefaultURLRedirectProbeTimeout = 5 * time.Second
	// DefaultURLRedirectHedgeDelay 候选阶梯竞速的放行间隔缺省值：2s。
	// 显式配置 0 = 纯串行（禁用竞速：上一候选出结果后才放行下一个）。
	DefaultURLRedirectHedgeDelay = 2 * time.Second
)

// url-redirect 模板占位符（变量名，不含 $ / ${} 包裹形式；每个变量均支持 $name 与 ${name} 写法）。
// 占位符名统一定义在本包：模板校验（validateRedirectItems）与 proxy 包的模板展开
// （ExpandCandidates）共用同一套常量，避免两处维护漂移。
const (
	// PlaceholderTarget 目标串（$1 / ${1}，兼容保留）：完整 URL 请求 = 目标 URL 原文；
	// 路径目标请求（如 docker registry 的 /v2/xxx/manifests/latest）= 路径目标本身（不带 scheme）。
	PlaceholderTarget = "1"
	// PlaceholderFullURL 本次入站请求的完整 URL（scheme://host/path?query）：
	// scheme 优先取请求头 X-Forwarded-Proto 首个值（小写），否则按 TLS 判定 https/http；host 含端口原样。
	PlaceholderFullURL = "http.server.header.full_url"
	// PlaceholderFullURLNoServer 入站请求 URL 去掉 scheme://host 的部分
	// （/path?query，以 / 开头、含查询串）。
	PlaceholderFullURLNoServer = "http.server.header.full_url_no_server"
)

// RedirectPlaceholders 返回全部已知 url-redirect 占位符名（含 $1），
// 供模板校验与错误提示使用；新增变量时需同步维护本列表与 proxy 包的展开逻辑。
func RedirectPlaceholders() []string {
	return []string{PlaceholderTarget, PlaceholderFullURL, PlaceholderFullURLNoServer}
}

// Duration 是 YAML 配置里的时长类型，解析规则：
//   - Go duration 格式：30s / 30m / 1h30m
//   - 纯数字：按秒解析（1800 => 1800s）
//   - 简单单位 d（天）、w（周）：2d、1w
type Duration time.Duration

// ParseDuration 把时长字符串解析为 time.Duration，无法解析时返回错误。
func ParseDuration(s string) (time.Duration, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, nil
	}
	// 优先按 Go duration 格式解析
	if d, err := time.ParseDuration(s); err == nil {
		return d, nil
	}
	// 纯数字：按秒
	if n, err := strconv.ParseFloat(s, 64); err == nil {
		return time.Duration(n * float64(time.Second)), nil
	}
	// 简单单位 d / w
	var num float64
	var unit string
	if _, err := fmt.Sscanf(s, "%f%s", &num, &unit); err == nil {
		switch strings.ToLower(unit) {
		case "d":
			return time.Duration(num * float64(24*time.Hour)), nil
		case "w":
			return time.Duration(num * float64(7*24*time.Hour)), nil
		}
	}
	return 0, fmt.Errorf("无法解析时长 %q（支持 30m / 1h30m / 1800 / 2d 等格式）", s)
}

// D 返回 time.Duration 表示。
func (d Duration) D() time.Duration { return time.Duration(d) }

// durationPtr 返回 Duration 指针（用于预置默认值）。
func durationPtr(d time.Duration) *Duration {
	v := Duration(d)
	return &v
}

// String 实现 fmt.Stringer，便于日志输出。
func (d Duration) String() string { return d.D().String() }

// Bytes 是 YAML 配置里的字节数类型（底层 int64 字节），解析规则：
//   - 纯整数：按字节解析（1048576 => 1MB）；
//   - 带单位字符串：1024 进制，单位大小写不敏感，支持 B / KB / MB / GB / TB 与小数
//     （"512KB" / "1MB" / "1.5MB" / "64MB" / "2GB"）。
//
// 解析失败启动快速失败；负值由 Validate 拒绝（本类型只负责解析）。
type Bytes int64

// bytesUnitRe 匹配「数值 + 单位」形态的字节串（数值支持小数，单位为字母）。
var bytesUnitRe = regexp.MustCompile(`^([0-9]+(?:\.[0-9]+)?)\s*([A-Za-z]+)$`)

// ParseBytes 把字节串解析为字节数（1024 进制），无法解析时返回错误。
func ParseBytes(s string) (int64, error) {
	s = strings.TrimSpace(s)
	if s == "" {
		return 0, nil
	}
	// 纯整数：按字节
	if n, err := strconv.ParseInt(s, 10, 64); err == nil {
		return n, nil
	}
	m := bytesUnitRe.FindStringSubmatch(s)
	if m == nil {
		return 0, fmt.Errorf("无法解析字节数 %q（支持纯整数 1048576 或 512KB / 1MB / 1.5MB / 64MB / 2GB 等写法，1024 进制）", s)
	}
	num, err := strconv.ParseFloat(m[1], 64)
	if err != nil {
		return 0, fmt.Errorf("无法解析字节数 %q: %w", s, err)
	}
	var mult float64
	switch strings.ToLower(m[2]) {
	case "b":
		mult = 1
	case "kb":
		mult = 1 << 10
	case "mb":
		mult = 1 << 20
	case "gb":
		mult = 1 << 30
	case "tb":
		mult = 1 << 40
	default:
		return 0, fmt.Errorf("无法解析字节数 %q：未知单位 %q（支持 B / KB / MB / GB / TB，大小写不敏感）", s, m[2])
	}
	return int64(num * mult), nil
}

// Int64 返回 int64 字节表示。
func (b Bytes) Int64() int64 { return int64(b) }

// String 实现 fmt.Stringer，便于日志输出（复用 proxy 包同款人类可读格式太重，这里按
// MB 起换算输出两位小数，足够配置留痕使用）。
func (b Bytes) String() string {
	if b < 1<<20 {
		return strconv.FormatInt(b.Int64(), 10) + "B"
	}
	units := [...]string{"B", "KB", "MB", "GB", "TB"}
	v := float64(b)
	i := 0
	for v >= 1024 && i < len(units)-1 {
		v /= 1024
		i++
	}
	s := strconv.FormatFloat(v, 'f', 2, 64)
	s = strings.TrimRight(strings.TrimRight(s, "0"), ".")
	return s + units[i]
}

// UnmarshalYAML 实现 yaml.Unmarshaler，让 Bytes 可直接用于 YAML 字段。
func (b *Bytes) UnmarshalYAML(node *yaml.Node) error {
	if node == nil || node.Tag == "!!null" || strings.TrimSpace(node.Value) == "" {
		*b = 0
		return nil
	}
	v, err := ParseBytes(node.Value)
	if err != nil {
		return fmt.Errorf("第 %d 行: %w", node.Line, err)
	}
	*b = Bytes(v)
	return nil
}

// bytesPtr 返回 Bytes 指针（用于预置默认值）。
func bytesPtr(n int64) *Bytes {
	v := Bytes(n)
	return &v
}

// UnmarshalYAML 实现 yaml.Unmarshaler，让 Duration 可直接用于 YAML 字段。
func (d *Duration) UnmarshalYAML(node *yaml.Node) error {
	if node == nil || node.Tag == "!!null" || strings.TrimSpace(node.Value) == "" {
		*d = 0
		return nil
	}
	v, err := ParseDuration(node.Value)
	if err != nil {
		return fmt.Errorf("第 %d 行: %w", node.Line, err)
	}
	*d = Duration(v)
	return nil
}

// RuleCache 域名规则中的缓存覆盖项；指针为 nil 表示该项不覆盖、沿用全局配置。
// 规则可配独立缓存目录（cache.path）与独立清理间隔（cache.clean-interval），
// 支持「全局 cache.enabled: false 关闭缓存、命中规则的流量按规则开启并写入各自目录」。
type RuleCache struct {
	// Enabled 覆盖缓存开关。
	Enabled *bool `yaml:"enabled"`
	// TTL 覆盖该规则的缓存时长：nil（未配置）沿用全局 cache.ttl；
	// 显式 0 表示永不过期；负数为配置错误（Validate 拒绝）。
	TTL *Duration `yaml:"ttl"`
	// Path 覆盖该规则的缓存目录（独立目录，实现按规则分目录缓存）；
	// trim 后为空表示未配置、沿用全局 cache.path。
	Path *string `yaml:"path"`
	// CleanInterval 覆盖该规则缓存目录的后台过期清理扫描间隔；
	// 非 nil 且 <0 视为配置错误（Validate 拒绝），0 表示关闭该目录的后台清理。
	CleanInterval *Duration `yaml:"clean-interval"`
}

// urlRedirectItemReservedFields 候选对象的「预留字段」清单（语义已规划、本期未实现）：
// 配置它们会在解析期报错（绝不静默忽略），错误信息会提示暂未实现。
// 历史预留的 http-proxy 与 probe-timeout 已实现（候选级 http-proxy 与候选级超时覆盖，
// 见 URLRedirectItem 与 Options.CandidateHTTPProxy 等），故从预留清单移除；
// hedge-delay 是候选阶梯竞速的放行间隔（作用于候选序列之间的调度，而非单个候选的
// 独立属性），不适合按候选对象配置，继续保持预留报错。
const urlRedirectItemReservedFields = "enabled / weight / hedge-delay"

// URLRedirectItem 是 url-redirect 候选列表的对象形态（破坏性变更：不再支持旧版纯字符串写法，
// 解析期报错并给出迁移示例，见 UnmarshalYAML）。本期实现的字段：
//   - template（必填非空）：即旧版的模板字符串，占位符语义不变（$1 / ${1} 与
//     $http.server.header.* 变量，见 RedirectPlaceholders）；
//   - insecure-skip-verify（可选）：候选级「忽略 TLS 证书校验」；
//   - http-proxy（可选）：候选级回源代理（显式空串 = 该候选强制直连）；
//   - connect-timeout / http-timeout / read-timeout / probe-timeout（可选）：
//     候选级超时覆盖（probe-timeout 是规则/全局 url-redirect-probe-timeout 的候选级短名）。
//
// 除 template 外的全部字段均为三级优先级（高 → 低）：候选对象显式值 > 规则级 > 全局，
// 生效值经 Options.CandidateHTTPProxy / CandidateConnectTimeout / CandidateHTTPTimeout /
// CandidateReadTimeout / CandidateProbeTimeout / CandidateInsecureSkipVerify 逐候选解析。
//
// 预留字段（本期未实现，配置即报错）：enabled（临时下线候选）、weight（加权轮询）、
// hedge-delay（候选竞速放行间隔，作用于候选序列调度、非单候选属性，见 urlRedirectItemReservedFields）。
type URLRedirectItem struct {
	// Template 候选模板（必填，trim 后非空；Validate 阶段校验 http(s):// 前缀与占位符）。
	Template string `yaml:"template"`
	// InsecureSkipVerify 候选级「忽略 TLS 证书校验」（可选）：nil = 继承规则级
	// insecure-skip-verify（仍为 nil 再继承全局），生效值经 Options.CandidateInsecureSkipVerify 解析。
	// 显式 true 表示「信任该候选」：该候选路径上的所有上游连接——含 30x 重定向目标
	//（如 1ms.run → cloudfront）与 token dance 的第二次请求——都跳过证书校验。
	// 典型场景：单个镜像站证书链不全（如 docker.actima.top 的
	// `tls: failed to verify certificate: x509: certificate signed by unknown authority`），
	// 推荐在该候选上精确开启，而非打开规则级/全局开关。
	InsecureSkipVerify *bool `yaml:"insecure-skip-verify"`
	// HTTPProxy 候选级回源代理（可选）：三级优先级候选 > 规则 > 全局，生效值经
	// Options.CandidateHTTPProxy 解析。三种取值语义：
	//   - nil（未配置）：继承规则级 http-proxy（仍为 nil 再继承全局）；
	//   - 显式空串（trim 后为空）：该候选**强制直连**——显式覆盖上层已配置的代理
	//    （「个别镜像站走代理、其余直连」或反过来都靠它精确表达）；
	//   - 非空：该候选走指定代理（http/https/socks5 地址，Validate 阶段过 validateProxyAddr）。
	HTTPProxy *string `yaml:"http-proxy"`
	// ConnectTimeout 候选级建连超时（TCP 拨号 + TLS 握手共用；可选）：
	// nil = 继承规则级 connect-timeout（仍为 nil 再继承全局），生效值经
	// Options.CandidateConnectTimeout 解析；显式 0 = 不限制；负数为配置错误（Validate 拒绝）。
	// 典型场景：走慢代理的候选放宽建连预算，不影响其他候选的快速失败节奏。
	ConnectTimeout *Duration `yaml:"connect-timeout"`
	// HTTPTimeout 候选级单次尝试总超时（可选）：覆盖该候选「建连 + 响应头 + 完整读 body +
	// 401 后 token dance 及其带 token 重试」整段，按每次尝试独立计时。nil = 继承规则级
	// http-timeout（仍为 nil 再继承全局），生效值经 Options.CandidateHTTPTimeout 解析；
	// 显式 0 = 不限制；负数为配置错误（Validate 拒绝）。
	HTTPTimeout *Duration `yaml:"http-timeout"`
	// ReadTimeout 候选级「无新增数据超时」（idle，可选）：窗口内有进度就重置计时器，
	// 无任何新数据才触发超时并取消该次尝试——慢速但持续传输的大文件不会被误砍。
	// nil = 继承规则级 read-timeout 的最终回落结果（含旧 timeout 退化别名链，见
	// Options.ReadTimeout），生效值经 Options.CandidateReadTimeout 解析；
	// 显式 0 = 不限制；负数为配置错误（Validate 拒绝）。
	ReadTimeout *Duration `yaml:"read-timeout"`
	// ProbeTimeout 候选级探测预算（可选）：规则/全局 url-redirect-probe-timeout 的候选级短名
	//（沿用原预留字段规划命名）——从发出请求到「响应头可用（含 401 token dance 的第二次响应）
	// 且成功判据可判定」的时限，到期取消该次尝试并硬失败快切下一候选；仅约束探测阶段。
	// nil = 继承规则级 url-redirect-probe-timeout（仍为 nil 再继承全局，全局未配置缺省 5s），
	// 生效值经 Options.CandidateProbeTimeout 解析；显式 0 = 不限制；负数为配置错误（Validate 拒绝）。
	// 典型场景：走慢代理的候选放宽探测预算（代理握手耗时长，全局 5s 会在代理上全部超时）。
	ProbeTimeout *Duration `yaml:"probe-timeout"`
}

// UnmarshalYAML 自定义解析（yaml.Node 按 Kind 判别形态）：
//   - 纯字符串（旧格式）→ 报错并给出迁移示例（破坏性变更，不做字符串兼容）；
//   - 键值对象 → 校验字段名：未知字段（含预留未实现字段）报错并列出支持字段，
//     避免配置拼写错误被静默忽略；
//   - 其他形态（列表/别名异常等）→ 报错说明必须是对象。
func (it *URLRedirectItem) UnmarshalYAML(node *yaml.Node) error {
	if node.Kind == yaml.ScalarNode {
		// 旧格式（纯字符串模板）：给出迁移示例，配置错误启动快速失败
		return fmt.Errorf(
			"第 %d 行: url-redirect 候选必须是对象，不再支持纯字符串写法 %q（旧格式已移除），迁移示例:\n"+
				"  url-redirect:\n"+
				"    - template: %s\n"+
				"    - template: %s\n"+
				"      insecure-skip-verify: true   # 可选：仅该候选忽略 TLS 证书校验",
			node.Line, node.Value, node.Value, node.Value)
	}
	if node.Kind != yaml.MappingNode {
		return fmt.Errorf("第 %d 行: url-redirect 候选必须是键值对象（- template: ...），收到非法形态", node.Line)
	}
	// 未知字段检查（键名 trim + 小写归一化后比对，风格与 type / success-check 一致）
	for i := 0; i+1 < len(node.Content); i += 2 {
		key := node.Content[i]
		switch strings.ToLower(strings.TrimSpace(key.Value)) {
		case "template", "insecure-skip-verify", "http-proxy",
			"connect-timeout", "http-timeout", "read-timeout", "probe-timeout":
			// 本期支持的合法字段（http-proxy 与四个超时为候选级三级覆盖，见 URLRedirectItem 字段注释）
		default:
			return fmt.Errorf(
				"第 %d 行: url-redirect 候选对象不支持字段 %q（支持字段: template / insecure-skip-verify / http-proxy / connect-timeout / http-timeout / read-timeout / probe-timeout；预留字段 %s 本期未实现，暂不可配置）",
				key.Line, key.Value, urlRedirectItemReservedFields)
		}
	}
	// 解码实体：经由别名类型避免递归触发本 UnmarshalYAML
	type plain URLRedirectItem
	var p plain
	if err := node.Decode(&p); err != nil {
		return err
	}
	*it = URLRedirectItem(p)
	return nil
}

// DomainRule 单条域名/URL 规则。match 为正则表达式，对
// 「http://<入站Host>/<目标URL>」整串做非锚定匹配（Host 归一化为小写并去除首尾空白）：
//   - 按入站域名匹配：如 `http://github\.path\..+`（入站 Host 形如 github.path.xxx 时命中）；
//   - 按目标 URL 匹配（旧写法兼容）：因非锚定，`raw\.githubusercontent\.com` 与
//     `.*https://[^.]+\.githubusercontent\.com` 等原写法照常命中。
//
// exclude 为可选的排除正则列表（同样对该整串非锚定匹配），
// match 命中后任一 exclude 命中即视为不匹配、继续向下尝试后续规则；
// 规则自上而下，第一条命中的规则生效。
type DomainRule struct {
	// Name 规则名（可选，仅用于日志与观测）。
	Name string `yaml:"name"`
	// Type 请求类型过滤（可选）：限定本规则只承接哪类请求。取值（Validate 阶段做
	// trim + 小写归一化，"Docker " 视为 "docker"，归一化结果参与匹配与日志；
	// 非法值启动快速失败）：
	//   - ""（留空/未配置，默认）：不过滤请求类型，任何请求都可命中本规则
	//    （完全兼容旧配置，现有行为不变）；
	//   - "docker"（RuleTypeDocker）：仅路径目标请求可命中本规则
	//    （如 docker registry 的 /v2/xxx/manifests/latest）；
	//   - "web"（RuleTypeWeb）：仅完整 URL 请求可命中本规则
	//    （如 /https://raw.githubusercontent.com/...）。
	// 典型用途：同一入站域名/同一段 match 下，把 docker 流量与 web 流量分流到
	// 两条不同规则（各自的 cache / 超时 / url-redirect 候选互不影响）。
	Type string `yaml:"type"`
	// Match 匹配「http://<入站Host>/<目标URL>」整串的正则表达式（必填，非锚定匹配）。
	Match string `yaml:"match"`
	// Exclude 排除正则列表（可选，可配置多个）：match 命中后逐个检查（同样对整串非锚定匹配），
	// 任一命中即排除该规则（视为不匹配，继续向下尝试后续规则）；
	// 全部不命中才应用该规则。
	Exclude []string `yaml:"exclude"`
	// Cache 该规则下的缓存覆盖项（enabled / ttl）。
	Cache RuleCache `yaml:"cache"`
	// HTTPProxy 该规则回源时使用的上游代理（覆盖全局 http-proxy；空串表示直连）。
	HTTPProxy *string `yaml:"http-proxy"`
	// InsecureSkipVerify 规则级「忽略 TLS 证书校验」（可选）：nil = 继承全局
	// insecure-skip-verify；显式 true/false 覆盖全局（url-redirect 候选对象内的显式值
	// 优先级更高，三级优先级见 URLRedirectItem.InsecureSkipVerify 与
	// Options.CandidateInsecureSkipVerify）。
	// 开启后该规则回源路径上的所有上游连接（候选、fallback 直连、30x 重定向目标、
	// token dance 的取 token 与带 token 重试）都跳过 TLS 证书校验：等于关闭该路径的
	// 服务端身份验证与防窃听保护，仅建议在内网自签/镜像站证书链不全时开启；
	// 更推荐用候选级（URLRedirectItem.InsecureSkipVerify）对个别镜像站精确开启。
	InsecureSkipVerify *bool `yaml:"insecure-skip-verify"`
	// URLRedirect 该规则专用的 url-redirect 候选对象列表（覆盖全局；设为 [] 表示该规则直连回源）。
	URLRedirect *[]URLRedirectItem `yaml:"url-redirect"`
	// SuccessCheck 该规则的成功判据（status / content）。
	SuccessCheck *string `yaml:"success-check"`
	// FallbackDirect 该规则下候选全部失败后是否回退直连。
	FallbackDirect *bool `yaml:"fallback-direct"`
	// Timeout 该规则回源的「无新增数据超时」退化别名（旧字段，语义已变更）：
	// 仅当本规则未配置 read-timeout 时，才作为该规则生效的 read-timeout；
	// 不再是回源总超时（总超时用 http-timeout），也不再有任何默认值兜底。
	// 0 = 不限制；负数为配置错误（Validate 拒绝）。
	Timeout *Duration `yaml:"timeout"`
	// HTTPTimeout 该规则单次上游尝试的总超时：覆盖「建连 + 响应头 + 完整读 body +
	// 401 后 token dance 及其带 token 重试」整个尝试过程，按每次尝试独立计时
	//（候选重试/整轮重试时每次重新计时）。nil（未配置）沿用全局 http-timeout；
	// 0 = 不限制；负数为配置错误（Validate 拒绝）。
	HTTPTimeout *Duration `yaml:"http-timeout"`
	// ReadTimeout 该规则的「无新增数据超时」（idle）：以本次尝试累计收到的字节数为
	// 进度标志（响应头到达也算一次进度），窗口内有进度就重置计时器，窗口内无任何
	// 新数据才触发超时并取消该次尝试——慢速但持续传输的大文件不会被误砍。
	// 生效优先级：本字段 > 本规则 timeout > 全局 read-timeout > 全局 timeout > 0（不限制）；
	// 负数为配置错误（Validate 拒绝）。
	ReadTimeout *Duration `yaml:"read-timeout"`
	// ConnectTimeout 该规则的建连超时（TCP 拨号 + TLS 握手共用）。
	// nil（未配置）沿用全局 connect-timeout；0 = 不限制；负数为配置错误（Validate 拒绝）。
	ConnectTimeout *Duration `yaml:"connect-timeout"`
	// URLRedirectItemErrorRetry 该规则单个候选失败后的原地立即重试次数（无退避）：
	// 某候选一次尝试失败（网络错误/超时/未过 success-check）后原地重试 N 次
	//（总尝试 = 1+N），全部失败才轮到下一候选。
	// nil（未配置）沿用全局；0 = 不重试；负数为配置错误（Validate 拒绝）。
	URLRedirectItemErrorRetry *int `yaml:"url-redirect-item-error-retry"`
	// URLRedirectAllErrorRetry 该规则「整轮候选全部失败」后的重试轮数：
	// 一轮 = 所有候选（含各自 item 重试）都失败；全轮失败后整体重来最多 N 轮
	//（总轮数 = 1+N），重试轮沿用同一候选顺序。
	// nil（未配置）沿用全局；0 = 不重试；负数为配置错误（Validate 拒绝）。
	URLRedirectAllErrorRetry *int `yaml:"url-redirect-all-error-retry"`
	// URLRedirectProbeTimeout 该规则单次候选尝试的探测预算（语义同全局
	// url-redirect-probe-timeout）。nil（未配置）沿用全局；0 = 不限制；
	// 负数为配置错误（Validate 拒绝）。
	URLRedirectProbeTimeout *Duration `yaml:"url-redirect-probe-timeout"`
	// URLRedirectHedgeDelay 该规则候选阶梯竞速的放行间隔（语义同全局
	// url-redirect-hedge-delay）。nil（未配置）沿用全局；0 = 纯串行禁用竞速；
	// 负数为配置错误（Validate 拒绝）。
	URLRedirectHedgeDelay *Duration `yaml:"url-redirect-hedge-delay"`
	// SlowDownloadSpeed 该规则慢速升级多源并发续传的判慢阈值（B/s，语义同全局
	// slow-download-speed）。nil（未配置）沿用全局；<=0 = 关闭整个特性；
	// 负数为配置错误（Validate 拒绝）。仅 type=docker 规则生效。
	SlowDownloadSpeed *Bytes `yaml:"slow-download-speed"`
	// SlowDownloadWindow 该规则判速滑动窗口时长（语义同全局 slow-download-window）。
	// nil（未配置）沿用全局；启用时（生效 speed>0）小于 1s 被 Validate 钳制为 1s。
	SlowDownloadWindow *Duration `yaml:"slow-download-window"`
	// SlowDownloadMinRemaining 该规则触发升级所需最小剩余字节数（语义同全局
	// slow-download-min-remaining）。nil（未配置）沿用全局；0 合法（不设剩余量门槛）。
	SlowDownloadMinRemaining *Bytes `yaml:"slow-download-min-remaining"`
	// SlowDownloadMaxBehind 该规则多源合并的剪枝阈值（语义同全局
	// slow-download-max-behind）。nil（未配置）沿用全局；启用时（生效 speed>0）
	// 必须 >0，Validate 拒绝。
	SlowDownloadMaxBehind *Bytes `yaml:"slow-download-max-behind"`

	// re 是 Match 编译后的正则；excludeRe 是 Exclude 逐项编译后的正则列表
	//（均在 Validate 阶段填充，不参与 YAML 解析）。
	re        *regexp.Regexp
	excludeRe []*regexp.Regexp
}

// Label 返回规则的可读标识（优先 name，否则 match 原文）；
// 供本包日志与 main 启动时的规则摘要日志复用。
func (r *DomainRule) Label() string {
	if r.Name != "" {
		return r.Name
	}
	return r.Match
}

// matchResult 返回该规则对匹配串的匹配明细（match 与 exclude 分别命中与否）：
//   - matched：match 正则是否命中；
//   - excluded：match 命中后是否有任一 exclude 命中（未配置 exclude 恒为 false）。
//
// matched=true 且 excluded=false 才视为该规则命中。
func (r *DomainRule) matchResult(matchStr string) (matched, excluded bool) {
	if r.re == nil || !r.re.MatchString(matchStr) {
		return false, false
	}
	for _, ex := range r.excludeRe {
		if ex.MatchString(matchStr) {
			return true, true // 命中任一 exclude，该规则被排除
		}
	}
	return true, false
}

// LogConfig 日志配置。
type LogConfig struct {
	// Level 日志级别：debug / info / warn / error（大小写不敏感；留空默认 info；
	// 非法值在 Validate 阶段报错，启动快速失败）。
	Level string `yaml:"level"`
	// ProgressInterval 流式透传进度日志的输出间隔（GET 路径目标流式回源期间每隔该
	// 间隔输出一条回源进度日志）：nil（未配置）默认 10s（DefaultProgressLogInterval）；
	// 显式 <=0 关闭进度日志（负数归一化为 0，Validate 不报错，风格同 cache.clean-interval）。
	ProgressInterval *Duration `yaml:"progress-interval"`
}

// CacheConfig 全局缓存配置。
type CacheConfig struct {
	// Enabled 是否启用缓存。
	Enabled bool `yaml:"enabled"`
	// Path 缓存目录（默认 ./cache-data，命名避免与源码 cache/ 包目录混用）。
	Path string `yaml:"path"`
	// TTL 默认缓存时长（可被域名规则覆盖）：nil（未配置）按 30m 处理
	//（Default 预置、Validate 兜底）；显式 0 表示永不过期；负数为配置错误（Validate 拒绝）。
	TTL *Duration `yaml:"ttl"`
	// CleanInterval 后台过期清理扫描间隔，<=0 关闭。
	CleanInterval Duration `yaml:"clean-interval"`
}

// Config 顶层配置结构，与 config.yaml 字段一一对应。
type Config struct {
	// Listen HTTP 服务监听地址，默认 0.0.0.0:8080。
	Listen string `yaml:"listen"`
	// Timeout 回源「无新增数据超时」的退化别名（旧字段，语义已变更）：
	// 仅当全局未配置 read-timeout 时，才作为全局生效的 read-timeout；
	// 不再是回源总超时（总超时用 http-timeout），也不再有任何默认值兜底（旧版为 60s）。
	// 未配置/0 = 不限制；负数为配置错误（Validate 拒绝）。
	Timeout Duration `yaml:"timeout"`
	// HTTPTimeout 全局单次上游尝试的总超时（语义同 DomainRule.HTTPTimeout）。
	// nil（未配置）= 不限制；负数为配置错误（Validate 拒绝）。
	HTTPTimeout *Duration `yaml:"http-timeout"`
	// ReadTimeout 全局「无新增数据超时」（语义同 DomainRule.ReadTimeout）。
	// nil（未配置）回落到全局 timeout（旧字段退化别名），仍无 = 不限制；
	// 负数为配置错误（Validate 拒绝）。
	ReadTimeout *Duration `yaml:"read-timeout"`
	// ConnectTimeout 全局建连超时（TCP 拨号 + TLS 握手共用）。
	// nil（未配置）= 不限制；负数为配置错误（Validate 拒绝）。
	ConnectTimeout *Duration `yaml:"connect-timeout"`
	// URLRedirectItemErrorRetry 全局单个候选失败后的原地立即重试次数。
	// nil（未配置）= 0（不重试）；负数为配置错误（Validate 拒绝）。
	URLRedirectItemErrorRetry *int `yaml:"url-redirect-item-error-retry"`
	// URLRedirectAllErrorRetry 全局「整轮候选全部失败」后的重试轮数。
	// nil（未配置）= 0（不重试）；负数为配置错误（Validate 拒绝）。
	URLRedirectAllErrorRetry *int `yaml:"url-redirect-all-error-retry"`
	// URLRedirectProbeTimeout 全局单次候选尝试的探测预算：从发出请求到「响应头可用
	//（含 401 token dance 的第二次响应）且成功判据可判定（首块读出或 EOF）」的时限，
	// 覆盖建连后的等待与 token dance 全程；到期取消该次尝试并硬失败快切下一候选。
	// 仅约束探测阶段：探测通过后的 body 读取（流式透传/缓冲排干）不受它约束，
	// 仍由 http-timeout（尝试总超时）与 read-timeout（无新增数据）负责；
	// 建连本身由 connect-timeout 负责。nil（未配置）= 缺省 5s；显式 0 = 不限制；
	// 负数为配置错误（Validate 拒绝）。
	URLRedirectProbeTimeout *Duration `yaml:"url-redirect-probe-timeout"`
	// URLRedirectHedgeDelay 全局候选阶梯竞速（hedge）的放行间隔：候选按 round-robin
	// 顺序起步，首个候选立即发出，之后每隔该间隔放行下一个候选并发探测（前一个不取消）；
	// 第一个通过成功判据的候选胜出，其余全部取消（context 取消，连接及时释放）。
	// 0 = 纯串行禁用竞速（上一候选出结果后才放行下一个）；负数为配置错误（Validate 拒绝）。
	URLRedirectHedgeDelay *Duration `yaml:"url-redirect-hedge-delay"`
	// SlowDownloadSpeed 全局慢速升级多源并发续传的判慢阈值（B/s）：流式回源的滑动窗口
	// 平均送出速率低于它判「过慢」，对（候选数>1 的）docker 类流式下载向全部其余候选并发
	// 发起 Range 续传并有序合并。默认 1MB/s；显式 <=0 = 关闭整个特性；负数为配置错误
	//（Validate 拒绝）。仅 type=docker 规则生效。
	SlowDownloadSpeed *Bytes `yaml:"slow-download-speed"`
	// SlowDownloadWindow 全局判速滑动窗口时长：窗口满时按窗口平均送出速率判慢；
	// 默认 10s，启用时（生效 speed>0）小于 1s 被 Validate 钳制为 1s。
	SlowDownloadWindow *Duration `yaml:"slow-download-window"`
	// SlowDownloadMinRemaining 全局触发升级所需最小剩余字节数：剩余字节（已知总长 -
	// 已传）不足该值时不升级（尾巴太小没必要并发拉满）。默认 64MB；0 合法；
	// 负数为配置错误（Validate 拒绝）。
	SlowDownloadMinRemaining *Bytes `yaml:"slow-download-min-remaining"`
	// SlowDownloadMaxBehind 全局多源合并的剪枝阈值：合并中某源落后领先源超过该字节数
	// 即被关闭移除（收敛「每个源都拉满剩余区间自己的流」的带宽放大）。默认 8MB；
	// 启用时（生效 speed>0）必须 >0，Validate 拒绝。
	SlowDownloadMaxBehind *Bytes `yaml:"slow-download-max-behind"`
	// HTTPProxy 全局上游代理（http/https/socks5 地址，如 http://127.0.0.1:7890），空串表示直连。
	HTTPProxy string `yaml:"http-proxy"`
	// SuccessCheck url-redirect 候选成功判据：status 或 content（默认 content）。
	SuccessCheck string `yaml:"success-check"`
	// FallbackDirect url-redirect 候选全部失败后是否回退直连原始 URL（默认 true）。
	// 指针用于区分「未配置」与「显式配置 false」。
	FallbackDirect *bool `yaml:"fallback-direct"`
	// InsecureSkipVerify 全局「忽略 TLS 证书校验」（默认 false）：
	// 三级优先级（高 → 低）：url-redirect 候选对象 insecure-skip-verify >
	// 域名规则 insecure-skip-verify > 本全局值（解析链路见 ResolveOptions 与
	// Options.CandidateInsecureSkipVerify）。
	// 生效范围：该路径上的所有上游连接——url-redirect 候选、fallback 直连/直连透传、
	// 30x 重定向目标（如 1ms.run → cloudfront）、token dance 的取 token 与带 token 重试。
	// 安全代价：跳过证书校验等于放弃对上游的服务端身份验证与防中间人/窃听保护
	//（连接仍加密，但无法确认对端身份）；典型场景是内网自签证书或镜像站证书链不全
	//（如候选报 `tls: failed to verify certificate: x509: certificate signed by unknown authority`，
	// 每轮候选探测全部烧在证书错误上）。建议默认关闭，仅对个别候选/规则精确开启。
	InsecureSkipVerify bool `yaml:"insecure-skip-verify"`
	// URLRedirect 全局 url-redirect 候选对象列表（如 {template: https://gh-proxy.com/$1}，
	// 对象字段与三级 insecure 优先级见 URLRedirectItem）。
	URLRedirect []URLRedirectItem `yaml:"url-redirect"`
	// Cache 全局缓存配置。
	Cache CacheConfig `yaml:"cache"`
	// Log 日志配置（level / progress-interval）。
	Log LogConfig `yaml:"log"`
	// DomainRules 域名/URL 规则列表（自上而下第一条命中生效）。
	DomainRules []DomainRule `yaml:"domain-rules"`
	// AllowList 允许列表（正则）；非空时为白名单模式，未命中的目标一律拒绝。
	AllowList []string `yaml:"allow-list"`
	// DenyList 拒绝列表（正则）；命中即拒绝。
	DenyList []string `yaml:"deny-list"`

	// logLevel 是 log.level 归一化后的生效级别（Validate 阶段填充，不参与 YAML 解析），
	// 经 LogLevel() 访问器读取。
	logLevel slog.Level
	// allow / deny 是两个列表编译后的正则（Validate 阶段填充）。
	allow []*regexp.Regexp
	deny  []*regexp.Regexp
}

// Options 是单次请求最终生效的配置（全局默认叠加命中的域名规则覆盖项）。
type Options struct {
	// Rule 命中的域名规则标识（未命中为空串，仅用于日志/观测）。
	Rule string
	// CacheEnabled 该请求是否启用缓存。
	CacheEnabled bool
	// CacheTTL 该请求的缓存时长（0 = 永不过期；负数已被 Validate 拦截）。
	CacheTTL time.Duration
	// CachePath 该请求读写缓存使用的目录（规则可配独立目录；未配置沿用全局 cache.path）。
	CachePath string
	// CacheCleanInterval 该请求生效的缓存后台清理扫描间隔（<=0 表示关闭后台清理）。
	CacheCleanInterval time.Duration
	// HTTPProxy 该请求回源使用的上游代理（空串表示直连）。
	HTTPProxy string
	// InsecureSkipVerify 该请求生效的「忽略 TLS 证书校验」：规则显式值 ?? 全局值
	//（ResolveOptions 解析）。作用于直连回退 / 完整 URL 直连透传路径；
	// 候选路径按候选对象生效值另行解析（见 CandidateInsecureSkipVerify）。
	InsecureSkipVerify bool
	// URLRedirect 该请求生效的 url-redirect 候选对象列表。
	URLRedirect []URLRedirectItem
	// SuccessCheck 该请求生效的成功判据（status / content）。
	SuccessCheck string
	// FallbackDirect 候选全部失败后是否回退直连。
	FallbackDirect bool
	// HTTPTimeout 单次上游尝试的总超时（0 = 不限制）：覆盖建连 + 响应头 + 完整读
	// body + 401 后 token dance 及其带 token 重试，按每次尝试独立计时。
	HTTPTimeout time.Duration
	// ReadTimeout 该请求生效的「无新增数据超时」（0 = 不限制）：
	// rule.read-timeout > rule.timeout > global.read-timeout > global.timeout 的最终结果。
	ReadTimeout time.Duration
	// ConnectTimeout 该请求的建连超时（TCP 拨号 + TLS 握手共用；0 = 不限制）。
	ConnectTimeout time.Duration
	// URLRedirectItemErrorRetry 单个候选失败后的原地立即重试次数（0 = 不重试，
	// 总尝试 = 1+N）。
	URLRedirectItemErrorRetry int
	// URLRedirectAllErrorRetry 「整轮候选全部失败」后的重试轮数（0 = 不重试，
	// 总轮数 = 1+N；重试轮沿用同一候选顺序）。
	URLRedirectAllErrorRetry int
	// URLRedirectProbeTimeout 单次候选尝试的探测预算（0 = 不限制）：从发出请求到
	//「响应头可用（含 token dance 的第二次响应）且成功判据可判定」的时限，到期取消
	// 该次尝试并硬失败快切下一候选；仅约束探测阶段，探测后的 body 读取不受它约束。
	URLRedirectProbeTimeout time.Duration
	// URLRedirectHedgeDelay 候选阶梯竞速的放行间隔（0 = 纯串行禁用竞速）：
	// 首个候选立即发出，之后每隔该间隔放行下一个候选并发探测（前一个不取消），
	// 第一个通过成功判据的候选胜出，其余全部取消。
	URLRedirectHedgeDelay time.Duration
	// ProgressLogInterval 流式透传进度日志的输出间隔（<=0 关闭进度日志；未配置默认 10s）：
	// GET 路径目标流式回源期间每隔该间隔输出一条回源进度日志（已传字节数/百分比/速率等），
	// 便于观测 docker pull 大 blob 这类长耗时流式传输的回源进度。
	ProgressLogInterval time.Duration
	// RuleType 生效规则的 type 归一化值（空串 = 未命中规则或规则未配置 type 过滤）：
	// 供慢速升级多源并发续传做类型门控（仅 RuleTypeDocker 的流式回源启用）。
	RuleType string
	// SlowDownloadSpeed 慢速升级多源并发续传的判慢阈值（B/s）：<=0 = 关闭整个特性；
	// 规则显式 ?? 全局（ResolveOptions 解析）。仅 type=docker 规则生效。
	SlowDownloadSpeed int64
	// SlowDownloadWindow 判速滑动窗口时长（启用时经 Validate 保证 >=1s）。
	SlowDownloadWindow time.Duration
	// SlowDownloadMinRemaining 触发升级所需最小剩余字节数（0 = 不设剩余量门槛）。
	SlowDownloadMinRemaining int64
	// SlowDownloadMaxBehind 多源合并的剪枝阈值（落后超过该字节数的源被剪枝关闭）。
	SlowDownloadMaxBehind int64
}

// CandidateInsecureSkipVerify 返回候选对象的「忽略 TLS 证书校验」最终生效值——
// 三级优先级的最后一步：候选显式值 ?? o.InsecureSkipVerify（后者已是
// 规则显式 ?? 全局 的合并结果，见 ResolveOptions）。
// 即：候选对象 insecure-skip-verify > 规则 insecure-skip-verify > 全局 insecure-skip-verify。
func (o Options) CandidateInsecureSkipVerify(item URLRedirectItem) bool {
	if item.InsecureSkipVerify != nil {
		return *item.InsecureSkipVerify
	}
	return o.InsecureSkipVerify
}

// candidateDurationOverride 候选级时长三级优先级的公共实现：候选对象显式指针非 nil
// 时取候选值（含显式 0 = 不限制），否则回落 o 内对应字段（已是规则显式 ?? 全局 的
// 合并结果，见 ResolveOptions）。负数已在 Validate 拦截，这里不做重复校验。
func candidateDurationOverride(item *Duration, eff time.Duration) time.Duration {
	if item != nil {
		return item.D()
	}
	return eff
}

// CandidateHTTPProxy 返回候选对象的回源代理最终生效值——三级优先级的最后一步：
// 候选显式值（trim 后原样返回）?? o.HTTPProxy（后者已是规则显式 ?? 全局 的合并结果，
// 见 ResolveOptions）。即：候选对象 http-proxy > 规则 http-proxy > 全局 http-proxy。
// 特别的，候选显式空串（含仅空白）是「该候选强制直连」的显式语义：返回空串覆盖
// 上层已配置的代理，实现「个别候选走代理、其余直连」或其反向的精确表达；
// 非 nil 但含空白时 trim 后返回（与全局/规则级的 trim 语义一致）。
func (o Options) CandidateHTTPProxy(item URLRedirectItem) string {
	if item.HTTPProxy != nil {
		return strings.TrimSpace(*item.HTTPProxy)
	}
	return o.HTTPProxy
}

// CandidateConnectTimeout 返回候选对象的建连超时最终生效值（TCP 拨号 + TLS 握手共用）——
// 候选对象 connect-timeout > 规则 connect-timeout > 全局 connect-timeout；
// 显式 0 = 不限制（覆盖上层已配置的预算），nil = 逐级继承。
func (o Options) CandidateConnectTimeout(item URLRedirectItem) time.Duration {
	return candidateDurationOverride(item.ConnectTimeout, o.ConnectTimeout)
}

// CandidateHTTPTimeout 返回候选对象单次尝试总超时最终生效值（覆盖建连 + 响应头 +
// 完整读 body + token dance 及其带 token 重试，按每次尝试独立计时）——
// 候选对象 http-timeout > 规则 http-timeout > 全局 http-timeout；
// 显式 0 = 不限制，nil = 逐级继承。
func (o Options) CandidateHTTPTimeout(item URLRedirectItem) time.Duration {
	return candidateDurationOverride(item.HTTPTimeout, o.HTTPTimeout)
}

// CandidateReadTimeout 返回候选对象「无新增数据超时」最终生效值——
// 候选对象 read-timeout > o.ReadTimeout（后者已是 rule.read-timeout > rule.timeout >
// global.read-timeout > global.timeout 的最终回落结果，见 ResolveOptions）；
// 显式 0 = 不限制，nil = 逐级继承。
func (o Options) CandidateReadTimeout(item URLRedirectItem) time.Duration {
	return candidateDurationOverride(item.ReadTimeout, o.ReadTimeout)
}

// CandidateProbeTimeout 返回候选对象单次探测预算最终生效值（URLRedirectItem.ProbeTimeout
// 是规则/全局 url-redirect-probe-timeout 的候选级短名）——
// 候选对象 probe-timeout > 规则 url-redirect-probe-timeout > 全局 url-redirect-probe-timeout。
// 注意全局缺省语义：未逐级配置时 o.URLRedirectProbeTimeout 已兜底缺省 5s
// （GlobalURLRedirectProbeTimeout），显式 0 = 不限制，nil = 逐级继承。
func (o Options) CandidateProbeTimeout(item URLRedirectItem) time.Duration {
	return candidateDurationOverride(item.ProbeTimeout, o.URLRedirectProbeTimeout)
}

// Default 返回内置默认配置（配置文件缺失时兜底，也是 Load 时叠加解析的底版：
// YAML 中未出现的字段保持这里的默认值）。
func Default() *Config {
	yes := true
	return &Config{
		Listen:         "0.0.0.0:8080",
		SuccessCheck:   SuccessCheckContent,
		FallbackDirect: &yes,
		Cache: CacheConfig{
			Enabled:       true,
			Path:          "./cache-data",
			TTL:           durationPtr(DefaultCacheTTL),
			CleanInterval: Duration(10 * time.Minute),
		},
	}
}

// Load 读取并解析配置文件，随后做默认值补全与合法性校验。
// 文件不存在时返回包装了 fs.ErrNotExist 的错误，由调用方决定是否回退默认配置。
func Load(path string) (*Config, error) {
	data, err := os.ReadFile(path)
	if err != nil {
		return nil, err
	}
	cfg := Default()
	if err := yaml.Unmarshal(data, cfg); err != nil {
		return nil, fmt.Errorf("解析配置文件 %s 失败: %w", path, err)
	}
	if err := cfg.Validate(); err != nil {
		return nil, fmt.Errorf("配置文件 %s 校验失败: %w", path, err)
	}
	return cfg, nil
}

// Validate 补全默认值并校验配置合法性（正则编译、取值范围、模板格式等）。
// 校验通过的配置即视为运行期只读安全。
func (c *Config) Validate() error {
	// //////////////////  默认值补全  start  ////////////////////////////////////////////////
	if strings.TrimSpace(c.Listen) == "" {
		c.Listen = "0.0.0.0:8080"
	}
	// 旧 timeout 字段不再做任何默认值兜底（旧版此处会兜底 60s，属有意的行为变更）：
	// 未配置/0 = read-timeout 不限制，负数在下方取值校验阶段报错
	if strings.TrimSpace(c.Cache.Path) == "" {
		c.Cache.Path = "./cache-data"
	}
	// cache.ttl 为指针：nil（未配置/显式置空）兜底默认 30m；显式 0（永不过期）与负数
	// 保持原值，负数在下方「全局取值校验」阶段报错
	if c.Cache.TTL == nil {
		c.Cache.TTL = durationPtr(DefaultCacheTTL)
	}
	if c.Cache.CleanInterval < 0 {
		c.Cache.CleanInterval = 0
	}
	c.SuccessCheck = strings.ToLower(strings.TrimSpace(c.SuccessCheck))
	if c.SuccessCheck == "" {
		c.SuccessCheck = SuccessCheckContent
	}
	// log.level 归一化：trim + 小写；空串默认 info（生效级别映射在下方取值校验阶段填充）
	c.Log.Level = strings.ToLower(strings.TrimSpace(c.Log.Level))
	if c.Log.Level == "" {
		c.Log.Level = "info"
	}
	// log.progress-interval：nil（未配置）默认 10s（生效值经 ProgressLogInterval() 读取）；
	// 显式负数归一化为 0（关闭进度日志，与 cache.clean-interval 同风格，Validate 不报错）
	if c.Log.ProgressInterval != nil && c.Log.ProgressInterval.D() < 0 {
		v := Duration(0)
		c.Log.ProgressInterval = &v
	}
	// //////////////////  默认值补全  end  ////////////////////////////////////////////////

	// //////////////////  全局取值校验  start  ////////////////////////////////////////////////
	if c.SuccessCheck != SuccessCheckStatus && c.SuccessCheck != SuccessCheckContent {
		return fmt.Errorf("success-check 仅支持 status 或 content，当前为 %q", c.SuccessCheck)
	}
	// 全局超时/重试参数：负数为配置错误；0/缺省 = 该项不限制或不重试
	//（旧 timeout 已是 read-timeout 的退化别名，同样无默认值兜底）
	if c.Timeout < 0 {
		return fmt.Errorf("timeout 不能为负数（未配置或 0 = read-timeout 不限制，已无默认 60s 兜底），当前为 %s", c.Timeout)
	}
	if err := validateNonNegativeDuration(c.HTTPTimeout, "http-timeout"); err != nil {
		return err
	}
	if err := validateNonNegativeDuration(c.ReadTimeout, "read-timeout"); err != nil {
		return err
	}
	if err := validateNonNegativeDuration(c.ConnectTimeout, "connect-timeout"); err != nil {
		return err
	}
	if err := validateNonNegativeInt(c.URLRedirectItemErrorRetry, "url-redirect-item-error-retry"); err != nil {
		return err
	}
	if err := validateNonNegativeInt(c.URLRedirectAllErrorRetry, "url-redirect-all-error-retry"); err != nil {
		return err
	}
	if err := validateNonNegativeDuration(c.URLRedirectProbeTimeout, "url-redirect-probe-timeout"); err != nil {
		return err
	}
	if err := validateNonNegativeDuration(c.URLRedirectHedgeDelay, "url-redirect-hedge-delay"); err != nil {
		return err
	}
	// 慢速升级多源并发续传四键：负数为配置错误；启用（生效 speed>0）时钳制 window>=1s、
	// 要求 max-behind>0（min-remaining 允许 0；speed<=0 = 关闭特性，不做启用性校验）
	if err := validateSlowDownload("", c.SlowDownloadSpeed, c.SlowDownloadWindow, c.SlowDownloadMinRemaining, c.SlowDownloadMaxBehind, c.GlobalSlowDownloadSpeed()); err != nil {
		return err
	}
	// log.level 仅允许四级，非法值启动快速失败；合法值转成 slog.Level 存私有字段供 LogLevel() 使用
	switch c.Log.Level {
	case "debug":
		c.logLevel = slog.LevelDebug
	case "info":
		c.logLevel = slog.LevelInfo
	case "warn":
		c.logLevel = slog.LevelWarn
	case "error":
		c.logLevel = slog.LevelError
	default:
		return fmt.Errorf("log.level 仅支持 debug/info/warn/error，当前为 %q", c.Log.Level)
	}
	// 经上方默认值补全后 TTL 必然非 nil：负数是配置错误（0 = 永不过期，合法）
	if c.Cache.TTL.D() < 0 {
		return fmt.Errorf("cache.ttl 不能为负数（0 表示永不过期，未配置默认 %s），当前为 %s", DefaultCacheTTL, c.Cache.TTL)
	}
	if strings.TrimSpace(c.HTTPProxy) != "" {
		if err := validateProxyAddr(c.HTTPProxy); err != nil {
			return fmt.Errorf("http-proxy 配置无效: %w", err)
		}
	}
	if err := validateRedirectItems(c.URLRedirect, "url-redirect"); err != nil {
		return err
	}
	// //////////////////  全局取值校验  end  ////////////////////////////////////////////////

	// //////////////////  域名规则校验  start  ////////////////////////////////////////////////
	for i := range c.DomainRules {
		r := &c.DomainRules[i]
		if strings.TrimSpace(r.Match) == "" {
			return fmt.Errorf("domain-rules[%d] 缺少 match 正则", i)
		}
		re, err := regexp.Compile(r.Match)
		if err != nil {
			return fmt.Errorf("domain-rules[%d] (%s) 的 match 正则无效: %w", i, r.Label(), err)
		}
		r.re = re
		// type 归一化（trim + 小写，"Docker " 视为 "docker"）：归一化结果参与
		// ResolveOptions 的请求类型过滤与「域名规则已加载」启动日志；
		// 仅允许 docker / web，留空 = 不过滤请求类型，其他值启动快速失败
		r.Type = strings.ToLower(strings.TrimSpace(r.Type))
		if r.Type != "" && r.Type != RuleTypeDocker && r.Type != RuleTypeWeb {
			return fmt.Errorf("domain-rules[%d] (%s) 的 type 仅支持 docker / web（留空 = 不过滤请求类型），收到 %q", i, r.Label(), r.Type)
		}
		// exclude 正则逐个编译缓存（与 match 同样对「http://<Host>/<目标URL>」整串非锚定匹配）
		r.excludeRe = make([]*regexp.Regexp, 0, len(r.Exclude))
		for j, pat := range r.Exclude {
			pat = strings.TrimSpace(pat)
			if pat == "" {
				return fmt.Errorf("domain-rules[%d] (%s) 的 exclude 第 %d 项为空", i, r.Label(), j)
			}
			ex, err := regexp.Compile(pat)
			if err != nil {
				return fmt.Errorf("domain-rules[%d] (%s) 的 exclude 第 %d 项正则无效: %w", i, r.Label(), j, err)
			}
			r.excludeRe = append(r.excludeRe, ex)
		}
		// 规则级覆盖项合法性：ttl 负数为配置错误；显式 0 = 永不过期（合法）
		if r.Cache.TTL != nil && r.Cache.TTL.D() < 0 {
			return fmt.Errorf("domain-rules[%d] (%s) 的 cache.ttl 不能为负数（0 表示永不过期，不配置则沿用全局 cache.ttl）", i, r.Label())
		}
		// cache.path 允许为空（trim 后空 = 未配置，沿用全局 cache.path），无需校验
		if r.Cache.CleanInterval != nil && r.Cache.CleanInterval.D() < 0 {
			return fmt.Errorf("domain-rules[%d] (%s) 的 cache.clean-interval 必须大于等于 0", i, r.Label())
		}
		if r.HTTPProxy != nil && strings.TrimSpace(*r.HTTPProxy) != "" {
			if err := validateProxyAddr(*r.HTTPProxy); err != nil {
				return fmt.Errorf("domain-rules[%d] (%s) 的 http-proxy 无效: %w", i, r.Label(), err)
			}
		}
		if r.URLRedirect != nil {
			if err := validateRedirectItems(*r.URLRedirect, fmt.Sprintf("domain-rules[%d] (%s) 的 url-redirect", i, r.Label())); err != nil {
				return err
			}
		}
		if r.SuccessCheck != nil {
			v := strings.ToLower(strings.TrimSpace(*r.SuccessCheck))
			if v == "" {
				v = c.SuccessCheck // 显式留空 = 沿用全局取值
			}
			if v != SuccessCheckStatus && v != SuccessCheckContent {
				return fmt.Errorf("domain-rules[%d] (%s) 的 success-check 仅支持 status 或 content", i, r.Label())
			}
			r.SuccessCheck = &v
		}
		// 规则级超时/重试参数：负数为配置错误；0/缺省 = 不限制、不重试或按优先级回落
		//（旧 timeout 不再要求 >0：0 = read-timeout 不限制，属有意的行为变更）
		if r.Timeout != nil && r.Timeout.D() < 0 {
			return fmt.Errorf("domain-rules[%d] (%s) 的 timeout 不能为负数（旧字段，现仅作 read-timeout 的退化别名；未配置或 0 = 不限制）", i, r.Label())
		}
		if err := validateNonNegativeDuration(r.HTTPTimeout, "http-timeout"); err != nil {
			return fmt.Errorf("domain-rules[%d] (%s) 的 %w", i, r.Label(), err)
		}
		if err := validateNonNegativeDuration(r.ReadTimeout, "read-timeout"); err != nil {
			return fmt.Errorf("domain-rules[%d] (%s) 的 %w", i, r.Label(), err)
		}
		if err := validateNonNegativeDuration(r.ConnectTimeout, "connect-timeout"); err != nil {
			return fmt.Errorf("domain-rules[%d] (%s) 的 %w", i, r.Label(), err)
		}
		if err := validateNonNegativeInt(r.URLRedirectItemErrorRetry, "url-redirect-item-error-retry"); err != nil {
			return fmt.Errorf("domain-rules[%d] (%s) 的 %w", i, r.Label(), err)
		}
		if err := validateNonNegativeInt(r.URLRedirectAllErrorRetry, "url-redirect-all-error-retry"); err != nil {
			return fmt.Errorf("domain-rules[%d] (%s) 的 %w", i, r.Label(), err)
		}
		if err := validateNonNegativeDuration(r.URLRedirectProbeTimeout, "url-redirect-probe-timeout"); err != nil {
			return fmt.Errorf("domain-rules[%d] (%s) 的 %w", i, r.Label(), err)
		}
		if err := validateNonNegativeDuration(r.URLRedirectHedgeDelay, "url-redirect-hedge-delay"); err != nil {
			return fmt.Errorf("domain-rules[%d] (%s) 的 %w", i, r.Label(), err)
		}
		// 规则级 slow-download-* 四键：生效 speed = 规则显式 ?? 全局（与 ResolveOptions
		// 的解析顺序一致），启用时同样钳制 window / 要求 max-behind>0
		ruleSpeed := c.GlobalSlowDownloadSpeed()
		if r.SlowDownloadSpeed != nil {
			ruleSpeed = r.SlowDownloadSpeed.Int64()
		}
		if err := validateSlowDownload(fmt.Sprintf("domain-rules[%d] (%s) 的", i, r.Label()),
			r.SlowDownloadSpeed, r.SlowDownloadWindow, r.SlowDownloadMinRemaining, r.SlowDownloadMaxBehind, ruleSpeed); err != nil {
			return err
		}
	}
	// //////////////////  域名规则校验  end  ////////////////////////////////////////////////

	// //////////////////  allow/deny 列表编译  start  ////////////////////////////////////////////////
	var err error
	if c.allow, err = compilePatternList(c.AllowList, "allow-list"); err != nil {
		return err
	}
	if c.deny, err = compilePatternList(c.DenyList, "deny-list"); err != nil {
		return err
	}
	// //////////////////  allow/deny 列表编译  end  ////////////////////////////////////////////////
	return nil
}

// fallbackDirect 返回全局 fallback-direct 生效值（未配置时默认 true）。
func (c *Config) fallbackDirect() bool {
	if c.FallbackDirect == nil {
		return true
	}
	return *c.FallbackDirect
}

// validateNonNegativeDuration 校验可选时长字段非负（nil = 未配置，合法）。
func validateNonNegativeDuration(d *Duration, name string) error {
	if d != nil && d.D() < 0 {
		return fmt.Errorf("%s 不能为负数（未配置或 0 = 不限制），当前为 %s", name, d)
	}
	return nil
}

// validateNonNegativeInt 校验可选次数字段非负（nil = 未配置，合法）。
func validateNonNegativeInt(n *int, name string) error {
	if n != nil && *n < 0 {
		return fmt.Errorf("%s 不能为负数（未配置或 0 = 不重试），当前为 %d", name, *n)
	}
	return nil
}

// validateSlowDownload 校验 slow-download-* 四键（全局与规则级共用，where 为空串时
// 按全局键名报错，否则按「<where> <键名>」拼装）：
//   - 任一键负数为配置错误（快速失败）；
//   - 启用（effSpeed>0）时：window 非 nil 且 <1s 就地钳制为 1s（窗口过短会把网络抖动
//     误判为持续过慢）；显式 max-behind <=0 为配置错误（多源合并没有剪枝阈值会放大带宽
//     浪费）；min-remaining 允许 0（不设剩余量门槛）；
//   - 关闭（effSpeed<=0）时只做负数检查（其余键不生效，无启用性要求）。
func validateSlowDownload(where string, speed *Bytes, window *Duration, minRemaining, maxBehind *Bytes, effSpeed int64) error {
	key := func(name string) string {
		if where == "" {
			return name
		}
		return where + " " + name
	}
	if speed != nil && *speed < 0 {
		return fmt.Errorf("%s 不能为负数（<=0 = 关闭慢速升级多源并发续传），当前为 %s", key("slow-download-speed"), *speed)
	}
	if window != nil && window.D() < 0 {
		return fmt.Errorf("%s 不能为负数，当前为 %s", key("slow-download-window"), *window)
	}
	if minRemaining != nil && *minRemaining < 0 {
		return fmt.Errorf("%s 不能为负数（0 = 不设剩余量门槛），当前为 %s", key("slow-download-min-remaining"), *minRemaining)
	}
	if maxBehind != nil && *maxBehind < 0 {
		return fmt.Errorf("%s 不能为负数（启用时必须 >0），当前为 %s", key("slow-download-max-behind"), *maxBehind)
	}
	if effSpeed > 0 {
		if window != nil && window.D() < time.Second {
			*window = Duration(time.Second) // 钳制最小 1s
		}
		if maxBehind != nil && *maxBehind <= 0 {
			return fmt.Errorf("%s 必须大于 0（slow-download-speed 启用时它是多源合并的剪枝阈值）", key("slow-download-max-behind"))
		}
	}
	return nil
}

// GlobalCacheTTL 返回全局 cache.ttl 的生效时长：Validate 之后必然非 nil
// （未配置已兜底 DefaultCacheTTL）；显式 0 = 永不过期，原样返回 0。
// 方法内做 nil 防御，供未经过 Validate 的构造场景兜底，
// 供 ResolveOptions 与 main 启动日志复用。
func (c *Config) GlobalCacheTTL() time.Duration {
	if c.Cache.TTL == nil {
		return DefaultCacheTTL
	}
	return c.Cache.TTL.D()
}

// GlobalReadTimeout 返回全局 read-timeout 生效值（旧 timeout 退化别名的全局半段）：
// 全局 read-timeout 未配置时回落到全局 timeout；两者都未配置 = 0（不限制）。
// 规则级回落见 ResolveOptions（rule.read-timeout > rule.timeout > 本方法）。
func (c *Config) GlobalReadTimeout() time.Duration {
	if c.ReadTimeout != nil {
		return c.ReadTimeout.D()
	}
	return c.Timeout.D()
}

// GlobalHTTPTimeout 返回全局 http-timeout 生效值（nil = 0，不限制）。
func (c *Config) GlobalHTTPTimeout() time.Duration {
	if c.HTTPTimeout == nil {
		return 0
	}
	return c.HTTPTimeout.D()
}

// GlobalConnectTimeout 返回全局 connect-timeout 生效值（nil = 0，不限制）。
func (c *Config) GlobalConnectTimeout() time.Duration {
	if c.ConnectTimeout == nil {
		return 0
	}
	return c.ConnectTimeout.D()
}

// GlobalURLRedirectItemErrorRetry 返回全局单个候选失败后的原地重试次数（nil = 0，不重试）。
func (c *Config) GlobalURLRedirectItemErrorRetry() int {
	if c.URLRedirectItemErrorRetry == nil {
		return 0
	}
	return *c.URLRedirectItemErrorRetry
}

// GlobalURLRedirectAllErrorRetry 返回全局整轮候选全败后的重试轮数（nil = 0，不重试）。
func (c *Config) GlobalURLRedirectAllErrorRetry() int {
	if c.URLRedirectAllErrorRetry == nil {
		return 0
	}
	return *c.URLRedirectAllErrorRetry
}

// GlobalURLRedirectProbeTimeout 返回全局单次候选尝试的探测预算生效值：
// nil（未配置）= 缺省 5s；显式 0 = 不限制。
func (c *Config) GlobalURLRedirectProbeTimeout() time.Duration {
	if c.URLRedirectProbeTimeout == nil {
		return DefaultURLRedirectProbeTimeout
	}
	return c.URLRedirectProbeTimeout.D()
}

// GlobalURLRedirectHedgeDelay 返回全局候选阶梯竞速放行间隔生效值：
// nil（未配置）= 缺省 2s；显式 0 = 纯串行禁用竞速。
func (c *Config) GlobalURLRedirectHedgeDelay() time.Duration {
	if c.URLRedirectHedgeDelay == nil {
		return DefaultURLRedirectHedgeDelay
	}
	return c.URLRedirectHedgeDelay.D()
}

// GlobalSlowDownloadSpeed 返回全局判慢阈值生效值（B/s）：
// nil（未配置）= 缺省 1MB/s；显式 <=0 = 关闭慢速升级多源并发续传。
// 方法内做 nil 防御，供未经过 Validate 的构造场景兜底，供 ResolveOptions 复用。
func (c *Config) GlobalSlowDownloadSpeed() int64 {
	if c.SlowDownloadSpeed == nil {
		return DefaultSlowDownloadSpeed
	}
	return c.SlowDownloadSpeed.Int64()
}

// GlobalSlowDownloadWindow 返回全局判速滑动窗口时长生效值：
// nil（未配置）= 缺省 10s（DefaultSlowDownloadWindow）。
func (c *Config) GlobalSlowDownloadWindow() time.Duration {
	if c.SlowDownloadWindow == nil {
		return DefaultSlowDownloadWindow
	}
	return c.SlowDownloadWindow.D()
}

// GlobalSlowDownloadMinRemaining 返回全局触发升级所需最小剩余字节数生效值：
// nil（未配置）= 缺省 64MB（DefaultSlowDownloadMinRemaining）。
func (c *Config) GlobalSlowDownloadMinRemaining() int64 {
	if c.SlowDownloadMinRemaining == nil {
		return DefaultSlowDownloadMinRemaining
	}
	return c.SlowDownloadMinRemaining.Int64()
}

// GlobalSlowDownloadMaxBehind 返回全局多源合并剪枝阈值生效值：
// nil（未配置）= 缺省 8MB（DefaultSlowDownloadMaxBehind）。
func (c *Config) GlobalSlowDownloadMaxBehind() int64 {
	if c.SlowDownloadMaxBehind == nil {
		return DefaultSlowDownloadMaxBehind
	}
	return c.SlowDownloadMaxBehind.Int64()
}

// ProgressLogInterval 返回全局流式透传进度日志的输出间隔生效值：
// nil（未配置）= 缺省 10s（DefaultProgressLogInterval）；显式 <=0 = 0（关闭进度日志）。
// 方法内做 nil 防御，供未经过 Validate 的构造场景兜底，供 ResolveOptions 复用。
func (c *Config) ProgressLogInterval() time.Duration {
	if c.Log.ProgressInterval == nil {
		return DefaultProgressLogInterval
	}
	return c.Log.ProgressInterval.D()
}

// LogLevel 返回 log.level 对应的 slog.Level。必须在 Validate 之后调用
// （归一化结果存于私有字段 logLevel）；未经过 Validate 的构造场景兜底返回 Info。
func (c *Config) LogLevel() slog.Level {
	if c.logLevel == slog.LevelInfo {
		return slog.LevelInfo
	}
	return c.logLevel
}

// BuildMatchStr 构造域名规则匹配串「http://<入站Host>/<目标>」整串：
// Host 归一化为小写并去除首尾空白；target 可为完整目标 URL，也可为路径目标
// （不带 scheme，如 v2/xxx/manifests/latest）。域名规则的 match/exclude 匹配、
// 路径目标请求的 allow-list / deny-list 匹配与路径目标的缓存 key 输入均使用该整串。
func BuildMatchStr(host, target string) string {
	return "http://" + strings.ToLower(strings.TrimSpace(host)) + "/" + target
}

// targetKindOf 返回请求目标类型标识（日志用，与 proxy 包的 targetKind 同语义）：
// full-url（完整 URL 目标请求，fullURL=true）/ path（路径目标请求，fullURL=false）。
// 供 ResolveOptions 的规则 type 过滤留痕使用。
func targetKindOf(fullURL bool) string {
	if fullURL {
		return "full-url"
	}
	return "path"
}

// ResolveOptions 把全局配置与第一条命中「入站 Host + 目标」且请求类型相符的域名规则
// 覆盖项合并，返回该请求最终生效的配置。必须在 Validate 之后调用。
//
// fullURL 标识请求目标类型（与 proxy.ExtractTarget 的 fullURL 语义一致）：
// true = 完整 URL 请求（如 /https://raw.githubusercontent.com/...），
// false = 路径目标请求（如 docker registry 的 /v2/xxx/manifests/latest）。
// 它仅参与规则的 type 请求类型过滤（type: docker 仅承接路径目标 / type: web 仅承接
// 完整 URL / 留空不过滤），不影响 match/exclude 的匹配串构造。
//
// 域名规则匹配串构造为「http://<入站Host>/<目标>」整串（Host 归一化为小写并去除
// 首尾空白），规则 match 与 exclude 均对该整串做非锚定匹配：既支持按入站域名路由
// （如 match: 'http://github\.path\..+'），也兼容旧的目标 URL 正则写法；
// 路径目标请求（targetURL 不带 scheme，如 docker registry 的 /v2/xxx/manifests/latest）
// 同样按该整串匹配（形如 http://registry-proxy-cache.xxx:5480/v2/xxx/manifests/latest）。
// 注意：完整 URL 请求的缓存 key（目标 URL 的 sha256）与 allow-list / deny-list 匹配
// 仍只对目标 URL 进行，不受入站 Host 影响；路径目标请求则对「http://<入站Host>/<路径目标>」
// 整串做 ACL 匹配与缓存 key 输入（见 proxy 包），把入站 Host 纳入以保持拦截能力并避免跨域名缓存污染。
func (c *Config) ResolveOptions(host, targetURL string, fullURL bool) Options {
	// //////////////////  全局默认值  start  ////////////////////////////////////////////////
	o := Options{
		CacheEnabled:              c.Cache.Enabled,
		CacheTTL:                  c.GlobalCacheTTL(),
		CachePath:                 c.Cache.Path,
		CacheCleanInterval:        c.Cache.CleanInterval.D(),
		HTTPProxy:                 strings.TrimSpace(c.HTTPProxy),
		URLRedirect:               c.URLRedirect,
		InsecureSkipVerify:        c.InsecureSkipVerify,
		SuccessCheck:              c.SuccessCheck,
		FallbackDirect:            c.fallbackDirect(),
		HTTPTimeout:               c.GlobalHTTPTimeout(),
		ReadTimeout:               c.GlobalReadTimeout(),
		ConnectTimeout:            c.GlobalConnectTimeout(),
		URLRedirectItemErrorRetry: c.GlobalURLRedirectItemErrorRetry(),
		URLRedirectAllErrorRetry:  c.GlobalURLRedirectAllErrorRetry(),
		URLRedirectProbeTimeout:   c.GlobalURLRedirectProbeTimeout(),
		URLRedirectHedgeDelay:     c.GlobalURLRedirectHedgeDelay(),
		ProgressLogInterval:       c.ProgressLogInterval(),
		SlowDownloadSpeed:         c.GlobalSlowDownloadSpeed(),
		SlowDownloadWindow:        c.GlobalSlowDownloadWindow(),
		SlowDownloadMinRemaining:  c.GlobalSlowDownloadMinRemaining(),
		SlowDownloadMaxBehind:     c.GlobalSlowDownloadMaxBehind(),
	}
	// //////////////////  全局默认值  end  ////////////////////////////////////////////////

	// //////////////////  构造规则匹配串（入站 Host + 目标）  start  ////////////////////////////////////////////////
	// 形如 http://github.path.proxy-cache.xxx.sslip.io/https://raw.githubusercontent.com/...
	//（路径目标请求同理：http://<入站Host>/v2/xxx/manifests/latest）
	matchStr := BuildMatchStr(host, targetURL)
	// //////////////////  构造规则匹配串（入站 Host + 目标）  end  ////////////////////////////////////////////////

	// //////////////////  应用第一条命中的域名规则覆盖项  start  ////////////////////////////////////////////////
	for i := range c.DomainRules {
		r := &c.DomainRules[i]

		// 请求类型过滤：发生在 match/exclude 匹配之前——规则配置了 type 且与请求类型
		// 不符时直接跳过该规则（不消耗 match/exclude 判定），继续向下尝试后续规则。
		//   - type: docker 仅承接路径目标请求（fullURL=false，target_kind=path）；
		//   - type: web 仅承接完整 URL 请求（fullURL=true，target_kind=full-url）；
		//   - type 为空（未配置）= 不过滤，任何请求类型都可参与本规则匹配（默认，兼容旧配置）。
		if r.Type != "" {
			// 规则期望的请求类型：docker=路径目标 / web=完整 URL
			wantFullURL := r.Type == RuleTypeWeb
			if wantFullURL != fullURL {
				slog.Debug("域名规则请求类型不符，跳过",
					"rule", r.Label(), "rule_type", r.Type, "target_kind", targetKindOf(fullURL))
				continue
			}
		}

		// match 命中且该规则所有 exclude 均不命中才应用；
		// 被 exclude 排除的规则视为不匹配，继续向下尝试后续规则。
		matched, excluded := r.matchResult(matchStr)
		slog.Debug("域名规则匹配", "rule", r.Label(), "matched", matched, "excluded", excluded)
		if !matched || excluded {
			continue
		}
		if r.Cache.Enabled != nil {
			o.CacheEnabled = *r.Cache.Enabled
		}
		if r.Cache.TTL != nil {
			o.CacheTTL = r.Cache.TTL.D()
		}
		// 规则可配独立缓存目录：trim 后非空才覆盖（空 = 未配置，沿用全局 cache.path）
		if r.Cache.Path != nil {
			if p := strings.TrimSpace(*r.Cache.Path); p != "" {
				o.CachePath = p
			}
		}
		if r.Cache.CleanInterval != nil {
			o.CacheCleanInterval = r.Cache.CleanInterval.D()
		}
		if r.HTTPProxy != nil {
			o.HTTPProxy = strings.TrimSpace(*r.HTTPProxy)
		}
		if r.URLRedirect != nil {
			o.URLRedirect = *r.URLRedirect
		}
		// 规则级 insecure-skip-verify 显式覆盖全局（nil = 沿用全局；候选对象的显式值
		// 优先级更高，在 proxy 层经 Options.CandidateInsecureSkipVerify 逐候选解析）
		if r.InsecureSkipVerify != nil {
			o.InsecureSkipVerify = *r.InsecureSkipVerify
		}
		if r.SuccessCheck != nil {
			o.SuccessCheck = *r.SuccessCheck
		}
		if r.FallbackDirect != nil {
			o.FallbackDirect = *r.FallbackDirect
		}
		// 超时/重试覆盖：规则没配（nil）沿用全局值；read-timeout 特殊——规则未配置
		// read-timeout 时回落到规则的 timeout（旧字段退化别名），仍未配置才是全局值
		if r.HTTPTimeout != nil {
			o.HTTPTimeout = r.HTTPTimeout.D()
		}
		switch {
		case r.ReadTimeout != nil:
			o.ReadTimeout = r.ReadTimeout.D()
		case r.Timeout != nil:
			o.ReadTimeout = r.Timeout.D()
		}
		if r.ConnectTimeout != nil {
			o.ConnectTimeout = r.ConnectTimeout.D()
		}
		if r.URLRedirectItemErrorRetry != nil {
			o.URLRedirectItemErrorRetry = *r.URLRedirectItemErrorRetry
		}
		if r.URLRedirectAllErrorRetry != nil {
			o.URLRedirectAllErrorRetry = *r.URLRedirectAllErrorRetry
		}
		if r.URLRedirectProbeTimeout != nil {
			o.URLRedirectProbeTimeout = r.URLRedirectProbeTimeout.D()
		}
		if r.URLRedirectHedgeDelay != nil {
			o.URLRedirectHedgeDelay = r.URLRedirectHedgeDelay.D()
		}
		// 慢速升级多源并发续传覆盖：规则没配（nil）沿用全局值；规则 type 记入 RuleType
		//（归一化生效值），供 proxy 层做「仅 type=docker 生效」的类型门控
		if r.SlowDownloadSpeed != nil {
			o.SlowDownloadSpeed = r.SlowDownloadSpeed.Int64()
		}
		if r.SlowDownloadWindow != nil {
			o.SlowDownloadWindow = r.SlowDownloadWindow.D()
		}
		if r.SlowDownloadMinRemaining != nil {
			o.SlowDownloadMinRemaining = r.SlowDownloadMinRemaining.Int64()
		}
		if r.SlowDownloadMaxBehind != nil {
			o.SlowDownloadMaxBehind = r.SlowDownloadMaxBehind.Int64()
		}
		o.RuleType = r.Type
		o.Rule = r.Label()
		break // 自上而下第一条命中即生效
	}
	// //////////////////  应用第一条命中的域名规则覆盖项  end  ////////////////////////////////////////////////

	// //////////////////  匹配结果日志  start  ////////////////////////////////////////////////
	// 全部规则未命中时按 debug 留痕，最终生效结果（命中规则名或 global-default）按 info 输出；
	// 同时输出参与匹配的入站 Host，便于核对「match 含 Host 段时是否因 Host 不符而未命中」
	if o.Rule == "" {
		slog.Debug("未命中任何域名规则，使用全局默认配置", "host", strings.ToLower(strings.TrimSpace(host)), "target", targetURL)
	}
	rule := o.Rule
	if rule == "" {
		rule = "global-default"
	}
	slog.Info("规则匹配结果", "rule", rule, "host", strings.ToLower(strings.TrimSpace(host)), "target", targetURL)
	// //////////////////  匹配结果日志  end  ////////////////////////////////////////////////
	return o
}

// ActiveCacheDirs 返回当前配置下可能被写入的缓存目录 -> 生效清理间隔，供启动预热使用
// （启动时按此逐目录创建 Cache 实例并启动各自后台清理）。必须在 Validate 之后调用：
//   - 全局 cache.enabled 为 true 时包含全局 cache.path + 全局 clean-interval；
//   - 逐条规则判定其命中后缓存是否开启（规则 cache.enabled 显式值优先，否则沿用全局值），
//     开启则取其生效 path（未配置沿用全局 path）与生效 clean-interval（未配置沿用全局）；
//   - 同一路径首次出现的清理间隔生效（与 cache.Manager 按目录复用实例的语义一致）。
func (c *Config) ActiveCacheDirs() map[string]time.Duration {
	dirs := make(map[string]time.Duration)
	if c.Cache.Enabled {
		dirs[c.Cache.Path] = c.Cache.CleanInterval.D()
	}
	for i := range c.DomainRules {
		r := &c.DomainRules[i]
		// 规则 cache.enabled 显式值优先，否则沿用全局开关
		enabled := c.Cache.Enabled
		if r.Cache.Enabled != nil {
			enabled = *r.Cache.Enabled
		}
		if !enabled {
			continue
		}
		// 生效 path：规则未配置（trim 后为空）沿用全局目录
		path := c.Cache.Path
		if r.Cache.Path != nil {
			if p := strings.TrimSpace(*r.Cache.Path); p != "" {
				path = p
			}
		}
		// 生效 clean-interval：规则未配置沿用全局值
		clean := c.Cache.CleanInterval.D()
		if r.Cache.CleanInterval != nil {
			clean = r.Cache.CleanInterval.D()
		}
		if _, ok := dirs[path]; !ok { // 同一路径首次出现的清理间隔生效
			dirs[path] = clean
		}
	}
	return dirs
}

// CheckACL 校验目标 URL 是否被 allow-list / deny-list 放行（判定逻辑不变：
// deny-list 命中即拒绝；allow-list 非空时为白名单模式，未命中任何 allow 规则同样拒绝）。
// 额外返回命中的列表项（matched）：deny 命中时为命中的 deny 正则原文，
// allow 命中时为命中的 allow 正则原文；未命中白名单或未配置列表时为空串，
// 供调用方记录拒绝日志。
func (c *Config) CheckACL(targetURL string) (matched string, err error) {
	for _, re := range c.deny {
		if re.MatchString(targetURL) {
			return re.String(), fmt.Errorf("目标 URL 命中 deny-list 规则 %q，已拒绝代理", re.String())
		}
	}
	if len(c.allow) > 0 {
		for _, re := range c.allow {
			if re.MatchString(targetURL) {
				return re.String(), nil
			}
		}
		return "", fmt.Errorf("目标 URL 未命中 allow-list 任何规则，已拒绝代理（allow-list 非空时为白名单模式）")
	}
	return "", nil
}

// validateProxyAddr 校验上游代理地址格式（支持 http/https/socks5）。
func validateProxyAddr(addr string) error {
	u, err := url.Parse(strings.TrimSpace(addr))
	if err != nil {
		return fmt.Errorf("无法解析 %q: %w", addr, err)
	}
	switch u.Scheme {
	case "http", "https", "socks5":
		if u.Host != "" {
			return nil
		}
	}
	return fmt.Errorf("%q 需为 http(s)/socks5 代理地址，例如 http://127.0.0.1:7890", addr)
}

// validateRedirectItems 校验 url-redirect 候选列表（就地去除模板首尾空白）：
//   - 每个候选的 template 必须非空、以 http(s):// 开头，且至少包含一个已知占位符——
//     $1 / ${1}，或任一已知 $http.server.header.* 变量的 $ / ${} 形式（错误信息列出全部合法写法）；
//   - 候选级 http-proxy：trim 后非空必须过 validateProxyAddr（nil = 继承上级、
//     显式空串 = 该候选强制直连，两者均合法无需校验）；
//   - 候选级四个超时（connect-timeout / http-timeout / read-timeout / probe-timeout）：
//     负数为配置错误（显式 0 = 不限制，合法）；错误信息带候选序号定位；
//   - 候选的 insecure-skip-verify 为指针 bool 可选项，无需在此校验。
//
// 全局与规则级候选列表共用本函数（where 提供定位前缀，如
// 「domain-rules[0] (r) 的 url-redirect」）。
func validateRedirectItems(list []URLRedirectItem, where string) error {
	for i := range list {
		t := strings.TrimSpace(list[i].Template)
		if t == "" {
			return fmt.Errorf("%s 第 %d 个候选的 template 为空", where, i)
		}
		if !strings.HasPrefix(t, "http://") && !strings.HasPrefix(t, "https://") {
			return fmt.Errorf("%s 第 %d 个候选模板 %q 必须以 http:// 或 https:// 开头", where, i, t)
		}
		if !hasKnownPlaceholder(t) {
			return fmt.Errorf("%s 第 %d 个候选模板 %q 缺少占位符（合法占位符: %s）",
				where, i, t, placeholderHelp())
		}
		// 候选级 http-proxy：trim 后非空才校验地址格式（与全局/规则级 http-proxy 同一套校验）
		if list[i].HTTPProxy != nil && strings.TrimSpace(*list[i].HTTPProxy) != "" {
			if err := validateProxyAddr(*list[i].HTTPProxy); err != nil {
				return fmt.Errorf("%s 第 %d 个候选的 http-proxy 无效: %w", where, i, err)
			}
		}
		// 候选级超时四键：负数为配置错误（复用 validateNonNegativeDuration 的错误文案，
		// 外层补候选序号定位；显式 0 = 不限制，合法）
		for _, f := range []struct {
			name string
			d    *Duration
		}{
			{"connect-timeout", list[i].ConnectTimeout},
			{"http-timeout", list[i].HTTPTimeout},
			{"read-timeout", list[i].ReadTimeout},
			{"probe-timeout", list[i].ProbeTimeout},
		} {
			if err := validateNonNegativeDuration(f.d, f.name); err != nil {
				return fmt.Errorf("%s 第 %d 个候选的 %w", where, i, err)
			}
		}
		list[i].Template = t
	}
	return nil
}

// hasKnownPlaceholder 判断模板是否包含任一已知占位符（$name 或 ${name} 形式）。
func hasKnownPlaceholder(t string) bool {
	for _, name := range RedirectPlaceholders() {
		if strings.Contains(t, "$"+name) || strings.Contains(t, "${"+name+"}") {
			return true
		}
	}
	return false
}

// placeholderHelp 生成全部合法占位符写法（每个变量 $name 与 ${name} 两种形式），用于校验错误信息。
func placeholderHelp() string {
	names := RedirectPlaceholders()
	parts := make([]string, 0, 2*len(names))
	for _, name := range names {
		parts = append(parts, "$"+name, "${"+name+"}")
	}
	return strings.Join(parts, " / ")
}

// compilePatternList 把字符串正则列表编译为 []*regexp.Regexp。
func compilePatternList(list []string, where string) ([]*regexp.Regexp, error) {
	if len(list) == 0 {
		return nil, nil
	}
	out := make([]*regexp.Regexp, 0, len(list))
	for i, s := range list {
		s = strings.TrimSpace(s)
		if s == "" {
			return nil, fmt.Errorf("%s 第 %d 项为空", where, i)
		}
		re, err := regexp.Compile(s)
		if err != nil {
			return nil, fmt.Errorf("%s 第 %d 项正则无效: %w", where, i, err)
		}
		out = append(out, re)
	}
	return out, nil
}
