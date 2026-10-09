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

// DefaultCacheTTL 全局 cache.ttl 未配置时的默认缓存时长。
const DefaultCacheTTL = 30 * time.Minute

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
	// URLRedirect 该规则专用的 url-redirect 模板列表（覆盖全局；设为 [] 表示该规则直连回源）。
	URLRedirect *[]string `yaml:"url-redirect"`
	// SuccessCheck 该规则的成功判据（status / content）。
	SuccessCheck *string `yaml:"success-check"`
	// FallbackDirect 该规则下候选全部失败后是否回退直连。
	FallbackDirect *bool `yaml:"fallback-direct"`
	// Timeout 该规则回源请求的总超时。
	Timeout *Duration `yaml:"timeout"`

	// re 是 Match 编译后的正则；excludeRe 是 Exclude 逐项编译后的正则列表
	//（均在 Validate 阶段填充，不参与 YAML 解析）。
	re        *regexp.Regexp
	excludeRe []*regexp.Regexp
}

// label 返回规则的可读标识（优先 name，否则 match 原文）。
func (r *DomainRule) label() string {
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
	// Timeout 回源请求（含 url-redirect 候选请求）的总超时，默认 60s。
	Timeout Duration `yaml:"timeout"`
	// HTTPProxy 全局上游代理（http/https/socks5 地址，如 http://127.0.0.1:7890），空串表示直连。
	HTTPProxy string `yaml:"http-proxy"`
	// SuccessCheck url-redirect 候选成功判据：status 或 content（默认 content）。
	SuccessCheck string `yaml:"success-check"`
	// FallbackDirect url-redirect 候选全部失败后是否回退直连原始 URL（默认 true）。
	// 指针用于区分「未配置」与「显式配置 false」。
	FallbackDirect *bool `yaml:"fallback-direct"`
	// URLRedirect 全局 url-redirect 模板列表（如 https://gh-proxy.com/$1）。
	URLRedirect []string `yaml:"url-redirect"`
	// Cache 全局缓存配置。
	Cache CacheConfig `yaml:"cache"`
	// Log 日志配置（level）。
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
	// URLRedirect 该请求生效的 url-redirect 模板列表。
	URLRedirect []string
	// SuccessCheck 该请求生效的成功判据（status / content）。
	SuccessCheck string
	// FallbackDirect 候选全部失败后是否回退直连。
	FallbackDirect bool
	// Timeout 该请求回源总超时。
	Timeout time.Duration
}

// Default 返回内置默认配置（配置文件缺失时兜底，也是 Load 时叠加解析的底版：
// YAML 中未出现的字段保持这里的默认值）。
func Default() *Config {
	yes := true
	return &Config{
		Listen:         "0.0.0.0:8080",
		Timeout:        Duration(60 * time.Second),
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
	if c.Timeout <= 0 {
		c.Timeout = Duration(60 * time.Second)
	}
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
	// //////////////////  默认值补全  end  ////////////////////////////////////////////////

	// //////////////////  全局取值校验  start  ////////////////////////////////////////////////
	if c.SuccessCheck != SuccessCheckStatus && c.SuccessCheck != SuccessCheckContent {
		return fmt.Errorf("success-check 仅支持 status 或 content，当前为 %q", c.SuccessCheck)
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
	if err := validateRedirectTemplates(c.URLRedirect, "url-redirect"); err != nil {
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
			return fmt.Errorf("domain-rules[%d] (%s) 的 match 正则无效: %w", i, r.label(), err)
		}
		r.re = re
		// exclude 正则逐个编译缓存（与 match 同样对「http://<Host>/<目标URL>」整串非锚定匹配）
		r.excludeRe = make([]*regexp.Regexp, 0, len(r.Exclude))
		for j, pat := range r.Exclude {
			pat = strings.TrimSpace(pat)
			if pat == "" {
				return fmt.Errorf("domain-rules[%d] (%s) 的 exclude 第 %d 项为空", i, r.label(), j)
			}
			ex, err := regexp.Compile(pat)
			if err != nil {
				return fmt.Errorf("domain-rules[%d] (%s) 的 exclude 第 %d 项正则无效: %w", i, r.label(), j, err)
			}
			r.excludeRe = append(r.excludeRe, ex)
		}
		// 规则级覆盖项合法性：ttl 负数为配置错误；显式 0 = 永不过期（合法）
		if r.Cache.TTL != nil && r.Cache.TTL.D() < 0 {
			return fmt.Errorf("domain-rules[%d] (%s) 的 cache.ttl 不能为负数（0 表示永不过期，不配置则沿用全局 cache.ttl）", i, r.label())
		}
		// cache.path 允许为空（trim 后空 = 未配置，沿用全局 cache.path），无需校验
		if r.Cache.CleanInterval != nil && r.Cache.CleanInterval.D() < 0 {
			return fmt.Errorf("domain-rules[%d] (%s) 的 cache.clean-interval 必须大于等于 0", i, r.label())
		}
		if r.HTTPProxy != nil && strings.TrimSpace(*r.HTTPProxy) != "" {
			if err := validateProxyAddr(*r.HTTPProxy); err != nil {
				return fmt.Errorf("domain-rules[%d] (%s) 的 http-proxy 无效: %w", i, r.label(), err)
			}
		}
		if r.URLRedirect != nil {
			if err := validateRedirectTemplates(*r.URLRedirect, fmt.Sprintf("domain-rules[%d] (%s) 的 url-redirect", i, r.label())); err != nil {
				return err
			}
		}
		if r.SuccessCheck != nil {
			v := strings.ToLower(strings.TrimSpace(*r.SuccessCheck))
			if v == "" {
				v = c.SuccessCheck // 显式留空 = 沿用全局取值
			}
			if v != SuccessCheckStatus && v != SuccessCheckContent {
				return fmt.Errorf("domain-rules[%d] (%s) 的 success-check 仅支持 status 或 content", i, r.label())
			}
			r.SuccessCheck = &v
		}
		if r.Timeout != nil && r.Timeout.D() <= 0 {
			return fmt.Errorf("domain-rules[%d] (%s) 的 timeout 必须大于 0", i, r.label())
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

// LogLevel 返回 log.level 对应的 slog.Level。必须在 Validate 之后调用
// （归一化结果存于私有字段 logLevel）；未经过 Validate 的构造场景兜底返回 Info。
func (c *Config) LogLevel() slog.Level {
	if c.logLevel == slog.LevelInfo {
		return slog.LevelInfo
	}
	return c.logLevel
}

// ResolveOptions 把全局配置与第一条命中「入站 Host + 目标 URL」的域名规则覆盖项合并，
// 返回该请求最终生效的配置。必须在 Validate 之后调用。
//
// 域名规则匹配串构造为「http://<入站Host>/<目标URL>」整串（Host 归一化为小写并去除
// 首尾空白），规则 match 与 exclude 均对该整串做非锚定匹配：既支持按入站域名路由
// （如 match: 'http://github\.path\..+'），也兼容旧的目标 URL 正则写法。
// 注意：缓存 key（目标 URL 的 sha256）与 allow-list / deny-list 仍只对目标 URL 匹配，
// 不受入站 Host 影响。
func (c *Config) ResolveOptions(host, targetURL string) Options {
	// //////////////////  全局默认值  start  ////////////////////////////////////////////////
	o := Options{
		CacheEnabled:       c.Cache.Enabled,
		CacheTTL:           c.GlobalCacheTTL(),
		CachePath:          c.Cache.Path,
		CacheCleanInterval: c.Cache.CleanInterval.D(),
		HTTPProxy:          strings.TrimSpace(c.HTTPProxy),
		URLRedirect:        c.URLRedirect,
		SuccessCheck:       c.SuccessCheck,
		FallbackDirect:     c.fallbackDirect(),
		Timeout:            c.Timeout.D(),
	}
	// //////////////////  全局默认值  end  ////////////////////////////////////////////////

	// //////////////////  构造规则匹配串（入站 Host + 目标 URL）  start  ////////////////////
	// 形如 http://github.path.proxy-cache.xxx.sslip.io/https://raw.githubusercontent.com/...
	matchStr := "http://" + strings.ToLower(strings.TrimSpace(host)) + "/" + targetURL
	// //////////////////  构造规则匹配串（入站 Host + 目标 URL）  end  ////////////////////

	// //////////////////  应用第一条命中的域名规则覆盖项  start  ////////////////////////////////////////////////
	for i := range c.DomainRules {
		r := &c.DomainRules[i]
		// match 命中且该规则所有 exclude 均不命中才应用；
		// 被 exclude 排除的规则视为不匹配，继续向下尝试后续规则。
		matched, excluded := r.matchResult(matchStr)
		slog.Debug("域名规则匹配", "rule", r.label(), "matched", matched, "excluded", excluded)
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
		if r.SuccessCheck != nil {
			o.SuccessCheck = *r.SuccessCheck
		}
		if r.FallbackDirect != nil {
			o.FallbackDirect = *r.FallbackDirect
		}
		if r.Timeout != nil {
			o.Timeout = r.Timeout.D()
		}
		o.Rule = r.label()
		break // 自上而下第一条命中即生效
	}
	// //////////////////  应用第一条命中的域名规则覆盖项  end  ////////////////////////////////////////////////

	// //////////////////  匹配结果日志  start  ////////////////////////////////////////////////
	// 全部规则未命中时按 debug 留痕，最终生效结果（命中规则名或 global-default）按 info 输出
	if o.Rule == "" {
		slog.Debug("未命中任何域名规则，使用全局默认配置", "target", targetURL)
	}
	rule := o.Rule
	if rule == "" {
		rule = "global-default"
	}
	slog.Info("规则匹配结果", "rule", rule, "target", targetURL)
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

// validateRedirectTemplates 校验 url-redirect 模板列表（就地去除首尾空白）：
// 每个模板必须以 http(s):// 开头且包含 $1 或 ${1} 占位符。
func validateRedirectTemplates(list []string, where string) error {
	for i := range list {
		t := strings.TrimSpace(list[i])
		if t == "" {
			return fmt.Errorf("%s 第 %d 个模板为空", where, i)
		}
		if !strings.HasPrefix(t, "http://") && !strings.HasPrefix(t, "https://") {
			return fmt.Errorf("%s 第 %d 个模板 %q 必须以 http:// 或 https:// 开头", where, i, t)
		}
		if !strings.Contains(t, "$1") && !strings.Contains(t, "${1}") {
			return fmt.Errorf("%s 第 %d 个模板 %q 缺少 $1 占位符", where, i, t)
		}
		list[i] = t
	}
	return nil
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
