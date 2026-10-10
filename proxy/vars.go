// vars.go —— url-redirect 模板的请求级变量上下文与占位符展开。
// 占位符变量名统一定义在 config 包（Placeholder* 常量），模板校验与展开共用同一套常量；
// 新增变量时：在 config 包加常量并登记 RedirectPlaceholders，再在 RequestVars 加字段并登记 pairs()。

package proxy

import (
	"net/http"
	"sort"
	"strings"

	"proxy-cache/config"
)

// RequestVars 是一次请求可用的 url-redirect 模板变量集合（可扩展结构，后续新增变量加字段即可）。
// 字段与占位符的对应关系：
//   - Target          -> $1 / ${1}（兼容保留，见 config.PlaceholderTarget）；
//   - FullURL         -> $http.server.header.full_url / ${...}；
//   - FullURLNoServer -> $http.server.header.full_url_no_server / ${...}。
type RequestVars struct {
	// Target 目标串：完整 URL 请求 = 目标 URL 原文；路径目标请求 = 路径目标本身
	// （不带 scheme，如 v2/xxx/manifests/latest）。
	Target string
	// FullURL 本次入站请求的完整 URL：scheme://host/path?query。
	FullURL string
	// FullURLNoServer 入站请求 URL 去掉 scheme://host 的部分：/path?query
	//（以 / 开头、含查询串）。
	FullURLNoServer string
}

// redirectVar 是一个已就绪的占位符（变量名 -> 替换值）。
type redirectVar struct {
	// name 变量名（不含 $ / ${} 包裹形式），取自 config 包的占位符常量。
	name string
	// value 该变量的替换值。
	value string
}

// NewRequestVars 基于入站请求与 ExtractTarget 的结果构建模板变量上下文：
//   - Target 即 target（完整 URL 或路径目标），对应 $1；
//   - FullURLNoServer 用 r.URL.EscapedPath()（RawQuery 非空时拼 "?" + RawQuery）；
//   - FullURL = scheme://host + FullURLNoServer：host 用入站 r.Host 原样（含端口），
//     scheme 优先取请求头 X-Forwarded-Proto 的首个值（小写），否则按 r.TLS 判定 https/http。
func NewRequestVars(r *http.Request, target string) RequestVars {
	noServer := r.URL.EscapedPath()
	if r.URL.RawQuery != "" {
		noServer += "?" + r.URL.RawQuery
	}
	return RequestVars{
		Target:          target,
		FullURL:         inboundScheme(r) + "://" + r.Host + noServer,
		FullURLNoServer: noServer,
	}
}

// inboundScheme 推断入站请求的 scheme：优先取请求头 X-Forwarded-Proto 的首个值
//（同一头可逗号分隔多个值，取第 1 个；归一化小写），未提供时按 r.TLS 判定 https，否则 http。
func inboundScheme(r *http.Request) string {
	if fp := r.Header.Get("X-Forwarded-Proto"); fp != "" {
		if first := strings.ToLower(strings.TrimSpace(strings.SplitN(fp, ",", 2)[0])); first != "" {
			return first
		}
	}
	if r.TLS != nil {
		return "https"
	}
	return "http"
}

// pairs 返回全部占位符（变量名 -> 替换值），按变量名长度降序排序：
// full_url_no_server 必须先于 full_url 展开，避免长变量名被同前缀的短变量名抢先遮蔽
//（如 $http.server.header.full_url 抢先吃掉 $http.server.header.full_url_no_server 的前缀）。
func (v RequestVars) pairs() []redirectVar {
	pairs := []redirectVar{
		{name: config.PlaceholderTarget, value: v.Target},
		{name: config.PlaceholderFullURL, value: v.FullURL},
		{name: config.PlaceholderFullURLNoServer, value: v.FullURLNoServer},
	}
	sort.SliceStable(pairs, func(i, j int) bool { return len(pairs[i].name) > len(pairs[j].name) })
	return pairs
}

// ExpandCandidates 把 url-redirect 模板列表展开为候选 URL 列表：
// 逐个模板替换占位符，每个变量支持 ${name} 与 $name 两种写法，按「变量名长度降序」
// 顺序替换（见 pairs）；$1 兼容保留（完整 URL 请求 = 目标 URL，路径目标请求 = 路径目标）。
// 长变量先替换也顺带规避了递归展开：后替换的 $1 值（目标串）里即使含变量名字面量也不会被二次替换。
// 未知的 $ 序列原样保留为字面量。
// 斜杠接缝去重：full_url_no_server 的值以 / 开头，若模板中占位符前紧邻 /（如
// https://docker.1panel.live/$http.server.header.full_url_no_server），接缝处的两个 /
// 合并为一个，保证展开结果为 https://docker.1panel.live/v2/... 而非 ...//v2/...（见 replaceVar）。
func ExpandCandidates(templates []string, vars RequestVars) []string {
	if len(templates) == 0 {
		return nil
	}
	pairs := vars.pairs()
	out := make([]string, 0, len(templates))
	for _, t := range templates {
		for _, v := range pairs {
			t = replaceVar(t, v.name, v.value)
		}
		out = append(out, t)
	}
	return out
}

// replaceVar 把模板 t 中一个占位符的全部出现替换为 value（先 ${name} 后 $name 写法）：
//   - 斜杠接缝去重：value 以 / 开头且匹配处前一个字符是 / 时，模板里的这个 / 一并吞掉，
//     使「https://mirror/$http.server.header.full_url_no_server」+「/v2/...」展开为
//     https://mirror/v2/...（而非 //v2/...），与镜像站 base 以 / 结尾的常见写法天然拼接；
//   - 仅向前扫描、不重扫已写入的替换值：value 内即使再出现占位符字面量也不会被二次替换
//    （与 strings.ReplaceAll 的非递归语义一致，且避免死循环）。
func replaceVar(t, name, value string) string {
	for _, pat := range []string{"${" + name + "}", "$" + name} {
		var b strings.Builder
		i := 0
		for {
			idx := strings.Index(t[i:], pat)
			if idx < 0 {
				break
			}
			at := i + idx
			start := at
			if strings.HasPrefix(value, "/") && at > 0 && t[at-1] == '/' {
				start = at - 1 // 接缝斜杠去重：吞掉模板中紧邻占位符的 /
			}
			b.WriteString(t[i:start])
			b.WriteString(value)
			i = at + len(pat)
		}
		b.WriteString(t[i:])
		t = b.String()
	}
	return t
}
