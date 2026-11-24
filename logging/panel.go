// panel.go —— LOG_TYPE=line / json 的多行「下载面板」渲染器（纯格式化，不含任何 IO 与
// 状态收集；数据由 proxy 包的活跃下载注册表组装成 DownloadBlock / SummaryBlock）。
//
// 设计目标（见 README「日志」章节的面板说明）：
//   - 一次输出多行：每个「正在连接的 docker 下载」一个块，首尾各一行 15 个 '='；
//   - 块内把「客户端已收」与「代理侧已收」分成两行——多源并发/异步预取时代理侧会领先
//     客户端，只看一个 bytes 分不清是客户端慢还是上游慢；
//   - 分块行写明当前每一路下载的远端 host（用户明确要求：要说清「现在下载的远端是哪个」）；
//   - 末尾一个汇总块：活跃数、吞吐、本轮新登记/完成/失败、缓存写入量。
//
// 本文件只做字符串拼接，方便单测直接断言渲染结果（无需启动 Server / 抓 stderr）。
//
// 与 text 模式的关系：text 模式（默认）不经过本文件，逐条 slog 输出与旧版逐字节一致。
package logging

import (
	"encoding/json"
	"fmt"
	"strconv"
	"strings"
	"time"
)

// PanelSeparator 面板块的包裹分隔线（15 个 '='，前后各一行）。
const PanelSeparator = "==============="

// PanelLabelWidth 面板块的标签列显示宽度（中文按 2 列计，含标签后的分隔空格）：
// 「镜像    x」与「客户端  x」的 x 落在同一列。
const PanelLabelWidth = 8

// PartialInfo 半截续传信息（仅 resume 且存在半截前缀时 Present=true）。
type PartialInfo struct {
	// Present 是否有半截前缀。
	Present bool
	// Bytes 半截已保留字节数（本次续传的前缀长度）。
	Bytes int64
	// Start 本次响应从哪个绝对偏移开始回放（通常 0；客户端带 Range 起点时为其起点）。
	Start int64
	// Gap 半截与上游剩余区间之间的缺口字节数（0 = 无缺口）。
	Gap int64
}

// PollEntry 一次候选尝试（轮询段一行；每个面板周期全量输出，不折叠、不截断）。
type PollEntry struct {
	// N 尝试序号（按发生顺序，1 基）。
	N int
	// OK 是否通过成功判据并成为最终返回。
	OK bool
	// Host 候选远端 host（解析失败时为截断后的 URL）。
	Host string
	// Status 上游状态码（无响应时为空，渲染为 —）。
	Status string
	// Cost 本次尝试耗时（人类可读，如 0.31s）。
	Cost string
	// Result 判定（固定枚举，面板状态汇总按它归类）：PollResultSelected / PollResultFailed /
	// PollResultCanceled / PollResultSkipped，见下面四个常量。
	Result string
	// Reason 为什么是这个结果（面向人的一句话）：选中 / 超时 / EOF / 状态码未过
	// success-check / hard_fail=… / 其他候选胜出（带 #N host）/ 扫描总预算到期（带秒数）/
	// 客户端断开或请求结束 / 未放行。面板渲染为 reason=…，JSON 用独立 reason 键。
	// 规范：每个候选都必须带原因，禁止只给结果不给原因。
	Reason string
	// HardFail 硬失败原因（Result=PollResultFailed 且属硬失败时同值，便于 JSON 单独取）。
	HardFail string
}

// 轮询段结果枚举（PollEntry.Result 的取值；面板状态汇总按这四个分类统计）。
const (
	// PollResultSelected 通过成功判据并成为最终返回（面板归类「成功」）。
	PollResultSelected = "选中"
	// PollResultFailed 尝试过但没有通过（含硬失败、自引用候选、未过 success-check）。
	PollResultFailed = "失败"
	// PollResultCanceled 被取消（其他候选胜出 / 扫描总预算到期 / 客户端断开或请求结束）。
	PollResultCanceled = "已取消"
	// PollResultSkipped 未放行（扫描已结束，本候选一次都没轮到）。
	PollResultSkipped = "未放行"
)

// ChunkEntry 一路分块下载（分块段一行）。
type ChunkEntry struct {
	// N 分块序号（1 基）。
	N int
	// Start 本路请求的绝对起始偏移。
	Start int64
	// End 本路请求的绝对结束偏移（含）；HasEnd=false 表示开区间（到对象末尾）。
	End int64
	// HasEnd End 是否有效。
	HasEnd bool
	// Remote 本路远端 host（用户明确要求：分块行要写明现在下载的远端是哪个）。
	Remote string
	// State 状态：下载中 / EOF / 停滞 / 剪枝 / 待定 / 已完成。
	State string
	// Got 本路已收字节数（readPos - start，可能领先客户端游标）。
	Got int64
	// SpeedBPS 本路区间速率（两次面板快照的 readPos 差 / 窗口；首轮为 0）。
	SpeedBPS float64
	// Reason 剪枝原因（State=剪枝 时）。
	Reason string
	// At 剪枝/停滞发生的绝对偏移（Reason 相关，0 = 不适用）。
	At int64
}

// DownloadBlock 一个活跃 docker 下载的面板块数据。
type DownloadBlock struct {
	// ID 下载 ID（注册表自增，渲染为 dl#ID）。
	ID uint64
	// Mode 来源：client / ridealong / resume / prefetch。
	Mode string
	// Image 镜像仓库名（非 docker 下载为空，渲染时省略该行）。
	Image string
	// Digest blob digest 原文（渲染为前 12 + … + 后 4）。
	Digest string
	// Method 请求方法。
	Method string
	// Target 请求目标（已截断）。
	Target string
	// Req 请求关联 id（8 位十六进制，同一客户端请求的日志可据此串起来）。
	Req string
	// Via 本次回源通道（空串 = 直连，渲染为 via=direct）。
	Via string
	// Total 已知总长（0 = 未知）。
	Total int64
	// TotalSrc 总长来源：upstream / manifest / unknown。
	TotalSrc string
	// ClientBytes 已发给客户端的字节数（prefetch 恒 0）。
	ClientBytes int64
	// UpstreamBytes 代理侧已从上游收到的字节数（多源时含全部并发源）。
	UpstreamBytes int64
	// ClientState 客户端状态：ok / disconnected。
	ClientState string
	// Partial 半截续传信息。
	Partial PartialInfo
	// SpeedBPS 客户端区间速率（字节/秒）。
	SpeedBPS float64
	// UpstreamSpeedBPS 上游合计区间速率（字节/秒）。
	UpstreamSpeedBPS float64
	// AvgBPS 自下载开始以来的平均速率（字节/秒）。
	AvgBPS float64
	// Elapsed 已耗时。
	Elapsed time.Duration
	// Polls 全部候选尝试（每个面板周期全量逐行输出；不折叠、不截断）。
	Polls []PollEntry
	// PollTotal 本下载累计候选尝试次数（含「未放行」补记条目）。
	PollTotal int
	// Chunks 分块（逐行，最多 8 行）；单源直连时为空且 SingleSource=true。
	Chunks []ChunkEntry
	// ChunkTotal 源总数。
	ChunkTotal int
	// ChunkAlive 存活源数。
	ChunkAlive int
	// ChunkPruned 已剪枝源数。
	ChunkPruned int
	// ChunksFolded 被折叠省略的源数（>0 时输出「… 其余 N 源」）。
	ChunksFolded int
	// ChunkFoldedIdle 折叠行里「下载中」的源数。
	ChunkFoldedIdle int
	// ChunkFoldedEOF 折叠行里 EOF 的源数。
	ChunkFoldedEOF int
	// SingleSource 单源直连（无 Range）标记。
	SingleSource bool
	// SourceHost 单源直连时的远端 host。
	SourceHost string
	// Cursor 有序拼接游标（已连续写出的绝对字节数）。
	Cursor int64
	// Gap 缺口字节数（0 = 无缺口）。
	Gap int64
	// GapOff 缺口起始绝对偏移。
	GapOff int64
	// Result 收尾结果（空 = 下载进行中）：完成 / 客户端断开 / 全败 502 等。
	Result string
}

// SummaryBlock 面板末尾的总总结块数据。
type SummaryBlock struct {
	// Time 汇总时刻（本地时区）。
	Time time.Time
	// Active 活跃下载数。
	Active int
	// Client / Ridealong / Resume / Prefetch 各来源的活跃数。
	Client, Ridealong, Resume, Prefetch int
	// ClientBPS 客户端类下载合计速率（字节/秒）。
	ClientBPS float64
	// PrefetchBPS 后台预取合计速率（字节/秒）。
	PrefetchBPS float64
	// TotalBPS 全部合计速率（字节/秒）。
	TotalBPS float64
	// New 本轮新登记数。
	New int
	// Done 本轮完成数。
	Done int
	// Failed 本轮失败数。
	Failed int
	// CacheWritten 本轮写入缓存的字节数。
	CacheWritten int64
	// Details 每下载一行明细（`dl#3 resume 16%`），最多 8 条，超出用 +N 收尾。
	Details []string
}

// modeLabels 面板「模式」行的中文注解。
var modeLabels = map[string]string{
	"client":    "客户端回源",
	"ridealong": "搭车在途预取层",
	"resume":    "半截续传",
	"prefetch":  "manifest 驱动异步预取",
}

// RenderDownload 把一个下载块渲染成多行文本（不含结尾换行）。
func RenderDownload(b DownloadBlock) string {
	var sb strings.Builder
	title := fmt.Sprintf("下载 dl#%d · %s", b.ID, b.Mode)
	if b.Result != "" {
		title += " · 收尾"
	}
	sb.WriteString(PanelSeparator + " " + title + " " + PanelSeparator + "\n")
	if b.Image != "" {
		sb.WriteString(panelLine("镜像", b.Image))
	}
	if b.Digest != "" {
		sb.WriteString(panelLine("层", shortDigest(b.Digest)))
	}
	sb.WriteString(panelLine("模式", fmt.Sprintf("%s（%s）   通道 via=%s   总长 %s (%s)",
		b.Mode, modeLabel(b.Mode), viaLabel(b.Via), formatSize(b.Total), totalSrcLabel(b.TotalSrc))))
	if b.Method != "" || b.Target != "" {
		sb.WriteString(panelLine("请求", strings.TrimSpace(b.Method+" "+b.Target)))
	}
	// 客户端进度：预取没有客户端，明确写出来（而不是显示一个 0）。
	if b.Mode == "prefetch" {
		sb.WriteString(panelLine("客户端", "—（后台预取，无客户端）"))
	} else {
		sb.WriteString(panelLine("客户端", progressText(b.ClientBytes, b.Total, b.ClientState)))
	}
	// 代理侧进度：多源并发时可能领先客户端，领先量单独标注。
	proxyText := progressText(b.UpstreamBytes, b.Total, "")
	if lead := b.UpstreamBytes - b.ClientBytes; b.Mode != "prefetch" && lead > 0 {
		proxyText += "  多源已收（领先 " + formatSize(lead) + "）"
	}
	sb.WriteString(panelLine("代理", proxyText))
	if b.Partial.Present {
		sb.WriteString(panelLine("半截", fmt.Sprintf("%s（起点 %s · 缺口 %s）",
			formatSize(b.Partial.Bytes), formatSize(b.Partial.Start), gapLabel(b.Partial.Gap))))
	}
	sb.WriteString(panelLine("速率", rateText(b)))
	if b.Result != "" {
		sb.WriteString(panelLine("结果", b.Result))
	}
	// 轮询段：先一行状态汇总，随后把**全部**候选尝试逐行列出（不折叠、不截断、每条带原因）。
	if len(b.Polls) > 0 {
		sb.WriteString(panelLine("轮询", pollSummaryText(b.Polls)))
		for _, p := range b.Polls {
			sb.WriteString(panelLine("", pollText(p)))
		}
	}
	// 分块段：单源直连一行；多源逐行（带远端 host）。
	if b.SingleSource {
		sb.WriteString(panelLine("分块", "单源直连（无 Range）  ← "+emptyDash(b.SourceHost)))
	} else if len(b.Chunks) > 0 {
		for i, c := range b.Chunks {
			label := "分块"
			if i > 0 {
				label = ""
			}
			sb.WriteString(panelLine(label, chunkText(c)))
		}
		if b.ChunksFolded > 0 {
			sb.WriteString(panelLine("", fmt.Sprintf("… 其余 %d 源（下载中 %d · EOF %d · 剪枝 %d）",
				b.ChunksFolded, b.ChunkFoldedIdle, b.ChunkFoldedEOF, b.ChunkPruned)))
		}
		sb.WriteString(panelLine("拼接", fmt.Sprintf("连续 [0 – %s] · 下一块 %s · 缺口 %s",
			formatSize(b.Cursor), formatSize(b.Cursor), gapLabel(b.Gap))))
	}
	sb.WriteString(PanelSeparator)
	return sb.String()
}

// RenderSummary 把汇总块渲染成多行文本（不含结尾换行）。
func RenderSummary(s SummaryBlock) string {
	var sb strings.Builder
	sb.WriteString(PanelSeparator + " 汇总 " + s.Time.Format("15:04:05") + " " + PanelSeparator + "\n")
	sb.WriteString(panelLine("活跃", fmt.Sprintf("%d 个下载（client %d · 搭车 %d · 续传 %d · 预取 %d）",
		s.Active, s.Client, s.Ridealong, s.Resume, s.Prefetch)))
	var rates []string
	if s.Client > 0 {
		rates = append(rates, "client "+formatBPS(s.ClientBPS))
	}
	if s.Prefetch > 0 {
		rates = append(rates, "prefetch "+formatBPS(s.PrefetchBPS))
	}
	rates = append(rates, "合计 "+formatBPS(s.TotalBPS))
	sb.WriteString(panelLine("吞吐", strings.Join(rates, " · ")))
	if len(s.Details) > 0 {
		shown := s.Details
		extra := 0
		if len(shown) > 8 {
			extra = len(shown) - 8
			shown = shown[:8]
		}
		text := strings.Join(shown, " · ")
		if extra > 0 {
			text += fmt.Sprintf(" · +%d", extra)
		}
		sb.WriteString(panelLine("明细", text))
	}
	sb.WriteString(panelLine("本轮", fmt.Sprintf("新登记 %d · 完成 %d · 失败 %d · 写缓存 +%s",
		s.New, s.Done, s.Failed, formatSize(s.CacheWritten))))
	sb.WriteString(PanelSeparator)
	return sb.String()
}

// jsonDownloadJSON 是 JSON 模式的下载块（字段名全英文短键、数值保持原始字节）。
type jsonDownloadJSON struct {
	Type           string       `json:"type"`
	TS             string       `json:"ts"`
	DL             uint64       `json:"dl"`
	Mode           string       `json:"mode"`
	Image          string       `json:"image,omitempty"`
	Digest         string       `json:"digest,omitempty"`
	Method         string       `json:"method,omitempty"`
	Target         string       `json:"target,omitempty"`
	Req            string       `json:"req,omitempty"`
	Via            string       `json:"via"`
	Total          int64        `json:"total"`
	TotalSrc       string       `json:"total_src"`
	ClientBytes    int64        `json:"client_bytes"`
	UpstreamBytes  int64        `json:"upstream_bytes"`
	Percent        int          `json:"percent"`
	SpeedBPS       int64        `json:"speed_bps"`
	ClientSpeedBPS int64        `json:"client_speed_bps"`
	AvgBPS         int64        `json:"avg_bps"`
	ETASeconds     int64        `json:"eta_s"`
	Elapsed        float64      `json:"elapsed_s"`
	Client         string       `json:"client"`
	Partial        *jsonPartial `json:"partial,omitempty"`
	Polls          []jsonPoll   `json:"polls,omitempty"`
	PollTotal      int          `json:"poll_total"`
	Chunks         []jsonChunk  `json:"chunks,omitempty"`
	Cursor         int64        `json:"cursor"`
	Gap            int64        `json:"gap"`
	Alive          int          `json:"alive"`
	Sources        int          `json:"sources"`
	Result         string       `json:"result,omitempty"`
}

type jsonPartial struct {
	Present bool  `json:"present"`
	Bytes   int64 `json:"bytes"`
	Start   int64 `json:"start"`
	Gap     int64 `json:"gap"`
}

type jsonPoll struct {
	N      int    `json:"n"`
	Cand   string `json:"cand"`
	Status string `json:"status,omitempty"`
	CostMS int64  `json:"cost_ms"`
	Result string `json:"result"`
	// Reason 结果原因（选中 / 超时 / EOF / 状态码未过 success-check / hard_fail=… /
	// 其他候选胜出（#N host）/ 扫描总预算到期（Ns）/ 请求结束 / 未放行）。
	// 只新增字段：既有键名与结构完全不变，老消费者不受影响。
	Reason   string `json:"reason,omitempty"`
	HardFail string `json:"hard_fail,omitempty"`
}

type jsonChunk struct {
	N        int    `json:"n"`
	Start    int64  `json:"start"`
	End      int64  `json:"end,omitempty"`
	Remote   string `json:"remote,omitempty"`
	Got      int64  `json:"got"`
	State    string `json:"state"`
	SpeedBPS int64  `json:"speed_bps"`
	Reason   string `json:"reason,omitempty"`
	At       int64  `json:"at,omitempty"`
}

// JSONDownload 把一个下载块渲染成一行 JSON（JSONL，供 LOG_TYPE=json 使用）。
func JSONDownload(b DownloadBlock) string {
	now := time.Now().Format(time.RFC3339Nano)
	v := jsonDownloadJSON{
		Type: "download", TS: now, DL: b.ID, Mode: b.Mode,
		Image: b.Image, Digest: b.Digest, Method: b.Method, Target: b.Target, Req: b.Req,
		Via: viaLabel(b.Via), Total: b.Total, TotalSrc: totalSrcLabel(b.TotalSrc),
		ClientBytes: b.ClientBytes, UpstreamBytes: b.UpstreamBytes,
		Percent:  percentOf(b.UpstreamBytes, b.Total),
		SpeedBPS: int64(b.UpstreamSpeedBPS), ClientSpeedBPS: int64(b.SpeedBPS),
		AvgBPS: int64(b.AvgBPS), ETASeconds: etaSeconds(b.UpstreamBytes, b.Total, b.AvgBPS),
		Elapsed: b.Elapsed.Seconds(), Client: b.ClientState,
		PollTotal: b.PollTotal, Cursor: b.Cursor, Gap: b.Gap,
		Alive: b.ChunkAlive, Sources: b.ChunkTotal, Result: b.Result,
	}
	if b.Partial.Present {
		v.Partial = &jsonPartial{Present: true, Bytes: b.Partial.Bytes, Start: b.Partial.Start, Gap: b.Partial.Gap}
	}
	for _, p := range b.Polls {
		costMS := parseCostMS(p.Cost)
		v.Polls = append(v.Polls, jsonPoll{N: p.N, Cand: p.Host, Status: p.Status, CostMS: costMS,
			Result: p.Result, Reason: p.Reason, HardFail: p.HardFail})
	}
	for _, c := range b.Chunks {
		v.Chunks = append(v.Chunks, jsonChunk{N: c.N, Start: c.Start, End: c.End, Remote: c.Remote,
			Got: c.Got, State: c.State, SpeedBPS: int64(c.SpeedBPS), Reason: c.Reason, At: c.At})
	}
	buf, err := json.Marshal(v)
	if err != nil { // 防御：结构体只含基础类型，正常不可能失败
		return ""
	}
	return string(buf)
}

// jsonSummaryJSON 是 JSON 模式的汇总块。
type jsonSummaryJSON struct {
	Type    string           `json:"type"`
	TS      string           `json:"ts"`
	Active  int              `json:"active"`
	ByS     map[string]int   `json:"by_source"`
	BPS     map[string]int64 `json:"throughput_bps"`
	New     int              `json:"new"`
	Done    int              `json:"done"`
	Failed  int              `json:"failed"`
	Written int64            `json:"cache_written_bytes"`
}

// JSONSummary 把汇总块渲染成一行 JSON。
func JSONSummary(s SummaryBlock) string {
	v := jsonSummaryJSON{
		Type: "summary", TS: time.Now().Format(time.RFC3339Nano),
		Active: s.Active,
		ByS: map[string]int{"client": s.Client, "ridealong": s.Ridealong,
			"resume": s.Resume, "prefetch": s.Prefetch},
		BPS: map[string]int64{"client": int64(s.ClientBPS), "prefetch": int64(s.PrefetchBPS),
			"total": int64(s.TotalBPS)},
		New: s.New, Done: s.Done, Failed: s.Failed, Written: s.CacheWritten,
	}
	buf, err := json.Marshal(v)
	if err != nil {
		return ""
	}
	return string(buf)
}

// //////////////////  渲染小工具  start  ////////////////////////////////////////////////////

// panelLine 渲染一行「标签 + 内容」：标签按 PanelLabelWidth 显示列对齐（中文按 2 列）。
func panelLine(label, text string) string {
	if label == "" {
		return strings.Repeat(" ", PanelLabelWidth) + text + "\n"
	}
	return padDisplay(label, PanelLabelWidth) + text + "\n"
}

// padDisplay 按显示列宽右补空格（中文/全角按 2 列）。
func padDisplay(s string, width int) string {
	n := displayWidth(s)
	if n >= width {
		return s
	}
	return s + strings.Repeat(" ", width-n)
}

// displayWidth 近似显示列宽：CJK/全角字符按 2 列，其余按 1 列（面板只用于终端对齐，
// 无需精确到 Unicode 宽表；标签集合固定为中文短词）。
func displayWidth(s string) int {
	w := 0
	for _, r := range s {
		switch {
		case r >= 0x2E80: // CJK 部首/汉字/全角标点/假名/谚文等
			w += 2
		default:
			w++
		}
	}
	return w
}

// shortDigest 把 sha256 digest 缩写为「前 12 + … + 后 4」；非 sha256 形式原样返回。
func shortDigest(d string) string {
	hexPart := d
	if i := strings.IndexByte(d, ':'); i >= 0 {
		hexPart = d[i+1:]
	}
	if len(hexPart) <= 20 {
		return d
	}
	return d[:len(d)-len(hexPart)] + hexPart[:12] + "…" + hexPart[len(hexPart)-4:]
}

// formatSize 人类可读字节数（与 text 模式 humanBytes 同口径：1024 进制、最多两位小数）。
func formatSize(n int64) string {
	if n < 1024 {
		return strconv.FormatInt(n, 10) + "B"
	}
	// 面板面向人读：从 KB 起换档（text 模式沿用的 humanBytes 是 1MB 起换档，
	// 74KB/s 在那边会渲染成 75997B/s，面板里不好读）
	units := [...]string{"B", "KB", "MB", "GB", "TB", "PB", "EB"}
	v := float64(n)
	i := 0
	for v >= 1024 && i < len(units)-1 {
		v /= 1024
		i++
	}
	s := strconv.FormatFloat(v, 'f', 2, 64)
	s = strings.TrimRight(strings.TrimRight(s, "0"), ".")
	return s + units[i]
}

// formatBPS 速率（字节/秒）渲染。
func formatBPS(bps float64) string {
	if bps <= 0 {
		return "0B/s"
	}
	return formatSize(int64(bps)) + "/s"
}

// progressText 渲染「进度条 + 百分比 + 已收/总长」；总长未知时退化为只报已收。
func progressText(bytes, total int64, clientState string) string {
	if total <= 0 {
		text := fmt.Sprintf("已传 %s（总长未知）", formatSize(bytes))
		if clientState == "disconnected" {
			text += " · 客户端已断开"
		}
		return text
	}
	pct := percentOf(bytes, total)
	text := fmt.Sprintf("▕%s▏ %3d%%   %s / %s", progressBar(pct), pct, formatSize(bytes), formatSize(total))
	if clientState == "disconnected" {
		text += " · 客户端已断开"
	}
	return text
}

// progressBar 生成 24 格的进度条（█ 已到 / ░ 未到）。
func progressBar(pct int) string {
	const cells = 24
	if pct < 0 {
		pct = 0
	}
	if pct > 100 {
		pct = 100
	}
	filled := (pct*cells + 50) / 100
	return strings.Repeat("█", filled) + strings.Repeat("░", cells-filled)
}

// percentOf 0-100 整数百分比（总长未知/非正返回 0；超界截断）。
func percentOf(bytes, total int64) int {
	if total <= 0 || bytes <= 0 {
		return 0
	}
	pct := int(float64(bytes) / float64(total) * 100)
	if pct < 0 {
		return 0
	}
	if pct > 100 {
		return 100
	}
	return pct
}

// etaSeconds 剩余时间（秒）；总长或平均速率不可得时返回 0。
func etaSeconds(bytes, total int64, avgBPS float64) int64 {
	if total <= 0 || avgBPS <= 0 || bytes >= total {
		return 0
	}
	return int64(float64(total-bytes) / avgBPS)
}

// rateText 渲染速率行：客户端速率 · 上游合计 · 平均 · ETA · 已耗时。
func rateText(b DownloadBlock) string {
	parts := make([]string, 0, 5)
	if b.Mode != "prefetch" {
		parts = append(parts, "客户端 "+formatBPS(b.SpeedBPS))
	}
	parts = append(parts, "上游合计 "+formatBPS(b.UpstreamSpeedBPS))
	if b.AvgBPS > 0 {
		parts = append(parts, "平均 "+formatBPS(b.AvgBPS))
	}
	if eta := etaSeconds(b.UpstreamBytes, b.Total, b.AvgBPS); eta > 0 {
		parts = append(parts, "ETA "+formatDuration(time.Duration(eta)*time.Second))
	}
	parts = append(parts, "已耗时 "+formatDuration(b.Elapsed))
	return strings.Join(parts, " · ")
}

// formatDuration 把耗时渲染为「48m」「9m32s」「1h2m」这类紧凑形式。
func formatDuration(d time.Duration) string {
	if d <= 0 {
		return "0s"
	}
	d = d.Round(time.Second)
	if d < time.Minute {
		return strconv.Itoa(int(d.Seconds())) + "s"
	}
	if d < time.Hour {
		m := int(d / time.Minute)
		s := int((d % time.Minute) / time.Second)
		if s == 0 {
			return strconv.Itoa(m) + "m"
		}
		return fmt.Sprintf("%dm%ds", m, s)
	}
	h := int(d / time.Hour)
	m := int((d % time.Hour) / time.Minute)
	return fmt.Sprintf("%dh%dm", h, m)
}

// pollText 渲染一行轮询结果：`#1 ✔ host 206 0.31s 选中  reason=…`。
func pollText(p PollEntry) string {
	icon := "✘"
	if p.OK {
		icon = "✔"
	}
	status := p.Status
	if status == "" {
		status = "—"
	}
	line := fmt.Sprintf("#%d %s %s %s %s %s", p.N, icon, emptyDash(p.Host), status,
		emptyDash(p.Cost), emptyDash(p.Result))
	// 原因行内跟在判定后面：面板规范要求每个候选都带原因（不许只给「已取消」三个字）
	if reason := strings.TrimSpace(p.Reason); reason != "" {
		line += "  reason=" + reason
	}
	return line
}

// //////////////////  轮询段汇总（分类统计）  start  ////////////////////////////////////////

// 轮询段状态分类名（汇总行按此顺序输出）。
const (
	pollCatOK         = "成功"
	pollCatFail       = "失败"
	pollCatCancel     = "取消"
	pollCatUnreleased = "未放行"
)

// pollSummaryText 轮询段首行汇总：`共 5 · 成功 1 · 失败 3 · 取消 1`（未放行非 0 时追加
// `· 未放行 N`）——先给一眼能看的账，再逐行给明细（用户明确要求：先汇总再逐个列出）。
func pollSummaryText(polls []PollEntry) string {
	counts := map[string]int{}
	for _, p := range polls {
		counts[pollCategory(p)]++
	}
	parts := []string{fmt.Sprintf("共 %d", len(polls))}
	for _, cat := range []string{pollCatOK, pollCatFail, pollCatCancel} {
		parts = append(parts, fmt.Sprintf("%s %d", cat, counts[cat]))
	}
	if n := counts[pollCatUnreleased]; n > 0 {
		parts = append(parts, fmt.Sprintf("%s %d", pollCatUnreleased, n))
	}
	return strings.Join(parts, " · ")
}

// pollCategory 把一个候选尝试归类到汇总用的状态：OK 或「选中」恒为成功；「已取消」「未放行」
// 按字面归类；其余（失败 / 硬失败 / 自引用 / 未过判据）一律算失败。
func pollCategory(p PollEntry) string {
	if p.OK || p.Result == PollResultSelected {
		return pollCatOK
	}
	switch p.Result {
	case PollResultCanceled:
		return pollCatCancel
	case PollResultSkipped:
		return pollCatUnreleased
	}
	return pollCatFail
}

// //////////////////  轮询段汇总（分类统计）  end  //////////////////////////////////////////

// chunkText 渲染一行分块：`#1 [0.0B – 30.9MB]  ← host  ● 74KB/s  已收 30.9MB  EOF`。
func chunkText(c ChunkEntry) string {
	rng := "[" + formatSize(c.Start) + " – "
	if c.HasEnd {
		rng += formatSize(c.End)
	} else {
		rng += "末尾"
	}
	rng += "]"
	speed := "—"
	if c.SpeedBPS > 0 {
		speed = formatBPS(c.SpeedBPS)
	}
	state := c.State
	if c.Reason != "" {
		state += " reason=" + c.Reason
	}
	if c.At > 0 {
		state += " @" + formatSize(c.At)
	}
	return fmt.Sprintf("#%d %s  ← %s  ● %s  已收 %s  %s", c.N, rng, emptyDash(c.Remote), speed,
		formatSize(c.Got), state)
}

// parseCostMS 把「0.31s」这类渲染串解析回毫秒（JSON 用；解析失败返回 0）。
func parseCostMS(s string) int64 {
	s = strings.TrimSuffix(strings.TrimSpace(s), "s")
	if s == "" || s == "—" {
		return 0
	}
	f, err := strconv.ParseFloat(s, 64)
	if err != nil {
		return 0
	}
	return int64(f * 1000)
}

// modeLabel 模式的中文注解。
func modeLabel(mode string) string {
	if s, ok := modeLabels[mode]; ok {
		return s
	}
	return mode
}

// viaLabel 回源通道：空串 = 直连。
func viaLabel(via string) string {
	via = strings.TrimSpace(via)
	if via == "" {
		return "direct"
	}
	return via
}

// totalSrcLabel 总长来源（空值兜底 unknown，与 text 模式枚举一致）。
func totalSrcLabel(src string) string {
	src = strings.TrimSpace(src)
	if src == "" {
		return "unknown"
	}
	return src
}

// gapLabel 缺口渲染（0 = 无）。
func gapLabel(gap int64) string {
	if gap <= 0 {
		return "无"
	}
	return formatSize(gap)
}

// emptyDash 空字符串渲染为破折号（表格列不留空）。
func emptyDash(s string) string {
	if strings.TrimSpace(s) == "" {
		return "—"
	}
	return s
}

// //////////////////  渲染小工具  end  //////////////////////////////////////////////////////
