// keys.go —— 日志键名的中文含义表（日志规范：字段名称必须「英文 + 中文意义」一起写）：
//
// 输出侧由本文件的 ReplaceAttr 统一改写，调用点仍然只写英文键（source/bytes/...），
// 因此业务代码无需改动、键序与语义完全不变；这里只维护「英文键 -> 中文含义」对照表。
//
// 新增日志键时必须在此登记（logging/bilingual_test.go 会扫描全仓库的 slog 调用点校验，
// 漏登记直接测试失败）——否则该键会以纯英文输出，不符合日志规范。
package logging

import (
	"log/slog"
	"strings"
)

// logKeyZh 日志键名 -> 中文含义。
//
// 取值约定：
//   - 只写简短名词（如「字节数」「缓存键」），不写句子，避免日志行过长；
//   - 同名键在不同链路上含义必须一致才可复用（如 bytes 恒为「字节数」，不区分收/发）。
var logKeyZh = map[string]string{
	// slog 内置字段（TextHandler 固定输出的 time/level/msg，同样按规范双写）
	"time":   "时间",
	"level":  "级别",
	"msg":    "消息",
	"source": "来源",

	// 请求与目标
	"method":          "请求方法",
	"target":          "目标",
	"target_kind":     "目标类型",
	"url":             "地址",
	"host":            "主机",
	"path":            "路径",
	"file":            "文件",
	"dir":             "目录",
	"key":             "缓存键",
	"reason":          "原因",
	"result":          "结果",
	"type":            "类型",
	"version":         "版本",
	"status":          "状态码",
	"upstream":        "上游",
	"remote":          "远端地址",
	"from":            "原路径",
	"to":              "新路径",
	"proxy":           "代理",
	"http_proxy":      "代理地址",
	"listen":          "监听地址",
	"config":          "配置文件",
	"log_level":       "日志级别",
	"insecure":        "跳过校验",
	"connect_timeout": "连接超时",
	"probe_timeout":   "探测超时",
	"wait":            "等待上限",
	"budget":          "预算",

	// 规则与候选
	"rule":          "规则",
	"rule_type":     "规则类型",
	"match":         "匹配表达式",
	"matched":       "是否命中",
	"excluded":      "是否被排除",
	"domain_rules":  "域名规则数",
	"url_redirect":  "重定向模板数",
	"candidate":     "候选源",
	"candidates":    "候选数",
	"template":      "模板",
	"index":         "模板下标",
	"start_index":   "起始下标",
	"round":         "当前轮",
	"round_total":   "总轮数",
	"attempt":       "当前尝试",
	"attempt_total": "尝试总数",
	"slot":          "并发槽位",
	"slot_total":    "槽位总数",
	"hedge_delay":   "竞速延迟",
	"cost":          "耗时",
	"last_error":    "最近错误",
	"hard_fail":     "硬失败原因",
	"check":         "成功判据",

	// 缓存
	"cache":          "缓存状态",
	"cache_enabled":  "缓存开关",
	"cache_path":     "缓存路径",
	"cache_ttl":      "缓存有效期",
	"cache_dirs":     "缓存目录数",
	"clean_interval": "清理间隔",
	"ttl":            "有效期",
	"removed":        "清理数量",
	"expires_in":     "剩余有效期",

	// docker 链路标签
	"image":            "镜像",
	"digest":           "摘要",
	"actual_digest":    "实际摘要",
	"ref":              "引用",
	"blobs":            "层数",
	"token":            "令牌",
	"realm":            "认证域",
	"service":          "服务名",
	"scope":            "权限范围",
	"account":          "账号",
	"www_authenticate": "认证头",

	// 传输与进度
	"bytes":          "字节数",
	"body_bytes":     "响应体字节",
	"content_length": "声明长度",
	"content_type":   "内容类型",
	"total":          "总长",
	"known_total":    "已知总长",
	"remaining":      "剩余字节",
	"min_remaining":  "剩余阈值",
	"percent":        "进度百分比",
	"elapsed":        "已耗时",
	"speed":          "速率",
	"window":         "统计窗口",
	"window_speed":   "窗口速率",
	"sources":        "源数量",
	"live_sources":   "存活源数",
	"success":        "成功源数",
	"merged":         "合并条数",
	"agree":          "共识源数",
	"disagree":       "分歧源数",
	"next":           "下一源下标",
	"idle":           "停滞超时",
	"done":           "已完成",
	"count":          "数量",
	"concurrency":    "并发数",
	"offset":         "偏移",

	// blob 半截与续传
	"part_bytes":    "半截字节",
	"isolated_gap":  "隔离时长",
	"grace":         "宽限时长",
	"subscribers":   "搭车订阅数",
	"prefix_bytes":  "前缀字节",
	"gap_bytes":     "缺口字节",
	"kept":          "保留字节",
	"dropped":       "丢弃字节",
	"invalid":       "非法字节",
	"partial":       "半截保留",
	"sent":          "已发送字节",
	"range":         "区间",
	"range_start":   "区间起点",
	"range_end":     "区间终点",
	"range_has_end": "含终点",
	"range_last":    "末段起点",

	// 活跃下载汇总
	"active":    "活跃下载数",
	"started":   "新登记数",
	"client":    "客户端",
	"ridealong": "搭车数",
	"resume":    "续传数",
	"prefetch":  "预取数",
	"interval":  "汇总间隔",

	// 错误
	"err":   "错误",
	"error": "错误信息",
	"panic": "异常",
}

// ReplaceAttr 给 TextHandler 用的键名改写钩子：把英文键名渲染成「英文(中文)」。
//
// 作用范围：每条记录的普通属性、WithAttrs 预置属性、组内嵌套属性，以及 slog 内置的
// time/level/msg 三个固定字段（TextHandler 同样会回调本函数），因此一条日志里不会再
// 出现没有中文注解的键。已经是「英文(中文)」形态的键原样保留（幂等，避免重复括注）。
func ReplaceAttr(_ []string, a slog.Attr) slog.Attr {
	a.Key = localizeKey(a.Key)
	return a
}

// localizeKey 返回「英文(中文)」形态的键名：表里没有的键原样返回（由单元测试拦截），
// 已含括号的键视为已注解、不重复改写。
func localizeKey(key string) string {
	if key == "" || strings.ContainsRune(key, '(') {
		return key
	}
	zh, ok := logKeyZh[key]
	if !ok {
		return key
	}
	return key + "(" + zh + ")"
}
