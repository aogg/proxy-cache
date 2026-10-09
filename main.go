// proxy-cache：GitHub 反向代理 + 本地文件缓存服务（用法与 gh-proxy / hk.gh-proxy.org 一致）。
//
// 请求路径形如 /https://raw.githubusercontent.com/xxx/yyy 时，把 / 后面的整段作为目标 URL，
// 服务端回源（可经 url-redirect 多上游轮询与 http-proxy 代理）并把状态码、响应头、Body
// 原样透传给客户端，同时按配置缓存内容。
package main

import (
	"context"
	"errors"
	"flag"
	"fmt"
	"io/fs"
	"log/slog"
	"net"
	"net/http"
	"os"
	"os/signal"
	"sort"
	"strings"
	"syscall"
	"time"

	"proxy-cache/cache"
	"proxy-cache/config"
	"proxy-cache/proxy"
)

func main() {
	if err := run(); err != nil {
		// 启动失败走 slog Error（此时若 run 已重建默认日志器，则按生效级别输出）
		slog.Default().Error("启动失败", "err", err)
		os.Exit(1)
	}
}

// run 承载全部启动流程：解析参数 -> 加载配置 -> 初始化缓存/服务 -> 监听并优雅退出。
func run() error {
	// //////////////////  解析启动参数  start  ////////////////////////////////////////////////
	var flagConfig string
	flag.StringVar(&flagConfig, "c", "config.yaml", "配置文件路径（也可通过环境变量 PROXY_CACHE_CONFIG 指定）")
	flag.Parse()
	// 环境变量优先于 -c 参数
	path := os.Getenv("PROXY_CACHE_CONFIG")
	if path == "" {
		path = flagConfig
	}
	// //////////////////  解析启动参数  end  ////////////////////////////////////////////////

	// //////////////////  初始化日志  start  ////////////////////////////////////////////////
	logger := slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: slog.LevelInfo}))
	slog.SetDefault(logger)
	// //////////////////  初始化日志  end  ////////////////////////////////////////////////

	// //////////////////  加载配置  start  ////////////////////////////////////////////////
	cfg, err := loadConfig(path, logger)
	if err != nil {
		return err
	}
	// //////////////////  加载配置  end  ////////////////////////////////////////////////

	// //////////////////  按配置重建日志器  start  ////////////////////////////////////////////////
	// loadConfig 成功后立刻按生效的 log.level 重建 slog 默认日志器：
	// 后续 proxy / cache / config 全链路日志均按该级别生效
	logger = slog.New(slog.NewTextHandler(os.Stderr, &slog.HandlerOptions{Level: cfg.LogLevel()}))
	slog.SetDefault(logger)
	// //////////////////  按配置重建日志器  end  ////////////////////////////////////////////////

	// //////////////////  初始化缓存与代理服务  start  ////////////////////////////////////////////////
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	// 缓存管理器：按目录复用 Cache 实例（全局目录 + 各规则独立目录），janitor 随信号 ctx 取消退出
	cacheMgr := cache.NewManager(ctx)
	// 启动预热：一次性创建全部可能被写入的缓存目录并启动各自后台清理，失败直接终止启动
	//（含全局关闭缓存、命中规则才开启的规则独立目录）
	activeDirs := cfg.ActiveCacheDirs()
	preheatDirs := make([]string, 0, len(activeDirs))
	for dir := range activeDirs {
		preheatDirs = append(preheatDirs, dir)
	}
	sort.Strings(preheatDirs) // 排序保证预热与日志输出顺序稳定
	for _, dir := range preheatDirs {
		if _, err := cacheMgr.Acquire(dir, activeDirs[dir]); err != nil {
			return err
		}
	}

	srv, err := proxy.New(cfg, cacheMgr, logger)
	if err != nil {
		return err
	}

	httpSrv := &http.Server{
		Handler:           srv,
		ReadHeaderTimeout: 10 * time.Second,
	}
	ln, err := net.Listen("tcp", cfg.Listen)
	if err != nil {
		return fmt.Errorf("监听 %s 失败: %w", cfg.Listen, err)
	}
	// //////////////////  初始化缓存与代理服务  end  ////////////////////////////////////////////////

	// //////////////////  启动 HTTP 服务  start  ////////////////////////////////////////////////
	errCh := make(chan error, 1)
	go func() { errCh <- httpSrv.Serve(ln) }()

	// 启动日志的 cache_ttl 字段：显式 0（永不过期）输出 never，否则输出 duration 字符串
	//（GlobalCacheTTL 内含 nil 防御；Load 必经 Validate，正常情况非 nil）
	logger.Info("proxy-cache 已启动",
		"version", proxy.Version,
		"listen", ln.Addr().String(),
		"config", path,
		"log_level", cfg.LogLevel().String(),
		"cache_enabled", cfg.Cache.Enabled,
		"cache_dirs", len(activeDirs),
		"cache_ttl", cacheTTLLog(cfg.GlobalCacheTTL()),
		"url_redirect", len(cfg.URLRedirect),
		"domain_rules", len(cfg.DomainRules),
		"http_proxy", cfg.HTTPProxy,
	)
	// 逐目录输出缓存预热结果（source 标明全局目录还是规则独立目录）
	for _, dir := range preheatDirs {
		source := "rule" // 规则独立缓存目录
		if dir == cfg.Cache.Path {
			source = "global"
		}
		logger.Info("缓存目录已就绪", "dir", dir, "clean_interval", activeDirs[dir].String(), "source", source)
	}
	// 逐条输出域名规则摘要：match 正则与规则级 cache / url-redirect 的生效值（未配置项沿用全局），
	// 启动时即可核对「规则是否注册了独立缓存目录与轮询候选」——排查不写缓存先看这里
	for i := range cfg.DomainRules {
		r := &cfg.DomainRules[i]
		// 规则 cache.enabled 显式值优先，否则沿用全局开关（与 ResolveOptions 合并语义一致）
		cacheEnabled := cfg.Cache.Enabled
		if r.Cache.Enabled != nil {
			cacheEnabled = *r.Cache.Enabled
		}
		// 规则 cache.path 未配置（trim 后空）沿用全局目录
		cachePath := cfg.Cache.Path
		if r.Cache.Path != nil {
			if p := strings.TrimSpace(*r.Cache.Path); p != "" {
				cachePath = p
			}
		}
		// 规则 cache.ttl 未配置沿用全局；0 = 永不过期
		cacheTTL := cfg.GlobalCacheTTL()
		if r.Cache.TTL != nil {
			cacheTTL = r.Cache.TTL.D()
		}
		// 规则 url-redirect 未配置沿用全局候选列表
		redirects := cfg.URLRedirect
		if r.URLRedirect != nil {
			redirects = *r.URLRedirect
		}
		logger.Info("域名规则已加载",
			"rule", r.Label(),
			"match", r.Match,
			"cache_enabled", cacheEnabled,
			"cache_path", cachePath,
			"cache_ttl", cacheTTLLog(cacheTTL),
			"url_redirect", len(redirects),
		)
	}
	logger.Info("使用示例: curl -i http://" + displayAddr(ln.Addr()) + "/https://raw.githubusercontent.com/user/repo/main/README.md")
	// //////////////////  启动 HTTP 服务  end  ////////////////////////////////////////////////

	// //////////////////  等待退出信号并优雅关闭  start  ////////////////////////////////////////////////
	select {
	case err := <-errCh:
		if err != nil && !errors.Is(err, http.ErrServerClosed) {
			return fmt.Errorf("HTTP 服务异常退出: %w", err)
		}
	case <-ctx.Done():
		shutdownCtx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
		defer cancel()
		if err := httpSrv.Shutdown(shutdownCtx); err != nil {
			logger.Warn("优雅关闭超时/失败", "err", err)
		}
		logger.Info("proxy-cache 已退出")
	}
	// //////////////////  等待退出信号并优雅关闭  end  ////////////////////////////////////////////////
	return nil
}

// loadConfig 加载配置文件；默认路径的文件不存在时回退内置默认配置（仅告警不阻断，
// 便于 Docker 等环境零配置启动），显式指定的路径不存在同样回退但会给出明显告警。
func loadConfig(path string, logger *slog.Logger) (*config.Config, error) {
	cfg, err := config.Load(path)
	if err != nil {
		if errors.Is(err, fs.ErrNotExist) {
			logger.Warn("配置文件不存在，使用内置默认配置启动", "path", path)
			def := config.Default()
			if verr := def.Validate(); verr != nil {
				return nil, verr
			}
			return def, nil
		}
		return nil, err
	}
	logger.Info("已加载配置文件", "path", path)
	return cfg, nil
}

// displayAddr 把监听地址转换为对用户友好的展示地址（0.0.0.0 -> 127.0.0.1）。
func displayAddr(a net.Addr) string {
	if tcp, ok := a.(*net.TCPAddr); ok && tcp.IP.IsUnspecified() {
		return "127.0.0.1:" + fmt.Sprint(tcp.Port)
	}
	return a.String()
}

// cacheTTLLog 把缓存 TTL 格式化为启动/规则摘要日志展示值：
// <=0（0 = 永不过期）输出 never，否则输出 duration 字符串（如 30m0s / 666h0m0s）。
func cacheTTLLog(d time.Duration) string {
	if d <= 0 {
		return "never"
	}
	return d.String()
}
