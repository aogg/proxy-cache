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
	"syscall"
	"time"

	"proxy-cache/cache"
	"proxy-cache/config"
	"proxy-cache/proxy"
)

func main() {
	if err := run(); err != nil {
		fmt.Fprintln(os.Stderr, "启动失败:", err)
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

	// //////////////////  初始化缓存与代理服务  start  ////////////////////////////////////////////////
	ctx, stop := signal.NotifyContext(context.Background(), os.Interrupt, syscall.SIGTERM)
	defer stop()

	diskCache, err := cache.New(cfg.Cache.Path)
	if err != nil {
		return err
	}
	diskCache.StartJanitor(ctx, cfg.Cache.CleanInterval.D())

	srv, err := proxy.New(cfg, diskCache, logger)
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

	logger.Info("proxy-cache 已启动",
		"version", proxy.Version,
		"listen", ln.Addr().String(),
		"config", path,
		"cache_enabled", cfg.Cache.Enabled,
		"cache_path", cfg.Cache.Path,
		"cache_ttl", cfg.Cache.TTL.String(),
		"url_redirect", len(cfg.URLRedirect),
		"domain_rules", len(cfg.DomainRules),
		"http_proxy", cfg.HTTPProxy,
	)
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
