package config

// config_docker_prefetch_test.go —— docker 层预取全局两键（docker-prefetch-enabled /
// docker-prefetch-concurrency，提交 6f61108）的配置层单测：
//   - 默认值：Default() 与零值 Config 的 nil 防御访问器（默认开 / 缺省并发
//     DefaultDockerPrefetchConcurrency=4）；
//   - 显式配置生效（关 / 并发 2）且 Validate 通过；
//   - Validate 拒绝并发数 <=0（报错点名 docker-prefetch-concurrency 并提示用
//     docker-prefetch-enabled 关闭预取）；
//   - config.example.yaml 能加载且两键生效值与默认一致（示例以注释形式保留默认）。

import (
	"strings"
	"testing"
)

// intPtr 生成 int 指针，便于写 docker-prefetch-concurrency 显式值。
func intPtr(n int) *int { return &n }

// TestDockerPrefetchDefaults 验证默认值与 nil 防御：
// Default() 未显式配置两键时访问器返回「开 / 4」；零值 Config（未经 Validate 的
// 构造场景）经 nil 防御返回同样的默认。
func TestDockerPrefetchDefaults(t *testing.T) {
	cfg := Default()
	if !cfg.GlobalDockerPrefetchEnabled() {
		t.Error("Default().GlobalDockerPrefetchEnabled() = false, want true（默认开）")
	}
	if got := cfg.GlobalDockerPrefetchConcurrency(); got != DefaultDockerPrefetchConcurrency {
		t.Errorf("Default().GlobalDockerPrefetchConcurrency() = %d, want %d", got, DefaultDockerPrefetchConcurrency)
	}
	if DefaultDockerPrefetchConcurrency != 4 {
		t.Errorf("DefaultDockerPrefetchConcurrency = %d, want 4", DefaultDockerPrefetchConcurrency)
	}

	var zero Config
	if !zero.GlobalDockerPrefetchEnabled() {
		t.Error("零值 Config 的 nil 防御应返回开启")
	}
	if got := zero.GlobalDockerPrefetchConcurrency(); got != DefaultDockerPrefetchConcurrency {
		t.Errorf("零值 Config 的 nil 防御并发 = %d, want %d", got, DefaultDockerPrefetchConcurrency)
	}
}

// TestDockerPrefetchExplicitValues 验证显式配置生效：
// docker-prefetch-enabled: false 关闭开关、docker-prefetch-concurrency: 2 覆盖缺省并发，
// 且该组合能通过 Validate（显式配置合法）。
func TestDockerPrefetchExplicitValues(t *testing.T) {
	cfg := Default()
	cfg.DockerPrefetchEnabled = boolPtr(false)
	cfg.DockerPrefetchConcurrency = intPtr(2)
	if err := cfg.Validate(); err != nil {
		t.Fatalf("显式配置 Validate() 意外报错: %v", err)
	}
	if cfg.GlobalDockerPrefetchEnabled() {
		t.Error("显式 docker-prefetch-enabled: false 应生效（访问器返回 false）")
	}
	if got := cfg.GlobalDockerPrefetchConcurrency(); got != 2 {
		t.Errorf("显式 docker-prefetch-concurrency 生效值 = %d, want 2", got)
	}
}

// TestDockerPrefetchValidateRejectsNonPositive 表驱动验证 Validate 拒绝 <=0 的并发数：
// 报错应点名 docker-prefetch-concurrency，并提示关闭预取应使用 docker-prefetch-enabled 开关。
func TestDockerPrefetchValidateRejectsNonPositive(t *testing.T) {
	for _, n := range []int{0, -1, -100} {
		cfg := Default()
		cfg.DockerPrefetchConcurrency = intPtr(n)
		err := cfg.Validate()
		if err == nil {
			t.Errorf("docker-prefetch-concurrency=%d 应被 Validate 拒绝，实际通过", n)
			continue
		}
		if !strings.Contains(err.Error(), "docker-prefetch-concurrency") {
			t.Errorf("报错 %q 应点名 docker-prefetch-concurrency", err.Error())
		}
		if !strings.Contains(err.Error(), "docker-prefetch-enabled") {
			t.Errorf("报错 %q 应提示用 docker-prefetch-enabled 关闭预取", err.Error())
		}
	}
}

// TestExampleConfigDockerPrefetchDefaults 保证 config.example.yaml 始终能通过
// Load + Validate（参照 example_load_test.go 既有机制），且预取两键的生效值
// 与全局默认一致（示例文件以注释保留默认 → 字段为 nil → 走访问器默认）。
func TestExampleConfigDockerPrefetchDefaults(t *testing.T) {
	cfg, err := Load("../config.example.yaml")
	if err != nil {
		t.Fatalf("config.example.yaml 应能通过 Load + Validate，意外报错: %v", err)
	}
	if !cfg.GlobalDockerPrefetchEnabled() {
		t.Error("config.example.yaml 的 docker-prefetch-enabled 生效值应与默认一致（开）")
	}
	if got := cfg.GlobalDockerPrefetchConcurrency(); got != DefaultDockerPrefetchConcurrency {
		t.Errorf("config.example.yaml 的 docker-prefetch-concurrency 生效值 = %d, want 默认 %d",
			got, DefaultDockerPrefetchConcurrency)
	}
}
