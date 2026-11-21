package config

import (
	"testing"
	"time"
)

// TestLogTypeEnvOverride LOG_TYPE 环境变量优先于配置文件的 log.type，且非法值启动失败。
func TestLogTypeEnvOverride(t *testing.T) {
	t.Setenv("LOG_TYPE", " LINE ")
	cfg := Default()
	cfg.Log.Type = "text" // 配置文件写了 text，环境变量应覆盖为 line
	cfg.ApplyEnvOverrides()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if got := cfg.LogType(); got != LogTypeLine {
		t.Fatalf("LogType() = %q, want line（环境变量优先）", got)
	}
	if !cfg.LogPanelEnabled() {
		t.Fatalf("line 模式下面板应启用")
	}
}

// TestLogTypeDefaults 未配置 = text（零回归），未配置面板间隔 = 30s。
func TestLogTypeDefaults(t *testing.T) {
	cfg := Default()
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if got := cfg.LogType(); got != LogTypeText {
		t.Fatalf("默认 LogType() = %q, want text", got)
	}
	if cfg.LogPanelEnabled() {
		t.Fatalf("text 模式下面板不应启用")
	}
	if got := cfg.LogPanelInterval(); got != DefaultLogPanelInterval {
		t.Fatalf("默认面板间隔 = %s, want %s", got, DefaultLogPanelInterval)
	}
}

// TestLogTypeInvalid 非法 log.type 启动快速失败；负数面板间隔归一化为 0（关闭面板）。
func TestLogTypeInvalid(t *testing.T) {
	cfg := Default()
	cfg.Log.Type = "xml"
	if err := cfg.Validate(); err == nil {
		t.Fatalf("非法 log.type 应校验失败")
	}

	cfg2 := Default()
	cfg2.Log.Type = LogTypeJSON
	neg := Duration(-time.Minute)
	cfg2.Log.PanelInterval = &neg
	if err := cfg2.Validate(); err != nil {
		t.Fatalf("Validate: %v", err)
	}
	if cfg2.LogPanelInterval() != 0 {
		t.Fatalf("负数 panel-interval 应归一化为 0，实际 %s", cfg2.LogPanelInterval())
	}
	if cfg2.LogPanelEnabled() {
		t.Fatalf("panel-interval=0 时面板应关闭")
	}
}
