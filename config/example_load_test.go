package config

// example_load_test.go —— 保证 config.example.yaml 始终能被 Load + Validate 通过
//（示例文件与实现同步演进的回归保护；验证后随特性一并保留/按需删除）。

import "testing"

func TestExampleConfigLoads(t *testing.T) {
	if _, err := Load("../config.example.yaml"); err != nil {
		t.Fatalf("config.example.yaml 应能通过 Load + Validate，意外报错: %v", err)
	}
}
