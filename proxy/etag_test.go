package proxy

import (
	"net/http"
	"testing"
)

// TestEtagNotModified 表驱动验证缓存 HIT 场景下 If-None-Match 弱比较匹配：
// 强-强、弱-弱、强-弱、弱-强、* 通配、不匹配及方法/缺头边界。
func TestEtagNotModified(t *testing.T) {
	getReq := func(inm string) *http.Request {
		r, _ := http.NewRequest(http.MethodGet, "http://localhost/x", nil)
		if inm != "" {
			r.Header.Set("If-None-Match", inm)
		}
		return r
	}

	tests := []struct {
		name string
		etag string // 存储侧 ETag 响应头
		inm  string // 客户端 If-None-Match 请求头
		want bool
	}{
		{"强-强相同", `"abc"`, `"abc"`, true},
		{"弱-弱相同", `W/"abc"`, `W/"abc"`, true},
		{"存储强-客户端弱", `"abc"`, `W/"abc"`, true},
		{"存储弱-客户端强", `W/"abc"`, `"abc"`, true},
		{"存储弱-客户端列表含弱", `W/"467f9a"`, `"zzz", W/"467f9a"`, true},
		{"通配星号", `"abc"`, `*`, true},
		{"通配星号对弱存储", `W/"abc"`, `*`, true},
		{"内容不同", `"abc"`, `"def"`, false},
		{"弱比较后仍不同", `W/"abc"`, `W/"abd"`, false},
		{"列表全不匹配", `"abc"`, `"def", "ghi"`, false},
		{"存储无 ETag", ``, `"abc"`, false},
		{"请求无 If-None-Match", `"abc"`, ``, false},
	}

	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			h := http.Header{}
			if tt.etag != "" {
				h.Set("ETag", tt.etag)
			}
			if got := etagNotModified(getReq(tt.inm), h); got != tt.want {
				t.Errorf("etagNotModified() = %v, want %v (etag=%q, inm=%q)", got, tt.want, tt.etag, tt.inm)
			}
		})
	}

	t.Run("非GET方法不生效", func(t *testing.T) {
		r, _ := http.NewRequest(http.MethodHead, "http://localhost/x", nil)
		r.Header.Set("If-None-Match", `"abc"`)
		h := http.Header{}
		h.Set("ETag", `"abc"`)
		if got := etagNotModified(r, h); got {
			t.Errorf("etagNotModified() = true, want false (HEAD 方法)")
		}
	})
}
