// bilingual_test.go —— 日志键名中英双写的单元测试与全仓库键名登记校验：
//
//   - 输出形态：TextHandler + ReplaceAttr 后，普通属性、WithAttrs 预置属性、组内属性、
//     slog 内置的 time/level/msg 全部渲染成「英文(中文)」；
//   - 登记完整性：用 go/parser 扫描仓库内全部非测试源码的 slog 调用点，任何一个键名
//     没在 logKeyZh 登记都直接失败（防止新增日志键悄悄退回纯英文）。
package logging

import (
	"bytes"
	"fmt"
	"go/ast"
	"go/parser"
	"go/token"
	"io/fs"
	"log/slog"
	"path/filepath"
	"sort"
	"strconv"
	"strings"
	"testing"
)

// newTestLogger 构造一个走 ReplaceAttr 的文本 logger（输出落到返回的缓冲区）。
func newTestLogger() (*slog.Logger, *bytes.Buffer) {
	var buf bytes.Buffer
	h := slog.NewTextHandler(&buf, &slog.HandlerOptions{Level: slog.LevelDebug, ReplaceAttr: ReplaceAttr})
	return slog.New(h), &buf
}

// TestReplaceAttrRendersBuiltinAndAttrs 校验普通属性与 slog 内置字段都带中文注解。
func TestReplaceAttrRendersBuiltinAndAttrs(t *testing.T) {
	log, buf := newTestLogger()
	log.Info("活跃下载进度汇总", "active", 3, "started", 5, "source", "client", "bytes", 1024)

	line := buf.String()
	want := []string{
		"time(时间)=",
		"level(级别)=INFO",
		"msg(消息)=活跃下载进度汇总",
		"active(活跃下载数)=3",
		"started(新登记数)=5",
		"source(来源)=client",
		"bytes(字节数)=1024",
	}
	for _, w := range want {
		if !strings.Contains(line, w) {
			t.Fatalf("日志缺少双写键 %q，实际输出：%s", w, line)
		}
	}
}

// TestReplaceAttrWithAttrsAndGroup 校验 WithAttrs 预置属性与组内嵌套属性同样被改写。
func TestReplaceAttrWithAttrsAndGroup(t *testing.T) {
	log, buf := newTestLogger()
	log.With("image", "adockero/proxy-cache").
		Info("搭车在途预取层", slog.Group("blob", "digest", "sha256:abc", "prefix_bytes", 2048))

	line := buf.String()
	want := []string{
		"image(镜像)=adockero/proxy-cache",
		"blob.digest(摘要)=sha256:abc",
		"blob.prefix_bytes(前缀字节)=2048",
	}
	for _, w := range want {
		if !strings.Contains(line, w) {
			t.Fatalf("日志缺少双写键 %q，实际输出：%s", w, line)
		}
	}
}

// TestLocalizeKeyIdempotent 校验已带中文注解的键不会被重复括注，未登记的键原样返回。
func TestLocalizeKeyIdempotent(t *testing.T) {
	cases := map[string]string{
		"bytes":          "bytes(字节数)",
		"bytes(字节数)":     "bytes(字节数)",
		"not_registered": "not_registered",
		"":               "",
	}
	for in, want := range cases {
		if got := localizeKey(in); got != want {
			t.Fatalf("localizeKey(%q) = %q，期望 %q", in, got, want)
		}
	}
}

// TestLogKeyCoverage 扫描仓库内全部非测试源码的 slog 调用点，校验每个日志键名都已在
// logKeyZh 登记（漏登记 = 又出现纯英文键，日志规范不允许）。
func TestLogKeyCoverage(t *testing.T) {
	missing := map[string][]string{}
	for key, pos := range scanLogKeys(t) {
		if _, ok := logKeyZh[key]; !ok {
			missing[key] = append(missing[key], pos...)
		}
	}
	if len(missing) == 0 {
		return
	}
	names := make([]string, 0, len(missing))
	for k := range missing {
		names = append(names, k)
	}
	sort.Strings(names)
	var sb strings.Builder
	sb.WriteString("以下日志键名未在 logging/keys.go 的 logKeyZh 登记（日志规范要求键名英文+中文双写）：\n")
	for _, k := range names {
		sort.Strings(missing[k])
		fmt.Fprintf(&sb, "  %-16s %s\n", k, strings.Join(missing[k], ", "))
	}
	t.Fatal(sb.String())
}

// scanLogKeys 用 go/parser 扫描仓库源码，返回「日志键名 -> 出现位置」。
//
// 覆盖的调用形态（与仓库现有日志 helper 一致）：
//   - logger.Debug/Info/Warn/Error(msg, k1, v1, ...)：键从第 2 个参数起两两成对；
//   - logger.Log/LogAttrs(ctx, level, msg, k1, v1, ...)：键从第 4 个参数起；
//   - dockerLogAttrs(source, image, k1, v1, ...) 与 xxx.attrs(source, k1, v1, ...)：同上；
//   - append(args/attrs/extra 等, k1, v1, ...)：拼接日志参数切片（键从第 2 个参数起）；
//   - logger.With(k1, v1, ...)：预置属性（仓库暂未使用，一并覆盖）。
func scanLogKeys(t *testing.T) map[string][]string {
	t.Helper()
	root, err := filepath.Abs("..")
	if err != nil {
		t.Fatalf("解析仓库根目录失败：%v", err)
	}
	fset := token.NewFileSet()
	keys := map[string][]string{}
	walkErr := filepath.WalkDir(root, func(path string, d fs.DirEntry, err error) error {
		if err != nil {
			return nil
		}
		if d.IsDir() {
			name := d.Name()
			if path != root && (strings.HasPrefix(name, ".") || name == "node_modules" || name == "vendor") {
				return fs.SkipDir
			}
			return nil
		}
		if !strings.HasSuffix(path, ".go") || strings.HasSuffix(path, "_test.go") {
			return nil
		}
		f, perr := parser.ParseFile(fset, path, nil, 0)
		if perr != nil {
			t.Fatalf("解析 %s 失败：%v", path, perr)
		}
		rel, rerr := filepath.Rel(root, path)
		if rerr != nil {
			rel = path
		}
		ast.Inspect(f, func(n ast.Node) bool {
			call, ok := n.(*ast.CallExpr)
			if !ok {
				return true
			}
			start := keyStartIndex(calleeName(call.Fun), call)
			if start < 0 {
				return true
			}
			for i := start; i < len(call.Args); i += 2 {
				lit, ok := call.Args[i].(*ast.BasicLit)
				if !ok || lit.Kind != token.STRING {
					continue
				}
				key, uerr := strconv.Unquote(lit.Value)
				if uerr != nil || key == "" {
					continue
				}
				keys[key] = append(keys[key], fmt.Sprintf("%s:%d", rel, fset.Position(lit.Pos()).Line))
			}
			return true
		})
		return nil
	})
	if walkErr != nil {
		t.Fatalf("遍历仓库源码失败：%v", walkErr)
	}
	return keys
}

// calleeName 取调用表达式的函数名（方法调用取方法名，普通调用取标识符）。
func calleeName(fun ast.Expr) string {
	switch f := fun.(type) {
	case *ast.SelectorExpr:
		return f.Sel.Name
	case *ast.Ident:
		return f.Name
	}
	return ""
}

// keyStartIndex 返回该调用里第一个「键」参数的下标；不是日志调用返回 -1。
func keyStartIndex(name string, call *ast.CallExpr) int {
	switch name {
	case "Debug", "Info", "Warn", "Error":
		return 1
	case "Log", "LogAttrs":
		return 3 // (ctx, level, msg, k1, v1, ...)
	case "dockerLogAttrs":
		return 2 // (source, image, k1, v1, ...)
	case "attrs":
		return 1 // (source, k1, v1, ...)
	case "With":
		return 0
	case "append":
		// 只认日志参数切片的拼接（args/attrs/extra/doneArgs 等），避免误判普通 append
		if len(call.Args) > 0 {
			if id, ok := call.Args[0].(*ast.Ident); ok {
				l := strings.ToLower(id.Name)
				if strings.Contains(l, "arg") || strings.Contains(l, "attr") || l == "extra" {
					return 1
				}
			}
		}
	}
	return -1
}
