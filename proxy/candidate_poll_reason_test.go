package proxy

// candidate_poll_reason_test.go —— 面板「轮询」段的 reason 质量单测（candidates.go）：
//   - 成功 = 「选中」并写明判据；
//   - 已取消必须能分辨原因：其他候选胜出（带 #N 与胜出 host）/ 扫描总预算 Ns 到期 /
//     客户端断开或请求结束（旧版合并成一句「其他候选胜出或扫描预算到期」，面板上只剩
//     「已取消」三个字，用户无法判断到底是谁取消的）；
//   - 从未放行（未轮到）的候选必须补一条「未放行」并写明为什么没轮到，否则面板上的
//     条目数会少于配置的候选数（看起来像候选凭空消失）；
//   - 失败原因要点：超时 / EOF / hard_fail=<reason>。
//
// 全部使用 httptest 假上游（不发外网请求），直接调用未导出方法验证。

import (
	"context"
	"errors"
	"io"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

	"proxy-cache/config"
	"proxy-cache/logging"
)

// pollTestServer 构造一个带 url-redirect 候选与指定 hedge 放行间隔的 Server（无缓存）。
func pollTestServer(t *testing.T, templates []config.URLRedirectItem, hedge time.Duration) *Server {
	t.Helper()
	cfg := config.Default()
	cfg.URLRedirect = templates
	d := config.Duration(hedge)
	cfg.URLRedirectHedgeDelay = &d
	if err := cfg.Validate(); err != nil {
		t.Fatalf("Validate() 意外报错: %v", err)
	}
	s, err := New(cfg, nil, quietLogger())
	if err != nil {
		t.Fatalf("New() 意外报错: %v", err)
	}
	return s
}

// runSelectWithRecorder 在候选列表上跑一次 selectCandidate，并把面板记录器挂到请求 ctx。
func runSelectWithRecorder(t *testing.T, s *Server, target string) ([]logging.PollEntry, *probeResult) {
	t.Helper()
	eff := s.cfg.ResolveOptions("", target, true)
	r := httptest.NewRequest(http.MethodGet, "/"+target, nil)
	rec := newPanelRecorder()
	r = r.WithContext(context.WithValue(r.Context(), panelRecorderKey{}, rec))
	candidates := ExpandCandidates(eff.URLRedirect, NewRequestVars(r, target), eff)
	res, err := s.selectCandidate(r, candidates, eff, target)
	if err != nil {
		t.Fatalf("selectCandidate() 意外报错: %v", err)
	}
	return rec.snapshot(), res
}

// slowUpstream 启动一个「响应头都不给、一直挂着直到请求 ctx 被取消」的假上游
// （模拟慢镜像站：会被其他候选胜出后取消）。
func slowUpstream(t *testing.T) *httptest.Server {
	t.Helper()
	return httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, r *http.Request) {
		<-r.Context().Done()
	}))
}

// TestSelectCandidatePollReasonsWinnerAndCanceled 胜出 + 被取消两条记录都要带原因：
// 胜出者写「第一个通过 success-check=… 的候选」，被取消者写「其他候选胜出（#N host 已命中）」
// ——必须能分辨是「别人赢了」而不是「预算到期」。
func TestSelectCandidatePollReasonsWinnerAndCanceled(t *testing.T) {
	slow := slowUpstream(t)
	defer slow.Close()
	good := contentUpstream(t)
	defer good.Close()

	// 候选顺序 [慢, 快] + 20ms hedge：快候选胜出时慢候选一定还在途 → 必被取消
	s := pollTestServer(t, []config.URLRedirectItem{
		{Template: slow.URL + "/$1"},
		{Template: good.URL + "/$1"},
	}, 20*time.Millisecond)

	polls, res := runSelectWithRecorder(t, s, candidatesTargetURL)
	defer res.Close()

	if len(polls) != 2 {
		t.Fatalf("应记录 2 条候选尝试，实际 %d: %+v", len(polls), polls)
	}
	winner, loser := polls[0], polls[1]
	if !winner.OK || winner.Result != logging.PollResultSelected {
		t.Fatalf("第 1 条应是胜出（选中），实际 %+v", winner)
	}
	if !strings.Contains(winner.Reason, "success-check=") {
		t.Fatalf("胜出原因应写明成功判据，实际 %q", winner.Reason)
	}
	if loser.OK || loser.Result != logging.PollResultCanceled {
		t.Fatalf("第 2 条应是已取消，实际 %+v", loser)
	}
	wantReason := "其他候选胜出（#" + "1 " // 胜出者在面板上的序号是 #1
	if !strings.Contains(loser.Reason, wantReason) {
		t.Fatalf("取消原因应写明胜出者 #N，实际 %q", loser.Reason)
	}
	if !strings.Contains(loser.Reason, hostOf(good.URL+"/"+candidatesTargetURL)) {
		t.Fatalf("取消原因应写明胜出 host，实际 %q", loser.Reason)
	}
	if strings.Contains(loser.Reason, "预算") {
		t.Fatalf("本用例是「其他候选胜出」，不应写成预算到期: %q", loser.Reason)
	}
}

// TestSelectCandidateUnreleasedPolls 从未放行的候选必须补一条「未放行」并写明原因：
// 「候选全量列出」是用户的硬要求，未轮到的候选不能凭空消失（现场 11 个候选只试了 5 个）。
func TestSelectCandidateUnreleasedPolls(t *testing.T) {
	good := contentUpstream(t)
	defer good.Close()

	// 3 个候选 + hedge 1h：只有第 1 个被放行，命中后扫描结束 → 另 2 个必为「未放行」
	s := pollTestServer(t, []config.URLRedirectItem{
		{Template: good.URL + "/$1"},
		{Template: good.URL + "/$1?alt=2"},
		{Template: good.URL + "/$1?alt=3"},
	}, time.Hour)

	polls, res := runSelectWithRecorder(t, s, candidatesTargetURL)
	defer res.Close()

	if len(polls) != 3 {
		t.Fatalf("候选全量应记 3 条（1 选中 + 2 未放行），实际 %d: %+v", len(polls), polls)
	}
	if polls[0].Result != logging.PollResultSelected {
		t.Fatalf("第 1 条应是选中，实际 %+v", polls[0])
	}
	for _, p := range polls[1:] {
		if p.Result != logging.PollResultSkipped {
			t.Fatalf("未轮到的候选应记「未放行」，实际 %+v", p)
		}
		if !strings.Contains(p.Reason, "扫描在轮到本候选前结束") {
			t.Fatalf("未放行原因应说明扫描已结束，实际 %q", p.Reason)
		}
		if !strings.Contains(p.Reason, "其他候选胜出（#1 ") {
			t.Fatalf("未放行原因应写明是被谁抢先，实际 %q", p.Reason)
		}
	}
}

// TestScanCancelCauseText 取消原因文本必须能一眼分辨三种来源（胜出 / 预算到期 / 请求结束）。
func TestScanCancelCauseText(t *testing.T) {
	cause := &scanCancelCause{}
	if got := cause.text(); !strings.Contains(got, "客户端断开或请求结束") {
		t.Fatalf("未知来源应兜底为「客户端断开或请求结束」，实际 %q", got)
	}
	cause.setWinner(5, "docker.1ms.run")
	if got := cause.text(); got != "其他候选胜出（#5 docker.1ms.run 已命中）" {
		t.Fatalf("胜出原因不符: %q", got)
	}
	budget := &scanCancelCause{}
	budget.setBudget(70 * time.Second)
	got := budget.text()
	for _, want := range []string{"候选扫描总预算", "1m10s", "70 秒"} {
		if !strings.Contains(got, want) {
			t.Fatalf("预算到期原因应含 %q，实际 %q", want, got)
		}
	}
}

// TestCandidateFailureReason 失败原因要点：超时 / EOF / hard_fail=<reason>（不能只写「失败」）。
func TestCandidateFailureReason(t *testing.T) {
	cases := []struct {
		name     string
		err      error
		hardFail string
		want     string
	}{
		{"超时", context.DeadlineExceeded, "", "超时"},
		{"探测预算耗尽", errors.New("probe-timeout（候选探测预算 30s 内响应头未就绪）"), "", "超时"},
		{"EOF", io.ErrUnexpectedEOF, "", "EOF"},
		{"硬失败", errors.New("上游 403"), "upstream-403", "hard_fail=upstream-403"},
		{"其它错误原样带上", errors.New("dial tcp: no such host"), "", "no such host"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			if got := candidateFailureReason(tc.err, tc.hardFail); !strings.Contains(got, tc.want) {
				t.Fatalf("candidateFailureReason(%v, %q) = %q, 应含 %q", tc.err, tc.hardFail, got, tc.want)
			}
		})
	}
}

// TestRecordUnreleasedPollsOrder 未放行补记按 slot 顺序映射回候选列表（round-robin 起始
// 下标非 0 时同样对得上），保证面板列出的 host 与「本轮实际尝试顺序」一致。
func TestRecordUnreleasedPollsOrder(t *testing.T) {
	candidates := []candidate{
		{url: "https://a.example/x"}, {url: "https://b.example/x"},
		{url: "https://c.example/x"}, {url: "https://d.example/x"},
	}
	rec := newPanelRecorder()
	ctx := context.WithValue(context.Background(), panelRecorderKey{}, rec)
	cause := &scanCancelCause{}
	cause.setWinner(1, "a.example")

	// start=2 → 放行顺序 c,d,a,b；released=1 表示只放行了 c，未放行为 d,a,b
	recordUnreleasedPolls(ctx, candidates, 2, 1, cause)
	polls := rec.snapshot()
	var hosts []string
	for _, p := range polls {
		if p.Result != logging.PollResultSkipped {
			t.Fatalf("补记条目应为「未放行」: %+v", p)
		}
		hosts = append(hosts, p.Host)
	}
	want := []string{"d.example", "a.example", "b.example"}
	if strings.Join(hosts, ",") != strings.Join(want, ",") {
		t.Fatalf("未放行候选顺序 = %v, 期望 %v", hosts, want)
	}
}

// TestSelectCandidateUnreleasedConcurrentSafe 并发跑多次扫描：记录器与取消原因在并发下
// 不丢条目、不串原因（-race 下同时验证数据竞争）。
func TestSelectCandidateUnreleasedConcurrentSafe(t *testing.T) {
	good := contentUpstream(t)
	defer good.Close()
	s := pollTestServer(t, []config.URLRedirectItem{
		{Template: good.URL + "/$1"},
		{Template: good.URL + "/$1?alt=2"},
	}, time.Hour)

	var wg sync.WaitGroup
	for i := 0; i < 8; i++ {
		wg.Add(1)
		go func() {
			defer wg.Done()
			eff := s.cfg.ResolveOptions("", candidatesTargetURL, true)
			r := httptest.NewRequest(http.MethodGet, "/"+candidatesTargetURL, nil)
			rec := newPanelRecorder()
			r = r.WithContext(context.WithValue(r.Context(), panelRecorderKey{}, rec))
			candidates := ExpandCandidates(eff.URLRedirect, NewRequestVars(r, candidatesTargetURL), eff)
			res, err := s.selectCandidate(r, candidates, eff, candidatesTargetURL)
			if err != nil {
				return
			}
			res.Close()
			if got := len(rec.snapshot()); got != 2 {
				t.Errorf("每次扫描都应记满 2 条候选（1 选中 + 1 未放行），实际 %d", got)
			}
		}()
	}
	wg.Wait()
}
