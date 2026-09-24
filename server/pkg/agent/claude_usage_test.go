package agent

import (
	"bufio"
	"context"
	"encoding/json"
	"fmt"
	"log/slog"
	"os"
	"path/filepath"
	"reflect"
	"strings"
	"testing"
	"time"
)

// Re-exec the test binary, never an installed Claude CLI. The fixture is a
// stream-json response; no model account or network connection is involved.
func runFakeClaudeUsageFixture() {
	if !bufio.NewScanner(os.Stdin).Scan() {
		os.Exit(2)
	}
	data, err := os.ReadFile(os.Getenv("CLAUDE_USAGE_FIXTURE"))
	if err != nil {
		fmt.Fprintln(os.Stderr, err)
		os.Exit(2)
	}
	if _, err := os.Stdout.Write(data); err != nil {
		os.Exit(2)
	}
	if os.Getenv("CLAUDE_USAGE_FIXTURE_FAIL") == "1" {
		os.Exit(1)
	}
}

func TestClaudeExecuteFallbackUsage(t *testing.T) {
	t.Parallel()
	const model = "claude-sonnet-4-6"
	const otherModel = "claude-haiku-4-5"
	// output_tokens on assistant messages is a message_start placeholder, not
	// the completed response's output count. Only terminal usage can supply it.
	usage := &claudeUsage{InputTokens: 100, OutputTokens: 1, CacheReadInputTokens: 200, CacheCreationInputTokens: 10}
	secondUsage := &claudeUsage{InputTokens: 30, OutputTokens: 1, CacheReadInputTokens: 60, CacheCreationInputTokens: 5}
	blockIndex := 0
	assistant := func(id, model, parent string, u *claudeUsage) json.RawMessage {
		blockIndex++
		return mustMarshal(t, map[string]any{
			"type": "assistant", "parent_tool_use_id": parent,
			"message": map[string]any{"id": id, "role": "assistant", "model": model, "usage": u,
				"content": []any{
					map[string]any{"type": "thinking", "thinking": "fixture thinking", "signature": "fixture-signature"},
					map[string]any{"type": "text", "text": "visible text"},
					map[string]any{"type": "tool_use", "id": fmt.Sprintf("tool_%d", blockIndex), "name": "Read", "input": map[string]string{"file_path": "fixture.txt"}},
				}},
		})
	}
	terminal := func(failed bool, usage any, modelUsage any) json.RawMessage {
		subtype := "success"
		if failed {
			subtype = "error_during_execution"
		}
		return mustMarshal(t, map[string]any{"type": "result", "subtype": subtype, "is_error": failed, "result": "fixture result", "model": model, "usage": usage, "modelUsage": modelUsage})
	}
	a := assistant("msg_a", model, "", usage)
	b := assistant("msg_b", model, "", secondUsage)
	a2 := assistant("msg_a", model, "", usage)
	split := []json.RawMessage{a, a2}
	baseWant := map[string]TokenUsage{model: {InputTokens: 100, CacheReadTokens: 200, CacheWriteTokens: 10}}
	finalUsage := map[string]claudeResultModelUsage{
		model:      {InputTokens: 150, OutputTokens: 50, CacheReadInputTokens: 300, CacheCreationInputTokens: 15},
		otherModel: {InputTokens: 20, OutputTokens: 10, CacheReadInputTokens: 40, CacheCreationInputTokens: 2},
	}
	finalWant := map[string]TokenUsage{
		model:      {InputTokens: 150, OutputTokens: 50, CacheReadTokens: 300, CacheWriteTokens: 15},
		otherModel: {InputTokens: 20, OutputTokens: 10, CacheReadTokens: 40, CacheWriteTokens: 2},
	}
	cases := []struct {
		name    string
		events  []json.RawMessage
		result  json.RawMessage
		success bool
		want    map[string]TokenUsage
	}{
		{name: "split_response_without_result", events: split, want: baseWant},
		{name: "split_response_zero_result_usage", events: split, result: terminal(true, &claudeUsage{}, map[string]claudeResultModelUsage{model: {}}), want: baseWant},
		{name: "zero_usage_arrives_before_real_usage", events: []json.RawMessage{assistant("msg_a", model, "", &claudeUsage{OutputTokens: 1}), a, a2}, want: baseWant},
		{name: "cache_only_response", events: []json.RawMessage{assistant("msg_cache", model, "", &claudeUsage{CacheReadInputTokens: 200, CacheCreationInputTokens: 10}), assistant("msg_cache", model, "", &claudeUsage{CacheReadInputTokens: 200, CacheCreationInputTokens: 10})}, want: map[string]TokenUsage{model: {CacheReadTokens: 200, CacheWriteTokens: 10}}},
		{name: "interleaved_response_ids", events: []json.RawMessage{a, b, a2}, want: map[string]TokenUsage{model: {InputTokens: 130, CacheReadTokens: 260, CacheWriteTokens: 15}}},
		{name: "distinct_models", events: []json.RawMessage{a, assistant("msg_b", otherModel, "", secondUsage), a}, want: map[string]TokenUsage{model: baseWant[model], otherModel: {InputTokens: 30, CacheReadTokens: 60, CacheWriteTokens: 5}}},
		{name: "usage_arrives_on_later_block", events: []json.RawMessage{assistant("msg_a", model, "", nil), a, a}, want: baseWant},
		{name: "model_arrives_on_later_block", events: []json.RawMessage{assistant("msg_a", "", "", usage), a, a}, want: baseWant},
		{name: "no_response_ids_remain_best_effort", events: []json.RawMessage{assistant("", model, "", usage), assistant("", model, "", secondUsage)}, want: map[string]TokenUsage{model: {InputTokens: 130, CacheReadTokens: 260, CacheWriteTokens: 15}}},
		{name: "subagent_events_are_not_main_loop_fallback", events: []json.RawMessage{assistant("msg_a", model, "parent_tool", usage), a, a}, want: baseWant},
		{name: "placeholder_output_alone_is_not_usage", events: []json.RawMessage{assistant("msg_a", model, "", &claudeUsage{OutputTokens: 1})}, want: map[string]TokenUsage{}},
		{name: "successful_model_totals_override_fallback", events: split, result: terminal(false, &claudeUsage{InputTokens: 999}, finalUsage), success: true, want: finalWant},
		{name: "failed_model_totals_override_fallback", events: split, result: terminal(true, &claudeUsage{InputTokens: 999}, finalUsage), want: finalWant},
		{name: "terminal_usage_without_model_totals", events: split, result: terminal(true, &claudeUsage{InputTokens: 150, OutputTokens: 50, CacheReadInputTokens: 300, CacheCreationInputTokens: 15}, nil), want: map[string]TokenUsage{model: finalWant[model]}},
	}
	for _, tt := range cases {
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()
			backend := claudeUsageFixtureBackend(t, tt.events, tt.result, !tt.success, "")
			result := executeClaudeUsageFixture(t, backend, len(tt.events), "")
			wantStatus := "failed"
			if tt.success {
				wantStatus = "completed"
			}
			if result.Status != wantStatus {
				t.Fatalf("status = %q, want %q: %s", result.Status, wantStatus, result.Error)
			}
			if !reflect.DeepEqual(result.Usage, tt.want) {
				t.Fatalf("usage = %#v, want %#v", result.Usage, tt.want)
			}
		})
	}
}

func TestClaudeExecuteKeepsResumedUsageTaskLocal(t *testing.T) {
	t.Parallel()
	const model = "claude-sonnet-4-6"
	run1 := TokenUsage{InputTokens: 100, OutputTokens: 20, CacheReadTokens: 30, CacheWriteTokens: 40}
	run2 := TokenUsage{InputTokens: 40, OutputTokens: 6, CacheReadTokens: 8, CacheWriteTokens: 12}
	run3 := TokenUsage{InputTokens: 60, OutputTokens: 10, CacheReadTokens: 15, CacheWriteTokens: 5}

	for _, tc := range []struct {
		name            string
		resumeSessionID string
		current         TokenUsage
		cumulative      TokenUsage
	}{
		{name: "fresh", current: run1, cumulative: run1},
		{name: "resume_2", resumeSessionID: "session-1", current: run2, cumulative: TokenUsage{InputTokens: 140, OutputTokens: 26, CacheReadTokens: 38, CacheWriteTokens: 52}},
		{name: "resume_3", resumeSessionID: "session-1", current: run3, cumulative: TokenUsage{InputTokens: 200, OutputTokens: 36, CacheReadTokens: 53, CacheWriteTokens: 57}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			resultEvent := mustMarshal(t, map[string]any{
				"type": "result", "subtype": "success", "is_error": false, "session_id": "session-1", "model": model,
				"usage": map[string]int64{
					"input_tokens": tc.current.InputTokens, "output_tokens": tc.current.OutputTokens,
					"cache_read_input_tokens": tc.current.CacheReadTokens, "cache_creation_input_tokens": tc.current.CacheWriteTokens,
				},
				"modelUsage": map[string]any{model: map[string]int64{
					"inputTokens": tc.cumulative.InputTokens, "outputTokens": tc.cumulative.OutputTokens,
					"cacheReadInputTokens": tc.cumulative.CacheReadTokens, "cacheCreationInputTokens": tc.cumulative.CacheWriteTokens,
				}},
			})
			backend := claudeUsageFixtureBackend(t, nil, resultEvent, false, "")
			result := executeClaudeUsageFixture(t, backend, 0, tc.resumeSessionID)
			if want := map[string]TokenUsage{model: tc.current}; !reflect.DeepEqual(result.Usage, want) {
				t.Fatalf("usage = %#v, want current invocation usage %#v", result.Usage, want)
			}
		})
	}
}

func TestClaudeExecuteResumedSubagentUsageGap(t *testing.T) {
	t.Parallel()
	const mainModel = "claude-sonnet-4-6"
	const subagentModel = "claude-haiku-4-5"
	mainTurn := TokenUsage{InputTokens: 40, OutputTokens: 6, CacheReadTokens: 8, CacheWriteTokens: 12}
	resultEvent := mustMarshal(t, map[string]any{
		"type": "result", "subtype": "success", "is_error": false, "session_id": "session-1", "model": mainModel,
		"usage": map[string]int64{
			"input_tokens": mainTurn.InputTokens, "output_tokens": mainTurn.OutputTokens,
			"cache_read_input_tokens": mainTurn.CacheReadTokens, "cache_creation_input_tokens": mainTurn.CacheWriteTokens,
		},
		"modelUsage": map[string]any{
			mainModel:     map[string]int64{"inputTokens": 140, "outputTokens": 26, "cacheReadInputTokens": 38, "cacheCreationInputTokens": 52},
			subagentModel: map[string]int64{"inputTokens": 50, "outputTokens": 18, "cacheReadInputTokens": 9, "cacheCreationInputTokens": 4},
		},
	})
	backend := claudeUsageFixtureBackend(t, nil, resultEvent, false, "")
	result := executeClaudeUsageFixture(t, backend, 0, "session-1")
	// Document the current protocol limit: result.usage excludes subagents,
	// while modelUsage carries their session-cumulative totals.
	if want := map[string]TokenUsage{mainModel: mainTurn}; !reflect.DeepEqual(result.Usage, want) {
		t.Fatalf("usage = %#v, want documented main-loop-only usage %#v", result.Usage, want)
	}
}

func TestClaudeExecuteResumedEmptyUsageKeepsAssistantFallback(t *testing.T) {
	t.Parallel()
	const model = "claude-sonnet-4-6"
	event := json.RawMessage(`{"type":"assistant","message":{"id":"msg_current","role":"assistant","model":"claude-sonnet-4-6","usage":{"input_tokens":40,"output_tokens":1,"cache_read_input_tokens":8,"cache_creation_input_tokens":12},"content":[{"type":"text","text":"visible text"},{"type":"tool_use","id":"tool_current","name":"Read","input":{"file_path":"fixture.txt"}}]}}`)
	resultEvent := mustMarshal(t, map[string]any{
		"type": "result", "subtype": "success", "is_error": false, "session_id": "session-1",
		"modelUsage": map[string]any{
			model:              map[string]int64{"inputTokens": 140, "outputTokens": 26, "cacheReadInputTokens": 38, "cacheCreationInputTokens": 52},
			"claude-haiku-4-5": map[string]int64{"inputTokens": 50, "outputTokens": 18, "cacheReadInputTokens": 9, "cacheCreationInputTokens": 4},
		},
	})
	for _, tc := range []struct {
		name   string
		events []json.RawMessage
		want   map[string]TokenUsage
	}{
		{name: "assistant_fallback", events: []json.RawMessage{event}, want: map[string]TokenUsage{model: {InputTokens: 40, CacheReadTokens: 8, CacheWriteTokens: 12}}},
		{name: "no_local_fallback", want: map[string]TokenUsage{}},
	} {
		t.Run(tc.name, func(t *testing.T) {
			t.Parallel()
			backend := claudeUsageFixtureBackend(t, tc.events, resultEvent, false, "")
			result := executeClaudeUsageFixture(t, backend, len(tc.events), "session-1")
			if !reflect.DeepEqual(result.Usage, tc.want) {
				t.Fatalf("usage = %#v, want task-local fallback %#v", result.Usage, tc.want)
			}
		})
	}
}

func TestClaudeExecuteResumedProxyUsesTurnLocalUsage(t *testing.T) {
	t.Parallel()
	const model = "claude-sonnet-4-6"
	previousTurn := TokenUsage{InputTokens: 100, OutputTokens: 20, CacheReadTokens: 30, CacheWriteTokens: 40}
	currentTurn := TokenUsage{InputTokens: 40, OutputTokens: 6, CacheReadTokens: 8, CacheWriteTokens: 12}
	resultEvent := mustMarshal(t, map[string]any{
		"type": "result", "subtype": "success", "is_error": false, "session_id": "session-1", "model": model,
		"usage": map[string]int64{
			"input_tokens": currentTurn.InputTokens, "output_tokens": currentTurn.OutputTokens,
			"cache_read_input_tokens": currentTurn.CacheReadTokens, "cache_creation_input_tokens": currentTurn.CacheWriteTokens,
		},
		"modelUsage": map[string]any{model: map[string]int64{
			"inputTokens":              previousTurn.InputTokens + currentTurn.InputTokens,
			"outputTokens":             previousTurn.OutputTokens + currentTurn.OutputTokens,
			"cacheReadInputTokens":     previousTurn.CacheReadTokens + currentTurn.CacheReadTokens,
			"cacheCreationInputTokens": previousTurn.CacheWriteTokens + currentTurn.CacheWriteTokens,
		}},
	})
	backend := claudeUsageFixtureBackend(t, nil, resultEvent, false, "http://127.0.0.1:4318")
	result := executeClaudeUsageFixture(t, backend, 0, "session-1")
	want := map[string]TokenUsage{model: currentTurn}
	if !reflect.DeepEqual(result.Usage, want) {
		t.Fatalf("proxy resumed usage = %#v, want current invocation usage %#v", result.Usage, want)
	}
}

func TestClaudeFallbackUsageIsScopedToExecution(t *testing.T) {
	t.Parallel()
	event := json.RawMessage(`{"type":"assistant","message":{"id":"msg_reused","role":"assistant","model":"claude-sonnet-4-6","usage":{"input_tokens":100,"output_tokens":1,"cache_read_input_tokens":200,"cache_creation_input_tokens":10},"content":[{"type":"text","text":"visible text"},{"type":"tool_use","id":"tool_reused","name":"Read","input":{"file_path":"fixture.txt"}}]}}`)
	backend := claudeUsageFixtureBackend(t, []json.RawMessage{event, event}, nil, true, "")
	for i := 0; i < 2; i++ {
		t.Run(fmt.Sprintf("execution_%d", i), func(t *testing.T) {
			t.Parallel()
			result := executeClaudeUsageFixture(t, backend, 2, "")
			want := TokenUsage{InputTokens: 100, CacheReadTokens: 200, CacheWriteTokens: 10}
			if got := result.Usage["claude-sonnet-4-6"]; got != want {
				t.Fatalf("usage = %+v, want %+v", got, want)
			}
		})
	}
}

func claudeUsageFixtureBackend(t *testing.T, events []json.RawMessage, result json.RawMessage, fail bool, anthropicBaseURL string) Backend {
	t.Helper()
	var stream strings.Builder
	for _, event := range events {
		stream.Write(event)
		stream.WriteByte('\n')
	}
	if result != nil {
		stream.Write(result)
		stream.WriteByte('\n')
	}
	fixture := filepath.Join(t.TempDir(), "stream.jsonl")
	if err := os.WriteFile(fixture, []byte(stream.String()), 0o600); err != nil {
		t.Fatal(err)
	}
	failEnv := "0"
	if fail {
		failEnv = "1"
	}
	executable, err := os.Executable()
	if err != nil {
		t.Fatal(err)
	}
	env := map[string]string{
		"IS_SANDBOX": "1", "CLAUDE_FAKE_MODE": "usage_fixture", "CLAUDE_USAGE_FIXTURE": fixture, "CLAUDE_USAGE_FIXTURE_FAIL": failEnv,
	}
	if anthropicBaseURL != "" {
		env["ANTHROPIC_BASE_URL"] = anthropicBaseURL
	}
	backend, err := New("claude", Config{ExecutablePath: executable, Logger: slog.Default(), Env: env})
	if err != nil {
		t.Fatal(err)
	}
	return backend
}

func executeClaudeUsageFixture(t *testing.T, backend Backend, eventCount int, resumeSessionID string) Result {
	t.Helper()
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	session, err := backend.Execute(ctx, "fixture", ExecOptions{Timeout: 5 * time.Second, ResumeSessionID: resumeSessionID})
	if err != nil {
		t.Fatal(err)
	}
	texts, tools := 0, 0
	for message := range session.Messages {
		switch message.Type {
		case MessageText:
			texts++
		case MessageToolUse:
			tools++
		}
	}
	result, ok := <-session.Result
	if !ok {
		t.Fatal("missing result")
	}
	// Repeated response IDs must suppress only usage, never text or tool blocks.
	if texts != eventCount || tools != eventCount {
		t.Fatalf("forwarded text/tool events = %d/%d, want %d each", texts, tools, eventCount)
	}
	return result
}
