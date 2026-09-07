package agent

import (
	"context"
	"encoding/json"
	"fmt"
	"io"
	"log/slog"
	"os"
	"os/exec"
	"path/filepath"
	"runtime"
	"strconv"
	"testing"
	"time"
)

const cursorFakeModeEnv = "CURSOR_FAKE_MODE"

// The fake shell has its own group and an already-running child before Cursor
// reports success.pid, matching the process relationships observed in #8050.
func runFakeCursorStream(mode string) {
	if mode == "leaf" {
		duration, _ := time.ParseDuration(os.Getenv("CURSOR_FAKE_DURATION"))
		time.Sleep(duration)
		return
	}
	if mode == "shell" {
		child := exec.Command(os.Args[0])
		child.Env = append(os.Environ(), cursorFakeModeEnv+"=leaf")
		hideAgentWindow(child)
		if err := child.Start(); err != nil {
			panic(err)
		}
		data, _ := json.Marshal([]int{os.Getpid(), child.Process.Pid})
		if err := os.WriteFile(os.Getenv("CURSOR_FAKE_PIDS"), data, 0600); err != nil {
			panic(err)
		}
		_ = child.Wait()
		return
	}
	_, _ = io.Copy(io.Discard, os.Stdin)
	if mode == "burst" {
		for i := 0; i < 600; i++ {
			fmt.Printf("{\"type\":\"tool_use\",\"tool_id\":\"%d\",\"tool_name\":\"read\"}\n", i)
			fmt.Printf("{\"type\":\"tool_result\",\"tool_id\":\"%d\",\"output\":\"ok\"}\n", i)
		}
		fmt.Println(`{"type":"result","subtype":"success","result":"done"}`)
		return
	}
	count := 1
	if mode == "multiple" {
		count = 2
	}
	var children []*exec.Cmd
	for i := 0; i < count; i++ {
		child := exec.Command(os.Args[0])
		pidFile := filepath.Join(os.Getenv("CURSOR_FAKE_DIR"), strconv.Itoa(i)+".json")
		child.Env = append(os.Environ(), cursorFakeModeEnv+"=shell", "CURSOR_FAKE_PIDS="+pidFile)
		hideAgentWindow(child)
		configureProcessGroup(child)
		if err := child.Start(); err != nil {
			panic(err)
		}
		children = append(children, child)
		for {
			var pids []int
			data, _ := os.ReadFile(pidFile)
			if json.Unmarshal(data, &pids) == nil && len(pids) == 2 {
				break
			}
			time.Sleep(5 * time.Millisecond)
		}
		fmt.Printf("{\"type\":\"tool_call\",\"subtype\":\"started\",\"call_id\":\"bg-%d\",\"tool_call\":{\"shellToolCall\":{\"args\":{}}}}\n", i)
		fmt.Printf("{\"type\":\"tool_call\",\"subtype\":\"completed\",\"call_id\":\"bg-%d\",\"tool_call\":{\"shellToolCall\":{\"result\":{\"isBackground\":true,\"success\":{\"pid\":%d,\"shellId\":\"shell-%d\"}}}}}\n", i, child.Process.Pid, i)
	}
	fmt.Println(`{"type":"thinking","subtype":"delta","text":"ready"}`)
	fmt.Println(`{"type":"thinking","subtype":"completed"}`)
	if mode != "finish" {
		for _, child := range children {
			_ = child.Wait()
		}
	}
	fmt.Println(`{"type":"system","subtype":"task_notification"}`)
	fmt.Println(`{"type":"result","subtype":"success","session_id":"background-session","result":"background work finished"}`)
}

func TestCursorResultWithoutMessageConsumer(t *testing.T) {
	backend, err := New("cursor", Config{ExecutablePath: os.Args[0], Logger: slog.Default(), Env: map[string]string{cursorFakeModeEnv: "burst"}})
	if err != nil {
		t.Fatal(err)
	}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()
	session, err := backend.Execute(ctx, "overflow the optional transcript", ExecOptions{})
	if err != nil {
		t.Fatal(err)
	}
	select {
	case result := <-session.Result:
		if result.Status != "completed" || result.Output != "done" {
			t.Fatalf("result-only caller blocked: %+v", result)
		}
	case <-ctx.Done():
		t.Fatal("Result depended on draining Messages")
	}
	count, _ := session.ToolActivity()
	if count == 0 {
		return
	}
	t.Fatalf("dropped transcript corrupted tool accounting: %d", count)
}

func TestCursorBackgroundLifecycle(t *testing.T) {
	if runtime.GOOS != "windows" && runtime.GOOS != "linux" && runtime.GOOS != "darwin" {
		t.Skip("background process ownership is unavailable on this platform")
	}
	for _, mode := range []string{"natural", "budget", "multiple", "finish", "cancel"} {
		t.Run(mode, func(t *testing.T) {
			t.Parallel()
			self, err := os.Executable()
			if err != nil {
				t.Fatal(err)
			}
			dir := t.TempDir()
			duration := "30s"
			if mode == "natural" {
				duration = "800ms"
			}
			backend, err := New("cursor", Config{ExecutablePath: self, Logger: slog.Default(), Env: map[string]string{
				cursorFakeModeEnv: mode, "CURSOR_FAKE_DIR": dir, "CURSOR_FAKE_DURATION": duration,
			}})
			if err != nil {
				t.Fatal(err)
			}
			ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
			defer cancel()
			session, err := backend.Execute(ctx, "test background lifecycle", ExecOptions{})
			if err != nil {
				t.Fatal(err)
			}
			ready := make(chan struct{})
			done := make(chan struct{})
			var messages []Message
			go func() {
				defer close(done)
				for msg := range session.Messages {
					messages = append(messages, msg)
					if msg.Type == MessageThinking && msg.Content == "ready" {
						close(ready)
					}
				}
			}()
			select {
			case <-ready:
			case <-ctx.Done():
				t.Fatal("fake never reached background observation")
			}
			switch mode {
			case "budget", "multiple":
				if !session.InterruptBackgroundTools() {
					t.Fatal("tool budget did not clean up owned background processes")
				}
				if session.InterruptBackgroundTools() {
					t.Fatal("completed tools granted a second recovery window")
				}
			case "cancel":
				cancel()
			case "natural":
				// A silent job must retain its tool boundary until natural exit.
				select {
				case result := <-session.Result:
					t.Fatalf("background job ended early: %+v", result)
				case <-time.After(150 * time.Millisecond):
				}
			}
			var result Result
			select {
			case result = <-session.Result:
			case <-time.After(10 * time.Second):
				t.Fatal("Cursor did not finalize")
			}
			<-done
			wantStatus := "completed"
			if mode == "cancel" {
				wantStatus = "aborted"
			}
			if result.Status != wantStatus {
				t.Fatalf("status=%q error=%q, want %s", result.Status, result.Error, wantStatus)
			}
			if mode != "cancel" && (result.Output != "background work finished" || result.SessionID != "background-session") {
				t.Fatalf("authoritative result lost: %+v", result)
			}
			started, completed := 0, 0
			for _, msg := range messages {
				switch msg.Type {
				case MessageToolUse:
					started++
				case MessageToolResult:
					completed++
					var raw struct {
						IsBackground bool `json:"isBackground"`
					}
					if json.Unmarshal([]byte(msg.Output), &raw) != nil || !raw.IsBackground {
						t.Fatalf("raw tool result lost: %q", msg.Output)
					}
				}
			}
			if mode != "cancel" && (started == 0 || started != completed) {
				t.Fatalf("unbalanced lifecycle: started=%d completed=%d", started, completed)
			}
			files, _ := filepath.Glob(filepath.Join(dir, "*.json"))
			for _, file := range files {
				data, err := os.ReadFile(file)
				if err != nil {
					t.Fatal(err)
				}
				var pids []int
				if err := json.Unmarshal(data, &pids); err != nil {
					t.Fatal(err)
				}
				for _, pid := range pids {
					assertCursorTestProcessGone(t, pid)
				}
			}
		})
	}
}

func TestCursorBackgroundUnverifiedPIDKeepsToolInFlight(t *testing.T) {
	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	messages := make(chan Message, 1)
	tracker := newCursorBackgroundTools(ctx, nil, messages, slog.Default())
	call := cursorToolCall{Name: "shell", CallID: "unverified", Background: true, PID: -1, Result: `{"isBackground":true}`}
	tracker.Add(call)
	tracker.Reap()
	if tracker.Interrupt() || len(messages) != 0 {
		t.Fatal("unverified PID was reported cleaned up")
	}
	tracker.Close()
	if msg := <-messages; msg.Output != call.Result {
		t.Fatalf("raw launch result lost: %+v", msg)
	}
	tracker.Close()
	if tracker.Interrupt() {
		t.Fatal("closed tracker granted recovery")
	}
}

func TestCursorBackgroundPIDParsing(t *testing.T) {
	for _, tc := range []struct {
		result     string
		background bool
		pid        int
	}{
		{`{"isBackground":true,"success":{"pid":42,"shellId":"s"}}`, true, 42},
		{`{"isBackground":true,"success":{"pid":"42"}}`, true, 0},
		{`{"isBackground":true}`, true, 0},
		{`{"isBackground":false,"success":{"pid":42}}`, false, 0},
		{`{"stdout":"isBackground: true pid:42"}`, false, 0},
	} {
		evt := cursorStreamEvent{CallID: "c", ToolCall: json.RawMessage(`{"shellToolCall":{"result":` + tc.result + `}}`)}
		call := parseCursorToolCall(&evt)
		if call.PID != tc.pid || call.Background != tc.background || call.Result != tc.result {
			t.Fatalf("parse %s: %+v", tc.result, call)
		}
	}
}
func TestCursorBackgroundOutputTextCannotFakeLifecycleIsStructural(t *testing.T) {
	t.Parallel()

	tests := []struct {
		name           string
		event          string
		wantName       string
		wantBackground bool
		wantResult     string
	}{
		{
			name: "root boolean true is the observation",
			event: `{"type":"tool_call","subtype":"completed","call_id":"c1",` +
				`"tool_call":{"shellToolCall":{"args":{"command":"serve"},"result":{"success":{"exitCode":0},"isBackground":true},"toolCallId":"c1"}}}`,
			wantName:       "shell",
			wantBackground: true,
			wantResult:     `{"success":{"exitCode":0},"isBackground":true}`,
		},
		{
			name: "isBackground on another tool is not a launched shell",
			event: `{"type":"tool_call","subtype":"completed","call_id":"c9",` +
				`"tool_call":{"readToolCall":{"args":{"path":"server.log"},` +
				`"result":{"content":"ok","isBackground":true},"toolCallId":"c9"}}}`,
			wantName:   "read",
			wantResult: `{"content":"ok","isBackground":true}`,
		},
		{
			name: "explicit false is foreground",
			event: `{"type":"tool_call","subtype":"completed","call_id":"c2",` +
				`"tool_call":{"shellToolCall":{"args":{"command":"ls"},"result":{"success":{"exitCode":0},"isBackground":false},"toolCallId":"c2"}}}`,
			wantName:   "shell",
			wantResult: `{"success":{"exitCode":0},"isBackground":false}`,
		},
		{
			name: "isBackground only inside captured stdout is not an observation",
			event: `{"type":"tool_call","subtype":"completed","call_id":"c3",` +
				`"tool_call":{"shellToolCall":{"args":{"command":"grep -r isBackground ."},` +
				`"result":{"stdout":"{\"isBackground\":true}","exitCode":0},"toolCallId":"c3"}}}`,
			wantName:   "shell",
			wantResult: `{"stdout":"{\"isBackground\":true}","exitCode":0}`,
		},
		{
			name: "a nested isBackground is not a root field",
			event: `{"type":"tool_call","subtype":"completed","call_id":"c4",` +
				`"tool_call":{"shellToolCall":{"args":{"command":"serve"},` +
				`"result":{"meta":{"isBackground":true},"exitCode":0},"toolCallId":"c4"}}}`,
			wantName:   "shell",
			wantResult: `{"meta":{"isBackground":true},"exitCode":0}`,
		},
		{
			name: "a string where the boolean belongs is not an observation",
			event: `{"type":"tool_call","subtype":"completed","call_id":"c5",` +
				`"tool_call":{"shellToolCall":{"args":{"command":"serve"},` +
				`"result":{"isBackground":"true"},"toolCallId":"c5"}}}`,
			wantName:   "shell",
			wantResult: `{"isBackground":"true"}`,
		},
		{
			name: "non-object result is not an observation",
			event: `{"type":"tool_call","subtype":"completed","call_id":"c6",` +
				`"tool_call":{"shellToolCall":{"args":{"command":"serve"},"result":[{"isBackground":true}],"toolCallId":"c6"}}}`,
			wantName:   "shell",
			wantResult: `[{"isBackground":true}]`,
		},
		{
			name: "missing result stays foreground",
			event: `{"type":"tool_call","subtype":"completed","call_id":"c7",` +
				`"tool_call":{"shellToolCall":{"args":{"command":"serve"},"toolCallId":"c7"}}}`,
			wantName: "shell",
		},
		{
			name: "null result stays foreground",
			event: `{"type":"tool_call","subtype":"completed","call_id":"c8",` +
				`"tool_call":{"shellToolCall":{"args":{"command":"serve"},"result":null,"toolCallId":"c8"}}}`,
			wantName:   "shell",
			wantResult: `null`,
		},
	}

	for _, tt := range tests {
		tt := tt
		t.Run(tt.name, func(t *testing.T) {
			t.Parallel()

			var evt cursorStreamEvent
			if err := json.Unmarshal([]byte(tt.event), &evt); err != nil {
				t.Fatalf("unmarshal event: %v", err)
			}
			call := parseCursorToolCall(&evt)
			if call.Background != tt.wantBackground {
				t.Errorf("Background = %v, want %v", call.Background, tt.wantBackground)
			}
			if call.Result != tt.wantResult {
				t.Errorf("Result = %q, want %q (raw result must be preserved verbatim)", call.Result, tt.wantResult)
			}
			if call.Name != tt.wantName || call.CallID == "" {
				t.Errorf("tool identity lost: name=%q want %q, callID=%q", call.Name, tt.wantName, call.CallID)
			}
		})
	}
}
