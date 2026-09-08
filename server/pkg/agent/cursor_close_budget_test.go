package agent

import (
	"context"
	"log/slog"
	"testing"
	"time"
)

// stuckBackgroundProcess never dies and takes a fixed time to signal, which is
// the shape that makes per-process bounds insufficient: each one is bounded,
// but a run may launch any number of them.
type stuckBackgroundProcess struct{ signal time.Duration }

func (p *stuckBackgroundProcess) alive() (bool, error) { return true, nil }
func (p *stuckBackgroundProcess) terminate() error     { time.Sleep(p.signal); return nil }
func (p *stuckBackgroundProcess) close()               {}

// TestCursorBackgroundCloseIsBoundedAcrossTools pins the bound that matters
// after a terminal result: Close() runs once the daemon's watchdog has stopped
// supervising the run, so "each process is bounded" is not enough — the whole
// pass has to be. Unconfirmed work must still surface its launch result.
func TestCursorBackgroundCloseIsBoundedAcrossTools(t *testing.T) {
	const (
		tools  = 40
		signal = 100 * time.Millisecond
		budget = 300 * time.Millisecond
	)
	// Without a whole-pass bound this would take tools*signal*2 = 8s.
	unbounded := tools * signal * 2

	messages := make(chan Message, 4*tools)
	b := newCursorBackgroundTools(context.Background(), nil, messages, slog.Default())
	b.closeBudget = budget
	for i := 0; i < tools; i++ {
		b.tools = append(b.tools, cursorBackgroundTool{
			call:    cursorToolCall{Name: "shell", CallID: "bg", Result: `{"isBackground":true}`},
			process: &cursorBackgroundProcess{platform: &stuckBackgroundProcess{signal: signal}},
		})
	}

	start := time.Now()
	b.Close()
	elapsed := time.Since(start)

	// One in-flight signal may still be running when the budget expires.
	if limit := budget + signal + 2*time.Second; elapsed > limit {
		t.Fatalf("Close took %s, want under %s (unbounded would be ~%s)", elapsed, limit, unbounded)
	}
	if elapsed >= unbounded {
		t.Fatalf("Close took %s: the whole-pass budget did not apply", elapsed)
	}

	// Every tool still hands back its original launch result, none of it
	// reported as confirmed cleanup.
	close(messages)
	results := 0
	for msg := range messages {
		if msg.Type == MessageToolResult {
			results++
		}
	}
	if results != tools {
		t.Fatalf("released %d tool results, want %d: unconfirmed work must not be dropped", results, tools)
	}
}
