package daemon

import (
	"context"
	"log/slog"
	"sync/atomic"
	"testing"
	"testing/synctest"
	"time"

	"github.com/multica-ai/multica/server/pkg/agent"
)

// The ordering both tests below pin is the one that loses a completed run:
//
//  1. the tool watchdog enters the background-cleanup callback,
//  2. Cursor reads its authoritative terminal result and blocks behind the same
//     cleanup lock, so nothing it does is visible yet,
//  3. cleanup fails, leaving native accounting and its timestamp untouched,
//  4. no transcript message arrives to rescue the tick.
//
// Before the terminal boundary existed, the watchdog then cancelled the run and
// executeAndDrain re-tagged Cursor's completed result as idle_watchdog.

// TestIdleWatchdogYieldsToTerminalObservedDuringCleanup drives that exact
// interleaving deterministically: the cleanup callback itself publishes the
// terminal observation, so step 2 provably happens while step 1 is in progress.
func TestIdleWatchdogYieldsToTerminalObservedDuringCleanup(t *testing.T) {
	synctest.Test(t, func(t *testing.T) {
		ctx, cancel := context.WithCancel(context.Background())
		defer cancel()

		var last, threshold atomic.Int64
		var fired atomic.Bool
		var terminal atomic.Bool
		last.Store(time.Now().UnixNano())

		// A held background tool: in flight, and its activity never moves.
		tools := func() int32 { return 1 }
		interrupts := 0
		interrupt := func() bool {
			interrupts++
			// Cursor read its terminal result while this call held the lock.
			terminal.Store(true)
			// Ownership could not be confirmed, so nothing is released.
			return false
		}

		go new(Daemon).runIdleWatchdog(ctx, time.Minute, time.Minute, &last, tools, &fired, &threshold,
			cancel, make(chan agent.Message), interrupt, terminal.Load, slog.Default())

		synctest.Wait()
		time.Sleep(time.Minute)
		synctest.Wait()

		if interrupts == 0 {
			t.Fatal("never reached the cleanup boundary; the interleaving was not exercised")
		}
		if fired.Load() || ctx.Err() != nil {
			t.Fatalf("a decided outcome was force-stopped: fired=%v err=%v", fired.Load(), ctx.Err())
		}

		// And it stays decided: further budgets must not revive the hang verdict.
		time.Sleep(5 * time.Minute)
		synctest.Wait()
		if fired.Load() || ctx.Err() != nil {
			t.Fatalf("terminal boundary expired: fired=%v err=%v", fired.Load(), ctx.Err())
		}
	})
}

// terminalRaceBackend reproduces the same ordering through executeAndDrain, so
// the re-tagging half of the defect is covered too. Ordering is enforced by
// channel handshakes; only the watchdog budget itself uses time.
type terminalRaceBackend struct {
	interrupted chan struct{}
	terminal    atomic.Bool
	interrupts  atomic.Int32
}

func (b *terminalRaceBackend) Execute(ctx context.Context, _ string, _ agent.ExecOptions) (*agent.Session, error) {
	messages := make(chan agent.Message, 4)
	results := make(chan agent.Result, 1)
	var nativeCount atomic.Int32
	var nativeActivity atomic.Int64
	nativeCount.Store(1)
	nativeActivity.Store(time.Now().UnixNano())
	messages <- agent.Message{Type: agent.MessageToolUse, Tool: "shell", CallID: "bg"}

	go func() {
		defer close(messages)
		defer close(results)
		select {
		case <-b.interrupted:
			// Cleanup already failed and the terminal observation is published;
			// Cursor now finishes normally with its authoritative result.
			results <- agent.Result{Status: "completed", Output: "Cursor terminal result"}
		case <-ctx.Done():
			results <- agent.Result{Status: "aborted"}
		}
	}()

	return &agent.Session{
		Messages:         messages,
		Result:           results,
		ToolActivity:     func() (int32, time.Time) { return nativeCount.Load(), time.Unix(0, nativeActivity.Load()) },
		TerminalObserved: b.terminal.Load,
		InterruptBackgroundTools: func() bool {
			b.interrupts.Add(1)
			// Step 2: the terminal result becomes decided while cleanup holds
			// the lock. Native accounting deliberately stays untouched.
			b.terminal.Store(true)
			select {
			case <-b.interrupted:
			default:
				close(b.interrupted)
			}
			// Step 3: ownership could not be confirmed.
			return false
		},
	}, nil
}

func TestExecuteAndDrainKeepsTerminalResultObservedDuringCleanup(t *testing.T) {
	d := newTestDaemon(t)
	d.cfg.AgentIdleWatchdog = 50 * time.Millisecond
	d.cfg.AgentToolWatchdog = 50 * time.Millisecond

	backend := &terminalRaceBackend{interrupted: make(chan struct{})}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	result, _, err := d.executeAndDrain(ctx, backend, "test", agent.ExecOptions{}, slog.Default(), "terminal-race", "", new(atomic.Int32))
	if err != nil {
		t.Fatalf("executeAndDrain: %v", err)
	}
	if backend.interrupts.Load() == 0 {
		t.Fatal("never reached the cleanup boundary; the interleaving was not exercised")
	}
	if result.Status != "completed" || result.Output != "Cursor terminal result" {
		t.Fatalf("Cursor's authoritative result was rewritten: %+v", result)
	}
}

// lateTerminalBackend closes the remaining window: the terminal result is
// published in the instant AFTER the watchdog's final gate read it and BEFORE
// the watchdog writes fired/cancel. The run then reaches executeAndDrain
// through drainCtx.Done() rather than the Result arm, because the backend
// cannot deliver Result until its own finalization is done.
type lateTerminalBackend struct {
	terminal atomic.Bool
	reads    atomic.Int32
	cancels  atomic.Int32
}

// TerminalObserved answers the caller with the value it had when the read
// started, then publishes. The reader that triggers the flip still sees false —
// which is exactly the watchdog's final gate losing the race by one instant.
func (b *lateTerminalBackend) TerminalObserved() bool {
	if b.terminal.Load() {
		return true
	}
	if b.reads.Add(1) >= 2 {
		b.terminal.Store(true)
	}
	return false
}

func (b *lateTerminalBackend) Execute(ctx context.Context, _ string, _ agent.ExecOptions) (*agent.Session, error) {
	messages := make(chan agent.Message, 4)
	results := make(chan agent.Result, 1)
	var nativeCount atomic.Int32
	var nativeActivity atomic.Int64
	nativeCount.Store(1)
	nativeActivity.Store(time.Now().UnixNano())
	messages <- agent.Message{Type: agent.MessageToolUse, Tool: "shell", CallID: "bg"}

	go func() {
		defer close(messages)
		defer close(results)
		<-ctx.Done()
		b.cancels.Add(1)
		// Cursor had already read its authoritative result, so the cancellation
		// it is finishing under does not change the outcome — the same thing
		// cursor.go does once resultSeen is true.
		results <- agent.Result{Status: "completed", Output: "Cursor terminal result"}
	}()

	return &agent.Session{
		Messages:         messages,
		Result:           results,
		ToolActivity:     func() (int32, time.Time) { return nativeCount.Load(), time.Unix(0, nativeActivity.Load()) },
		TerminalObserved: b.TerminalObserved,
		// Nothing can be released: cleanup could not confirm ownership.
		InterruptBackgroundTools: func() bool { return false },
	}, nil
}

// TestExecuteAndDrainKeepsTerminalResultPublishedAfterTheFinalGate is the case
// the previous regression test did not reach: there the watchdog returned at
// its final gate, so it never fired and the drain-timeout classifier never ran.
// Here it does fire, and the decided outcome still has to survive.
func TestExecuteAndDrainKeepsTerminalResultPublishedAfterTheFinalGate(t *testing.T) {
	d := newTestDaemon(t)
	d.cfg.AgentIdleWatchdog = 50 * time.Millisecond
	d.cfg.AgentToolWatchdog = 50 * time.Millisecond

	backend := &lateTerminalBackend{}
	ctx, cancel := context.WithTimeout(context.Background(), 5*time.Second)
	defer cancel()

	result, _, err := d.executeAndDrain(ctx, backend, "test", agent.ExecOptions{}, slog.Default(), "late-terminal", "", new(atomic.Int32))
	if err != nil {
		t.Fatalf("executeAndDrain: %v", err)
	}
	if backend.cancels.Load() == 0 {
		t.Fatal("the watchdog never cancelled; this test did not exercise the fired path")
	}
	if result.Status != "completed" || result.Output != "Cursor terminal result" {
		t.Fatalf("a decided outcome was reclassified after the final gate: %+v", result)
	}
}
