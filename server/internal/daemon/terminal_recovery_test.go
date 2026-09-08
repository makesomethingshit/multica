package daemon

import (
	"context"
	"fmt"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"sync/atomic"
	"testing"
	"time"
)

// terminalRecoveryServer is a fake control plane for the live-daemon terminal
// recovery tests (#8157). It models the authoritative task row: /status
// reports the current status, a successful /complete flips it to completed,
// and a successful /fail flips it to failed — mirroring the server's CAS
// semantics where terminal callbacks only apply to a running row.
type terminalRecoveryServer struct {
	mu           sync.Mutex
	status       string // authoritative task status answered by /status
	notFound     bool   // /status answers 404 task not found
	statusBroken bool   // /status answers 502
	completeCode int    // POST /complete answer (0 → 200)
	failCode     int    // POST /fail answer (0 → 200)

	completeCalls atomic.Int32
	failCalls     atomic.Int32
	statusCalls   atomic.Int32
}

func (s *terminalRecoveryServer) setLockStatus(status string) {
	s.mu.Lock()
	defer s.mu.Unlock()
	s.status = status
}

func (s *terminalRecoveryServer) handler() http.Handler {
	return http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch {
		case strings.HasSuffix(req.URL.Path, "/status"):
			s.statusCalls.Add(1)
			s.mu.Lock()
			notFound, broken, status := s.notFound, s.statusBroken, s.status
			s.mu.Unlock()
			if notFound {
				w.WriteHeader(http.StatusNotFound)
				_, _ = w.Write([]byte("task not found"))
				return
			}
			if broken {
				w.WriteHeader(http.StatusBadGateway)
				return
			}
			w.Header().Set("Content-Type", "application/json")
			_, _ = fmt.Fprintf(w, `{"status":%q}`, status)
		case strings.HasSuffix(req.URL.Path, "/complete"):
			s.completeCalls.Add(1)
			s.mu.Lock()
			code := s.completeCode
			if code == 0 {
				s.status = "completed"
			}
			s.mu.Unlock()
			if code == 0 {
				code = http.StatusOK
			}
			w.WriteHeader(code)
		case strings.HasSuffix(req.URL.Path, "/fail"):
			s.failCalls.Add(1)
			s.mu.Lock()
			code := s.failCode
			if code == 0 {
				s.status = "failed"
			}
			s.mu.Unlock()
			if code == 0 {
				code = http.StatusOK
			}
			w.WriteHeader(code)
		default:
			w.WriteHeader(http.StatusOK)
		}
	})
}

// collapseTerminalRetries swaps in a zero-length retry schedule (3 attempts,
// no wall-clock cost) for the duration of the test.
func collapseTerminalRetries(t *testing.T) {
	t.Helper()
	defer noSleepRetry(t)()

	prev := defaultTerminalRetrySchedule
	defaultTerminalRetrySchedule = []time.Duration{0, 0}
	t.Cleanup(func() { defaultTerminalRetrySchedule = prev })
}

// queuedCompleteReport places a complete-kind report directly into the store,
// as transient exhaustion of the normal path would (covered end-to-end by
// TestReportTaskResult_TransientCompleteExhaustedQueuesLiveRecovery and
// TestTerminalReportRecovery_DeliversWhileServerRunning).
func queuedCompleteReport(d *Daemon, taskID string) {
	d.enqueuePendingTerminalReport(terminalTaskReport{
		kind:   terminalTaskReportComplete,
		taskID: taskID,
		output: "provider finished",
	})
}

// Test 2 — live recovery succeeds while the authoritative row is still
// running: the queued report is replayed once per pass, and success removes
// it while the server lands on completed.
func TestTerminalReportRecovery_DeliversWhileServerRunning(t *testing.T) {
	collapseTerminalRetries(t)

	srvState := &terminalRecoveryServer{status: "running", completeCode: http.StatusBadGateway}
	srv := httptest.NewServer(srvState.handler())
	t.Cleanup(srv.Close)

	d := &Daemon{client: NewClient(srv.URL), logger: slog.Default()}
	// Initial phase: the normal complete path exhausts transient retries and
	// queues the report instead of losing it.
	d.reportTaskResult(context.Background(), "task-live", TaskResult{
		Status:  "completed",
		Comment: "provider finished",
	}, slog.Default())
	if got := srvState.completeCalls.Load(); got != 3 {
		t.Fatalf("initial complete attempts = %d, want 3", got)
	}
	if got := len(d.pendingTerminalReportSnapshot()); got != 1 {
		t.Fatalf("pending reports after exhaustion = %d, want 1", got)
	}

	// Recovery phase: server row still running, delivery now succeeds.
	srvState.mu.Lock()
	srvState.completeCode = 0 // 200; handler flips authoritative state
	srvState.mu.Unlock()
	d.recoverPendingTerminalReports(context.Background())

	if got := len(d.pendingTerminalReportSnapshot()); got != 0 {
		t.Fatalf("pending reports after recovery = %d, want 0", got)
	}
	srvState.mu.Lock()
	defer srvState.mu.Unlock()
	if srvState.status != "completed" {
		t.Fatalf("server state after recovery = %q, want completed", srvState.status)
	}
	if got := srvState.completeCalls.Load(); got != 4 {
		t.Fatalf("complete calls after recovery = %d, want 4 (3 exhausted + 1 recovery attempt)", got)
	}
}

// Test 3 — user cancellation wins: a pending completion meeting a cancelled
// row is dropped without any replayed mutation.
func TestTerminalReportRecovery_CancelledServerStateDropsPending(t *testing.T) {
	srvState := &terminalRecoveryServer{status: "cancelled"}
	srv := httptest.NewServer(srvState.handler())
	t.Cleanup(srv.Close)

	d := &Daemon{client: NewClient(srv.URL), logger: slog.Default()}
	queuedCompleteReport(d, "task-cancelled")

	d.recoverPendingTerminalReports(context.Background())

	if got := len(d.pendingTerminalReportSnapshot()); got != 0 {
		t.Fatalf("pending reports = %d, want 0 (cancellation is authoritative)", got)
	}
	if got := srvState.completeCalls.Load(); got != 0 {
		t.Fatalf("complete replays = %d, want 0 (no late success overwrite)", got)
	}
}

// Test 4 — #4579-style retry-child safety: the server sweeper may have
// failed the parent while a retry child runs. Recovery must drop the pending
// report without any reclaim or parent rewrite — a late parent completion
// must never turn a failed parent back into completed.
func TestTerminalReportRecovery_FailedParentNeverReclaimed(t *testing.T) {
	srvState := &terminalRecoveryServer{status: "failed"}
	srv := httptest.NewServer(srvState.handler())
	t.Cleanup(srv.Close)

	d := &Daemon{client: NewClient(srv.URL), logger: slog.Default()}
	queuedCompleteReport(d, "task-failed-parent")

	d.recoverPendingTerminalReports(context.Background())

	if got := len(d.pendingTerminalReportSnapshot()); got != 0 {
		t.Fatalf("pending reports = %d, want 0 (failed parent is authoritative)", got)
	}
	if got := srvState.completeCalls.Load(); got != 0 {
		t.Fatalf("complete replays = %d, want 0 (never reclaim a failed parent)", got)
	}
	if got := srvState.failCalls.Load(); got != 0 {
		t.Fatalf("fail replays = %d, want 0 (no reclaim callback either)", got)
	}
}

// Test 5 — lost success response: POST /complete committed server-side but
// the daemon saw a failure and queued the report. Recovery reads completed
// and removes the pending report without resending the mutation.
func TestTerminalReportRecovery_AlreadyCompletedIsLostResponseSuccess(t *testing.T) {
	srvState := &terminalRecoveryServer{status: "completed"}
	srv := httptest.NewServer(srvState.handler())
	t.Cleanup(srv.Close)

	d := &Daemon{client: NewClient(srv.URL), logger: slog.Default()}
	queuedCompleteReport(d, "task-lost-response")

	d.recoverPendingTerminalReports(context.Background())

	if got := len(d.pendingTerminalReportSnapshot()); got != 0 {
		t.Fatalf("pending reports = %d, want 0 (lost response converges)", got)
	}
	if got := srvState.completeCalls.Load(); got != 0 {
		t.Fatalf("complete replays = %d, want 0 (no second terminal mutation)", got)
	}
}

// Test 6 — transient status lookup keeps ownership: a 502 from /status must
// neither drop the report nor attempt delivery; a later healthy pass
// delivers it.
func TestTerminalReportRecovery_TransientStatusLookupKeepsPending(t *testing.T) {
	srvState := &terminalRecoveryServer{status: "running", statusBroken: true, completeCode: http.StatusBadGateway}
	srv := httptest.NewServer(srvState.handler())
	t.Cleanup(srv.Close)

	d := &Daemon{client: NewClient(srv.URL), logger: slog.Default()}
	queuedCompleteReport(d, "task-flaky-status")

	d.recoverPendingTerminalReports(context.Background())

	if got := len(d.pendingTerminalReportSnapshot()); got != 1 {
		t.Fatalf("pending reports after transient lookup failure = %d, want 1", got)
	}
	if got := srvState.completeCalls.Load(); got != 0 {
		t.Fatalf("complete replays = %d, want 0 (cannot settle without authoritative state)", got)
	}

	srvState.mu.Lock()
	srvState.statusBroken = false
	srvState.completeCode = 0
	srvState.mu.Unlock()
	d.recoverPendingTerminalReports(context.Background())

	if got := len(d.pendingTerminalReportSnapshot()); got != 0 {
		t.Fatalf("pending reports after successful pass = %d, want 0", got)
	}
	srvState.mu.Lock()
	defer srvState.mu.Unlock()
	if srvState.status != "completed" {
		t.Fatalf("server state = %q, want completed", srvState.status)
	}
}

// Test 7 — FailTask gets the same ownership protection: transient exhaustion
// of the fail callback queues a fail-kind report, and recovery delivers it
// while the row is running. The complete callback is never touched from the
// fail path.
func TestTerminalReportRecovery_FailTaskGetsSameOwnershipProtection(t *testing.T) {
	collapseTerminalRetries(t)

	srvState := &terminalRecoveryServer{status: "running", failCode: http.StatusBadGateway}
	srv := httptest.NewServer(srvState.handler())
	t.Cleanup(srv.Close)

	d := &Daemon{client: NewClient(srv.URL), logger: slog.Default()}
	d.reportTaskResult(context.Background(), "task-fail", TaskResult{
		Status:        "failed",
		Comment:       "boom",
		FailureReason: "process_failure",
	}, slog.Default())

	pending := d.pendingTerminalReportSnapshot()
	if len(pending) != 1 {
		t.Fatalf("pending reports = %d, want 1 (%v)", len(pending), pending)
	}
	report, ok := pending["task-fail"]
	if !ok {
		t.Fatalf("pending report for task-fail not found; got keys %v", pending)
	}
	if report.kind != terminalTaskReportFail {
		t.Fatalf("pending report kind = %v, want fail", report.kind)
	}
	if got := srvState.completeCalls.Load(); got != 0 {
		t.Fatalf("complete calls from the fail path = %d, want 0", got)
	}

	srvState.mu.Lock()
	srvState.failCode = 0
	srvState.mu.Unlock()
	d.recoverPendingTerminalReports(context.Background())

	if got := len(d.pendingTerminalReportSnapshot()); got != 0 {
		t.Fatalf("pending reports after recovery = %d, want 0", got)
	}
	srvState.mu.Lock()
	defer srvState.mu.Unlock()
	if srvState.status != "failed" {
		t.Fatalf("server state = %q, want failed", srvState.status)
	}
}

// Test 8 — deduplication: one pending terminal report per task. Re-enqueueing
// the same task is idempotent (latest report wins), and there is exactly one
// recovery worker — the single loop drains the map, so no duplicate retry
// stream can exist.
func TestPendingTerminalReportDeduplication(t *testing.T) {
	d := &Daemon{logger: slog.Default()}
	d.enqueuePendingTerminalReport(terminalTaskReport{
		kind:   terminalTaskReportComplete,
		taskID: "task-dup",
		output: "first",
	})
	d.enqueuePendingTerminalReport(terminalTaskReport{
		kind:   terminalTaskReportComplete,
		taskID: "task-dup",
		output: "second",
	})

	pending := d.pendingTerminalReportSnapshot()
	if len(pending) != 1 {
		t.Fatalf("pending reports = %d, want 1 (enqueue is idempotent)", len(pending))
	}
	if got := pending["task-dup"].output; got != "second" {
		t.Fatalf("pending report output = %q, want the latest report %q", got, "second")
	}
}

// Test 9 — daemon shutdown: cancelling the root context makes the loop exit
// promptly, so shutdown is never blocked on unsettled reports. Reports are
// memory-only — nothing in this path touches disk, and a daemon restart
// drops them by design (durable replay is #4579 and stays out of scope).
func TestPendingTerminalReportShutdownExitsLoop(t *testing.T) {
	srvState := &terminalRecoveryServer{status: "running", completeCode: http.StatusBadGateway}
	srv := httptest.NewServer(srvState.handler())
	t.Cleanup(srv.Close)

	prevInterval := terminalRecoveryInterval
	terminalRecoveryInterval = 5 * time.Millisecond
	t.Cleanup(func() { terminalRecoveryInterval = prevInterval })

	d := &Daemon{client: NewClient(srv.URL), logger: slog.Default()}
	queuedCompleteReport(d, "task-shutdown")

	ctx, cancel := context.WithCancel(context.Background())
	done := make(chan struct{})
	go func() {
		defer close(done)
		d.terminalRecoveryLoop(ctx)
	}()

	// Wait until the loop performed at least one pass (status lookup seen),
	// then cancel and require a prompt exit.
	waitFor(t, func() bool { return srvState.statusCalls.Load() > 0 }, "recovery loop never performed a pass")
	cancel()
	waitFor(t, func() bool {
		select {
		case <-done:
			return true
		default:
			return false
		}
	}, "recovery loop did not exit after daemon root context cancel")

	// The unresolved report stays owned in memory until the process ends —
	// still exactly one, never persisted anywhere.
	if got := len(d.pendingTerminalReportSnapshot()); got != 1 {
		t.Fatalf("pending reports after shutdown = %d, want 1 (memory-only store)", got)
	}
}
