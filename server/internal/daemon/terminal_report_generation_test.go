package daemon

import (
	"context"
	"encoding/json"
	"errors"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"strings"
	"sync/atomic"
	"testing"
	"time"

	"github.com/multica-ai/multica/server/pkg/protocol"
)

// The claim generation is the server-issued dispatched_at of the claim that
// produced a terminal result. These tests cover the daemon half of the fence:
// it has to survive the durable queue, reach the wire unchanged, and never be
// invented for a record that does not have one.

func testClaimGeneration() time.Time {
	// Sub-second precision on purpose: the queue must round-trip exactly what
	// the server issued, or the server-side CAS will not recognize its own claim.
	return time.Date(2026, time.September, 19, 4, 43, 58, 123456000, time.UTC)
}

// claimGenerationReport is one terminal report fenced to a known generation.
func claimGenerationReport(taskID string) terminalTaskReport {
	return terminalTaskReport{
		kind:              terminalTaskReportComplete,
		taskID:            taskID,
		claimDispatchedAt: testClaimGeneration(),
		output:            "the original answer",
		branchName:        "agent/fenced",
		sessionID:         "session-fenced",
		workDir:           "/tmp/fenced",
		durableWorkDir:    "/tmp/project",
	}
}

func TestTerminalReportQueuePersistsClaimGeneration(t *testing.T) {
	store := newTerminalReportStore(Config{
		WorkspacesRoot: t.TempDir(),
		ServerBaseURL:  "https://api.example.test",
		DaemonID:       "daemon-generation",
	})
	report := claimGenerationReport("task-generation")
	if err := store.enqueue(report); err != nil {
		t.Fatalf("enqueue terminal report: %v", err)
	}

	items, err := store.list()
	if err != nil {
		t.Fatalf("list terminal reports: %v", err)
	}
	if len(items) != 1 {
		t.Fatalf("queued reports = %d, want 1", len(items))
	}
	if items[0].report != report {
		t.Fatalf("round-tripped report = %+v, want %+v", items[0].report, report)
	}
	if !items[0].report.claimDispatchedAt.Equal(report.claimDispatchedAt) {
		t.Fatalf("round-tripped generation = %s, want %s", items[0].report.claimDispatchedAt, report.claimDispatchedAt)
	}

	// The generation has to be on disk, not just in memory: replay after a
	// restart reads only the file.
	body, err := os.ReadFile(filepath.Join(store.dir, items[0].fileName))
	if err != nil {
		t.Fatalf("read queued record: %v", err)
	}
	var decoded map[string]any
	if err := json.Unmarshal(body, &decoded); err != nil {
		t.Fatalf("decode queued record: %v", err)
	}
	raw, ok := decoded["claim_dispatched_at"].(string)
	if !ok {
		t.Fatalf("queued record has no claim_dispatched_at: %s", body)
	}
	stored, err := time.Parse(time.RFC3339Nano, raw)
	if err != nil || !stored.Equal(report.claimDispatchedAt) {
		t.Fatalf("persisted claim_dispatched_at = %q, want %s", raw, report.claimDispatchedAt)
	}

	// A report for the same task that differs only in generation is a different
	// claim's result, so it must not overwrite the queued one.
	conflict := report
	conflict.claimDispatchedAt = report.claimDispatchedAt.Add(time.Second)
	if err := store.enqueue(conflict); err == nil || !strings.Contains(err.Error(), "conflicts with the original") {
		t.Fatalf("conflicting enqueue error = %v, want original-payload conflict", err)
	}
}

func TestTerminalReportLegacyRecordWithoutGenerationIsNeverReplayed(t *testing.T) {
	cfg := Config{
		ServerBaseURL:  "https://api.example.test",
		WorkspacesRoot: t.TempDir(),
		DaemonID:       "daemon-legacy-record",
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	d := New(cfg, logger)

	// A record written by a daemon that predates the fence: same on-disk shape,
	// no claim generation.
	legacy := terminalTaskReport{kind: terminalTaskReportComplete, taskID: "task-legacy", output: "legacy answer"}
	if err := d.terminalReports.enqueue(legacy); err != nil {
		t.Fatalf("enqueue legacy report: %v", err)
	}
	name := terminalReportFileName(legacy.taskID)
	path := filepath.Join(d.terminalReports.dir, name)
	before, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("read legacy record: %v", err)
	}

	var delivered atomic.Int32
	d.terminalReportSend = func(context.Context, terminalTaskReport, []time.Duration) error {
		delivered.Add(1)
		return nil
	}
	pending, sent := d.replayPendingTerminalReports(context.Background())
	if sent != 0 || delivered.Load() != 0 {
		t.Fatalf("legacy record was delivered: delay=%d delivered=%d", sent, delivered.Load())
	}
	if pending != 1 {
		t.Fatalf("pending = %d, want 1 so the record keeps surfacing to operators", pending)
	}

	after, err := os.ReadFile(path)
	if err != nil {
		t.Fatalf("legacy record disappeared: %v", err)
	}
	if string(before) != string(after) {
		t.Fatalf("legacy record changed:\nbefore %s\nafter  %s", before, after)
	}
}

func TestTerminalReportStaleClaimGenerationIsRetiredWithoutCompensation(t *testing.T) {
	var failCalls atomic.Int32
	var completeCalls atomic.Int32
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		switch {
		case strings.HasSuffix(req.URL.Path, "/fail"):
			failCalls.Add(1)
			w.WriteHeader(http.StatusOK)
		case strings.HasSuffix(req.URL.Path, "/complete"):
			completeCalls.Add(1)
			w.Header().Set("Content-Type", "application/json")
			w.WriteHeader(http.StatusConflict)
			_, _ = w.Write([]byte("{\"error\":\"task claim generation is no longer current\",\"code\":\"" +
				protocol.DaemonTaskClaimGenerationMismatchCode + "\"}\n"))
		default:
			t.Errorf("unexpected request path %q", req.URL.Path)
			w.WriteHeader(http.StatusNotFound)
		}
	}))
	t.Cleanup(srv.Close)

	d := New(Config{
		ServerBaseURL:  srv.URL,
		WorkspacesRoot: t.TempDir(),
		DaemonID:       "daemon-stale-generation",
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	report := claimGenerationReport("task-stale")

	// The first delivery is refused as stale; the report must not be replayed.
	if err := d.reportTerminalTask(context.Background(), report); err == nil {
		t.Fatal("stale terminal report unexpectedly reported success")
	}
	if items, err := d.terminalReports.list(); err != nil || len(items) != 0 {
		t.Fatalf("pending reports after a stale rejection = %d, %v; want 0", len(items), err)
	}
	if failCalls.Load() != 0 {
		t.Fatalf("/fail calls = %d, want 0: a stale completion must never fail the reclaim that owns the task",
			failCalls.Load())
	}

	// The payload is retained for operators instead of being discarded.
	entries, err := os.ReadDir(d.terminalReports.failedDir())
	if err != nil {
		t.Fatalf("read failed queue: %v", err)
	}
	if len(entries) != 1 {
		t.Fatalf("failed queue entries = %d, want 1", len(entries))
	}
	body, err := os.ReadFile(filepath.Join(d.terminalReports.failedDir(), entries[0].Name()))
	if err != nil {
		t.Fatalf("read failed record: %v", err)
	}
	var record persistedTerminalTaskReport
	if err := json.Unmarshal(body, &record); err != nil {
		t.Fatalf("decode failed record: %v", err)
	}
	if record.SupersededAt == nil {
		t.Fatalf("failed record was not marked superseded: %s", body)
	}
	if record.Output != report.output {
		t.Fatalf("failed record output = %q, want the original %q", record.Output, report.output)
	}

	// Later replay passes must stay quiet rather than retrying the stale report.
	pending, delivered := d.replayPendingTerminalReports(context.Background())
	if pending != 0 || delivered != 0 {
		t.Fatalf("replay after retirement pending=%d delivered=%d, want 0/0", pending, delivered)
	}
	// A conflict is not a transient failure, so the send itself does not retry
	// it either: one authoritative response is all the daemon needs.
	if got := completeCalls.Load(); got != 1 {
		t.Fatalf("complete attempts = %d, want exactly one", got)
	}
}

func TestTerminalReportOrdinaryConflictStaysPending(t *testing.T) {
	d := New(Config{
		ServerBaseURL:  "https://api.example.test",
		WorkspacesRoot: t.TempDir(),
		DaemonID:       "daemon-ordinary-conflict",
	}, slog.New(slog.NewTextHandler(io.Discard, nil)))
	d.terminalReportSend = func(context.Context, terminalTaskReport, []time.Duration) error {
		// A 409 without the fence code is an ordinary conflict: the server may
		// recover, so the report must stay queued instead of being retired.
		return &requestError{Method: http.MethodPost, Path: "/complete", StatusCode: http.StatusConflict, Body: "{\"error\":\"some other conflict\"}"}
	}
	if err := d.terminalReports.enqueue(claimGenerationReport("task-ordinary-conflict")); err != nil {
		t.Fatalf("enqueue report: %v", err)
	}
	pending, delivered := d.replayPendingTerminalReports(context.Background())
	if pending != 1 || delivered != 0 {
		t.Fatalf("ordinary conflict pending=%d delivered=%d, want 1/0", pending, delivered)
	}
	if _, err := os.Stat(filepath.Join(d.terminalReports.failedDir(), terminalReportFileName("task-ordinary-conflict"))); !errors.Is(err, os.ErrNotExist) {
		t.Fatalf("ordinary conflict moved the report to failed/: %v", err)
	}
}

func TestTerminalReportReplayUsesThePersistedClaimGeneration(t *testing.T) {
	var bodies []map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		if !strings.HasSuffix(req.URL.Path, "/complete") {
			// A restart replay must not ask the server what the current claim is:
			// the original generation is the only one this report may carry.
			t.Errorf("replay looked up %q", req.URL.Path)
		}
		var body map[string]any
		if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
			t.Errorf("decode replay body: %v", err)
		}
		bodies = append(bodies, body)
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	cfg := Config{
		ServerBaseURL:  srv.URL,
		WorkspacesRoot: t.TempDir(),
		DaemonID:       "daemon-restart-generation",
	}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	report := claimGenerationReport("task-restart-generation")

	beforeRestart := New(cfg, logger)
	beforeRestart.terminalReportSend = func(context.Context, terminalTaskReport, []time.Duration) error {
		return errors.New("network unavailable")
	}
	if err := beforeRestart.reportTerminalTask(context.Background(), report); err == nil {
		t.Fatal("terminal report unexpectedly succeeded before restart")
	}

	afterRestart := New(cfg, logger)
	pending, delivered := afterRestart.replayPendingTerminalReports(context.Background())
	if pending != 0 || delivered != 1 {
		t.Fatalf("restart replay pending=%d delivered=%d, want 0/1", pending, delivered)
	}
	if len(bodies) != 1 {
		t.Fatalf("replay bodies = %d, want 1", len(bodies))
	}
	got, _ := bodies[0]["expected_dispatched_at"].(string)
	if want := report.claimDispatchedAt.Format(time.RFC3339Nano); got != want {
		t.Fatalf("replayed expected_dispatched_at = %q, want the persisted %q", got, want)
	}
	if got := bodies[0]["output"]; got != report.output {
		t.Fatalf("replayed output = %v, want %q", got, report.output)
	}
}

func TestClaimGenerationComesOnlyFromTheClaimPayload(t *testing.T) {
	generation := testClaimGeneration()
	formatted := generation.Format(time.RFC3339Nano)
	secondPrecision := generation.Truncate(time.Second).Format(time.RFC3339)

	tests := []struct {
		name string
		raw  *string
		want time.Time
	}{
		{name: "nano precision", raw: &formatted, want: generation},
		{name: "second precision", raw: &secondPrecision, want: generation.Truncate(time.Second)},
		{name: "absent", raw: nil},
		{name: "empty", raw: new(string)},
		{name: "unparseable", raw: ptrTo("not a timestamp")},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			got := claimGeneration(Task{DispatchedAt: tt.raw})
			if !got.Equal(tt.want) {
				t.Fatalf("claimGeneration() = %s, want %s", got, tt.want)
			}
		})
	}
}

func ptrTo(value string) *string { return &value }

func TestTaskDispatchedAtMirrorsTheClaimPayload(t *testing.T) {
	// internal/daemon.Task is a hand-kept mirror of the server's claim response.
	// A missing or renamed JSON tag here would silently drop the generation, and
	// every callback of that claim would go back to the unfenced path.
	var task Task
	if err := json.Unmarshal([]byte("{\"id\":\"task-1\",\"dispatched_at\":\"2026-09-19T04:43:58.123456Z\"}"), &task); err != nil {
		t.Fatalf("decode claim payload: %v", err)
	}
	want := time.Date(2026, time.September, 19, 4, 43, 58, 123456000, time.UTC)
	if got := claimGeneration(task); !got.Equal(want) {
		t.Fatalf("claim generation from the claim payload = %s, want %s", got, want)
	}

	// A server that predates the field must leave the generation unknown rather
	// than inventing one locally.
	var legacy Task
	if err := json.Unmarshal([]byte("{\"id\":\"task-2\"}"), &legacy); err != nil {
		t.Fatalf("decode legacy claim payload: %v", err)
	}
	if got := claimGeneration(legacy); !got.IsZero() {
		t.Fatalf("legacy claim payload produced a generation: %s", got)
	}
}

func TestTerminalReportEnqueueKeepsGenerationForEveryReportKind(t *testing.T) {
	// Both terminal kinds carry the generation; a fail report that lost it would
	// never be replayed and would be retained as unknown ownership instead.
	for _, kind := range []terminalTaskReportKind{terminalTaskReportComplete, terminalTaskReportFail} {
		store := newTerminalReportStore(Config{
			WorkspacesRoot: t.TempDir(),
			ServerBaseURL:  "https://api.example.test",
			DaemonID:       "daemon-kinds",
		})
		report := claimGenerationReport("task-kind")
		report.kind = kind
		report.errorMessage = "provider failed"
		if err := store.enqueue(report); err != nil {
			t.Fatalf("enqueue %d report: %v", kind, err)
		}
		items, err := store.list()
		if err != nil {
			t.Fatalf("list %d report: %v", kind, err)
		}
		if len(items) != 1 || !items[0].report.claimDispatchedAt.Equal(report.claimDispatchedAt) {
			t.Fatalf("kind %d lost its generation: %+v", kind, items)
		}
	}
}
