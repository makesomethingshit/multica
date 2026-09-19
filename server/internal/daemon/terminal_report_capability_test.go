package daemon

import (
	"context"
	"encoding/json"
	"io"
	"log/slog"
	"net/http"
	"net/http/httptest"
	"os"
	"path/filepath"
	"sync"
	"sync/atomic"
	"testing"
	"time"

	"github.com/multica-ai/multica/server/pkg/protocol"
)

// Fence support is negotiated, never inferred. Older servers already send
// dispatched_at in their claim payloads and still ignore expected_dispatched_at
// on /complete and /fail, so a timestamp can never be treated as evidence of the
// contract — that inference is what let an unfenced mutation look fenced.

// serverAdvertisesFence simulates the heartbeat ack that proves the connected
// server enforces the generation fence. Without it, replay intentionally holds
// every generation-aware report.
func serverAdvertisesFence(d *Daemon) {
	d.observeServerCapabilities([]string{protocol.TerminalReportGenerationFenceV1})
}

// fenceCapableServer is an httptest server that records every terminal callback
// path and body it receives and answers 200.
func fenceCapableServer(t *testing.T) (*httptest.Server, func() []map[string]any, func() []string) {
	t.Helper()
	var mu sync.Mutex
	var bodies []map[string]any
	var paths []string
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
			t.Errorf("decode %s body: %v", req.URL.Path, err)
		}
		mu.Lock()
		bodies = append(bodies, body)
		paths = append(paths, req.URL.Path)
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)
	return srv, func() []map[string]any {
			mu.Lock()
			defer mu.Unlock()
			return append([]map[string]any(nil), bodies...)
		}, func() []string {
			mu.Lock()
			defer mu.Unlock()
			return append([]string(nil), paths...)
		}
}

// TestOldServerClaimWithDispatchedAtStaysLegacy is the version-skew regression.
// The payload is exactly what an older server sends today: a dispatched_at and
// no fence capability. It must stay legacy — no fenced callback, no durable
// record — even though the timestamp parses perfectly.
func TestOldServerClaimWithDispatchedAtStaysLegacy(t *testing.T) {
	for _, dispatchedAt := range []string{
		"2026-09-19T04:43:58Z",             // second precision, the old shape
		"2026-09-19T04:43:58.123456Z",      // even a nanosecond-looking value
		"2026-09-19T13:43:58.123456+09:00", // and another timezone
	} {
		t.Run(dispatchedAt, func(t *testing.T) {
			var task Task
			payload := `{"id":"task-old-server","dispatched_at":"` + dispatchedAt + `"}`
			if err := json.Unmarshal([]byte(payload), &task); err != nil {
				t.Fatalf("decode claim payload: %v", err)
			}
			if got := claimGenerationForTask(task); !got.dispatchedAt.IsZero() || got.unreadable {
				t.Fatalf("old-server claim produced generation %s (unreadable=%v), want legacy/absent", got, got.unreadable)
			}

			srv, sentBodies, sentPaths := fenceCapableServer(t)
			d := New(Config{
				ServerBaseURL:  srv.URL,
				WorkspacesRoot: t.TempDir(),
				DaemonID:       "daemon-old-server",
			}, slog.New(slog.NewTextHandler(io.Discard, nil)))
			report := terminalTaskReport{
				kind: terminalTaskReportComplete, taskID: task.ID, output: "legacy answer",
				claimGeneration: claimGenerationForTask(task),
			}
			if err := d.reportTerminalTask(context.Background(), report); err != nil {
				t.Fatalf("legacy terminal report: %v", err)
			}

			bodies := sentBodies()
			if len(bodies) != 1 {
				t.Fatalf("terminal requests = %d, want the one legacy live callback", len(bodies))
			}
			if value, present := bodies[0]["expected_dispatched_at"]; present {
				t.Fatalf("legacy callback carried a fence (%v); an old server ignores it", value)
			}
			if got := sentPaths(); len(got) != 1 || got[0] != "/api/daemon/tasks/task-old-server/complete" {
				t.Fatalf("legacy callback paths = %v, want the legacy terminal route", got)
			}
			if items, err := d.terminalReports.list(); err != nil || len(items) != 0 {
				t.Fatalf("durable queue = %d records (%v), want none for an old-server claim", len(items), err)
			}
		})
	}
}

// TestNewServerClaimIsFencedAndReplayable is the counterpart: the advertised
// capability plus a dispatch timestamp is what makes a claim generation-aware,
// and the exact instant has to survive persistence and restart.
func TestNewServerClaimIsFencedAndReplayable(t *testing.T) {
	var task Task
	if err := json.Unmarshal([]byte(`{"id":"task-new-server","terminal_report_generation_fence_v1":true,"dispatched_at":"2026-09-19T04:43:58.123456Z"}`), &task); err != nil {
		t.Fatalf("decode claim payload: %v", err)
	}
	generation := claimGenerationForTask(task)
	want := time.Date(2026, time.September, 19, 4, 43, 58, 123456000, time.UTC)
	if generation.unreadable || !generation.dispatchedAt.Equal(want) {
		t.Fatalf("generation = %s (unreadable=%v), want %s", generation, generation.unreadable, want)
	}

	// The live callback is refused so the fenced report stays durable; a
	// successful live delivery acknowledges (and removes) it, which is exactly
	// what the acknowledgement tests pin.
	var healthy atomic.Bool
	var mu sync.Mutex
	var bodies []map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		var body map[string]any
		if err := json.NewDecoder(req.Body).Decode(&body); err != nil {
			t.Errorf("decode %s body: %v", req.URL.Path, err)
		}
		mu.Lock()
		bodies = append(bodies, body)
		mu.Unlock()
		if !healthy.Load() {
			w.WriteHeader(http.StatusBadGateway)
			return
		}
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	cfg := Config{ServerBaseURL: srv.URL, WorkspacesRoot: t.TempDir(), DaemonID: "daemon-new-server"}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	d := New(cfg, logger)
	report := terminalTaskReport{
		kind: terminalTaskReportComplete, taskID: task.ID, output: "fenced answer",
		claimGeneration: generation,
	}
	if err := d.reportTerminalTask(context.Background(), report); err == nil {
		t.Fatal("fenced terminal report unexpectedly delivered while the server was failing")
	}
	if items, err := d.terminalReports.list(); err != nil || len(items) != 1 {
		t.Fatalf("durable queue = %d records (%v), want the fenced report", len(items), err)
	}

	// Restart: replay uses the persisted generation, proven by the server
	// capability the heartbeat advertised.
	afterRestart := New(cfg, logger)
	serverAdvertisesFence(afterRestart)
	healthy.Store(true)
	if pending, delivered := afterRestart.replayPendingTerminalReports(context.Background()); pending != 0 || delivered != 1 {
		t.Fatalf("replay pending=%d delivered=%d, want 0/1", pending, delivered)
	}
	mu.Lock()
	defer mu.Unlock()
	last := bodies[len(bodies)-1]
	if got, _ := last["expected_dispatched_at"].(string); got != want.Format(time.RFC3339Nano) {
		t.Fatalf("replayed expected_dispatched_at = %q, want %q", got, want.Format(time.RFC3339Nano))
	}
}

// TestAdvertisedCapabilityWithInvalidGenerationFailsClosed: a claim that
// advertises the fence must carry a readable generation. Anything else is a
// protocol violation, never a legacy claim.
func TestAdvertisedCapabilityWithInvalidGenerationFailsClosed(t *testing.T) {
	tests := []struct {
		name string
		body string
	}{
		{name: "missing", body: `{"id":"t","terminal_report_generation_fence_v1":true}`},
		{name: "empty", body: `{"id":"t","terminal_report_generation_fence_v1":true,"dispatched_at":""}`},
		{name: "whitespace", body: `{"id":"t","terminal_report_generation_fence_v1":true,"dispatched_at":"   "}`},
		{name: "unparseable", body: `{"id":"t","terminal_report_generation_fence_v1":true,"dispatched_at":"yesterday"}`},
		{name: "zero instant", body: `{"id":"t","terminal_report_generation_fence_v1":true,"dispatched_at":"0001-01-01T00:00:00Z"}`},
	}
	for _, tt := range tests {
		t.Run(tt.name, func(t *testing.T) {
			var task Task
			if err := json.Unmarshal([]byte(tt.body), &task); err != nil {
				t.Fatalf("decode claim payload: %v", err)
			}
			generation := claimGenerationForTask(task)
			if !generation.unreadable {
				t.Fatalf("generation = %s, want unreadable", generation)
			}

			var mu sync.Mutex
			var sends int
			srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, _ *http.Request) {
				mu.Lock()
				sends++
				mu.Unlock()
				w.WriteHeader(http.StatusOK)
			}))
			t.Cleanup(srv.Close)
			d := New(Config{
				ServerBaseURL:  srv.URL,
				WorkspacesRoot: t.TempDir(),
				DaemonID:       "daemon-malformed-" + tt.name,
			}, slog.New(slog.NewTextHandler(io.Discard, nil)))
			err := d.reportTerminalTask(context.Background(), terminalTaskReport{
				kind: terminalTaskReportComplete, taskID: task.ID, output: "must not be sent",
				claimGeneration: generation,
			})
			if err == nil {
				t.Fatal("malformed fenced claim reported success")
			}
			mu.Lock()
			defer mu.Unlock()
			if sends != 0 {
				t.Fatalf("terminal requests = %d, want 0", sends)
			}
			if items, listErr := d.terminalReports.list(); listErr != nil || len(items) != 0 {
				t.Fatalf("durable queue = %d records (%v), want none", len(items), listErr)
			}
		})
	}
}

// TestGenerationAwareReportIsHeldUntilServerProvesFenceSupport is the rollback
// test: a stored report was produced under a server that promised the fence, and
// the daemon must not replay it against a server that cannot prove it enforces
// the fence — a rollback would otherwise apply it unfenced.
func TestGenerationAwareReportIsHeldUntilServerProvesFenceSupport(t *testing.T) {
	var mu sync.Mutex
	var bodies []map[string]any
	srv := httptest.NewServer(http.HandlerFunc(func(w http.ResponseWriter, req *http.Request) {
		var body map[string]any
		_ = json.NewDecoder(req.Body).Decode(&body)
		mu.Lock()
		bodies = append(bodies, body)
		mu.Unlock()
		w.WriteHeader(http.StatusOK)
	}))
	t.Cleanup(srv.Close)

	cfg := Config{ServerBaseURL: srv.URL, WorkspacesRoot: t.TempDir(), DaemonID: "daemon-rollback"}
	logger := slog.New(slog.NewTextHandler(io.Discard, nil))
	d := New(cfg, logger)
	serverAdvertisesFence(d)
	report := generationReport("task-rollback", testClaimGeneration(), "fenced answer")
	if err := d.terminalReports.enqueue(report); err != nil {
		t.Fatalf("enqueue: %v", err)
	}

	// A rollback: the connected server no longer advertises the fence.
	rolledBack := New(cfg, logger)
	rolledBack.observeServerCapabilities([]string{protocol.DaemonCapabilityRPCV1})
	pending, delivered := rolledBack.replayPendingTerminalReports(context.Background())
	if delivered != 0 {
		t.Fatalf("delivered %d reports against a rolled-back server, want 0", delivered)
	}
	if pending != 1 {
		t.Fatalf("pending = %d, want the report retained", pending)
	}
	mu.Lock()
	sends := len(bodies)
	mu.Unlock()
	if sends != 0 {
		t.Fatalf("terminal requests = %d, want 0 while the server cannot prove fence support", sends)
	}
	if items, err := rolledBack.terminalReports.list(); err != nil || len(items) != 1 {
		t.Fatalf("queue = %d records (%v), want the retained report", len(items), err)
	}

	// Capability restored: the same report replays normally, generation intact.
	rolledBack.observeServerCapabilities([]string{protocol.TerminalReportGenerationFenceV1})
	if pending, delivered := rolledBack.replayPendingTerminalReports(context.Background()); pending != 0 || delivered != 1 {
		t.Fatalf("replay after capability restored pending=%d delivered=%d, want 0/1", pending, delivered)
	}
	mu.Lock()
	defer mu.Unlock()
	if len(bodies) != 1 {
		t.Fatalf("terminal requests = %d, want the one replay", len(bodies))
	}
	if got, _ := bodies[0]["expected_dispatched_at"].(string); got != report.claimGeneration.dispatchedAt.Format(time.RFC3339Nano) {
		t.Fatalf("replayed expected_dispatched_at = %q, want the persisted generation", got)
	}
}

// TestUnobservedServerCapabilityHoldsReplayFailClosed: before the first
// heartbeat ack there is no proof either way, and silence must not be read as
// support.
func TestUnobservedServerCapabilityHoldsReplayFailClosed(t *testing.T) {
	d := newIdentityTestDaemon(t)
	report := generationReport("task-unobserved", testClaimGeneration(), "answer")
	if err := d.terminalReports.enqueue(report); err != nil {
		t.Fatalf("enqueue: %v", err)
	}
	var delivered atomic.Int32
	d.terminalReportSend = func(context.Context, terminalTaskReport, []time.Duration) error {
		delivered.Add(1)
		return nil
	}
	pending, sent := d.replayPendingTerminalReports(context.Background())
	if sent != 0 || delivered.Load() != 0 {
		t.Fatalf("replay before any capability proof delivered %d reports (pending=%d)", delivered.Load(), pending)
	}
	if pending != 1 {
		t.Fatalf("pending = %d, want the report retained", pending)
	}
	if d.serverSupportsTerminalReportFence() {
		t.Fatal("an unobserved server reported fence support")
	}
}

// TestHeartbeatCapabilitiesFeedTheFenceGate pins the production wiring: the
// capability the heartbeat ack advertises is what opens the replay gate.
func TestHeartbeatCapabilitiesFeedTheFenceGate(t *testing.T) {
	d := New(Config{ServerBaseURL: "https://api.example.test", WorkspacesRoot: t.TempDir(), DaemonID: "daemon-hb"},
		slog.New(slog.NewTextHandler(io.Discard, nil)))
	if d.serverSupportsTerminalReportFence() {
		t.Fatal("fence support was assumed before any heartbeat")
	}
	d.handleHeartbeatActions(context.Background(), "rt-1", &HeartbeatResponse{
		RuntimeID:          "rt-1",
		ServerCapabilities: []string{protocol.DaemonCapabilityRPCV1, protocol.TerminalReportGenerationFenceV1},
	})
	if !d.serverSupportsTerminalReportFence() {
		t.Fatal("heartbeat capability did not enable the fence gate")
	}
	d.handleHeartbeatActions(context.Background(), "rt-1", &HeartbeatResponse{RuntimeID: "rt-1"})
	if d.serverSupportsTerminalReportFence() {
		t.Fatal("a heartbeat without the capability left the fence gate open")
	}
}

func TestTerminalReportQueueHoldsGenerationlessRecordsWithoutCapability(t *testing.T) {
	// The v1 path is unchanged by the capability work: a legacy record is still
	// retained and still never replayed, capability or not.
	d := newIdentityTestDaemon(t)
	serverAdvertisesFence(d)
	legacy := persistedTerminalTaskReport{
		Version: legacyTerminalReportRecordVersion, CreatedAt: time.Now().UTC(),
		Kind: "complete", TaskID: "task-legacy-capability", Output: "legacy",
	}
	name := legacyTerminalReportFileName(legacy.TaskID)
	if err := d.terminalReports.ensureDir(); err != nil {
		t.Fatalf("prepare queue: %v", err)
	}
	if err := writeTerminalReportRecord(d.terminalReports.dir, name, legacy); err != nil {
		t.Fatalf("write legacy record: %v", err)
	}
	var delivered atomic.Int32
	d.terminalReportSend = func(context.Context, terminalTaskReport, []time.Duration) error {
		delivered.Add(1)
		return nil
	}
	pending, sent := d.replayPendingTerminalReports(context.Background())
	if sent != 0 || delivered.Load() != 0 {
		t.Fatalf("legacy record was delivered (%d sends, pending=%d)", delivered.Load(), pending)
	}
	if _, err := os.Stat(filepath.Join(d.terminalReports.dir, name)); err != nil {
		t.Fatalf("legacy record disappeared: %v", err)
	}
}

func TestTerminalReportExpectedDispatchedAtOmittedWithoutCapability(t *testing.T) {
	// The single place a terminal body is stamped must add the field exactly when
	// the report carries a generation — which only a capability-advertising claim
	// produces — and never merely because the claim had a timestamp.
	body := map[string]any{"output": "answer"}
	addClaimGeneration(body, claimGeneration{}.dispatchedAt)
	if _, present := body["expected_dispatched_at"]; present {
		t.Fatalf("legacy body carried a fence: %v", body)
	}
	fenced := claimGeneration{dispatchedAt: testClaimGeneration()}
	addClaimGeneration(body, fenced.dispatchedAt)
	if got, _ := body["expected_dispatched_at"].(string); got != fenced.dispatchedAt.Format(time.RFC3339Nano) {
		t.Fatalf("fenced body expected_dispatched_at = %q, want the generation", got)
	}
}
