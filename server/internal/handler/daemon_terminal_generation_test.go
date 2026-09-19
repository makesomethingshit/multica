package handler

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/multica-ai/multica/server/internal/testutil"
	"github.com/multica-ai/multica/server/pkg/protocol"
)

// These tests pin the claim-generation fence on the terminal callbacks: the
// dispatched_at the server issued on the claim is compared inside the very
// UPDATE that performs the terminal transition, so a report produced by an
// older claim generation can never settle a later reclaim of the same task id.
// They run against the real database because the guarantee lives in that SQL.

const terminalFenceDaemonID = "terminal-fence-daemon"

// fencedTerminalTask seeds one task in the given non-terminal status, with a
// claim generation already stamped on the row, and returns the task id and the
// exact generation the server issued for it.
func fencedTerminalTask(t *testing.T, status string, extra testutil.Cols) (string, time.Time) {
	t.Helper()

	var agentID, runtimeID string
	dbfx.QueryRow(t, `SELECT a.id, a.runtime_id FROM agent a WHERE a.workspace_id = $1 LIMIT 1`,
		testWorkspaceID).Scan(&agentID, &runtimeID)

	cols := testutil.Cols{
		"runtime_id": runtimeID,
		// The daemon access check resolves the task's workspace through one of
		// its source links, so the fixture needs a real issue behind the task.
		"issue_id":      dbfx.Issue(t, "terminal fence "+status),
		"status":        status,
		"dispatched_at": testutil.Raw("now()"),
	}
	if status == "running" {
		cols["started_at"] = testutil.Raw("now()")
	}
	for key, value := range extra {
		cols[key] = value
	}
	taskID := dbfx.Task(t, agentID, cols)

	var generation time.Time
	dbfx.QueryRow(t, `SELECT dispatched_at FROM agent_task_queue WHERE id = $1`, taskID).Scan(&generation)
	return taskID, generation
}

// postTerminalCallback drives the real handler for one terminal callback.
func postTerminalCallback(t *testing.T, taskID, endpoint string, body map[string]any) *httptest.ResponseRecorder {
	t.Helper()
	w := httptest.NewRecorder()
	req := newDaemonTokenRequest(http.MethodPost, "/api/daemon/tasks/"+taskID+"/"+endpoint, body,
		testWorkspaceID, terminalFenceDaemonID)
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("taskId", taskID)
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))
	if endpoint == "complete" {
		testHandler.CompleteTask(w, req)
	} else {
		testHandler.FailTask(w, req)
	}
	return w
}

// reclaimTask moves the row to a fresh claim generation, the way the stale
// reclaim sweep does, without touching the status.
func reclaimTask(t *testing.T, taskID string) time.Time {
	t.Helper()
	if _, err := testPool.Exec(context.Background(),
		`UPDATE agent_task_queue SET dispatched_at = now() + interval '1 second' WHERE id = $1`, taskID); err != nil {
		t.Fatalf("reclaim task: %v", err)
	}
	var generation time.Time
	dbfx.QueryRow(t, `SELECT dispatched_at FROM agent_task_queue WHERE id = $1`, taskID).Scan(&generation)
	return generation
}

func taskRowState(t *testing.T, taskID string) (status string, generation time.Time, result []byte, completedAt *time.Time) {
	t.Helper()
	dbfx.QueryRow(t, `SELECT status, dispatched_at, result, completed_at FROM agent_task_queue WHERE id = $1`, taskID).
		Scan(&status, &generation, &result, &completedAt)
	return status, generation, result, completedAt
}

func responseCode(t *testing.T, w *httptest.ResponseRecorder) string {
	t.Helper()
	var body map[string]any
	if err := json.Unmarshal(w.Body.Bytes(), &body); err != nil {
		t.Fatalf("decode response body %q: %v", w.Body.String(), err)
	}
	code, _ := body["code"].(string)
	return code
}

func TestCompleteTaskClaimGenerationFence(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database not available")
	}

	t.Run("matching generation completes", func(t *testing.T) {
		taskID, generation := fencedTerminalTask(t, "running", nil)
		w := postTerminalCallback(t, taskID, "complete", map[string]any{
			"output":                 "done",
			"expected_dispatched_at": generation.UTC().Format(time.RFC3339Nano),
		})
		if w.Code != http.StatusOK {
			t.Fatalf("complete with matching generation: got %d: %s", w.Code, w.Body.String())
		}
		if status, _, _, _ := taskRowState(t, taskID); status != "completed" {
			t.Fatalf("status = %q, want completed", status)
		}
	})

	t.Run("replayed callback after the terminal transition stays successful", func(t *testing.T) {
		taskID, generation := fencedTerminalTask(t, "running", nil)
		body := map[string]any{
			"output":                 "done",
			"expected_dispatched_at": generation.UTC().Format(time.RFC3339Nano),
		}
		if w := postTerminalCallback(t, taskID, "complete", body); w.Code != http.StatusOK {
			t.Fatalf("first callback: got %d: %s", w.Code, w.Body.String())
		}
		// The first response was lost in transit; the daemon replays the same
		// report. Server-terminal state wins and the answer stays a success.
		if w := postTerminalCallback(t, taskID, "complete", body); w.Code != http.StatusOK {
			t.Fatalf("replayed callback: got %d: %s", w.Code, w.Body.String())
		}
		if status, _, _, _ := taskRowState(t, taskID); status != "completed" {
			t.Fatalf("status = %q, want completed", status)
		}
	})

	t.Run("stale generation after a reclaim never completes the new claim", func(t *testing.T) {
		taskID, generation := fencedTerminalTask(t, "running", nil)
		reclaimed := reclaimTask(t, taskID)

		w := postTerminalCallback(t, taskID, "complete", map[string]any{
			"output":                 "result of the old claim",
			"expected_dispatched_at": generation.UTC().Format(time.RFC3339Nano),
		})
		if w.Code != http.StatusConflict {
			t.Fatalf("stale complete: got %d: %s, want 409", w.Code, w.Body.String())
		}
		if code := responseCode(t, w); code != protocol.DaemonTaskClaimGenerationMismatchCode {
			t.Fatalf("conflict code = %q, want %q", code, protocol.DaemonTaskClaimGenerationMismatchCode)
		}
		status, current, result, completedAt := taskRowState(t, taskID)
		if status != "running" {
			t.Fatalf("reclaimed task status = %q, want running (untouched)", status)
		}
		if !current.Equal(reclaimed) {
			t.Fatalf("reclaimed generation = %s, want %s", current, reclaimed)
		}
		if result != nil || completedAt != nil {
			t.Fatalf("reclaimed task was mutated: result=%s completed_at=%v", result, completedAt)
		}
	})

	t.Run("cancelled row keeps its outcome when an old report arrives", func(t *testing.T) {
		taskID, generation := fencedTerminalTask(t, "running", nil)
		if _, err := testPool.Exec(context.Background(),
			`UPDATE agent_task_queue SET status = 'cancelled', completed_at = now() WHERE id = $1`, taskID); err != nil {
			t.Fatalf("cancel task: %v", err)
		}
		w := postTerminalCallback(t, taskID, "complete", map[string]any{
			"output":                 "late result",
			"expected_dispatched_at": generation.UTC().Format(time.RFC3339Nano),
		})
		if w.Code != http.StatusOK {
			t.Fatalf("cancelled replay: got %d: %s, want idempotent 200", w.Code, w.Body.String())
		}
		if status, _, _, _ := taskRowState(t, taskID); status != "cancelled" {
			t.Fatalf("status = %q, want cancelled", status)
		}
	})

	t.Run("legacy callback without a generation still completes", func(t *testing.T) {
		taskID, _ := fencedTerminalTask(t, "running", nil)
		if w := postTerminalCallback(t, taskID, "complete", map[string]any{"output": "old daemon"}); w.Code != http.StatusOK {
			t.Fatalf("unfenced complete: got %d: %s", w.Code, w.Body.String())
		}
		if status, _, _, _ := taskRowState(t, taskID); status != "completed" {
			t.Fatalf("status = %q, want completed", status)
		}
	})

	t.Run("malformed generation is refused instead of silently unfenced", func(t *testing.T) {
		taskID, _ := fencedTerminalTask(t, "running", nil)
		w := postTerminalCallback(t, taskID, "complete", map[string]any{
			"output":                 "done",
			"expected_dispatched_at": "yesterday",
		})
		if w.Code != http.StatusBadRequest {
			t.Fatalf("malformed generation: got %d: %s, want 400", w.Code, w.Body.String())
		}
		if status, _, _, _ := taskRowState(t, taskID); status != "running" {
			t.Fatalf("status = %q, want running (untouched)", status)
		}
	})

	// The server-terminal case that is NOT idempotent: the row was settled by a
	// later claim. Acknowledging the older report as "already finalized" would
	// tell the daemon its stale result had landed, and the payload would be
	// deleted instead of retired.
	t.Run("stale completion after the reclaim already completed is a conflict", func(t *testing.T) {
		taskID, generation := fencedTerminalTask(t, "running", nil)
		reclaimed := reclaimTask(t, taskID)
		if w := postTerminalCallback(t, taskID, "complete", map[string]any{
			"output":                 "result of the new claim",
			"expected_dispatched_at": reclaimed.UTC().Format(time.RFC3339Nano),
		}); w.Code != http.StatusOK {
			t.Fatalf("new claim could not settle its own row: got %d: %s", w.Code, w.Body.String())
		}

		w := postTerminalCallback(t, taskID, "complete", map[string]any{
			"output":                 "result of the old claim",
			"expected_dispatched_at": generation.UTC().Format(time.RFC3339Nano),
		})
		if w.Code != http.StatusConflict {
			t.Fatalf("stale completion after a newer settlement: got %d: %s, want 409", w.Code, w.Body.String())
		}
		if code := responseCode(t, w); code != protocol.DaemonTaskClaimGenerationMismatchCode {
			t.Fatalf("conflict code = %q, want %q", code, protocol.DaemonTaskClaimGenerationMismatchCode)
		}
		status, current, result, _ := taskRowState(t, taskID)
		if status != "completed" || !current.Equal(reclaimed) {
			t.Fatalf("settled row changed: status=%s generation=%s", status, current)
		}
		if !json.Valid(result) || !strings.Contains(string(result), "result of the new claim") {
			t.Fatalf("settled result was overwritten: %s", result)
		}
	})
}

func TestFailTaskClaimGenerationFence(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database not available")
	}

	for _, status := range []string{"dispatched", "running", "waiting_local_directory"} {
		t.Run("matching generation fails a "+status+" task", func(t *testing.T) {
			taskID, generation := fencedTerminalTask(t, status, nil)
			w := postTerminalCallback(t, taskID, "fail", map[string]any{
				"error":                  "provider failed",
				"failure_reason":         "agent_error.process_failure",
				"expected_dispatched_at": generation.UTC().Format(time.RFC3339Nano),
			})
			if w.Code != http.StatusOK {
				t.Fatalf("fail with matching generation: got %d: %s", w.Code, w.Body.String())
			}
			if got, _, _, _ := taskRowState(t, taskID); got != "failed" {
				t.Fatalf("status = %q, want failed", got)
			}
		})
	}

	t.Run("stale generation leaves the reclaim untouched", func(t *testing.T) {
		taskID, generation := fencedTerminalTask(t, "running", nil)
		reclaimed := reclaimTask(t, taskID)

		w := postTerminalCallback(t, taskID, "fail", map[string]any{
			"error":                  "old claim failed",
			"expected_dispatched_at": generation.UTC().Format(time.RFC3339Nano),
		})
		if w.Code != http.StatusConflict {
			t.Fatalf("stale fail: got %d: %s, want 409", w.Code, w.Body.String())
		}
		if code := responseCode(t, w); code != protocol.DaemonTaskClaimGenerationMismatchCode {
			t.Fatalf("conflict code = %q, want %q", code, protocol.DaemonTaskClaimGenerationMismatchCode)
		}
		status, current, result, completedAt := taskRowState(t, taskID)
		if status != "running" {
			t.Fatalf("reclaim status = %q, want running", status)
		}
		if !current.Equal(reclaimed) {
			t.Fatalf("reclaim generation = %s, want %s", current, reclaimed)
		}
		if result != nil || completedAt != nil {
			t.Fatalf("reclaim was mutated: result=%s completed_at=%v", result, completedAt)
		}
	})

	t.Run("stale prelaunch failure leaves the reclaim untouched", func(t *testing.T) {
		taskID, generation := fencedTerminalTask(t, "waiting_local_directory", nil)
		reclaimTask(t, taskID)
		w := postTerminalCallback(t, taskID, "fail", map[string]any{
			"error":                  "old claim never launched",
			"expected_dispatched_at": generation.UTC().Format(time.RFC3339Nano),
		})
		if w.Code != http.StatusConflict {
			t.Fatalf("stale prelaunch fail: got %d: %s, want 409", w.Code, w.Body.String())
		}
		if status, _, _, _ := taskRowState(t, taskID); status != "waiting_local_directory" {
			t.Fatalf("status = %q, want waiting_local_directory (untouched)", status)
		}
	})

	t.Run("legacy callback without a generation still fails the task", func(t *testing.T) {
		taskID, _ := fencedTerminalTask(t, "dispatched", nil)
		if w := postTerminalCallback(t, taskID, "fail", map[string]any{"error": "old daemon"}); w.Code != http.StatusOK {
			t.Fatalf("unfenced fail: got %d: %s", w.Code, w.Body.String())
		}
		if got, _, _, _ := taskRowState(t, taskID); got != "failed" {
			t.Fatalf("status = %q, want failed", got)
		}
	})

	t.Run("stale failure after the reclaim already failed is a conflict", func(t *testing.T) {
		taskID, generation := fencedTerminalTask(t, "running", nil)
		reclaimed := reclaimTask(t, taskID)
		if w := postTerminalCallback(t, taskID, "fail", map[string]any{
			"error":                  "the new claim failed",
			"expected_dispatched_at": reclaimed.UTC().Format(time.RFC3339Nano),
		}); w.Code != http.StatusOK {
			t.Fatalf("new claim could not fail its own row: got %d: %s", w.Code, w.Body.String())
		}

		w := postTerminalCallback(t, taskID, "fail", map[string]any{
			"error":                  "the old claim failed",
			"expected_dispatched_at": generation.UTC().Format(time.RFC3339Nano),
		})
		if w.Code != http.StatusConflict {
			t.Fatalf("stale failure after a newer settlement: got %d: %s, want 409", w.Code, w.Body.String())
		}
		if code := responseCode(t, w); code != protocol.DaemonTaskClaimGenerationMismatchCode {
			t.Fatalf("conflict code = %q, want %q", code, protocol.DaemonTaskClaimGenerationMismatchCode)
		}
		status, current, _, _ := taskRowState(t, taskID)
		if status != "failed" || !current.Equal(reclaimed) {
			t.Fatalf("settled row changed: status=%s generation=%s", status, current)
		}
	})
}

// TestTerminalCallbackFenceIsTheUpdateAndNotAPriorLookup documents the TOCTOU
// shape: the generation the callback carries was read from the row before the
// reclaim, so any "GET the row, compare, then POST" implementation would have
// seen a live row and let the stale report through. Only the comparison inside
// the terminal UPDATE rejects it.
func TestTerminalCallbackFenceIsTheUpdateAndNotAPriorLookup(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database not available")
	}

	taskID, generation := fencedTerminalTask(t, "running", nil)
	// The reclaim lands after the daemon has its generation in hand and before
	// its callback reaches the terminal UPDATE.
	reclaimed := reclaimTask(t, taskID)

	w := postTerminalCallback(t, taskID, "complete", map[string]any{
		"output":                 "hard-fought result",
		"expected_dispatched_at": generation.UTC().Format(time.RFC3339Nano),
	})
	if w.Code != http.StatusConflict {
		t.Fatalf("reclaim race: got %d: %s, want 409", w.Code, w.Body.String())
	}
	status, current, result, _ := taskRowState(t, taskID)
	if status != "running" || !current.Equal(reclaimed) || result != nil {
		t.Fatalf("row changed under the race: status=%s generation=%s result=%s", status, current, result)
	}

	// The reclaim that owns the row can still settle it with its own generation.
	if w := postTerminalCallback(t, taskID, "complete", map[string]any{
		"output":                 "result of the new claim",
		"expected_dispatched_at": reclaimed.UTC().Format(time.RFC3339Nano),
	}); w.Code != http.StatusOK {
		t.Fatalf("new claim could not settle its own row: got %d: %s", w.Code, w.Body.String())
	}
}

// TestClaimPayloadRoundTripsClaimGeneration pins the other half of the fence:
// the generation the daemon echoes must be the one the claim payload carried.
// A claim payload truncated to seconds would make every fenced callback miss
// its own claim whenever the reclaim or the callback landed in a later second.
func TestClaimPayloadRoundTripsClaimGeneration(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database not available")
	}
	ctx := context.Background()

	runtimeID := dbfx.Runtime(t, "claim generation runtime", nil)
	agentID := dbfx.Agent(t, "claim generation agent", runtimeID)
	issueID := dbfx.Issue(t, "claim generation issue")
	taskID := dbfx.Task(t, agentID, testutil.Cols{
		"runtime_id": runtimeID,
		"issue_id":   issueID,
		"status":     "queued",
	})

	w := postBatchClaim(t, testWorkspaceID, []string{runtimeID}, 1)
	if w.Code != http.StatusOK {
		t.Fatalf("batch claim: got %d: %s", w.Code, w.Body.String())
	}
	var resp struct {
		Tasks []struct {
			ID           string `json:"id"`
			DispatchedAt string `json:"dispatched_at"`
		} `json:"tasks"`
	}
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode claim response: %v", err)
	}
	if len(resp.Tasks) != 1 || resp.Tasks[0].ID != taskID {
		t.Fatalf("claim returned %+v, want task %s", resp.Tasks, taskID)
	}

	var stored time.Time
	if err := testPool.QueryRow(ctx, `SELECT dispatched_at FROM agent_task_queue WHERE id = $1`, taskID).Scan(&stored); err != nil {
		t.Fatalf("read claimed generation: %v", err)
	}
	parsed, err := time.Parse(time.RFC3339Nano, resp.Tasks[0].DispatchedAt)
	if err != nil {
		t.Fatalf("claim dispatched_at %q is not RFC3339Nano: %v", resp.Tasks[0].DispatchedAt, err)
	}
	if !parsed.Equal(stored) {
		t.Fatalf("claim dispatched_at = %s, stored = %s: the daemon cannot echo what it never received",
			parsed.UTC(), stored.UTC())
	}
	if truncated := stored.Truncate(time.Second); !truncated.Equal(stored) && parsed.Equal(truncated) {
		t.Fatalf("claim dispatched_at = %s lost the sub-second precision of %s", parsed.UTC(), stored.UTC())
	}
}
