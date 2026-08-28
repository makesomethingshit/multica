package handler

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/multica-ai/multica/server/pkg/taskfailure"
)

// batchClaimResponse mirrors the {"tasks":[...]} envelope ClaimTasksByRuntime
// returns, with the few fields these tests assert on.
type batchClaimResponse struct {
	Tasks []struct {
		ID                string `json:"id"`
		RuntimeID         string `json:"runtime_id"`
		AuthToken         string `json:"auth_token"`
		ActiveSiblingRuns []struct {
			TaskID          string `json:"task_id"`
			IssueID         string `json:"issue_id"`
			IssueIdentifier string `json:"issue_identifier"`
			IssueTitle      string `json:"issue_title"`
			Stale           bool   `json:"stale"`
			AgentName       string `json:"agent_name"`
			Status          string `json:"status"`
			CreatedAt       string `json:"created_at"`
			StartedAt       string `json:"started_at"`
		} `json:"active_sibling_runs"`
	} `json:"tasks"`
}

// insertClaimTestIssue inserts a test issue, optionally under the given parent
// (non-nil for a sub-issue), and registers cleanup. Its number is drawn from the
// workspace max so the unique-number constraint cannot trip on repeated runs.
func insertClaimTestIssue(t *testing.T, ctx context.Context, title string, parentID *string) string {
	t.Helper()
	var id string
	if err := testPool.QueryRow(ctx, `
		INSERT INTO issue (workspace_id, title, status, priority, creator_id, creator_type, number, position, parent_issue_id)
		VALUES ($1, $2, 'todo', 'none', $3, 'member',
			(SELECT COALESCE(MAX(number), 82649) + 1 FROM issue WHERE workspace_id = $1), 0, $4)
		RETURNING id
	`, testWorkspaceID, title, testUserID, parentID).Scan(&id); err != nil {
		t.Fatalf("insert issue %q: %v", title, err)
	}
	t.Cleanup(func() { testPool.Exec(ctx, `DELETE FROM issue WHERE id = $1`, id) })
	return id
}

func seedQueuedIssueTask(t *testing.T, ctx context.Context, agentID, runtimeID, issueID string) string {
	t.Helper()
	var id string
	if err := testPool.QueryRow(ctx, `
		INSERT INTO agent_task_queue (agent_id, runtime_id, issue_id, status, priority)
		VALUES ($1, $2, $3, 'queued', 0)
		RETURNING id
	`, agentID, runtimeID, issueID).Scan(&id); err != nil {
		t.Fatalf("seed queued task: %v", err)
	}
	t.Cleanup(func() { testPool.Exec(ctx, `DELETE FROM agent_task_queue WHERE id = $1`, id) })
	return id
}

func seedActiveIssueTask(t *testing.T, ctx context.Context, agentID, issueID, status, age string) string {
	t.Helper()
	var id string
	if err := testPool.QueryRow(ctx, `
		INSERT INTO agent_task_queue (agent_id, runtime_id, issue_id, status, priority, created_at, started_at)
		VALUES ($1, (SELECT runtime_id FROM agent WHERE id = $1), $2, $3, 0,
			now() - ($4::interval), now() - ($4::interval))
		RETURNING id
	`, agentID, issueID, status, age).Scan(&id); err != nil {
		t.Fatalf("seed %s task: %v", status, err)
	}
	t.Cleanup(func() { testPool.Exec(ctx, `DELETE FROM agent_task_queue WHERE id = $1`, id) })
	return id
}

func postBatchClaim(t *testing.T, workspaceID string, runtimeIDs []string, maxTasks int) *httptest.ResponseRecorder {
	t.Helper()
	w := httptest.NewRecorder()
	req := newDaemonTokenRequest("POST", "/api/daemon/tasks/claim",
		map[string]any{"daemon_id": batchClaimTestDaemonID, "runtime_ids": runtimeIDs, "max_tasks": maxTasks},
		workspaceID, batchClaimTestDaemonID)
	testHandler.ClaimTasksByRuntime(w, req)
	return w
}

// batchClaimTestDaemonID is the daemon id used by both the mdt_ token context
// and the request body in batch-claim handler tests, so the daemon_id
// consistency check passes on the happy path.
const batchClaimTestDaemonID = "batch-claim-review"

// TestClaimTasksByRuntime_RoutesAcrossRuntimesAndMintsTokens covers the happy
// path: one call claims across two runtimes on the same machine, returns one
// task per runtime (per-agent dedup), and mints a task-scoped token for each.
func TestClaimTasksByRuntime_RoutesAcrossRuntimesAndMintsTokens(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database not available")
	}
	ctx := context.Background()

	rt1 := createClaimReclaimRuntime(t, ctx, "Batch claim rt1")
	rt2 := createClaimReclaimRuntime(t, ctx, "Batch claim rt2")
	a1, i1 := createClaimReclaimAgentAndIssue(t, ctx, rt1, "Batch claim a1")
	a2, i2 := createClaimReclaimAgentAndIssue(t, ctx, rt2, "Batch claim a2")
	seedQueuedIssueTask(t, ctx, a1, rt1, i1)
	seedQueuedIssueTask(t, ctx, a2, rt2, i2)

	w := postBatchClaim(t, testWorkspaceID, []string{rt1, rt2}, 5)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var resp batchClaimResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp.Tasks) != 2 {
		t.Fatalf("claimed %d tasks, want 2: %s", len(resp.Tasks), w.Body.String())
	}
	seen := map[string]int{}
	for _, task := range resp.Tasks {
		seen[task.RuntimeID]++
		if !strings.HasPrefix(task.AuthToken, "mat_") {
			t.Fatalf("task %s missing mat_ task token, got %q", task.ID, task.AuthToken)
		}
	}
	if seen[rt1] != 1 || seen[rt2] != 1 {
		t.Fatalf("runtime distribution = %v, want one task each for rt1/rt2", seen)
	}
}

func TestClaimTasksByRuntime_IncludesActiveSiblingRun(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database not available")
	}
	ctx := context.Background()
	runtimeID := createClaimReclaimRuntime(t, ctx, "Sibling claim runtime")
	agentID, _ := createClaimReclaimAgentAndIssue(t, ctx, runtimeID, "Sibling claim agent")
	if _, err := testPool.Exec(ctx, `UPDATE agent SET max_concurrent_tasks = 2 WHERE id = $1`, agentID); err != nil {
		t.Fatalf("raise concurrency: %v", err)
	}
	peerAgentID := dbfx.Agent(t, "Sibling peer agent", runtimeID)

	// Six active tasks under the same parent exercise cross-agent inclusion,
	// status priority, newest-first ordering, and the five-result cap.
	parentID := insertClaimTestIssue(t, ctx, "sibling parent", nil)
	seeds := []struct {
		title, status, age string
	}{
		{"running new", "running", "1 minute"},
		{"running old", "running", "2 minutes"},
		{"waiting new", "waiting_local_directory", "3 minutes"},
		{"waiting old", "waiting_local_directory", "4 minutes"},
		{"dispatched new", "dispatched", "5 minutes"},
		{"dispatched old", "dispatched", "6 minutes"},
	}
	for _, seed := range seeds {
		issueID := insertClaimTestIssue(t, ctx, seed.title, &parentID)
		seedActiveIssueTask(t, ctx, peerAgentID, issueID, seed.status, seed.age)
	}
	targetIssueID := insertClaimTestIssue(t, ctx, "sibling target", &parentID)
	queuedID := seedQueuedIssueTask(t, ctx, agentID, runtimeID, targetIssueID)

	// Queued work and active work under another parent must both be excluded.
	queuedSiblingIssueID := insertClaimTestIssue(t, ctx, "queued sibling", &parentID)
	seedQueuedIssueTask(t, ctx, agentID, runtimeID, queuedSiblingIssueID)
	otherParentID := insertClaimTestIssue(t, ctx, "other parent", nil)
	otherIssueID := insertClaimTestIssue(t, ctx, "other parent's child", &otherParentID)
	seedActiveIssueTask(t, ctx, peerAgentID, otherIssueID, "running", "30 seconds")

	w := postBatchClaim(t, testWorkspaceID, []string{runtimeID}, 1)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var resp batchClaimResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp.Tasks) != 1 || resp.Tasks[0].ID != queuedID {
		t.Fatalf("claimed task = %+v, want %s", resp.Tasks, queuedID)
	}
	runs := resp.Tasks[0].ActiveSiblingRuns
	if len(runs) != 5 {
		t.Fatalf("active_sibling_runs = %+v, want five", runs)
	}
	wantTitles := []string{"running new", "running old", "waiting new", "waiting old", "dispatched new"}
	for i, run := range runs {
		if run.IssueTitle != wantTitles[i] || run.AgentName != "Sibling peer agent" || run.TaskID == "" ||
			run.IssueIdentifier == "" || run.CreatedAt == "" {
			t.Fatalf("active_sibling_runs[%d] = %+v, want title %q with complete peer data", i, run, wantTitles[i])
		}
		if i < 2 && (run.Status != "running" || run.StartedAt == "") {
			t.Fatalf("active_sibling_runs[%d] = %+v, want started running task", i, run)
		}
		if !run.Stale {
			t.Fatalf("active_sibling_runs[%d] = %+v, want unseen peer marked stale", i, run)
		}
	}
}

// TestClaimTasksByRuntime_OmitsSiblingRunsForRootIssue pins that a top-level
// (parentless) issue task does not surface peer awareness at all — there is no
// same-parent cohort to reveal, so the read is skipped rather than warning
// about an unrelated top-level issue.
func TestClaimTasksByRuntime_OmitsSiblingRunsForRootIssue(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database not available")
	}
	ctx := context.Background()
	runtimeID := createClaimReclaimRuntime(t, ctx, "Root sibling runtime")
	agentID, withSiblingRoot := createClaimReclaimAgentAndIssue(t, ctx, runtimeID, "Root sibling agent")
	if _, err := testPool.Exec(ctx, `UPDATE agent SET max_concurrent_tasks = 2 WHERE id = $1`, agentID); err != nil {
		t.Fatalf("raise concurrency: %v", err)
	}
	// A running task on a DIFFERENT top-level issue for the same agent.
	insertRunningIssueTask(t, agentID, withSiblingRoot)
	targetRootID := insertClaimTestIssue(t, ctx, "root target", nil)
	queuedID := seedQueuedIssueTask(t, ctx, agentID, runtimeID, targetRootID)

	w := postBatchClaim(t, testWorkspaceID, []string{runtimeID}, 1)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var resp batchClaimResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp.Tasks) != 1 || resp.Tasks[0].ID != queuedID {
		t.Fatalf("claimed task = %+v, want %s", resp.Tasks, queuedID)
	}
	if len(resp.Tasks[0].ActiveSiblingRuns) != 0 {
		t.Fatalf("active_sibling_runs = %+v, want none for a root issue task", resp.Tasks[0].ActiveSiblingRuns)
	}
}

// TestClaimTasksByRuntime_SkipsCrossWorkspaceRuntime is the security-critical
// case: a daemon token scoped to workspace A must not claim a task routed to a
// runtime in workspace B, even when B's runtime_id is included in the request.
func TestClaimTasksByRuntime_SkipsCrossWorkspaceRuntime(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database not available")
	}
	ctx := context.Background()

	// A foreign workspace with its own runtime + agent + queued task.
	var foreignUser, foreignWS string
	if err := testPool.QueryRow(ctx, `INSERT INTO "user" (name, email) VALUES ('Foreign User', 'batch-foreign@multica.ai') RETURNING id`).Scan(&foreignUser); err != nil {
		t.Fatalf("foreign user: %v", err)
	}
	t.Cleanup(func() { testPool.Exec(ctx, `DELETE FROM "user" WHERE id = $1`, foreignUser) })
	if err := testPool.QueryRow(ctx, `INSERT INTO workspace (name, slug, description, issue_prefix) VALUES ('Foreign WS','batch-foreign-ws','x','FGN') RETURNING id`).Scan(&foreignWS); err != nil {
		t.Fatalf("foreign workspace: %v", err)
	}
	t.Cleanup(func() { testPool.Exec(ctx, `DELETE FROM workspace WHERE id = $1`, foreignWS) })
	if _, err := testPool.Exec(ctx, `INSERT INTO member (workspace_id, user_id, role) VALUES ($1,$2,'owner')`, foreignWS, foreignUser); err != nil {
		t.Fatalf("foreign member: %v", err)
	}
	var foreignRT, foreignAgent, foreignIssue string
	if err := testPool.QueryRow(ctx, `
		INSERT INTO agent_runtime (workspace_id, daemon_id, name, runtime_mode, provider, status, device_info, metadata, last_seen_at, visibility, owner_id)
		VALUES ($1, NULL, 'Foreign RT', 'cloud', 'handler_test_runtime', 'online', 'x', '{}'::jsonb, now(), 'private', $2)
		RETURNING id`, foreignWS, foreignUser).Scan(&foreignRT); err != nil {
		t.Fatalf("foreign runtime: %v", err)
	}
	if err := testPool.QueryRow(ctx, `
		INSERT INTO agent (workspace_id, name, description, runtime_mode, runtime_config, runtime_id, visibility, max_concurrent_tasks, owner_id)
		VALUES ($1, 'Foreign Agent', '', 'cloud', '{}'::jsonb, $2, 'private', 1, $3)
		RETURNING id`, foreignWS, foreignRT, foreignUser).Scan(&foreignAgent); err != nil {
		t.Fatalf("foreign agent: %v", err)
	}
	if err := testPool.QueryRow(ctx, `
		INSERT INTO issue (workspace_id, title, status, priority, creator_id, creator_type, number, position)
		VALUES ($1, 'foreign issue', 'in_progress', 'none', $2, 'member', 1, 0)
		RETURNING id`, foreignWS, foreignUser).Scan(&foreignIssue); err != nil {
		t.Fatalf("foreign issue: %v", err)
	}
	foreignTask := seedQueuedIssueTask(t, ctx, foreignAgent, foreignRT, foreignIssue)

	// Daemon token scoped to the (unrelated) handler-test workspace.
	w := postBatchClaim(t, testWorkspaceID, []string{foreignRT}, 5)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var resp batchClaimResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp.Tasks) != 0 {
		t.Fatalf("cross-workspace claim leaked %d tasks, want 0: %s", len(resp.Tasks), w.Body.String())
	}
	var status string
	if err := testPool.QueryRow(ctx, `SELECT status FROM agent_task_queue WHERE id = $1`, foreignTask).Scan(&status); err != nil {
		t.Fatalf("read foreign task status: %v", err)
	}
	if status != "queued" {
		t.Fatalf("foreign task status = %s, want still queued (untouched)", status)
	}
}

// TestClaimTasksByRuntime_CancelsTaskWhenRuntimeOwnerMissing pins the
// unscoped-credential guard: a runtime with no owner cannot mint a task token,
// so the claimed task must be cancelled and omitted from the response rather
// than shipped without a scoped credential.
func TestClaimTasksByRuntime_CancelsTaskWhenRuntimeOwnerMissing(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database not available")
	}
	ctx := context.Background()

	var rtNull string
	if err := testPool.QueryRow(ctx, `
		INSERT INTO agent_runtime (workspace_id, daemon_id, name, runtime_mode, provider, status, device_info, metadata, last_seen_at, visibility, owner_id)
		VALUES ($1, NULL, 'Ownerless RT', 'cloud', 'handler_test_runtime', 'online', 'x', '{}'::jsonb, now(), 'private', NULL)
		RETURNING id`, testWorkspaceID).Scan(&rtNull); err != nil {
		t.Fatalf("ownerless runtime: %v", err)
	}
	t.Cleanup(func() { testPool.Exec(ctx, `DELETE FROM agent_runtime WHERE id = $1`, rtNull) })

	agentID, issueID := createClaimReclaimAgentAndIssue(t, ctx, rtNull, "Ownerless agent")
	taskID := seedQueuedIssueTask(t, ctx, agentID, rtNull, issueID)

	w := postBatchClaim(t, testWorkspaceID, []string{rtNull}, 1)
	if w.Code != http.StatusOK {
		t.Fatalf("expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var resp batchClaimResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode: %v", err)
	}
	if len(resp.Tasks) != 0 {
		t.Fatalf("claimed %d tasks from owner-less runtime, want 0: %s", len(resp.Tasks), w.Body.String())
	}
	var status string
	if err := testPool.QueryRow(ctx, `SELECT status FROM agent_task_queue WHERE id = $1`, taskID).Scan(&status); err != nil {
		t.Fatalf("read task status: %v", err)
	}
	if status != "cancelled" {
		t.Fatalf("task status = %s, want cancelled (owner missing)", status)
	}
}

// TestFailClaimedTaskBeforeLaunchSettlesDispatchedTask pins the claim-build
// failure behavior used by required Plugin contributions. A durable rejection
// must become a visible terminal task instead of remaining dispatched until
// stale reclaim delivers the same impossible task again.
func TestFailClaimedTaskBeforeLaunchSettlesDispatchedTask(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database not available")
	}
	ctx := context.Background()
	runtimeID := createClaimReclaimRuntime(t, ctx, "Prelaunch failure rt")
	agentID, issueID := createClaimReclaimAgentAndIssue(t, ctx, runtimeID, "Prelaunch failure agent")
	taskID := seedQueuedIssueTask(t, ctx, agentID, runtimeID, issueID)
	if _, err := testPool.Exec(ctx, `
		UPDATE agent_task_queue
		SET status = 'dispatched', dispatched_at = now()
		WHERE id = $1
	`, taskID); err != nil {
		t.Fatalf("dispatch task: %v", err)
	}
	task, err := testHandler.Queries.GetAgentTask(ctx, parseUUID(taskID))
	if err != nil {
		t.Fatalf("load task: %v", err)
	}

	failure := testHandler.failClaimedTaskBeforeLaunch(
		ctx,
		&task,
		"Required Remote MCP is unavailable. Test the Plugin connection, then retry.",
		taskfailure.ReasonAgentMissingConfig,
		"error_required_remote_mcp",
		http.StatusConflict,
		"required Remote MCP contribution is unavailable",
	)
	if failure == nil || failure.outcome != "error_required_remote_mcp" || failure.status != http.StatusConflict {
		t.Fatalf("failure = %+v", failure)
	}

	var status, errorMessage, failureReason string
	if err := testPool.QueryRow(ctx, `
		SELECT status, error, failure_reason
		FROM agent_task_queue
		WHERE id = $1
	`, taskID).Scan(&status, &errorMessage, &failureReason); err != nil {
		t.Fatalf("read settled task: %v", err)
	}
	if status != "failed" {
		t.Fatalf("task status = %q, want failed", status)
	}
	if errorMessage != "Required Remote MCP is unavailable. Test the Plugin connection, then retry." {
		t.Fatalf("task error = %q", errorMessage)
	}
	if failureReason != taskfailure.ReasonAgentMissingConfig.String() {
		t.Fatalf("failure_reason = %q", failureReason)
	}
}
