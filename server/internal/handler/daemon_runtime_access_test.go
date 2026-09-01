package handler

import (
	"context"
	"encoding/json"
	"net/http"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/multica-ai/multica/server/internal/testutil"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
	"github.com/multica-ai/multica/server/pkg/taskfailure"
)

func TestClaimTaskByRuntime_OwnerlessAgentOnPrivateRuntimeRemainsClaimable(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database not available")
	}
	ctx := context.Background()
	runtimeID := createClaimReclaimRuntime(t, ctx, "Ownerless agent private runtime")
	agentID, issueID := createClaimReclaimAgentAndIssue(t, ctx, runtimeID, "Ownerless private agent")
	dbfx.Exec(t, `UPDATE agent SET owner_id = NULL WHERE id = $1`, agentID)
	taskID := seedQueuedIssueTask(t, ctx, agentID, runtimeID, issueID)

	req := newDaemonTokenRequest(http.MethodPost, "/api/daemon/runtimes/"+runtimeID+"/tasks/claim", nil,
		testWorkspaceID, "ownerless-agent-claim")
	req = withURLParam(req, "runtimeId", runtimeID)
	w := testutil.Call(t, testHandler.ClaimTaskByRuntime, req).Want(http.StatusOK)
	if !strings.Contains(w.Body.String(), taskID) {
		t.Fatalf("ClaimTaskByRuntime body = %q, want claimed task %s", w.Body.String(), taskID)
	}

	var status string
	dbfx.QueryRow(t, `
		SELECT status
		FROM agent_task_queue
		WHERE id = $1
	`, taskID).Scan(&status)
	if status != "dispatched" {
		t.Fatalf("task status = %q, want dispatched", status)
	}
}

func TestClaimTaskByRuntime_SettlesPrivateOwnerMismatch(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database not available")
	}
	ctx := context.Background()
	foreignOwnerID := dbfx.User(t, "Daemon mismatch owner", "daemon-mismatch-owner-"+uuid.NewString()+"@example.com")
	dbfx.Member(t, testWorkspaceID, foreignOwnerID, "member")
	runtimeID := createClaimReclaimRuntime(t, ctx, "Mismatch issue runtime")
	agentID, issueID := createClaimReclaimAgentAndIssue(t, ctx, runtimeID, "Mismatch issue agent")
	dbfx.Exec(t, `UPDATE agent SET owner_id = $1 WHERE id = $2`, foreignOwnerID, agentID)
	taskID := seedQueuedIssueTask(t, ctx, agentID, runtimeID, issueID)

	req := newDaemonTokenRequest(http.MethodPost, "/api/daemon/runtimes/"+runtimeID+"/tasks/claim", nil,
		testWorkspaceID, "mismatch-issue-claim")
	req = withURLParam(req, "runtimeId", runtimeID)
	w := testutil.Call(t, testHandler.ClaimTaskByRuntime, req).Want(http.StatusOK)
	if strings.TrimSpace(w.Body.String()) != `{"task":null}` {
		t.Fatalf("ClaimTaskByRuntime body = %q, want empty successful poll", w.Body.String())
	}

	var status, failureReason string
	dbfx.QueryRow(t, `SELECT status, failure_reason FROM agent_task_queue WHERE id = $1`, taskID).Scan(&status, &failureReason)
	if status != "failed" || failureReason != taskfailure.ReasonInvalidTaskIdentity.String() {
		t.Fatalf("task state = %q/%q, want failed/%s", status, failureReason, taskfailure.ReasonInvalidTaskIdentity)
	}
}

func TestClaimTaskByRuntime_SettlesPrivateOwnerMismatchChatFailure(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database not available")
	}
	ctx := context.Background()
	foreignOwnerID := dbfx.User(t, "Daemon chat mismatch owner", "daemon-chat-mismatch-owner-"+uuid.NewString()+"@example.com")
	dbfx.Member(t, testWorkspaceID, foreignOwnerID, "member")
	runtimeID := createClaimReclaimRuntime(t, ctx, "Mismatch chat runtime")
	agentID, _ := createClaimReclaimAgentAndIssue(t, ctx, runtimeID, "Mismatch chat agent")
	dbfx.Exec(t, `UPDATE agent SET owner_id = $1 WHERE id = $2`, foreignOwnerID, agentID)
	sessionID := createHandlerTestChatSession(t, agentID)
	var taskID string
	if err := testPool.QueryRow(ctx, `
		INSERT INTO agent_task_queue (agent_id, runtime_id, chat_session_id, status, priority)
		VALUES ($1, $2, $3, 'queued', 0)
		RETURNING id
	`, agentID, runtimeID, sessionID).Scan(&taskID); err != nil {
		t.Fatalf("seed chat task: %v", err)
	}
	t.Cleanup(func() { testPool.Exec(ctx, `DELETE FROM agent_task_queue WHERE id = $1`, taskID) })

	req := newDaemonTokenRequest(http.MethodPost, "/api/daemon/runtimes/"+runtimeID+"/tasks/claim", nil,
		testWorkspaceID, "mismatch-chat-claim")
	req = withURLParam(req, "runtimeId", runtimeID)
	w := testutil.Call(t, testHandler.ClaimTaskByRuntime, req).Want(http.StatusOK)
	if strings.TrimSpace(w.Body.String()) != `{"task":null}` {
		t.Fatalf("ClaimTaskByRuntime body = %q, want empty successful poll", w.Body.String())
	}

	var status, failureReason, assistantContent string
	dbfx.QueryRow(t, `SELECT status, failure_reason FROM agent_task_queue WHERE id = $1`, taskID).Scan(&status, &failureReason)
	dbfx.QueryRow(t, `SELECT content FROM chat_message WHERE task_id = $1 AND role = 'assistant'`, taskID).Scan(&assistantContent)
	if status != "failed" || failureReason != taskfailure.ReasonInvalidTaskIdentity.String() {
		t.Fatalf("task state = %q/%q, want failed/%s", status, failureReason, taskfailure.ReasonInvalidTaskIdentity)
	}
	if !strings.Contains(assistantContent, "agent and runtime have different owners") {
		t.Fatalf("assistant failure = %q, want owner-mismatch message", assistantContent)
	}
}

func TestClaimTasksByRuntime_SettlesMismatchAndReturnsValidTask(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database not available")
	}
	ctx := context.Background()
	foreignOwnerID := dbfx.User(t, "Daemon batch mismatch owner", "daemon-batch-mismatch-owner-"+uuid.NewString()+"@example.com")
	dbfx.Member(t, testWorkspaceID, foreignOwnerID, "member")
	runtimeID := createClaimReclaimRuntime(t, ctx, "Mismatch batch runtime")
	mismatchAgentID, mismatchIssueID := createClaimReclaimAgentAndIssue(t, ctx, runtimeID, "Mismatch batch agent")
	dbfx.Exec(t, `UPDATE agent SET owner_id = $1 WHERE id = $2`, foreignOwnerID, mismatchAgentID)
	validAgentID, validIssueID := createClaimReclaimAgentAndIssue(t, ctx, runtimeID, "Valid batch agent")
	mismatchTaskID := seedQueuedIssueTask(t, ctx, mismatchAgentID, runtimeID, mismatchIssueID)
	validTaskID := seedQueuedIssueTask(t, ctx, validAgentID, runtimeID, validIssueID)

	w := postBatchClaim(t, testWorkspaceID, []string{runtimeID}, 2)
	if w.Code != http.StatusOK {
		t.Fatalf("batch claim status = %d, want 200: %s", w.Code, w.Body.String())
	}
	var resp batchClaimResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode batch claim: %v", err)
	}
	if len(resp.Tasks) != 1 || resp.Tasks[0].ID != validTaskID {
		t.Fatalf("batch tasks = %+v, want valid task %s", resp.Tasks, validTaskID)
	}

	var mismatchStatus, mismatchReason, validStatus string
	dbfx.QueryRow(t, `SELECT status, failure_reason FROM agent_task_queue WHERE id = $1`, mismatchTaskID).Scan(&mismatchStatus, &mismatchReason)
	dbfx.QueryRow(t, `SELECT status FROM agent_task_queue WHERE id = $1`, validTaskID).Scan(&validStatus)
	if mismatchStatus != "failed" || mismatchReason != taskfailure.ReasonInvalidTaskIdentity.String() {
		t.Fatalf("mismatch task state = %q/%q, want failed/%s", mismatchStatus, mismatchReason, taskfailure.ReasonInvalidTaskIdentity)
	}
	if validStatus != "dispatched" {
		t.Fatalf("valid task status = %q, want dispatched", validStatus)
	}
}

func TestBuildClaimedTaskResponseRejectsAgentOwnerChangedAfterClaim(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database not available")
	}
	ctx := context.Background()
	newOwnerID := dbfx.User(t, "Claim owner mismatch", "claim-owner-mismatch-"+uuid.NewString()+"@example.com")
	dbfx.Member(t, testWorkspaceID, newOwnerID, "member")
	runtimeID := createClaimReclaimRuntime(t, ctx, "Claim then change owner runtime")
	agentID, issueID := createClaimReclaimAgentAndIssue(t, ctx, runtimeID, "Claim then change owner agent")
	taskID := seedQueuedIssueTask(t, ctx, agentID, runtimeID, issueID)

	task, err := testHandler.TaskService.ClaimTaskForRuntime(ctx, parseUUID(runtimeID))
	if err != nil {
		t.Fatalf("claim task: %v", err)
	}
	if task == nil || uuidToString(task.ID) != taskID {
		t.Fatalf("claimed task = %+v, want %s", task, taskID)
	}
	dbfx.Exec(t, `UPDATE agent SET owner_id = $1 WHERE id = $2`, newOwnerID, agentID)

	runtime, err := testHandler.Queries.GetAgentRuntimeForWorkspace(ctx, db.GetAgentRuntimeForWorkspaceParams{
		ID:          parseUUID(runtimeID),
		WorkspaceID: parseUUID(testWorkspaceID),
	})
	if err != nil {
		t.Fatalf("load runtime: %v", err)
	}
	req := newDaemonTokenRequest(http.MethodPost, "/api/daemon/runtimes/"+runtimeID+"/tasks/claim", nil,
		testWorkspaceID, "claim-then-owner-change")
	_, _, _, _, failure := testHandler.buildClaimedTaskResponse(
		req, task, runtime, runtimeID, testWorkspaceID,
	)
	if failure == nil || failure.status != http.StatusForbidden || failure.outcome != "error_runtime_access_denied" {
		t.Fatalf("failure = %+v, want runtime-access forbidden", failure)
	}

	var status, errorMessage, failureReason string
	dbfx.QueryRow(t, `
		SELECT status, error, failure_reason
		FROM agent_task_queue
		WHERE id = $1
	`, taskID).Scan(&status, &errorMessage, &failureReason)
	if status != "failed" {
		t.Fatalf("task status = %q, want failed", status)
	}
	if !strings.Contains(errorMessage, "agent and runtime have different owners") {
		t.Fatalf("task error = %q, want actionable owner-mismatch error", errorMessage)
	}
	if strings.Contains(errorMessage, "agent has no owner") {
		t.Fatalf("task error = %q, must not describe a non-null owner as missing", errorMessage)
	}
	if failureReason != taskfailure.ReasonInvalidTaskIdentity.String() {
		t.Fatalf("failure_reason = %q, want %q", failureReason, taskfailure.ReasonInvalidTaskIdentity)
	}
}

func TestBuildClaimedTaskResponseRejectsAgentReboundAfterClaim(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database not available")
	}
	ctx := context.Background()
	oldRuntimeID := createClaimReclaimRuntime(t, ctx, "Claim then rebind old runtime")
	newRuntimeID := createClaimReclaimRuntime(t, ctx, "Claim then rebind new runtime")
	agentID, issueID := createClaimReclaimAgentAndIssue(t, ctx, oldRuntimeID, "Claim then rebind agent")
	taskID := seedQueuedIssueTask(t, ctx, agentID, oldRuntimeID, issueID)

	task, err := testHandler.TaskService.ClaimTaskForRuntime(ctx, parseUUID(oldRuntimeID))
	if err != nil {
		t.Fatalf("claim task: %v", err)
	}
	if task == nil || uuidToString(task.ID) != taskID {
		t.Fatalf("claimed task = %+v, want %s", task, taskID)
	}
	dbfx.Exec(t, `UPDATE agent SET runtime_id = $1 WHERE id = $2`, newRuntimeID, agentID)

	runtime, err := testHandler.Queries.GetAgentRuntimeForWorkspace(ctx, db.GetAgentRuntimeForWorkspaceParams{
		ID:          parseUUID(oldRuntimeID),
		WorkspaceID: parseUUID(testWorkspaceID),
	})
	if err != nil {
		t.Fatalf("load old runtime: %v", err)
	}
	req := newDaemonTokenRequest(http.MethodPost, "/api/daemon/runtimes/"+oldRuntimeID+"/tasks/claim", nil,
		testWorkspaceID, "claim-then-rebind")
	_, _, _, _, failure := testHandler.buildClaimedTaskResponse(
		req, task, runtime, oldRuntimeID, testWorkspaceID,
	)
	if failure == nil || failure.status != http.StatusConflict || failure.outcome != "error_agent_runtime_changed" {
		t.Fatalf("failure = %+v, want runtime-changed conflict", failure)
	}

	var status, errorMessage, failureReason string
	dbfx.QueryRow(t, `
		SELECT status, error, failure_reason
		FROM agent_task_queue
		WHERE id = $1
	`, taskID).Scan(&status, &errorMessage, &failureReason)
	if status != "failed" {
		t.Fatalf("task status = %q, want failed", status)
	}
	if !strings.Contains(errorMessage, "moved to another runtime") {
		t.Fatalf("task error = %q, want actionable rebind error", errorMessage)
	}
	if failureReason != taskfailure.ReasonInvalidTaskIdentity.String() {
		t.Fatalf("failure_reason = %q, want %q", failureReason, taskfailure.ReasonInvalidTaskIdentity)
	}
}
