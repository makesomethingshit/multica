package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"strings"
	"testing"
	"time"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/multica-ai/multica/server/internal/auth"
	"github.com/multica-ai/multica/server/pkg/dbid"
	"github.com/multica-ai/multica/server/internal/testutil"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
	"github.com/multica-ai/multica/server/pkg/taskfailure"
)

func TestClaimTaskByRuntime_OwnerlessAgentOnPrivateRuntimeFailsExplicitly(t *testing.T) {
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
	if strings.TrimSpace(w.Body.String()) != `{"task":null}` {
		t.Fatalf("ClaimTaskByRuntime body = %q, want empty successful poll", w.Body.String())
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
	if !strings.Contains(errorMessage, "agent has no owner") {
		t.Fatalf("task error = %q, want actionable missing-owner error", errorMessage)
	}
	if failureReason != taskfailure.ReasonInvalidTaskIdentity.String() {
		t.Fatalf("failure_reason = %q, want %q", failureReason, taskfailure.ReasonInvalidTaskIdentity)
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
	taskID := dbfx.Task(t, agentID, testutil.Cols{"runtime_id": runtimeID, "chat_session_id": sessionID, "issue_id": nil})

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

// PUCK-89 blocker 1: the claim path reads the runtime BEFORE claiming, so a
// concurrent re-registration can flip runtime.owner_id between that read and
// final delivery. The final delivery gate must re-authorize against the
// CURRENT owner inside the finalize transaction — the singular claim must not
// return the task to the daemon, must settle it through the existing failure
// path, and must keep the empty successful-poll response.
// finalizeClaimDeliveryForTest drives the singular finalize+delivery path
// (shared gate included) against a task already claimed out-of-band, so a test
// can mutate the runtime between claim and finalize the way a concurrent
// re-registration would in production.
func (h *Handler) finalizeClaimDeliveryForTest(
	r *http.Request, task *db.AgentTaskQueue, runtimeID, runtimeWorkspaceID string,
) (AgentTaskResponse, []pgtype.UUID, int, int, *claimBuildFailure, error) {
	runtime, err := h.Queries.GetAgentRuntimeForWorkspace(r.Context(), db.GetAgentRuntimeForWorkspaceParams{
		ID:          parseUUID(runtimeID),
		WorkspaceID: parseUUID(runtimeWorkspaceID),
	})
	if err != nil {
		return AgentTaskResponse{}, nil, 0, 0, nil, fmt.Errorf("load runtime: %w", err)
	}
	resp, deliveredCommentIDs, agentSkillCount, builtinSkillCount, buildFailure := h.buildClaimedTaskResponse(
		r, task, runtime, runtimeID, runtimeWorkspaceID,
	)
	if buildFailure != nil {
		return resp, deliveredCommentIDs, agentSkillCount, builtinSkillCount, buildFailure, nil
	}
	if !runtime.OwnerID.Valid {
		return resp, deliveredCommentIDs, agentSkillCount, builtinSkillCount,
			&claimBuildFailure{outcome: "error_token", status: http.StatusInternalServerError, message: "runtime owner required"}, nil
	}
	tokenStr, terr := auth.GenerateAgentTaskToken()
	if terr != nil {
		return resp, deliveredCommentIDs, agentSkillCount, builtinSkillCount, nil, fmt.Errorf("generate token: %w", terr)
	}
	remoteMCPToken, daemonTokens, derr := remoteMCPDaemonTokenForClaim(resp, runtime)
	if derr != nil {
		return resp, deliveredCommentIDs, agentSkillCount, builtinSkillCount, nil, fmt.Errorf("remote mcp token: %w", derr)
	}
	commentBackedTask := task.TriggerCommentID.Valid || len(task.CoalescedCommentIds) > 0
	receipt, settledFailure, ferr := h.finalizeClaimDelivery(r.Context(), task, runtime, runtimeID, runtimeWorkspaceID, db.CreateTaskTokenParams{
		ID:          dbid.NewV7(),
		TokenHash:   auth.HashToken(tokenStr),
		TaskID:      task.ID,
		AgentID:     task.AgentID,
		WorkspaceID: parseUUID(resp.WorkspaceID),
		UserID:      runtime.OwnerID,
		ExpiresAt:   pgtype.Timestamptz{Time: time.Now().Add(24 * time.Hour), Valid: true},
	}, deliveredCommentIDs, commentBackedTask, daemonTokens...)
	if ferr != nil {
		return resp, deliveredCommentIDs, agentSkillCount, builtinSkillCount, nil, ferr
	}
	if settledFailure != nil {
		return AgentTaskResponse{}, deliveredCommentIDs, agentSkillCount, builtinSkillCount, settledFailure, nil
	}
	resp.AuthToken = tokenStr
	resp.RemoteMCPDaemonToken = remoteMCPToken
	resp.DeliveredCommentIDs = uuidStringsOrEmpty(receipt)
	return resp, deliveredCommentIDs, agentSkillCount, builtinSkillCount, nil, nil
}

func TestClaimTaskByRuntime_RuntimeOwnerChangedAfterClaimNeverDelivered(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database not available")
	}
	ctx := context.Background()
	newOwnerID := dbfx.User(t, "Delivery gate owner", "delivery-gate-owner-"+uuid.NewString()+"@example.com")
	dbfx.Member(t, testWorkspaceID, newOwnerID, "member")
	runtimeID := createClaimReclaimRuntime(t, ctx, "Delivery gate runtime")
	agentID, issueID := createClaimReclaimAgentAndIssue(t, ctx, runtimeID, "Delivery gate agent")
	taskID := seedQueuedIssueTask(t, ctx, agentID, runtimeID, issueID)

	// Initial runtime access read (owner = the workspace fixture user) happens
	// inside the handler; flip the runtime owner AFTER the task is claimed by
	// mutating the row directly between claim and finalize. To force that
	// interleaving deterministically, claim first, then change the owner, then
	// drive the finalize+delivery path through the handler's gate helper.
	task, err := testHandler.TaskService.ClaimTaskForRuntime(ctx, parseUUID(runtimeID))
	if err != nil {
		t.Fatalf("claim task: %v", err)
	}
	if task == nil || uuidToString(task.ID) != taskID {
		t.Fatalf("claimed task = %+v, want %s", task, taskID)
	}
	dbfx.Exec(t, `UPDATE agent_runtime SET owner_id = $1 WHERE id = $2`, newOwnerID, runtimeID)

	req := newDaemonTokenRequest(http.MethodPost, "/api/daemon/runtimes/"+runtimeID+"/tasks/claim", nil,
		testWorkspaceID, "delivery-gate-owner-change")
	resp, deliveredCommentIDs, _, _, settledFailure, finalizeErr := testHandler.finalizeClaimDeliveryForTest(
		req, task, runtimeID, testWorkspaceID,
	)
	if finalizeErr != nil {
		t.Fatalf("finalize delivery: %v", finalizeErr)
	}
	if settledFailure == nil || settledFailure.outcome != "error_runtime_access_denied" || !settledFailure.settled {
		t.Fatalf("settled failure = %+v, want settled runtime-access denial", settledFailure)
	}
	if len(deliveredCommentIDs) != 0 {
		t.Fatalf("delivered comment ids = %v, want none", deliveredCommentIDs)
	}
	if resp.AuthToken != "" || resp.RemoteMCPDaemonToken != "" {
		t.Fatalf("resp carries credentials %+v/%+v, want none — nothing may be delivered", resp.AuthToken, resp.RemoteMCPDaemonToken)
	}

	// No task token was minted and the task settled terminal.
	var tokenCount int
	dbfx.QueryRow(t, `SELECT count(*) FROM task_token WHERE task_id = $1`, taskID).Scan(&tokenCount)
	if tokenCount != 0 {
		t.Fatalf("task token count = %d, want 0 — no credential may accompany a rejected delivery", tokenCount)
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
	if failureReason != taskfailure.ReasonInvalidTaskIdentity.String() {
		t.Fatalf("failure_reason = %q, want %q", failureReason, taskfailure.ReasonInvalidTaskIdentity)
	}

	// And the full HTTP claim surface stays an empty successful poll.
	w := testutil.Call(t, testHandler.ClaimTaskByRuntime, func() *http.Request {
		r := newDaemonTokenRequest(http.MethodPost, "/api/daemon/runtimes/"+runtimeID+"/tasks/claim", nil,
			testWorkspaceID, "delivery-gate-poll")
		return testutil.WithURLParams(r, "runtimeId", runtimeID)
	}()).Want(http.StatusOK)
	if strings.TrimSpace(w.Body.String()) != `{"task":null}` {
		t.Fatalf("post-settle poll body = %q, want empty successful poll", w.Body.String())
	}
}

// PUCK-89 blocker 1 (batch): a task whose runtime owner changed after the
// claim-time snapshot must never appear in the batch response; the mismatch is
// settled and a valid task in the same batch still returns.
func TestClaimTasksByRuntime_OwnerChangedAfterSnapshotSettlesMismatchReturnsValid(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database not available")
	}
	ctx := context.Background()
	newOwnerID := dbfx.User(t, "Batch delivery gate owner", "batch-delivery-gate-owner-"+uuid.NewString()+"@example.com")
	dbfx.Member(t, testWorkspaceID, newOwnerID, "member")
	runtimeID := createClaimReclaimRuntime(t, ctx, "Batch delivery gate runtime")
	mismatchAgentID, mismatchIssueID := createClaimReclaimAgentAndIssue(t, ctx, runtimeID, "Batch delivery gate mismatch agent")
	validAgentID, validIssueID := createClaimReclaimAgentAndIssue(t, ctx, runtimeID, "Batch delivery gate valid agent")
	mismatchTaskID := seedQueuedIssueTask(t, ctx, mismatchAgentID, runtimeID, mismatchIssueID)
	validTaskID := seedQueuedIssueTask(t, ctx, validAgentID, runtimeID, validIssueID)

	// The batch handler resolves runtime snapshots (owner = fixture user)
	// before claiming. Flip the mismatch agent's owner after enqueue so the
	// agent/runtime owner mismatch only exists at the delivery gate, then let
	// the handler claim + finalize both tasks in one poll.
	dbfx.Exec(t, `UPDATE agent SET owner_id = $1 WHERE id = $2`, newOwnerID, mismatchAgentID)

	w := postBatchClaim(t, testWorkspaceID, []string{runtimeID}, 5)
	if w.Code != http.StatusOK {
		t.Fatalf("batch claim status = %d, want 200: %s", w.Code, w.Body.String())
	}
	var resp batchClaimResponse
	if err := json.Unmarshal(w.Body.Bytes(), &resp); err != nil {
		t.Fatalf("decode batch claim: %v", err)
	}
	for _, got := range resp.Tasks {
		if got.ID == mismatchTaskID {
			t.Fatalf("batch returned the mismatched task %s", mismatchTaskID)
		}
	}
	if len(resp.Tasks) != 1 || resp.Tasks[0].ID != validTaskID {
		t.Fatalf("batch tasks = %+v, want only valid task %s", resp.Tasks, validTaskID)
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
