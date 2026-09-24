package handler

import (
	"context"
	"errors"
	"fmt"
	"net/http"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/multica-ai/multica/server/internal/testutil"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
	"github.com/multica-ai/multica/server/pkg/protocol"
)

func claimWorkerReplyRun(t *testing.T, runtimeID string) *AgentTaskResponse {
	t.Helper()
	req := newDaemonTokenRequest(http.MethodPost, "/api/daemon/runtimes/"+runtimeID+"/tasks/claim", nil, testWorkspaceID, "worker-reply-handoff")
	req = withURLParam(req, "runtimeId", runtimeID)
	req.Header.Set("X-Client-Capabilities", protocol.DaemonCapabilityCoalescedCommentsV1)
	var response struct {
		Task *AgentTaskResponse `json:"task"`
	}
	testutil.Call(t, testHandler.ClaimTaskByRuntime, req).Want(http.StatusOK).JSON(&response)
	return response.Task
}

func completeWorkerReplyRun(t *testing.T, taskID string) {
	t.Helper()
	req := newDaemonTokenRequest(http.MethodPost, "/api/daemon/tasks/"+taskID+"/complete", map[string]any{"output": "Processed the inputs delivered to this run"}, testWorkspaceID, "worker-reply-handoff")
	req = withURLParam(req, "taskId", taskID)
	testutil.Call(t, testHandler.CompleteTask, req).Want(http.StatusOK)
}

// A delegated member run that posts nothing still wakes its coordinator (GH
// #8719). CompleteTask synthesizes the fallback comment from the final output;
// the completion path must route it like an explicit worker reply so the
// leader->worker->leader loop does not stall on the platform-generated comment.
func TestCompletionFallbackWakesSquadLeader(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database not available")
	}
	leaderRuntimeID := dbfx.Runtime(t, "Fallback handoff leader runtime")
	leaderID := dbfx.Agent(t, "Fallback handoff leader", leaderRuntimeID, testutil.Cols{"max_concurrent_tasks": 3})
	workerRuntimeID := dbfx.Runtime(t, "Fallback handoff worker runtime")
	workerID := dbfx.Agent(t, "Fallback handoff worker", workerRuntimeID)
	squadID := dbfx.Squad(t, "Fallback handoff squad", leaderID)
	dbfx.SquadMember(t, squadID, "agent", workerID)
	issueID := dbfx.Issue(t, "Fallback results must reach the coordinator", testutil.Cols{
		"status": "in_progress", "assignee_type": "squad", "assignee_id": squadID,
	})
	sourceID := dbfx.Task(t, leaderID, testutil.Cols{
		"runtime_id": leaderRuntimeID, "issue_id": issueID, "status": "completed",
		"is_leader_task": true, "squad_id": squadID,
		"originator_user_id": testUserID, "accountable_user_id": testUserID,
	})
	rootID := dbfx.Comment(t, issueID, fmt.Sprintf("[@Worker](mention://agent/%s) verify the implementation", workerID), testutil.Cols{
		"author_type": "agent", "author_id": leaderID, "source_task_id": sourceID,
	})
	workerTaskID := dbfx.Task(t, workerID, testutil.Cols{
		"runtime_id": workerRuntimeID, "issue_id": issueID, "status": "running",
		"trigger_comment_id": rootID, "squad_id": squadID, "delegated_from_task_id": sourceID,
		"originator_user_id": testUserID, "accountable_user_id": testUserID,
		// Production claim receipt: the delegation comment reached this run,
		// so completion reconcile must not replay the trigger back at the worker.
		"delivered_comment_ids": testutil.Raw("ARRAY['" + rootID + "'::uuid]"),
	})
	// The member posts nothing: completing synthesizes the fallback comment.
	completeWorkerReplyRun(t, workerTaskID)
	var fallbackID string
	dbfx.QueryRow(t, `SELECT id FROM comment WHERE source_task_id = $1 AND author_id = $2`, workerTaskID, workerID).Scan(&fallbackID)
	if fallbackID == "" {
		t.Fatal("completion fallback comment was not synthesized")
	}
	next := claimWorkerReplyRun(t, leaderRuntimeID)
	if next == nil {
		t.Fatal("completion fallback did not wake the squad leader")
	}
	if !next.IsLeaderTask || !slices.Contains(next.DeliveredCommentIDs, fallbackID) {
		t.Fatal("follow-up must deliver the fallback in the squad leader role")
	}
	if n := dbfx.Count(t, "SELECT count(*) FROM agent_task_queue WHERE issue_id = $1 AND agent_id = $2 AND status = 'queued'", issueID, workerID); n != 0 {
		t.Fatalf("completion must not re-enqueue the worker run, got %d queued worker task(s)", n)
	}
}

// A silent guest-squad member run wakes its own coordinator, not the issue's
// assigned squad (GH #8719). The fallback carries no mentions, so only the
// parent delegation chain proves the guest squad's routing authority.
func TestCompletionFallbackWakesGuestSquadLeader(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database not available")
	}
	assignedLeaderID := dbfx.Agent(t, "Fallback guest assigned leader", testRuntimeID)
	assignedSquadID := dbfx.Squad(t, "Fallback guest assigned squad", assignedLeaderID)
	guestLeaderID := dbfx.Agent(t, "Fallback guest squad leader", testRuntimeID)
	guestSquadID := dbfx.Squad(t, "Fallback guest squad", guestLeaderID)
	workerID := dbfx.Agent(t, "Fallback guest worker", testRuntimeID)
	issueID := dbfx.Issue(t, "Fallback guest delegation keeps its authority", testutil.Cols{
		"assignee_type": "squad", "assignee_id": assignedSquadID,
	})
	guestLeaderTaskID := dbfx.Task(t, guestLeaderID, testutil.Cols{
		"runtime_id": testRuntimeID, "issue_id": issueID, "status": "completed",
		"is_leader_task": true, "squad_id": guestSquadID,
		"originator_user_id": testUserID, "accountable_user_id": testUserID,
	})
	delegationID := dbfx.Comment(t, issueID, "delegate to guest worker", testutil.Cols{
		"author_type": "agent", "author_id": guestLeaderID, "source_task_id": guestLeaderTaskID,
	})
	workerTaskID := dbfx.Task(t, workerID, testutil.Cols{
		"runtime_id": testRuntimeID, "issue_id": issueID, "status": "running",
		"trigger_comment_id": delegationID, "squad_id": guestSquadID,
		"delegated_from_task_id": guestLeaderTaskID,
		"originator_user_id":     testUserID, "accountable_user_id": testUserID,
		"delivered_comment_ids": testutil.Raw("ARRAY['" + delegationID + "'::uuid]"),
	})
	completeWorkerReplyRun(t, workerTaskID)
	var fallbackID string
	dbfx.QueryRow(t, "SELECT id FROM comment WHERE source_task_id = $1 AND author_id = $2", workerTaskID, workerID).Scan(&fallbackID)
	if fallbackID == "" {
		t.Fatal("completion fallback comment was not synthesized")
	}
	if got := dbfx.Count(t, "SELECT count(*) FROM agent_task_queue WHERE issue_id = $1 AND agent_id = $2 AND status = 'queued' AND is_leader_task = TRUE AND squad_id = $3", issueID, guestLeaderID, guestSquadID); got != 1 {
		t.Fatalf("guest squad leader received %d task(s), want 1", got)
	}
	if got := dbfx.Count(t, "SELECT count(*) FROM agent_task_queue WHERE issue_id = $1 AND agent_id = $2 AND status = 'queued'", issueID, assignedLeaderID); got != 0 {
		t.Fatalf("assigned squad leader received %d task(s), want 0", got)
	}
	if got := dbfx.Count(t, "SELECT count(*) FROM agent_task_queue WHERE issue_id = $1 AND agent_id = $2 AND status = 'queued'", issueID, workerID); got != 0 {
		t.Fatalf("worker received %d task(s), want 0", got)
	}
}

// A synthesized reply whose parent cannot be resolved in the issue workspace
// is not routed at all. Without the parent the guest delegation chain is
// unprovable, and falling through to the assigned-squad fallback would change
// routing authority on a transient lookup (GH #8719 fail-closed).
func TestCompletionFallbackUnresolvedParentDoesNotRoute(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database not available")
	}
	foreignWS := dbfx.Workspace(t, "Fallback foreign workspace", "fb-8719-foreign")
	foreignIssue := dbfx.Issue(t, "Fallback foreign issue", testutil.Cols{"workspace_id": foreignWS})
	foreignComment := dbfx.Comment(t, foreignIssue, "foreign delegation", testutil.Cols{"workspace_id": foreignWS})
	workerID := dbfx.Agent(t, "Fallback foreign-parent worker", testRuntimeID)
	assignedLeaderID := dbfx.Agent(t, "Fallback unresolved assigned leader", testRuntimeID)
	assignedSquadID := dbfx.Squad(t, "Fallback unresolved assigned squad", assignedLeaderID)
	issueID := dbfx.Issue(t, "Fallback with unresolvable parent is not routed", testutil.Cols{
		"assignee_type": "squad", "assignee_id": assignedSquadID,
	})
	workerTaskID := dbfx.Task(t, workerID, testutil.Cols{
		"runtime_id": testRuntimeID, "issue_id": issueID, "status": "running",
		"trigger_comment_id": foreignComment,
		"originator_user_id": testUserID, "accountable_user_id": testUserID,
		"delivered_comment_ids": testutil.Raw("ARRAY['" + foreignComment + "'::uuid]"),
	})
	completeWorkerReplyRun(t, workerTaskID)
	var parentID string
	dbfx.QueryRow(t, "SELECT parent_id FROM comment WHERE source_task_id = $1 AND author_id = $2", workerTaskID, workerID).Scan(&parentID)
	if parentID != foreignComment {
		t.Fatal("completion fallback was not synthesized under the foreign parent")
	}
	if got := dbfx.Count(t, "SELECT count(*) FROM agent_task_queue WHERE issue_id = $1 AND agent_id = $2 AND status = 'queued'", issueID, assignedLeaderID); got != 0 {
		t.Fatalf("unresolved parent woke assigned squad leader: got %d task(s), want 0", got)
	}
	if n := dbfx.Count(t, "SELECT count(*) FROM agent_task_queue WHERE issue_id = $1 AND status = 'queued'", issueID); n != 0 {
		t.Fatalf("unroutable fallback enqueued %d task(s), want 0", n)
	}
}

// TestCompletionFallbackMentionDoesNotFanOut pins §10 Test A (GH #8719): a
// fallback body naming an unrelated agent must wake only the source
// coordinator, never the named target.
func TestCompletionFallbackMentionDoesNotFanOut(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database not available")
	}
	leaderRuntimeID := dbfx.Runtime(t, "Fallback fanout leader runtime")
	leaderID := dbfx.Agent(t, "Fallback fanout leader", leaderRuntimeID, testutil.Cols{"max_concurrent_tasks": 3})
	workerRuntimeID := dbfx.Runtime(t, "Fallback fanout worker runtime")
	workerID := dbfx.Agent(t, "Fallback fanout worker", workerRuntimeID)
	otherID := dbfx.Agent(t, "Fallback fanout other", workerRuntimeID)
	squadID := dbfx.Squad(t, "Fallback fanout squad", leaderID)
	dbfx.SquadMember(t, squadID, "agent", workerID)
	issueID := dbfx.Issue(t, "Fallback mention must not fan out", testutil.Cols{
		"status": "in_progress", "assignee_type": "squad", "assignee_id": squadID,
	})
	sourceID := dbfx.Task(t, leaderID, testutil.Cols{
		"runtime_id": leaderRuntimeID, "issue_id": issueID, "status": "completed",
		"is_leader_task": true, "squad_id": squadID,
		"originator_user_id": testUserID, "accountable_user_id": testUserID,
	})
	rootID := dbfx.Comment(t, issueID, fmt.Sprintf("[@Worker](mention://agent/%s) verify the implementation", workerID), testutil.Cols{
		"author_type": "agent", "author_id": leaderID, "source_task_id": sourceID,
	})
	workerTaskID := dbfx.Task(t, workerID, testutil.Cols{
		"runtime_id": workerRuntimeID, "issue_id": issueID, "status": "running",
		"trigger_comment_id": rootID, "squad_id": squadID, "delegated_from_task_id": sourceID,
		"originator_user_id": testUserID, "accountable_user_id": testUserID,
		"delivered_comment_ids": testutil.Raw("ARRAY['" + rootID + "'::uuid]"),
	})
	completeWorkerReplyRun(t, workerTaskID)
	var fallbackID string
	dbfx.QueryRow(t, `SELECT id FROM comment WHERE source_task_id = $1 AND author_id = $2`, workerTaskID, workerID).Scan(&fallbackID)
	if fallbackID == "" {
		t.Fatal("completion fallback comment was not synthesized")
	}
	// Rewrite the synthesized body to carry an unrelated @agent mention, then
	// re-run the narrow handoff resolver: only the source coordinator may wake.
	dbfx.Exec(t, `UPDATE comment SET content = $2 WHERE id = $1`, fallbackID,
		fmt.Sprintf("Done. [@Other](mention://agent/%s) please review as well", otherID))
	stored, err := testHandler.Queries.GetAgentTask(context.Background(), parseUUID(workerTaskID))
	if err != nil {
		t.Fatal(err)
	}
	fallback, err := testHandler.Queries.GetComment(context.Background(), parseUUID(fallbackID))
	if err != nil {
		t.Fatal(err)
	}
	// Drain the coordinator task already enqueued by the completion call so
	// this dispatch proves exactly-once behavior, not a second wake.
	dbfx.Exec(t, `DELETE FROM agent_task_queue WHERE issue_id = $1 AND agent_id = $2 AND status = 'queued'`, issueID, leaderID)
	issue, err := testHandler.Queries.GetIssue(context.Background(), parseUUID(issueID))
	if err != nil {
		t.Fatal(err)
	}
	var parent *db.Comment
	if fallback.ParentID.Valid {
		p, err := testHandler.Queries.GetComment(context.Background(), fallback.ParentID)
		if err != nil {
			t.Fatal(err)
		}
		parent = &p
	}
	routed, provenInvalid, err := testHandler.routeCompletionFallbackCoordinator(context.Background(), issue, stored, fallback, parent)
	if err != nil || provenInvalid || !routed {
		t.Fatalf("mention-carrying fallback must still route to its coordinator: routed=%v invalid=%v err=%v", routed, provenInvalid, err)
	}
	if got := dbfx.Count(t, "SELECT count(*) FROM agent_task_queue WHERE issue_id = $1 AND agent_id = $2 AND status = 'queued'", issueID, otherID); got != 0 {
		t.Fatalf("fallback mention fanned out to unrelated agent: got %d task(s), want 0", got)
	}
	if got := dbfx.Count(t, "SELECT count(*) FROM agent_task_queue WHERE issue_id = $1 AND agent_id = $2 AND status = 'queued' AND is_leader_task = TRUE", issueID, leaderID); got != 1 {
		t.Fatalf("coordinator received %d task(s), want 1", got)
	}
}

// TestCompletionFallbackTerminalOriginatorNotReused pins §10 Test B (GH
// #8719): the terminal worker task's persisted human originator must not
// authorize a generic invocation of an unrelated invocable agent named in the
// fallback body.
func TestCompletionFallbackTerminalOriginatorNotReused(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database not available")
	}
	leaderRuntimeID := dbfx.Runtime(t, "Fallback originator leader runtime")
	leaderID := dbfx.Agent(t, "Fallback originator leader", leaderRuntimeID, testutil.Cols{"max_concurrent_tasks": 3})
	workerRuntimeID := dbfx.Runtime(t, "Fallback originator worker runtime")
	workerID := dbfx.Agent(t, "Fallback originator worker", workerRuntimeID)
	invocableID := dbfx.Agent(t, "Fallback originator invocable", workerRuntimeID, testutil.Cols{
		"visibility": "workspace", "permission_mode": "public_to",
	})
	dbfx.InsertNoID(t, "agent_invocation_target", testutil.Cols{
		"agent_id":    invocableID,
		"target_type": "workspace",
		"target_id":   testWorkspaceID,
	}, "agent_id = $1 AND target_type = 'workspace' AND target_id = $2", invocableID, testWorkspaceID)
	squadID := dbfx.Squad(t, "Fallback originator squad", leaderID)
	dbfx.SquadMember(t, squadID, "agent", workerID)
	issueID := dbfx.Issue(t, "Terminal originator must not re-authorize", testutil.Cols{
		"status": "in_progress", "assignee_type": "squad", "assignee_id": squadID,
	})
	sourceID := dbfx.Task(t, leaderID, testutil.Cols{
		"runtime_id": leaderRuntimeID, "issue_id": issueID, "status": "completed",
		"is_leader_task": true, "squad_id": squadID,
		"originator_user_id": testUserID, "accountable_user_id": testUserID,
	})
	rootID := dbfx.Comment(t, issueID, fmt.Sprintf("[@Worker](mention://agent/%s) verify the implementation", workerID), testutil.Cols{
		"author_type": "agent", "author_id": leaderID, "source_task_id": sourceID,
	})
	workerTaskID := dbfx.Task(t, workerID, testutil.Cols{
		"runtime_id": workerRuntimeID, "issue_id": issueID, "status": "running",
		"trigger_comment_id": rootID, "squad_id": squadID, "delegated_from_task_id": sourceID,
		"originator_user_id": testUserID, "accountable_user_id": testUserID,
		"delivered_comment_ids": testutil.Raw("ARRAY['" + rootID + "'::uuid]"),
	})
	completeWorkerReplyRun(t, workerTaskID)
	var fallbackID string
	dbfx.QueryRow(t, `SELECT id FROM comment WHERE source_task_id = $1 AND author_id = $2`, workerTaskID, workerID).Scan(&fallbackID)
	if fallbackID == "" {
		t.Fatal("completion fallback comment was not synthesized")
	}
	dbfx.Exec(t, `UPDATE comment SET content = $2 WHERE id = $1`, fallbackID,
		fmt.Sprintf("Done. [@B](mention://agent/%s) please take over", invocableID))
	if got := dbfx.Count(t, "SELECT count(*) FROM agent_task_queue WHERE issue_id = $1 AND agent_id = $2 AND status = 'queued'", issueID, invocableID); got != 0 {
		t.Fatalf("terminal originator re-authorized unrelated agent B: got %d task(s), want 0", got)
	}
	if got := dbfx.Count(t, "SELECT count(*) FROM agent_task_queue WHERE issue_id = $1 AND agent_id = $2 AND status = 'queued' AND is_leader_task = TRUE", issueID, leaderID); got != 1 {
		t.Fatalf("coordinator received %d task(s), want 1", got)
	}
}

// TestCompletionFallbackInvalidParentStaysFailClosed pins §10 Test D (GH
// #8719): an unresolvable lineage must wake nobody — guest, assigned, or
// worker — and the resolver must report it permanently invalid, not transient.
func TestCompletionFallbackInvalidParentStaysFailClosed(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database not available")
	}
	ctx := context.Background()
	foreignWS := dbfx.Workspace(t, "Fallback failclosed foreign workspace", "fb-8719-failclosed")
	foreignIssue := dbfx.Issue(t, "Fallback failclosed foreign issue", testutil.Cols{"workspace_id": foreignWS})
	foreignComment := dbfx.Comment(t, foreignIssue, "foreign delegation", testutil.Cols{"workspace_id": foreignWS})
	workerID := dbfx.Agent(t, "Fallback failclosed worker", testRuntimeID)
	assignedLeaderID := dbfx.Agent(t, "Fallback failclosed assigned leader", testRuntimeID)
	assignedSquadID := dbfx.Squad(t, "Fallback failclosed assigned squad", assignedLeaderID)
	issueID := dbfx.Issue(t, "Invalid lineage stays fail-closed", testutil.Cols{
		"assignee_type": "squad", "assignee_id": assignedSquadID,
	})
	workerTaskID := dbfx.Task(t, workerID, testutil.Cols{
		"runtime_id": testRuntimeID, "issue_id": issueID, "status": "running",
		"trigger_comment_id": foreignComment,
		"originator_user_id": testUserID, "accountable_user_id": testUserID,
		"delivered_comment_ids": testutil.Raw("ARRAY['" + foreignComment + "'::uuid]"),
	})
	completeWorkerReplyRun(t, workerTaskID)
	var fallbackID string
	dbfx.QueryRow(t, "SELECT id FROM comment WHERE source_task_id = $1 AND author_id = $2", workerTaskID, workerID).Scan(&fallbackID)
	if fallbackID == "" {
		t.Fatal("completion fallback comment was not synthesized")
	}
	if got := dbfx.Count(t, "SELECT count(*) FROM agent_task_queue WHERE issue_id = $1 AND status = 'queued'", issueID); got != 0 {
		t.Fatalf("invalid lineage enqueued %d task(s), want 0", got)
	}
	stored, err := testHandler.Queries.GetAgentTask(ctx, parseUUID(workerTaskID))
	if err != nil {
		t.Fatal(err)
	}
	fallback, err := testHandler.Queries.GetComment(ctx, parseUUID(fallbackID))
	if err != nil {
		t.Fatal(err)
	}
	issue, err := testHandler.Queries.GetIssue(ctx, parseUUID(issueID))
	if err != nil {
		t.Fatal(err)
	}
	_, provenInvalid, err := testHandler.resolveCompletionFallbackCoordinator(ctx, issue, stored, fallback, nil)
	if err != nil || !provenInvalid {
		t.Fatalf("invalid parent must be proven-invalid: invalid=%v err=%v", provenInvalid, err)
	}
	routed, _, err := testHandler.dispatchCompletionFallback(ctx, &stored, parseUUID(fallbackID))
	if err != nil || routed {
		t.Fatalf("fail-closed replay must not route: routed=%v err=%v", routed, err)
	}
	if got := dbfx.Count(t, "SELECT count(*) FROM agent_task_queue WHERE issue_id = $1 AND status = 'queued'", issueID); got != 0 {
		t.Fatalf("fail-closed replay enqueued %d task(s), want 0", got)
	}
}

// TestCompletionFallbackCrossTaskReparseBlocked pins review blocker 1 (GH
// #8719): when another agent B completes later on the same thread, B's
// completion reconcile must NOT re-parse W's fallback body as a fresh generic
// comment — even when the fallback names B with an explicit @mention and B's
// originator could invoke the mention target. The fallback is owned solely by
// the narrow handoff path (and the sweeper replay), never by generic mention
// fan-out from any completing task.
func TestCompletionFallbackCrossTaskReparseBlocked(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database not available")
	}
	ctx := context.Background()
	leaderRuntimeID := dbfx.Runtime(t, "Fallback cross leader runtime")
	leaderID := dbfx.Agent(t, "Fallback cross leader", leaderRuntimeID, testutil.Cols{"max_concurrent_tasks": 3})
	workerRuntimeID := dbfx.Runtime(t, "Fallback cross worker runtime")
	workerID := dbfx.Agent(t, "Fallback cross worker", workerRuntimeID)
	otherRuntimeID := dbfx.Runtime(t, "Fallback cross other runtime")
	otherID := dbfx.Agent(t, "Fallback cross other", otherRuntimeID, testutil.Cols{"max_concurrent_tasks": 3})
	squadID := dbfx.Squad(t, "Fallback cross squad", leaderID)
	dbfx.SquadMember(t, squadID, "agent", workerID)
	issueID := dbfx.Issue(t, "Fallback cross-task reparse stays blocked", testutil.Cols{
		"status": "in_progress", "assignee_type": "squad", "assignee_id": squadID,
	})
	sourceID := dbfx.Task(t, leaderID, testutil.Cols{
		"runtime_id": leaderRuntimeID, "issue_id": issueID, "status": "completed",
		"is_leader_task": true, "squad_id": squadID,
		"originator_user_id": testUserID, "accountable_user_id": testUserID,
	})
	rootID := dbfx.Comment(t, issueID, fmt.Sprintf("[@Worker](mention://agent/%s) verify the implementation", workerID), testutil.Cols{
		"author_type": "agent", "author_id": leaderID, "source_task_id": sourceID,
	})
	workerTaskID := dbfx.Task(t, workerID, testutil.Cols{
		"runtime_id": workerRuntimeID, "issue_id": issueID, "status": "running",
		"trigger_comment_id": rootID, "squad_id": squadID, "delegated_from_task_id": sourceID,
		"originator_user_id": testUserID, "accountable_user_id": testUserID,
		"delivered_comment_ids": testutil.Raw("ARRAY['" + rootID + "'::uuid]"),
	})
	// B already runs on the same thread BEFORE W completes, so W's fallback
	// lands inside B's reconciliation window (created after B, same thread).
	// Creating B after W completes would exclude the fallback on the time
	// filter and let this test pass vacuously, even without the guard.
	otherTaskID := dbfx.Task(t, otherID, testutil.Cols{
		"runtime_id": otherRuntimeID, "issue_id": issueID, "status": "running",
		"trigger_comment_id": rootID,
		"originator_user_id": testUserID, "accountable_user_id": testUserID,
		"delivered_comment_ids": testutil.Raw("ARRAY['" + rootID + "'::uuid]"),
	})
	completeWorkerReplyRun(t, workerTaskID)
	var fallbackID string
	dbfx.QueryRow(t, `SELECT id FROM comment WHERE source_task_id = $1 AND author_id = $2`, workerTaskID, workerID).Scan(&fallbackID)
	if fallbackID == "" {
		t.Fatal("completion fallback comment was not synthesized")
	}
	// Rewrite the fallback body to mention an unrelated agent B explicitly.
	dbfx.Exec(t, `UPDATE comment SET content = $2 WHERE id = $1`, fallbackID,
		fmt.Sprintf("Done. [@B](mention://agent/%s) please take over", otherID))
	// Prove the test is not vacuous: W's fallback must be in B's
	// reconcilable set, so B's reconcile pass actually sees it.
	bTask, err := testHandler.Queries.GetAgentTask(ctx, parseUUID(otherTaskID))
	if err != nil {
		t.Fatal(err)
	}
	bPlanned := append([]pgtype.UUID{}, bTask.CoalescedCommentIds...)
	if bTask.TriggerCommentID.Valid {
		bPlanned = append(bPlanned, bTask.TriggerCommentID)
	}
	bReconcilable, err := testHandler.Queries.ListReconcilableCommentsForIssueSince(ctx, db.ListReconcilableCommentsForIssueSinceParams{
		CommentThreadID:   bTask.CommentThreadID,
		IssueID:           bTask.IssueID,
		Since:             bTask.CreatedAt,
		PlannedCommentIds: bPlanned,
	})
	if err != nil {
		t.Fatal(err)
	}
	bSeesFallback := false
	for _, rc := range bReconcilable {
		if uuidToString(rc.ID) == fallbackID {
			bSeesFallback = true
			break
		}
	}
	if !bSeesFallback {
		t.Fatal("W fallback is not in B's reconcilable set: cross-task guard would pass vacuously")
	}
	// B completes: its reconcile pass must not re-parse W's fallback body
	// into a fresh invocation of B.
	completeWorkerReplyRun(t, otherTaskID)
	if got := dbfx.Count(t, "SELECT count(*) FROM agent_task_queue WHERE issue_id = $1 AND agent_id = $2 AND status IN ('queued','dispatched','running','waiting_local_directory')", issueID, otherID); got != 0 {
		t.Fatalf("cross-task reconcile re-parsed fallback mention: got %d runnable B task(s), want 0", got)
	}
	// The fallback still belongs to W's coordinator handoff, not to B.
	if got := dbfx.Count(t, "SELECT count(*) FROM agent_task_queue WHERE issue_id = $1 AND agent_id = $2 AND status IN ('queued','dispatched','running','waiting_local_directory') AND is_leader_task = TRUE", issueID, leaderID); got != 1 {
		t.Fatalf("coordinator handoff lost during cross-task completion: got %d, want 1", got)
	}
}

// TestCompletionFallbackReplayIsIdempotent pins §10 Test E (GH #8719):
// dispatching the same fallback obligation twice must leave exactly one
// runnable coordinator task, and a completion-callback replay must duplicate
// neither the fallback comment nor the coordinator task.
func TestCompletionFallbackReplayIsIdempotent(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database not available")
	}
	ctx := context.Background()
	leaderRuntimeID := dbfx.Runtime(t, "Fallback idempotent leader runtime")
	leaderID := dbfx.Agent(t, "Fallback idempotent leader", leaderRuntimeID, testutil.Cols{"max_concurrent_tasks": 3})
	workerRuntimeID := dbfx.Runtime(t, "Fallback idempotent worker runtime")
	workerID := dbfx.Agent(t, "Fallback idempotent worker", workerRuntimeID)
	squadID := dbfx.Squad(t, "Fallback idempotent squad", leaderID)
	dbfx.SquadMember(t, squadID, "agent", workerID)
	issueID := dbfx.Issue(t, "Fallback replay stays idempotent", testutil.Cols{
		"status": "in_progress", "assignee_type": "squad", "assignee_id": squadID,
	})
	sourceID := dbfx.Task(t, leaderID, testutil.Cols{
		"runtime_id": leaderRuntimeID, "issue_id": issueID, "status": "completed",
		"is_leader_task": true, "squad_id": squadID,
		"originator_user_id": testUserID, "accountable_user_id": testUserID,
	})
	rootID := dbfx.Comment(t, issueID, fmt.Sprintf("[@Worker](mention://agent/%s) verify the implementation", workerID), testutil.Cols{
		"author_type": "agent", "author_id": leaderID, "source_task_id": sourceID,
	})
	workerTaskID := dbfx.Task(t, workerID, testutil.Cols{
		"runtime_id": workerRuntimeID, "issue_id": issueID, "status": "running",
		"trigger_comment_id": rootID, "squad_id": squadID, "delegated_from_task_id": sourceID,
		"originator_user_id": testUserID, "accountable_user_id": testUserID,
		"delivered_comment_ids": testutil.Raw("ARRAY['" + rootID + "'::uuid]"),
	})
	completeWorkerReplyRun(t, workerTaskID)
	var fallbackID string
	dbfx.QueryRow(t, `SELECT id FROM comment WHERE source_task_id = $1 AND author_id = $2`, workerTaskID, workerID).Scan(&fallbackID)
	if fallbackID == "" {
		t.Fatal("completion fallback comment was not synthesized")
	}
	stored, err := testHandler.Queries.GetAgentTask(ctx, parseUUID(workerTaskID))
	if err != nil {
		t.Fatal(err)
	}
	for i := 0; i < 2; i++ {
		if _, _, err := testHandler.dispatchCompletionFallback(ctx, &stored, parseUUID(fallbackID)); err != nil {
			t.Fatalf("replay %d failed: %v", i, err)
		}
	}
	if got := dbfx.Count(t, "SELECT count(*) FROM agent_task_queue WHERE issue_id = $1 AND agent_id = $2 AND status IN ('queued','dispatched','running','waiting_local_directory')", issueID, leaderID); got != 1 {
		t.Fatalf("coordinator has %d runnable task(s), want 1", got)
	}
	completeWorkerReplyRun(t, workerTaskID)
	if got := dbfx.Count(t, `SELECT count(*) FROM comment WHERE source_task_id = $1 AND author_id = $2`, workerTaskID, workerID); got != 1 {
		t.Fatalf("callback replay duplicated fallback comment: got %d, want 1", got)
	}
	if got := dbfx.Count(t, "SELECT count(*) FROM agent_task_queue WHERE issue_id = $1 AND agent_id = $2 AND status IN ('queued','dispatched','running','waiting_local_directory')", issueID, leaderID); got != 1 {
		t.Fatalf("callback replay duplicated coordinator task: got %d, want 1", got)
	}
}

// TestCompletionFallbackTransientHandoffFailureIsRecoverable pins §10 Test C
// (GH #8719): a failed first route leaves the persisted fallback replayable,
// and completion reconciliation eventually delivers it to one coordinator.
func TestCompletionFallbackTransientHandoffFailureIsRecoverable(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database not available")
	}
	ctx := context.Background()
	leaderRuntimeID := dbfx.Runtime(t, "Fallback recovery leader runtime")
	leaderID := dbfx.Agent(t, "Fallback recovery leader", leaderRuntimeID, testutil.Cols{"max_concurrent_tasks": 3})
	workerRuntimeID := dbfx.Runtime(t, "Fallback recovery worker runtime")
	workerID := dbfx.Agent(t, "Fallback recovery worker", workerRuntimeID)
	squadID := dbfx.Squad(t, "Fallback recovery squad", leaderID)
	dbfx.SquadMember(t, squadID, "agent", workerID)
	issueID := dbfx.Issue(t, "Transient fallback handoff failure is recoverable", testutil.Cols{
		"status": "in_progress", "assignee_type": "squad", "assignee_id": squadID,
	})
	sourceID := dbfx.Task(t, leaderID, testutil.Cols{
		"runtime_id": leaderRuntimeID, "issue_id": issueID, "status": "completed",
		"is_leader_task": true, "squad_id": squadID,
	})
	rootID := dbfx.Comment(t, issueID, fmt.Sprintf("[@Worker](mention://agent/%s) do the work", workerID), testutil.Cols{
		"author_type": "agent", "author_id": leaderID, "source_task_id": sourceID,
	})
	workerTaskID := dbfx.Task(t, workerID, testutil.Cols{
		"runtime_id": workerRuntimeID, "issue_id": issueID, "status": "running",
		"trigger_comment_id": rootID, "squad_id": squadID, "delegated_from_task_id": sourceID,
		"originator_user_id": testUserID, "accountable_user_id": testUserID,
		"delivered_comment_ids": testutil.Raw("ARRAY['" + rootID + "'::uuid]"),
	})
	completed, transitioned, fallbackID, err := testHandler.TaskService.CompleteTaskWithTransition(
		ctx, parseUUID(workerTaskID), []byte(`{"output":"The delegated work is complete."}`), "", "", "", false, "", "",
	)
	if err != nil || !transitioned || !fallbackID.Valid {
		t.Fatalf("complete worker task: transitioned=%v fallback_valid=%v err=%v", transitioned, fallbackID.Valid, err)
	}

	// Cancel only the first routing attempt after completion committed. This is
	// an injected transient DB error; the fallback row must remain the durable
	// obligation and the retryable error must not be classified as invalid.
	failedCtx, cancel := context.WithCancel(ctx)
	cancel()
	if _, provenInvalid, err := testHandler.dispatchCompletionFallback(failedCtx, completed, fallbackID); err == nil || provenInvalid {
		t.Fatalf("first route attempt = (invalid=%v, err=%v), want transient error", provenInvalid, err)
	}
	if got := dbfx.Count(t, "SELECT count(*) FROM comment WHERE id = $1", uuidToString(fallbackID)); got != 1 {
		t.Fatalf("fallback comment after failed route: got %d rows, want 1", got)
	}
	if got := dbfx.Count(t, "SELECT count(*) FROM agent_task_queue WHERE issue_id = $1 AND agent_id = $2 AND status IN ('queued','dispatched','running','waiting_local_directory')", issueID, leaderID); got != 0 {
		t.Fatalf("failed route created %d coordinator task(s), want 0", got)
	}

	// Fail the same-request reconcile too, then prove the post-request sweeper
	// replay — not the in-request path — delivers the handoff. This pins
	// review blocker 2: after HTTP success with every in-request attempt down,
	// a real later replay must still exist.
	if _, provenInvalid, err := testHandler.dispatchCompletionFallback(failedCtx, completed, fallbackID); err == nil || provenInvalid {
		t.Fatalf("second route attempt = (invalid=%v, err=%v), want transient error", provenInvalid, err)
	}
	testHandler.reconcileCommentsOnCompletion(failedCtx, completed)
	if got := dbfx.Count(t, "SELECT count(*) FROM agent_task_queue WHERE issue_id = $1 AND agent_id = $2 AND status IN ('queued','dispatched','running','waiting_local_directory')", issueID, leaderID); got != 0 {
		t.Fatalf("failed same-request reconcile created %d coordinator task(s), want 0", got)
	}
	result, err := testHandler.TaskService.RecoverPendingDelegatedFailures(ctx, 10)
	if err != nil {
		t.Fatalf("sweeper replay failed: %v", err)
	}
	if result.Replayed != 1 {
		t.Fatalf("sweeper replayed %d fallback(s), want 1 (scanned=%d)", result.Replayed, result.Scanned)
	}
	if got := dbfx.Count(t, "SELECT count(*) FROM agent_task_queue WHERE issue_id = $1 AND agent_id = $2 AND status IN ('queued','dispatched','running','waiting_local_directory')", issueID, leaderID); got != 1 {
		t.Fatalf("sweeper replay left %d runnable coordinator task(s), want exactly 1", got)
	}
	if got := dbfx.Count(t, "SELECT count(*) FROM comment WHERE id = $1", uuidToString(fallbackID)); got != 1 {
		t.Fatalf("fallback comment after recovery: got %d rows, want 1", got)
	}

	next := claimWorkerReplyRun(t, leaderRuntimeID)
	if next == nil || !next.IsLeaderTask || !slices.Contains(next.DeliveredCommentIDs, uuidToString(fallbackID)) {
		t.Fatalf("recovery did not deliver the fallback to the coordinator: task=%+v", next)
	}
	if got := dbfx.Count(t, "SELECT count(*) FROM agent_task_queue WHERE issue_id = $1 AND agent_id = $2 AND status IN ('queued','dispatched','running','waiting_local_directory')", issueID, leaderID); got != 1 {
		t.Fatalf("recovery created %d runnable coordinator task(s), want exactly 1", got)
	}
}

// TestCompletionFallbackExplicitReplyUnchanged pins §10 Test F (GH #8719): the
// explicit worker reply path keeps its normal routing and still wakes the
// leader exactly once.
func TestCompletionFallbackExplicitReplyUnchanged(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database not available")
	}
	leaderRuntimeID := dbfx.Runtime(t, "Fallback explicit leader runtime")
	leaderID := dbfx.Agent(t, "Fallback explicit leader", leaderRuntimeID, testutil.Cols{"max_concurrent_tasks": 3})
	workerRuntimeID := dbfx.Runtime(t, "Fallback explicit worker runtime")
	workerID := dbfx.Agent(t, "Fallback explicit worker", workerRuntimeID)
	squadID := dbfx.Squad(t, "Fallback explicit squad", leaderID)
	dbfx.SquadMember(t, squadID, "agent", workerID)
	issueID := dbfx.Issue(t, "Explicit worker reply unchanged", testutil.Cols{
		"status": "in_progress", "assignee_type": "squad", "assignee_id": squadID,
	})
	sourceID := dbfx.Task(t, leaderID, testutil.Cols{
		"runtime_id": leaderRuntimeID, "issue_id": issueID, "status": "completed",
		"is_leader_task": true, "squad_id": squadID,
		"originator_user_id": testUserID, "accountable_user_id": testUserID,
	})
	rootID := dbfx.Comment(t, issueID, fmt.Sprintf("[@Worker](mention://agent/%s) verify the implementation", workerID), testutil.Cols{
		"author_type": "agent", "author_id": leaderID, "source_task_id": sourceID,
	})
	workerTaskID := dbfx.Task(t, workerID, testutil.Cols{
		"runtime_id": workerRuntimeID, "issue_id": issueID, "status": "running",
		"started_at":         testutil.Raw("now()"),
		"trigger_comment_id": rootID, "squad_id": squadID, "delegated_from_task_id": sourceID,
		"originator_user_id": testUserID, "accountable_user_id": testUserID,
		"delivered_comment_ids": testutil.Raw("ARRAY['" + rootID + "'::uuid]"),
	})
	req := withURLParam(newRequest(http.MethodPost, "/api/issues/"+issueID+"/comments", map[string]any{
		"content": "Verification complete: all checks pass", "parent_id": rootID,
	}), "id", issueID)
	req.Header.Set("X-Agent-ID", workerID)
	req.Header.Set("X-Task-ID", workerTaskID)
	var response CommentResponse
	testutil.Call(t, testHandler.CreateComment, req).Want(http.StatusCreated).JSON(&response)
	_ = response
	// The review blocker-3 scenario is an explicit reply the leader ALREADY
	// handled and terminated. Model it without a raw status flip (which
	// would leave the reply uncovered and force a reconcile re-wake): claim
	// the wake the explicit reply enqueued, start it, and complete it —
	// recording the reply as delivered. The worker's later completion must
	// then produce no duplicate wake and no synthesized fallback.
	first := claimWorkerReplyRun(t, leaderRuntimeID)
	if first == nil || !first.IsLeaderTask {
		t.Fatal("explicit reply did not enqueue the first leader wake")
	}
	if _, err := testHandler.TaskService.StartTask(context.Background(), parseUUID(first.ID)); err != nil {
		t.Fatalf("start first leader run: %v", err)
	}
	completeWorkerReplyRun(t, first.ID)
	completeWorkerReplyRun(t, workerTaskID)
	if got := dbfx.Count(t, `SELECT count(*) FROM comment WHERE source_task_id = $1 AND author_id = $2`, workerTaskID, workerID); got != 1 {
		t.Fatalf("explicit reply plus completion left %d worker comment(s), want 1 (no synthesized fallback)", got)
	}
	next := claimWorkerReplyRun(t, leaderRuntimeID)
	if next != nil {
		t.Fatalf("handled reply must not re-wake the terminated leader, got task %s", next.ID)
	}
	if got := dbfx.Count(t, "SELECT count(*) FROM agent_task_queue WHERE issue_id = $1 AND agent_id = $2 AND status IN ('queued','dispatched','running','waiting_local_directory')", issueID, leaderID); got != 0 {
		t.Fatalf("leader has %d runnable task(s) after handled completion, want 0", got)
	}
}

// TestCompletionFallbackSuppressedReplyNotReclassified pins the suppression
// counterexample (GH #8719): an explicit worker reply that suppresses the
// coordinator at creation must not be reclassified as a synthesized
// completion fallback when the worker completes — neither by completion
// reconcile nor by the sweeper replay. The mention resolves (Test F proves
// the unsuppressed twin wakes the leader), so suppression is the only
// reason no task exists at creation.
func TestCompletionFallbackSuppressedReplyNotReclassified(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database not available")
	}
	ctx := context.Background()
	leaderRuntimeID := dbfx.Runtime(t, "Fallback suppressed leader runtime")
	leaderID := dbfx.Agent(t, "Fallback suppressed leader", leaderRuntimeID, testutil.Cols{"max_concurrent_tasks": 3})
	workerRuntimeID := dbfx.Runtime(t, "Fallback suppressed worker runtime")
	workerID := dbfx.Agent(t, "Fallback suppressed worker", workerRuntimeID)
	squadID := dbfx.Squad(t, "Fallback suppressed squad", leaderID)
	dbfx.SquadMember(t, squadID, "agent", workerID)
	issueID := dbfx.Issue(t, "Suppressed worker reply stays suppressed", testutil.Cols{
		"status": "in_progress", "assignee_type": "squad", "assignee_id": squadID,
	})
	sourceID := dbfx.Task(t, leaderID, testutil.Cols{
		"runtime_id": leaderRuntimeID, "issue_id": issueID, "status": "completed",
		"is_leader_task": true, "squad_id": squadID,
		"originator_user_id": testUserID, "accountable_user_id": testUserID,
	})
	rootID := dbfx.Comment(t, issueID, fmt.Sprintf("[@Worker](mention://agent/%s) verify the implementation", workerID), testutil.Cols{
		"author_type": "agent", "author_id": leaderID, "source_task_id": sourceID,
	})
	workerTaskID := dbfx.Task(t, workerID, testutil.Cols{
		"runtime_id": workerRuntimeID, "issue_id": issueID, "status": "running",
		"trigger_comment_id": rootID, "squad_id": squadID, "delegated_from_task_id": sourceID,
		"originator_user_id": testUserID, "accountable_user_id": testUserID,
		"delivered_comment_ids": testutil.Raw("ARRAY['" + rootID + "'::uuid]"),
	})
	// Explicit worker reply mentioning the leader, suppressed for the leader.
	req := withURLParam(newRequest(http.MethodPost, "/api/issues/"+issueID+"/comments", map[string]any{
		"content":            fmt.Sprintf("Done. [@Leader](mention://agent/%s) reviewed, no follow-up needed", leaderID),
		"parent_id":          rootID,
		"suppress_agent_ids": []string{leaderID},
	}), "id", issueID)
	req.Header.Set("X-Agent-ID", workerID)
	req.Header.Set("X-Task-ID", workerTaskID)
	var response CommentResponse
	testutil.Call(t, testHandler.CreateComment, req).Want(http.StatusCreated).JSON(&response)
	replyID := response.ID
	// Suppression honored at creation: no coordinator task.
	if got := dbfx.Count(t, "SELECT count(*) FROM agent_task_queue WHERE issue_id = $1 AND agent_id = $2 AND status IN ('queued','dispatched','running','waiting_local_directory')", issueID, leaderID); got != 0 {
		t.Fatalf("suppressed reply created %d leader task(s), want 0", got)
	}
	completeWorkerReplyRun(t, workerTaskID)
	// No synthesis: the run posted its explicit reply.
	if got := dbfx.Count(t, `SELECT count(*) FROM comment WHERE source_task_id = $1 AND author_id = $2`, workerTaskID, workerID); got != 1 {
		t.Fatalf("worker completion left %d worker comment(s), want exactly the explicit reply", got)
	}
	// The suppressed reply must not wake the leader via fallback reclassification.
	if got := dbfx.Count(t, "SELECT count(*) FROM agent_task_queue WHERE issue_id = $1 AND agent_id = $2 AND status IN ('queued','dispatched','running','waiting_local_directory')", issueID, leaderID); got != 0 {
		t.Fatalf("suppressed reply reclassified as fallback: got %d leader task(s), want 0", got)
	}
	// Nor may the sweeper replay resurrect it.
	pending, err := testHandler.Queries.ListPendingCompletionFallbacks(ctx, 10)
	if err != nil {
		t.Fatal(err)
	}
	for _, row := range pending {
		if uuidToString(row.FallbackID) == replyID {
			t.Fatal("suppressed reply listed as pending completion fallback")
		}
	}
	if _, err := testHandler.TaskService.RecoverPendingDelegatedFailures(ctx, 10); err != nil {
		t.Fatalf("sweeper replay failed: %v", err)
	}
	if got := dbfx.Count(t, "SELECT count(*) FROM agent_task_queue WHERE issue_id = $1 AND agent_id = $2 AND status IN ('queued','dispatched','running','waiting_local_directory')", issueID, leaderID); got != 0 {
		t.Fatalf("sweeper replayed suppressed reply: got %d leader task(s), want 0", got)
	}
}

// A worker progress comment wakes the leader; its final reply must also reach a
// leader run even if the first wake was claimed before the reply arrived.
func TestWorkerReplyDelivery(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database not available")
	}
	for _, state := range []string{"queued", "dispatched", "running"} {
		for _, explicit := range []bool{false, true} {
			t.Run(fmt.Sprintf("%s/explicit=%t", state, explicit), func(t *testing.T) {
				ctx := context.Background()
				leaderRuntimeID := dbfx.Runtime(t, "Worker handoff leader runtime")
				leaderID := dbfx.Agent(t, "Worker handoff leader", leaderRuntimeID, testutil.Cols{"max_concurrent_tasks": 3})
				workerRuntimeID := dbfx.Runtime(t, "Worker handoff worker runtime")
				workerID := dbfx.Agent(t, "Worker handoff worker", workerRuntimeID)
				squadID := dbfx.Squad(t, "Worker handoff squad", leaderID)
				dbfx.SquadMember(t, squadID, "agent", workerID)
				issueID := dbfx.Issue(t, "Worker results must reach the coordinator", testutil.Cols{
					"status": "in_progress", "assignee_type": "squad", "assignee_id": squadID,
				})
				sourceID := dbfx.Task(t, leaderID, testutil.Cols{
					"runtime_id": leaderRuntimeID, "issue_id": issueID, "status": "completed",
					"is_leader_task": true, "squad_id": squadID,
					"originator_user_id": testUserID, "accountable_user_id": testUserID,
				})
				rootID := dbfx.Comment(t, issueID, fmt.Sprintf("[@Worker](mention://agent/%s) verify the implementation", workerID), testutil.Cols{
					"author_type": "agent", "author_id": leaderID, "source_task_id": sourceID,
				})
				workerTaskID := dbfx.Task(t, workerID, testutil.Cols{
					"runtime_id": workerRuntimeID, "issue_id": issueID, "status": "running",
					"trigger_comment_id": rootID, "squad_id": squadID, "delegated_from_task_id": sourceID,
					"originator_user_id": testUserID, "accountable_user_id": testUserID,
				})
				post := func(content string) CommentResponse {
					req := withURLParam(newRequest(http.MethodPost, "/api/issues/"+issueID+"/comments", map[string]any{
						"content": content, "parent_id": rootID,
					}), "id", issueID)
					req.Header.Set("X-Agent-ID", workerID)
					req.Header.Set("X-Task-ID", workerTaskID)
					var response CommentResponse
					testutil.Call(t, testHandler.CreateComment, req).Want(http.StatusCreated).JSON(&response)
					if response.AuthorType != "agent" || response.SourceTaskID == nil || *response.SourceTaskID != workerTaskID {
						t.Fatal("worker reply lost its authenticated source task")
					}
					return response
				}
				progress := post("Verification started; final results will follow in this thread")
				var leaderTaskID string
				dbfx.QueryRow(t, `SELECT id FROM agent_task_queue WHERE issue_id = $1 AND agent_id = $2 AND status = 'queued'`, issueID, leaderID).Scan(&leaderTaskID)
				var first *AgentTaskResponse
				if state != "queued" {
					first = claimWorkerReplyRun(t, leaderRuntimeID)
					if first == nil || first.ID != leaderTaskID || !slices.Contains(first.DeliveredCommentIDs, progress.ID) {
						t.Fatal("first leader run did not receive the worker's progress")
					}
				}
				if state == "running" {
					if _, err := testHandler.TaskService.StartTask(ctx, parseUUID(leaderTaskID)); err != nil {
						t.Fatal(err)
					}
				}
				content := "Verification complete: found a correctness issue; please coordinate the repair"
				if explicit {
					content = fmt.Sprintf("[@Squad](mention://squad/%s) %s", squadID, content)
				}
				details := post("Evidence: the final reply can arrive after the leader claims its inputs")
				result := post(content)
				stored, err := testHandler.Queries.GetAgentTask(ctx, parseUUID(leaderTaskID))
				if err != nil {
					t.Fatal(err)
				}
				if state == "dispatched" && !explicit {
					for _, id := range []string{details.ID, result.ID} {
						if !slices.Contains(stored.CoalescedCommentIds, parseUUID(id)) || slices.Contains(stored.DeliveredCommentIds, parseUUID(id)) {
							t.Fatal("accepted worker reply must be planned without changing the earlier delivery receipt")
						}
					}
					if stored.TriggerCommentID != parseUUID(progress.ID) {
						t.Fatal("registering a worker reply must not replace an already claimed trigger")
					}
				}
				if state == "queued" {
					first = claimWorkerReplyRun(t, leaderRuntimeID)
					if first == nil || first.ID != leaderTaskID || !slices.Contains(first.DeliveredCommentIDs, result.ID) || !slices.Contains(first.DeliveredCommentIDs, details.ID) || first.TriggerCommentContent != content {
						t.Fatal("queued leader must coalesce and receive the worker result")
					}
				} else if slices.Contains(first.DeliveredCommentIDs, result.ID) {
					t.Fatal("an earlier claim cannot have delivered a later comment")
				}
				if state != "running" {
					if _, err := testHandler.TaskService.StartTask(ctx, parseUUID(leaderTaskID)); err != nil {
						t.Fatal(err)
					}
				}
				if other := claimWorkerReplyRun(t, leaderRuntimeID); other != nil {
					t.Fatal("same issue/leader must remain serialized")
				}
				completeWorkerReplyRun(t, leaderTaskID)
				next := claimWorkerReplyRun(t, leaderRuntimeID)
				if state == "queued" {
					if next != nil {
						t.Fatal("already delivered result must not create another leader run")
					}
					return
				}
				if next == nil {
					t.Fatal("worker result persisted but was neither delivered nor followed by another leader run")
				}
				if !next.IsLeaderTask || !slices.Contains(next.DeliveredCommentIDs, result.ID) || !slices.Contains(next.DeliveredCommentIDs, details.ID) || next.TriggerCommentContent != content {
					t.Fatal("follow-up must deliver the result in the squad leader role")
				}
				successor, err := testHandler.Queries.GetAgentTask(ctx, parseUUID(next.ID))
				if err != nil {
					t.Fatal(err)
				}
				if successor.SquadID != parseUUID(squadID) || successor.OriginatorUserID != parseUUID(testUserID) || successor.AccountableUserID != parseUUID(testUserID) || successor.DelegatedFromTaskID != parseUUID(workerTaskID) {
					t.Fatal("worker follow-up lost squad or human delegation provenance")
				}
				if _, err := testHandler.TaskService.StartTask(ctx, parseUUID(next.ID)); err != nil {
					t.Fatal(err)
				}
				completeWorkerReplyRun(t, next.ID)
				if another := claimWorkerReplyRun(t, leaderRuntimeID); another != nil {
					t.Fatal("worker result must not generate an endless leader follow-up loop")
				}
			})
		}
	}
}

// Unplanned replies must be in the completing run's thread and timestamp window:
// otherwise a passing negative case could merely be the SQL scope excluding it.
func TestWorkerReplyReconcileBoundaries(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database not available")
	}
	for _, mode := range []string{"accepted", "unplanned", "suppressed", "leader_reply", "note", "archived_leader", "reassigned", "registration_failure", "completed_before_registration"} {
		t.Run(mode, func(t *testing.T) {
			ctx := context.Background()
			runtimeID := dbfx.Runtime(t, "Worker replay boundary leader")
			leaderID := dbfx.Agent(t, "Worker replay boundary leader", runtimeID)
			workerRuntimeID := dbfx.Runtime(t, "Worker replay boundary worker")
			workerID := dbfx.Agent(t, "Worker replay boundary worker", workerRuntimeID)
			squadID := dbfx.Squad(t, "Worker replay boundary squad", leaderID)
			dbfx.SquadMember(t, squadID, "agent", workerID)
			issueID := dbfx.Issue(t, "Worker replay boundary", testutil.Cols{
				"status": "in_progress", "assignee_type": "squad", "assignee_id": squadID,
			})
			rootID := dbfx.Comment(t, issueID, "Coordinate this work", testutil.Cols{"created_at": testutil.Raw("now() - interval '6 minutes'")})
			taskID := dbfx.Task(t, leaderID, testutil.Cols{
				"runtime_id": runtimeID, "issue_id": issueID, "status": "queued",
				"trigger_comment_id": rootID, "is_leader_task": true, "squad_id": squadID,
				"originator_user_id": testUserID, "accountable_user_id": testUserID,
				"created_at": testutil.Raw("now() - interval '5 minutes'"),
			})
			workerTaskID := dbfx.Task(t, workerID, testutil.Cols{
				"runtime_id": workerRuntimeID, "issue_id": issueID, "status": "running",
				"trigger_comment_id": rootID, "squad_id": squadID,
				"originator_user_id": testUserID, "accountable_user_id": testUserID,
			})
			first := claimWorkerReplyRun(t, runtimeID)
			if first == nil || first.ID != taskID {
				t.Fatal("leader run was not claimed")
			}
			var replyID string
			if mode == "unplanned" {
				replyID = dbfx.Comment(t, issueID, "An ordinary reply with no accepted dispatch", testutil.Cols{
					"parent_id": rootID, "author_type": "agent", "author_id": workerID, "source_task_id": workerTaskID,
				})
			} else {
				body := map[string]any{"content": "The worker has results", "parent_id": rootID}
				if mode == "note" {
					body["content"] = "/note an informational update"
				}
				if mode == "suppressed" {
					body["suppress_agent_ids"] = []string{leaderID}
				}
				req := withURLParam(newRequest(http.MethodPost, "/api/issues/"+issueID+"/comments", body), "id", issueID)
				req.Header.Set("X-Agent-ID", workerID)
				req.Header.Set("X-Task-ID", workerTaskID)
				if mode == "leader_reply" {
					req.Header.Set("X-Agent-ID", leaderID)
					req.Header.Set("X-Task-ID", taskID)
				}
				var response CommentResponse
				h := *testHandler
				registration := &workerReplyRegistrationDB{DBTX: testPool}
				if mode == "registration_failure" {
					registration.err = errors.New("injected worker reply registration failure")
				}
				if mode == "completed_before_registration" {
					registration.before = func() {
						if _, err := testHandler.TaskService.StartTask(ctx, parseUUID(taskID)); err != nil {
							t.Fatal(err)
						}
						completeWorkerReplyRun(t, taskID)
					}
				}
				if mode == "registration_failure" || mode == "completed_before_registration" {
					h.Queries = db.New(registration)
				}
				testutil.Call(t, h.CreateComment, req).Want(http.StatusCreated).JSON(&response)
				if (mode == "registration_failure" || mode == "completed_before_registration") && registration.calls != 1 {
					t.Fatalf("registration probe called %d times", registration.calls)
				}
				replyID = response.ID
			}
			task, err := testHandler.Queries.GetAgentTask(ctx, parseUUID(taskID))
			if err != nil {
				t.Fatal(err)
			}
			comments, err := testHandler.Queries.ListReconcilableCommentsForIssueSince(ctx, db.ListReconcilableCommentsForIssueSinceParams{
				IssueID: task.IssueID, CommentThreadID: task.CommentThreadID, Since: task.CreatedAt,
				PlannedCommentIds: task.CoalescedCommentIds,
			})
			if err != nil {
				t.Fatal(err)
			}
			if !slices.ContainsFunc(comments, func(c db.Comment) bool { return c.ID == parseUUID(replyID) }) {
				t.Fatal("reply must reach the replay filter, not be excluded by the SQL thread/time window")
			}
			wantPlanned := mode == "accepted" || mode == "archived_leader" || mode == "reassigned"
			if slices.Contains(task.CoalescedCommentIds, parseUUID(replyID)) != wantPlanned {
				t.Fatalf("unexpected creation-time obligation for %s", mode)
			}
			if mode == "archived_leader" {
				dbfx.Exec(t, `UPDATE agent SET archived_at = now() WHERE id = $1`, leaderID)
			}
			if mode == "reassigned" {
				dbfx.Exec(t, `UPDATE issue SET assignee_type = 'agent', assignee_id = $2 WHERE id = $1`, issueID, workerID)
			}
			if mode != "completed_before_registration" {
				if _, err := testHandler.TaskService.StartTask(ctx, parseUUID(taskID)); err != nil {
					t.Fatal(err)
				}
				completeWorkerReplyRun(t, taskID)
			}
			queued := dbfx.Count(t, `SELECT count(*) FROM agent_task_queue WHERE issue_id = $1 AND status = 'queued'`, issueID)
			if mode != "accepted" && mode != "completed_before_registration" {
				if queued != 0 {
					t.Fatalf("%s must not create a completion-driven run, got %d", mode, queued)
				}
				return
			}
			if queued != 1 {
				t.Fatalf("accepted reply must create exactly one successor, got %d", queued)
			}
			next := claimWorkerReplyRun(t, runtimeID)
			if next == nil || !slices.Contains(next.DeliveredCommentIDs, replyID) {
				t.Fatal("accepted reply was not delivered")
			}
			if _, err := testHandler.TaskService.StartTask(ctx, parseUUID(next.ID)); err != nil {
				t.Fatal(err)
			}
			completeWorkerReplyRun(t, next.ID)
			if extra := claimWorkerReplyRun(t, runtimeID); extra != nil {
				t.Fatal("delivered worker reply replayed again")
			}
		})
	}
}

// If completion commits while registration waits for the task row lock, the
// caller must see a miss and enqueue fresh, not attach an obligation to a run
// whose completion snapshot can no longer include it.
func TestWorkerReplyRegistrationLosesCompletionRace(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database not available")
	}
	ctx, cancel := context.WithTimeout(context.Background(), 10*time.Second)
	defer cancel()
	runtimeID := dbfx.Runtime(t, "Worker registration race")
	agentID := dbfx.Agent(t, "Worker registration race", runtimeID)
	issueID := dbfx.Issue(t, "Worker registration race")
	rootID := dbfx.Comment(t, issueID, "Initial input")
	replyID := dbfx.Comment(t, issueID, "Worker result", testutil.Cols{"parent_id": rootID})
	taskID := dbfx.Task(t, agentID, testutil.Cols{
		"runtime_id": runtimeID, "issue_id": issueID, "status": "running", "trigger_comment_id": rootID,
	})
	tx, err := testPool.Begin(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer func() {
		if err := tx.Rollback(context.Background()); err != nil && !errors.Is(err, pgx.ErrTxClosed) {
			t.Error(err)
		}
	}()
	if _, err := tx.Exec(ctx, `UPDATE agent_task_queue SET status = 'completed', completed_at = now() WHERE id = $1`, taskID); err != nil {
		t.Fatal(err)
	}
	conn, err := testPool.Acquire(ctx)
	if err != nil {
		t.Fatal(err)
	}
	defer conn.Release()
	pid := conn.Conn().PgConn().PID()
	done := make(chan struct{})
	var registrationErr error
	go func() {
		_, registrationErr = db.New(conn).RegisterPlannedCommentForActiveTask(ctx, db.RegisterPlannedCommentForActiveTaskParams{
			IssueID: parseUUID(issueID), AgentID: parseUUID(agentID), CommentID: parseUUID(replyID),
		})
		close(done)
	}()
	// Stop the registration query before returning its connection to the pool,
	// including assertion failures while it is waiting for the task row lock.
	defer func() { cancel(); <-done }()
	for {
		var blocked bool
		if err := testPool.QueryRow(ctx, `SELECT cardinality(pg_blocking_pids($1)) > 0`, pid).Scan(&blocked); err != nil {
			t.Fatal(err)
		}
		if blocked {
			break
		}
		select {
		case <-ctx.Done():
			t.Fatal("registration did not block on the completing row")
		case <-time.After(5 * time.Millisecond):
		}
	}
	if err := tx.Commit(ctx); err != nil {
		t.Fatal(err)
	}
	<-done
	if !errors.Is(registrationErr, pgx.ErrNoRows) {
		t.Fatalf("registration after completion = %v, want no rows", registrationErr)
	}
	task, err := testHandler.Queries.GetAgentTask(ctx, parseUUID(taskID))
	if err != nil {
		t.Fatal(err)
	}
	if slices.Contains(task.CoalescedCommentIds, parseUUID(replyID)) {
		t.Fatal("completed run acquired an undeliverable obligation")
	}
}

// Intercept only the new obligation write; all routing, comment persistence,
// completion, and successor enqueue still use their real database paths.
type workerReplyRegistrationDB struct {
	db.DBTX
	before func()
	err    error
	calls  int
}

func (d *workerReplyRegistrationDB) QueryRow(ctx context.Context, query string, args ...any) pgx.Row {
	if strings.Contains(query, "-- name: RegisterPlannedCommentForActiveTask :one") {
		d.calls++
		if d.before != nil {
			d.before()
		}
		if d.err != nil {
			return errRow{err: d.err}
		}
	}
	return d.DBTX.QueryRow(ctx, query, args...)
}
