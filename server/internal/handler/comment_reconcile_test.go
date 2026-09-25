package handler

import (
	"context"
	"errors"
	"net/http"
	"net/http/httptest"
	"slices"
	"strings"
	"testing"
	"time"

	"github.com/go-chi/chi/v5"
	"github.com/jackc/pgx/v5"
	"github.com/jackc/pgx/v5/pgconn"
	"github.com/jackc/pgx/v5/pgtype"
	"github.com/multica-ai/multica/server/internal/testutil"
	"github.com/multica-ai/multica/server/internal/util"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

// completeTaskViaHandler drives the daemon CompleteTask endpoint for taskID.
func completeTaskViaHandler(t *testing.T, taskID, output string) *httptest.ResponseRecorder {
	t.Helper()
	w := httptest.NewRecorder()
	req := newDaemonTokenRequest("POST", "/api/daemon/tasks/"+taskID+"/complete",
		map[string]any{"output": output},
		testWorkspaceID, "legit-daemon")
	rctx := chi.NewRouteContext()
	rctx.URLParams.Add("taskId", taskID)
	req = req.WithContext(context.WithValue(req.Context(), chi.RouteCtxKey, rctx))
	testHandler.CompleteTask(w, req)
	return w
}

// pendingTaskCountForAgentIssue counts claimable (queued/dispatched) tasks for
// an (issue, agent) pair.
func pendingTaskCountForAgentIssue(t *testing.T, issueID, agentID string) int {
	t.Helper()
	var n int
	dbfx.QueryRow(t,
		`SELECT count(*) FROM agent_task_queue WHERE issue_id = $1 AND agent_id = $2 AND status IN ('queued', 'dispatched')`,
		issueID, agentID).Scan(&n)
	return n
}

// queuedTaskCountForAgentIssue counts only QUEUED (not dispatched) tasks for an
// (issue, agent) pair. Used to distinguish a freshly-enqueued follow-up from a
// pre-seeded dispatched task in the same assertion.
func queuedTaskCountForAgentIssue(t *testing.T, issueID, agentID string) int {
	t.Helper()
	var n int
	dbfx.QueryRow(t,
		`SELECT count(*) FROM agent_task_queue WHERE issue_id = $1 AND agent_id = $2 AND status = 'queued'`,
		issueID, agentID).Scan(&n)
	return n
}

// TestCompleteTask_ReconcilesMemberCommentPostedDuringRun proves the MUL-4195
// completion-reconciliation guarantee: a deliberate member comment that lands
// while the agent is busy (after the run's started_at) must earn a follow-up
// run instead of being silently lost.
func TestCompleteTask_ReconcilesMemberCommentPostedDuringRun(t *testing.T) {
	if testHandler == nil {
		t.Skip("database not available")
	}
	ctx := context.Background()

	var agentID, runtimeID string
	dbfx.QueryRow(t,
		`SELECT id, runtime_id FROM agent WHERE workspace_id = $1 AND runtime_id IS NOT NULL LIMIT 1`,
		testWorkspaceID).Scan(&agentID, &runtimeID)

	// Issue assigned to the agent so a plain member comment routes to it.
	issueID := dbfx.Issue(t, "reconcile-e2e fixture", testutil.Cols{
		"status":        "in_progress",
		"number":        999001,
		"assignee_type": "agent",
		"assignee_id":   agentID,
	})

	// Trigger comment created BEFORE the run starts.
	triggerCommentID := dbfx.Comment(t, issueID, "initial request", testutil.Cols{
		"created_at": testutil.Raw("now() - interval '10 minutes'"),
	})

	// A running task whose started_at is in the past.
	var taskID string
	dbfx.QueryRow(t, `
		INSERT INTO agent_task_queue (agent_id, runtime_id, issue_id, trigger_comment_id, delivered_comment_ids, status, priority, created_at, started_at)
		VALUES ($1, $2, $3, $4, ARRAY[$4::uuid], 'running', 0, now() - interval '10 minutes', now() - interval '5 minutes')
		RETURNING id
	`, agentID, runtimeID, issueID, triggerCommentID).Scan(&taskID)
	t.Cleanup(func() { testPool.Exec(ctx, `DELETE FROM agent_task_queue WHERE issue_id = $1`, issueID) })

	// A deliberate member comment that arrived DURING the run (after started_at).
	dbfx.Exec(t, `
		INSERT INTO comment (issue_id, workspace_id, author_type, author_id, content, type, created_at)
		VALUES ($1, $2, 'member', $3, 'wait, also handle this', 'comment', now() - interval '1 minute')
	`, issueID, testWorkspaceID, testUserID)

	dbfx.Exec(t, `UPDATE comment SET parent_id=$2 WHERE issue_id=$1 AND id<>$2`, issueID, triggerCommentID)

	if w := completeTaskViaHandler(t, taskID, "done"); w.Code != http.StatusOK {
		t.Fatalf("CompleteTask: expected 200, got %d: %s", w.Code, w.Body.String())
	}

	// A follow-up run must now be queued for the agent.
	if n := pendingTaskCountForAgentIssue(t, issueID, agentID); n != 1 {
		t.Fatalf("expected exactly 1 follow-up task after reconciliation, got %d", n)
	}
}

// A daemon may replay /complete after the server committed but its response was
// lost. The task CAS makes that replay a 200, but the handler must also skip the
// transaction-external reconciliation; otherwise a follow-up that finished
// between deliveries leaves no pending dedupe row and the same member comment
// creates a second real agent run.
func TestCompleteTask_ReplayDoesNotCreateSecondFollowUp(t *testing.T) {
	if testHandler == nil {
		t.Skip("database not available")
	}
	ctx := context.Background()

	var agentID, runtimeID string
	dbfx.QueryRow(t,
		`SELECT id, runtime_id FROM agent WHERE workspace_id = $1 AND runtime_id IS NOT NULL LIMIT 1`,
		testWorkspaceID).Scan(&agentID, &runtimeID)
	issueID := dbfx.Issue(t, "replayed-complete fixture", testutil.Cols{
		"status":        "in_progress",
		"number":        999009,
		"assignee_type": "agent",
		"assignee_id":   agentID,
	})
	triggerCommentID := dbfx.Comment(t, issueID, "initial request", testutil.Cols{
		"created_at": testutil.Raw("now() - interval '10 minutes'"),
	})
	var taskID string
	dbfx.QueryRow(t, `
		INSERT INTO agent_task_queue (agent_id, runtime_id, issue_id, trigger_comment_id, delivered_comment_ids, status, priority, created_at, started_at)
		VALUES ($1, $2, $3, $4, ARRAY[$4::uuid], 'running', 0, now() - interval '10 minutes', now() - interval '5 minutes')
		RETURNING id
	`, agentID, runtimeID, issueID, triggerCommentID).Scan(&taskID)
	t.Cleanup(func() { testPool.Exec(ctx, `DELETE FROM agent_task_queue WHERE issue_id = $1`, issueID) })
	dbfx.Exec(t, `
		INSERT INTO comment (issue_id, workspace_id, author_type, author_id, content, type, parent_id, created_at)
		VALUES ($1, $2, 'member', $3, 'also handle this once', 'comment', $4, now() - interval '1 minute')
	`, issueID, testWorkspaceID, testUserID, triggerCommentID)

	if w := completeTaskViaHandler(t, taskID, "done"); w.Code != http.StatusOK {
		t.Fatalf("first CompleteTask: expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var followUpID string
	dbfx.QueryRow(t, `
		SELECT id FROM agent_task_queue
		WHERE issue_id = $1 AND agent_id = $2 AND id <> $3 AND status = 'queued'
	`, issueID, agentID, taskID).Scan(&followUpID)
	dbfx.Exec(t, `UPDATE agent_task_queue SET status = 'completed', completed_at = now() WHERE id = $1`, followUpID)

	if w := completeTaskViaHandler(t, taskID, "done"); w.Code != http.StatusOK {
		t.Fatalf("replayed CompleteTask: expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var total int
	dbfx.QueryRow(t, `SELECT count(*) FROM agent_task_queue WHERE issue_id = $1 AND agent_id = $2`, issueID, agentID).Scan(&total)
	if total != 2 {
		t.Fatalf("task rows after replay = %d, want original + exactly one follow-up", total)
	}
	if pending := pendingTaskCountForAgentIssue(t, issueID, agentID); pending != 0 {
		t.Fatalf("replayed completion created %d additional pending follow-up(s)", pending)
	}
}

// TestCompleteTask_NoReconcileWhenNoNewMemberComment guards against spurious
// follow-ups: when no member comment arrived after the run started, completion
// must not enqueue any new task.
func TestCompleteTask_NoReconcileWhenNoNewMemberComment(t *testing.T) {
	if testHandler == nil {
		t.Skip("database not available")
	}
	ctx := context.Background()

	var agentID, runtimeID string
	dbfx.QueryRow(t,
		`SELECT id, runtime_id FROM agent WHERE workspace_id = $1 AND runtime_id IS NOT NULL LIMIT 1`,
		testWorkspaceID).Scan(&agentID, &runtimeID)

	issueID := dbfx.Issue(t, "reconcile-negative fixture", testutil.Cols{
		"status":        "in_progress",
		"number":        999002,
		"assignee_type": "agent",
		"assignee_id":   agentID,
	})

	triggerCommentID := dbfx.Comment(t, issueID, "the only request", testutil.Cols{
		"created_at": testutil.Raw("now() - interval '10 minutes'"),
	})

	var taskID string
	dbfx.QueryRow(t, `
		INSERT INTO agent_task_queue (agent_id, runtime_id, issue_id, trigger_comment_id, delivered_comment_ids, status, priority, created_at, started_at)
		VALUES ($1, $2, $3, $4, ARRAY[$4::uuid], 'running', 0, now() - interval '10 minutes', now() - interval '5 minutes')
		RETURNING id
	`, agentID, runtimeID, issueID, triggerCommentID).Scan(&taskID)
	t.Cleanup(func() { testPool.Exec(ctx, `DELETE FROM agent_task_queue WHERE issue_id = $1`, issueID) })

	if w := completeTaskViaHandler(t, taskID, "done"); w.Code != http.StatusOK {
		t.Fatalf("CompleteTask: expected 200, got %d: %s", w.Code, w.Body.String())
	}

	if n := pendingTaskCountForAgentIssue(t, issueID, agentID); n != 0 {
		t.Fatalf("expected no follow-up task when no new member comment, got %d", n)
	}
}

// TestCompleteTask_DoesNotReTriggerOtherAgentMentionedDuringRun is the MUL-4195
// review must-fix #2 regression test. Agent A is running on an issue when a
// member posts a comment that @-mentions a DIFFERENT agent B. B is triggered at
// comment-creation time (not exercised here). When A's run completes, the
// completion reconcile must NOT replay that comment through the full trigger
// pipeline and spawn a SECOND B run — reconcile is scoped to the agent that
// just ran (A). Before the fix, reconcile fanned the latest member comment out
// to every routed agent, so completing A re-woke B (and any other agent the
// comment mentioned), breaking the bounded-follow-up guarantee.
func TestCompleteTask_DoesNotReTriggerOtherAgentMentionedDuringRun(t *testing.T) {
	if testHandler == nil {
		t.Skip("database not available")
	}
	ctx := context.Background()

	var agentA, runtimeID string
	dbfx.QueryRow(t,
		`SELECT id, runtime_id FROM agent WHERE workspace_id = $1 AND runtime_id IS NOT NULL LIMIT 1`,
		testWorkspaceID).Scan(&agentA, &runtimeID)
	// A second, workspace-invocable agent that a member can @mention.
	agentB := createHandlerTestAgent(t, "Reconcile Other Agent B", nil)

	// Issue assigned to A so A's completion is the one that reconciles.
	issueID := dbfx.Issue(t, "reconcile-other-agent fixture", testutil.Cols{
		"status":        "in_progress",
		"number":        999003,
		"assignee_type": "agent",
		"assignee_id":   agentA,
	})

	// A's trigger comment, created before the run starts.
	triggerCommentID := dbfx.Comment(t, issueID, "initial request", testutil.Cols{
		"created_at": testutil.Raw("now() - interval '10 minutes'"),
	})

	// A running task for A whose started_at is in the past.
	var taskID string
	dbfx.QueryRow(t, `
		INSERT INTO agent_task_queue (agent_id, runtime_id, issue_id, trigger_comment_id, delivered_comment_ids, status, priority, created_at, started_at)
		VALUES ($1, $2, $3, $4, ARRAY[$4::uuid], 'running', 0, now() - interval '10 minutes', now() - interval '5 minutes')
		RETURNING id
	`, agentA, runtimeID, issueID, triggerCommentID).Scan(&taskID)
	t.Cleanup(func() { testPool.Exec(ctx, `DELETE FROM agent_task_queue WHERE issue_id = $1`, issueID) })

	// A member comment posted DURING A's run that @-mentions agent B.
	mention := "[@B](mention://agent/" + agentB + ") please take a look"
	dbfx.Exec(t, `
		INSERT INTO comment (issue_id, workspace_id, author_type, author_id, content, type, created_at)
		VALUES ($1, $2, 'member', $3, $4, 'comment', now() - interval '1 minute')
	`, issueID, testWorkspaceID, testUserID, mention)

	if w := completeTaskViaHandler(t, taskID, "done"); w.Code != http.StatusOK {
		t.Fatalf("CompleteTask: expected 200, got %d: %s", w.Code, w.Body.String())
	}

	// The @B comment routes to B, not A, so scoping reconcile to A means
	// NEITHER agent gets a completion-driven follow-up. B in particular must
	// not be re-woken by A's completion.
	if n := pendingTaskCountForAgentIssue(t, issueID, agentB); n != 0 {
		t.Fatalf("agent B must not be re-triggered by agent A's completion, got %d B task(s)", n)
	}
	if n := pendingTaskCountForAgentIssue(t, issueID, agentA); n != 0 {
		t.Fatalf("agent A must not enqueue a follow-up for a comment addressed to B, got %d A task(s)", n)
	}
}

// TestCompleteTask_ReconcilesAgentAuthoredMentionToCompletedAgent is the
// MUL-4304 regression test. It drives the ACTUAL drop path (review must-fix):
//
//   - Agent B already has a DISPATCHED task on the issue. (This is the only
//     state that drops the mention. `running`/`queued` do not: a queued task
//     merges the comment in, and a running-only target is not AlreadyPending so
//     it takes the normal fresh-enqueue path.)
//   - Agent A posts an explicit `@B` comment through the real trigger path
//     (triggerTasksForComment, same entry CreateComment uses). Because B's task
//     is dispatched, `AlreadyPending` is true, mergeCommentIntoPendingTask finds
//     no QUEUED row to fold into, and the active-task check `continue`s — the
//     mention is dropped at creation time and deferred to completion reconcile.
//   - Before the fix, reconcile listed only member comments, so A's
//     agent-authored `@B` mention was NEVER replayed and B was silently never
//     re-woken. With the fix, completing B's task must enqueue exactly one
//     follow-up for B.
//
// The test asserts BOTH halves: no queued follow-up at creation (proving the
// drop actually happens), then exactly one after completion (proving reconcile
// recovers it).
func TestCompleteTask_ReconcilesAgentAuthoredMentionToCompletedAgent(t *testing.T) {
	if testHandler == nil {
		t.Skip("database not available")
	}
	ctx := context.Background()

	var runtimeID string
	dbfx.QueryRow(t,
		`SELECT runtime_id FROM agent WHERE workspace_id = $1 AND runtime_id IS NOT NULL LIMIT 1`,
		testWorkspaceID).Scan(&runtimeID)
	// Two workspace-invocable agents: A authors the mention, B is the target
	// (and the agent whose run completes / reconciles).
	agentA := createHandlerTestAgent(t, "Reconcile A2A Author A", nil)
	agentB := createHandlerTestAgent(t, "Reconcile A2A Target B", nil)

	// Issue assigned to B so B's completion is the one that reconciles.
	issueID := dbfx.Issue(t, "reconcile-a2a-mention fixture", testutil.Cols{
		"status":        "in_progress",
		"number":        999007,
		"assignee_type": "agent",
		"assignee_id":   agentB,
	})
	t.Cleanup(func() { testPool.Exec(ctx, `DELETE FROM comment WHERE issue_id = $1`, issueID) })

	issue, err := testHandler.Queries.GetIssue(ctx, util.MustParseUUID(issueID))
	if err != nil {
		t.Fatalf("setup: load issue: %v", err)
	}

	// B's trigger comment, created before the run starts.
	triggerCommentID := dbfx.Comment(t, issueID, "initial request", testutil.Cols{
		"created_at": testutil.Raw("now() - interval '10 minutes'"),
	})

	// B's task is DISPATCHED (claim response already built, not yet running) —
	// the state that makes an incoming mention hit the merge-miss + active-task
	// drop at creation time.
	var taskID string
	dbfx.QueryRow(t, `
		INSERT INTO agent_task_queue (agent_id, runtime_id, issue_id, trigger_comment_id, delivered_comment_ids, status, priority, created_at, dispatched_at)
		VALUES ($1, $2, $3, $4, ARRAY[$4::uuid], 'dispatched', 0, now() - interval '10 minutes', now() - interval '5 minutes')
		RETURNING id
	`, agentB, runtimeID, issueID, triggerCommentID).Scan(&taskID)
	t.Cleanup(func() { testPool.Exec(ctx, `DELETE FROM agent_task_queue WHERE issue_id = $1`, issueID) })

	// Agent A posts an explicit @B mention through the real trigger path while
	// B is dispatched. Insert the row, then drive triggerTasksForComment exactly
	// as the CreateComment handler would for an agent-authored comment.
	var mentionCommentID string
	mention := "[@B](mention://agent/" + agentB + ") please also handle this"
	dbfx.QueryRow(t, `
		INSERT INTO comment (issue_id, workspace_id, author_type, author_id, content, type)
		VALUES ($1, $2, 'agent', $3, $4, 'comment')
		RETURNING id
	`, issueID, testWorkspaceID, agentA, mention).Scan(&mentionCommentID)
	dbfx.Exec(t, `UPDATE comment SET parent_id=$2 WHERE id=$1`, mentionCommentID, triggerCommentID)
	mentionComment, err := testHandler.Queries.GetComment(ctx, util.MustParseUUID(mentionCommentID))
	if err != nil {
		t.Fatalf("setup: load mention comment: %v", err)
	}
	testHandler.triggerTasksForComment(ctx, issue, mentionComment, nil, "agent", agentA, "", nil, nil)

	// Drop happened: the mention found no queued task to merge into and an
	// active (dispatched) task exists, so NO fresh queued follow-up was created.
	if n := queuedTaskCountForAgentIssue(t, issueID, agentB); n != 0 {
		t.Fatalf("expected the mention to be dropped at creation (0 queued follow-up), got %d", n)
	}

	// B's task progresses dispatched → running, then completes.
	dbfx.Exec(t, `UPDATE agent_task_queue SET status = 'running', started_at = now() - interval '1 minute' WHERE id = $1`, taskID)
	if w := completeTaskViaHandler(t, taskID, "done"); w.Code != http.StatusOK {
		t.Fatalf("CompleteTask: expected 200, got %d: %s", w.Code, w.Body.String())
	}

	// Reconcile recovers the dropped mention: exactly one queued follow-up for B.
	if n := queuedTaskCountForAgentIssue(t, issueID, agentB); n != 1 {
		t.Fatalf("expected exactly 1 follow-up for B from the agent-authored @B mention after completion, got %d", n)
	}
	// A authored the comment but is not its target, so A must not be enqueued.
	if n := pendingTaskCountForAgentIssue(t, issueID, agentA); n != 0 {
		t.Fatalf("comment author A must not be enqueued, got %d A task(s)", n)
	}
}

// TestCompleteTask_DoesNotReconcilePlainAgentReply guards the anti-loop
// boundary of MUL-4304 on an agent-assigned issue: an agent-authored comment
// with NO explicit @mention (a plain reply / acknowledgement) must never earn a
// follow-up, even though reconcile now considers agent comments. Only explicit
// @agent/@squad mentions are replayed.
func TestCompleteTask_DoesNotReconcilePlainAgentReply(t *testing.T) {
	if testHandler == nil {
		t.Skip("database not available")
	}
	ctx := context.Background()

	var runtimeID string
	dbfx.QueryRow(t,
		`SELECT runtime_id FROM agent WHERE workspace_id = $1 AND runtime_id IS NOT NULL LIMIT 1`,
		testWorkspaceID).Scan(&runtimeID)
	agentA := createHandlerTestAgent(t, "Reconcile PlainReply Author A", nil)
	agentB := createHandlerTestAgent(t, "Reconcile PlainReply Target B", nil)

	issueID := dbfx.Issue(t, "reconcile-plain-agent-reply fixture", testutil.Cols{
		"status":        "in_progress",
		"number":        999008,
		"assignee_type": "agent",
		"assignee_id":   agentB,
	})

	triggerCommentID := dbfx.Comment(t, issueID, "initial request", testutil.Cols{
		"created_at": testutil.Raw("now() - interval '10 minutes'"),
	})

	var taskID string
	dbfx.QueryRow(t, `
		INSERT INTO agent_task_queue (agent_id, runtime_id, issue_id, trigger_comment_id, delivered_comment_ids, status, priority, created_at, started_at)
		VALUES ($1, $2, $3, $4, ARRAY[$4::uuid], 'running', 0, now() - interval '10 minutes', now() - interval '5 minutes')
		RETURNING id
	`, agentB, runtimeID, issueID, triggerCommentID).Scan(&taskID)
	t.Cleanup(func() { testPool.Exec(ctx, `DELETE FROM agent_task_queue WHERE issue_id = $1`, issueID) })

	// A plain agent-authored reply during B's run — NO mention of anyone.
	dbfx.Exec(t, `
		INSERT INTO comment (issue_id, workspace_id, author_type, author_id, content, type, created_at)
		VALUES ($1, $2, 'agent', $3, 'thanks, looks good to me', 'comment', now() - interval '1 minute')
	`, issueID, testWorkspaceID, agentA)

	if w := completeTaskViaHandler(t, taskID, "done"); w.Code != http.StatusOK {
		t.Fatalf("CompleteTask: expected 200, got %d: %s", w.Code, w.Body.String())
	}

	if n := pendingTaskCountForAgentIssue(t, issueID, agentB); n != 0 {
		t.Fatalf("a plain agent reply (no mention) must not enqueue a follow-up, got %d B task(s)", n)
	}
	if n := pendingTaskCountForAgentIssue(t, issueID, agentA); n != 0 {
		t.Fatalf("a plain agent reply (no mention) must not enqueue a follow-up, got %d A task(s)", n)
	}
}

// TestCompleteTask_DoesNotReconcilePlainWorkerReplyOnSquadIssue is the MUL-4304
// review must-fix #2 regression test. On a SQUAD-assigned issue,
// computeCommentAgentTriggers routes a plain worker-agent reply (no mention) to
// the squad leader via routeAssignedSquadLeaderFallback (Source = issue
// assignee) — that is the create-time leader→worker→leader coordination path.
// This reply was never accepted into a run's input plan. Reconcile must NOT
// turn a timestamp-only plain reply into a new conversation. Accepted worker
// handoffs are covered separately by TestWorkerReplyDelivery.
func TestCompleteTask_DoesNotReconcilePlainWorkerReplyOnSquadIssue(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database not available")
	}
	fx := newSquadCommentTriggerFixture(t)
	issueID := uuidToString(fx.Issue.ID)
	t.Cleanup(func() {
		testPool.Exec(context.Background(), `DELETE FROM agent_task_queue WHERE issue_id = $1`, issueID)
		testPool.Exec(context.Background(), `DELETE FROM comment WHERE issue_id = $1`, issueID)
	})

	var leaderRuntimeID string
	dbfx.QueryRow(t, `SELECT runtime_id FROM agent WHERE id = $1`, fx.LeaderID).Scan(&leaderRuntimeID)
	// A running leader task whose completion drives reconcile.
	leaderTaskID := dbfx.Task(t, fx.LeaderID, testutil.Cols{
		"runtime_id":     leaderRuntimeID,
		"issue_id":       issueID,
		"status":         "running",
		"is_leader_task": true,
		"squad_id":       fx.SquadID,
		"created_at":     testutil.Raw("now() - interval '10 minutes'"),
		"started_at":     testutil.Raw("now() - interval '5 minutes'"),
	})

	// A plain worker-agent reply (no mention) posted during the leader's run.
	// At create time this WOULD route to the leader via the squad-leader
	// fallback; reconcile must not replay it.
	dbfx.Exec(t, `
		INSERT INTO comment (issue_id, workspace_id, author_type, author_id, content, type, created_at)
		VALUES ($1, $2, 'agent', $3, 'done — pushed the change', 'comment', now() - interval '1 minute')
	`, issueID, testWorkspaceID, fx.OtherID)

	if w := completeTaskViaHandler(t, leaderTaskID, "done"); w.Code != http.StatusOK {
		t.Fatalf("CompleteTask: expected 200, got %d: %s", w.Code, w.Body.String())
	}

	// The squad-leader fallback is a non-mention route, so reconcile must not
	// enqueue any follow-up for the leader from a plain worker reply.
	if n := pendingTaskCountForAgentIssue(t, issueID, fx.LeaderID); n != 0 {
		t.Fatalf("plain worker reply must not reconcile-wake the squad leader, got %d leader task(s)", n)
	}
}

// handlerWorkspaceMember inserts a fresh user + workspace member and returns
// the user id (for a second distinct originator).
func handlerWorkspaceMember(t *testing.T, slug string) string {
	t.Helper()
	var userID string
	email := slug + "-" + time.Now().Format("150405.000000") + "@example.test"
	dbfx.QueryRow(t, `INSERT INTO "user" (name, email) VALUES ($1, $2) RETURNING id`,
		"Reconcile Test "+slug, email).Scan(&userID)
	dbfx.Exec(t, `INSERT INTO member (workspace_id, user_id, role) VALUES ($1, $2, 'admin')`,
		testWorkspaceID, userID)
	t.Cleanup(func() {
		testPool.Exec(context.Background(), `DELETE FROM member WHERE user_id = $1`, userID)
		testPool.Exec(context.Background(), `DELETE FROM "user" WHERE id = $1`, userID)
	})
	return userID
}

// TestConsecutiveCommentsDifferentOriginatorsFullEnqueuePath is the MUL-4195
// second-round must-fix #1 regression test, driving the FULL handler enqueue
// path (computeCommentAgentTriggers → enqueueCommentAgentTriggers → merge), not
// just the SQL. Member A's comment creates a queued task; member B (a different
// originator) then comments before the run starts. The earlier build returned
// ErrNoRows from the originator gate and fell through to a fresh enqueue that
// tripped the one-pending-per-(issue,agent) unique index, silently dropping B's
// comment. With recompute-on-merge, B's comment folds into the single task:
// still one task (no drop, no collision), trigger repointed to B, originator
// re-stamped to B, and A's comment preserved as coalesced.
func TestConsecutiveCommentsDifferentOriginatorsFullEnqueuePath(t *testing.T) {
	if testHandler == nil {
		t.Skip("database not available")
	}
	ctx := context.Background()

	var agentID string
	dbfx.QueryRow(t,
		`SELECT id FROM agent WHERE workspace_id = $1 AND runtime_id IS NOT NULL ORDER BY created_at ASC LIMIT 1`,
		testWorkspaceID).Scan(&agentID)
	userB := handlerWorkspaceMember(t, "originatorB")

	issueID := dbfx.Issue(t, "diff-originator fixture", testutil.Cols{
		"status":        "in_progress",
		"number":        999004,
		"assignee_type": "agent",
		"assignee_id":   agentID,
	})
	t.Cleanup(func() {
		testPool.Exec(ctx, `DELETE FROM agent_task_queue WHERE issue_id = $1`, issueID)
		testPool.Exec(ctx, `DELETE FROM comment WHERE issue_id = $1`, issueID)
		testPool.Exec(ctx, `DELETE FROM issue WHERE id = $1`, issueID)
	})

	issue, err := testHandler.Queries.GetIssue(ctx, util.MustParseUUID(issueID))
	if err != nil {
		t.Fatalf("load issue: %v", err)
	}

	insertMemberComment := func(authorID, content string) db.Comment {
		t.Helper()
		id := dbfx.Comment(t, issueID, content, testutil.Cols{
			"author_id": authorID,
		})
		c, err := testHandler.Queries.GetComment(ctx, util.MustParseUUID(id))
		if err != nil {
			t.Fatalf("load comment: %v", err)
		}
		return c
	}

	// A's comment → creates the queued task (originator A).
	cA := insertMemberComment(testUserID, "first, from A")
	testHandler.triggerTasksForComment(ctx, issue, cA, nil, "member", testUserID, testUserID, nil, nil)
	if n := pendingTaskCountForAgentIssue(t, issueID, agentID); n != 1 {
		t.Fatalf("after A's comment expected exactly 1 queued task, got %d", n)
	}

	// B's comment (different originator) before start → must fold in, NOT drop.
	cB := insertMemberComment(userB, "second, from B — different user")
	dbfx.Exec(t, `UPDATE comment SET parent_id=$2 WHERE id=$1`, cB.ID, cA.ID)
	cB.ParentID = cA.ID
	testHandler.triggerTasksForComment(ctx, issue, cB, nil, "member", userB, userB, nil, nil)

	// Still exactly one task (bounded concurrency, no unique-index collision).
	if n := pendingTaskCountForAgentIssue(t, issueID, agentID); n != 1 {
		t.Fatalf("after B's comment expected still exactly 1 task (folded in, not dropped/duplicated), got %d", n)
	}
	// Trigger repointed to B, originator re-stamped to B, A coalesced.
	trigger, originator, coalesced := taskTriggerOriginatorCoalesced(t, issueID, agentID)
	if trigger != uuidToString(cB.ID) {
		t.Errorf("expected trigger repointed to B's comment %s, got %s", uuidToString(cB.ID), trigger)
	}
	if originator != userB {
		t.Errorf("expected originator re-stamped to B (%s), got %s", userB, originator)
	}
	if !containsUUID(coalesced, uuidToString(cA.ID)) {
		t.Errorf("expected A's comment %s preserved as coalesced, got %v", uuidToString(cA.ID), coalesced)
	}
}

// TestCompleteTask_ReconcilesDispatchedWindowComment is the MUL-4195
// second-round must-fix #2 regression test. A member comment that lands AFTER
// the claim response was built (after dispatched_at) but BEFORE StartTask
// (before started_at) must still earn a follow-up. The earlier reconcile
// anchored on started_at and missed this window; anchoring on dispatched_at
// catches it.
func TestCompleteTask_ReconcilesDispatchedWindowComment(t *testing.T) {
	if testHandler == nil {
		t.Skip("database not available")
	}
	ctx := context.Background()

	var agentID, runtimeID string
	dbfx.QueryRow(t,
		`SELECT id, runtime_id FROM agent WHERE workspace_id = $1 AND runtime_id IS NOT NULL LIMIT 1`,
		testWorkspaceID).Scan(&agentID, &runtimeID)

	issueID := dbfx.Issue(t, "dispatched-window fixture", testutil.Cols{
		"status":        "in_progress",
		"number":        999005,
		"assignee_type": "agent",
		"assignee_id":   agentID,
	})

	triggerCommentID := dbfx.Comment(t, issueID, "initial request", testutil.Cols{
		"created_at": testutil.Raw("now() - interval '10 minutes'"),
	})

	// Running task: dispatched 5m ago, started 2m ago. The claim response was
	// built at dispatch.
	var taskID string
	dbfx.QueryRow(t, `
		INSERT INTO agent_task_queue (agent_id, runtime_id, issue_id, trigger_comment_id, delivered_comment_ids, status, priority, created_at, dispatched_at, started_at)
		VALUES ($1, $2, $3, $4, ARRAY[$4::uuid], 'running', 0, now() - interval '10 minutes', now() - interval '5 minutes', now() - interval '2 minutes')
		RETURNING id
	`, agentID, runtimeID, issueID, triggerCommentID).Scan(&taskID)
	t.Cleanup(func() { testPool.Exec(ctx, `DELETE FROM agent_task_queue WHERE issue_id = $1`, issueID) })

	// A member comment in the dispatch→start window: after dispatched_at
	// (5m ago), before started_at (2m ago). A started_at anchor would miss it.
	dbfx.Exec(t, `
		INSERT INTO comment (issue_id, workspace_id, author_type, author_id, content, type, created_at)
		VALUES ($1, $2, 'member', $3, 'squeezed in before start', 'comment', now() - interval '3 minutes')
	`, issueID, testWorkspaceID, testUserID)

	dbfx.Exec(t, `UPDATE comment SET parent_id=$2 WHERE issue_id=$1 AND id<>$2`, issueID, triggerCommentID)

	if w := completeTaskViaHandler(t, taskID, "done"); w.Code != http.StatusOK {
		t.Fatalf("CompleteTask: expected 200, got %d: %s", w.Code, w.Body.String())
	}

	if n := pendingTaskCountForAgentIssue(t, issueID, agentID); n != 1 {
		t.Fatalf("expected exactly 1 follow-up for the dispatch-window comment, got %d", n)
	}
}

// taskTriggerOriginatorCoalesced returns (trigger_comment_id, originator_user_id,
// coalesced_comment_ids) as text for the most recent task of (issue, agent).
func taskTriggerOriginatorCoalesced(t *testing.T, issueID, agentID string) (string, string, []string) {
	t.Helper()
	var trigger, originator string
	var coalesced []string
	dbfx.QueryRow(t, `
		SELECT COALESCE(trigger_comment_id::text, ''),
		       COALESCE(originator_user_id::text, ''),
		       coalesced_comment_ids::text[]
		  FROM agent_task_queue
		 WHERE issue_id = $1 AND agent_id = $2
		 ORDER BY created_at DESC
		 LIMIT 1
	`, issueID, agentID).Scan(&trigger, &originator, &coalesced)
	return trigger, originator, coalesced
}

func containsUUID(ids []string, want string) bool {
	for _, id := range ids {
		if id == want {
			return true
		}
	}
	return false
}

// TestCompleteTask_ReconcilesPreDispatchMergeRaceComment is the MUL-4195
// round-3 must-fix regression test. A member comment is created while the task
// is still queued, but its merge loses the race to the daemon claiming the task
// (queued→dispatched); the merge then finds no pre-claim row and the enqueue
// path defers to reconcile. The comment's created_at is BEFORE dispatched_at,
// so a dispatched_at-anchored reconcile would skip it and it would vanish. The
// created_at anchor + delivered-set exclusion must catch it — while NOT
// re-firing a comment that WAS delivered as a pre-claim coalesced entry.
func TestCompleteTask_ReconcilesPreDispatchMergeRaceComment(t *testing.T) {
	if testHandler == nil {
		t.Skip("database not available")
	}
	ctx := context.Background()

	var agentID, runtimeID string
	dbfx.QueryRow(t,
		`SELECT id, runtime_id FROM agent WHERE workspace_id = $1 AND runtime_id IS NOT NULL LIMIT 1`,
		testWorkspaceID).Scan(&agentID, &runtimeID)

	issueID := dbfx.Issue(t, "pre-dispatch race fixture", testutil.Cols{
		"status":        "in_progress",
		"number":        999006,
		"assignee_type": "agent",
		"assignee_id":   agentID,
	})

	// Timeline: task created 10m ago; the run's trigger 10m ago; a delivered
	// pre-claim coalesced comment 8m ago; the RACE comment 7m ago (still before
	// dispatch); dispatched 6m ago; started 5m ago.
	insertComment := func(content, age string) string {
		t.Helper()
		var id string
		dbfx.QueryRow(t, `
			INSERT INTO comment (issue_id, workspace_id, author_type, author_id, content, type, created_at)
			VALUES ($1, $2, 'member', $3, $4, 'comment', now() - $5::interval) RETURNING id
		`, issueID, testWorkspaceID, testUserID, content, age).Scan(&id)
		return id
	}
	triggerCommentID := insertComment("initial request", "10 minutes")
	deliveredCoalescedID := insertComment("folded in while queued (delivered)", "8 minutes")
	raceCommentID := insertComment("posted before dispatch, merge lost the race", "7 minutes")

	// The running task: created before every comment window, with the delivered
	// comment recorded in coalesced_comment_ids, dispatched after the race
	// comment, started later still.
	var taskID string
	dbfx.QueryRow(t, `
		INSERT INTO agent_task_queue
			(agent_id, runtime_id, issue_id, trigger_comment_id, coalesced_comment_ids, delivered_comment_ids, status, priority, created_at, dispatched_at, started_at)
		VALUES ($1, $2, $3, $4, ARRAY[$5::uuid], ARRAY[$4::uuid, $5::uuid], 'running', 0,
			now() - interval '10 minutes', now() - interval '6 minutes', now() - interval '5 minutes')
		RETURNING id
	`, agentID, runtimeID, issueID, triggerCommentID, deliveredCoalescedID).Scan(&taskID)
	t.Cleanup(func() { testPool.Exec(ctx, `DELETE FROM agent_task_queue WHERE issue_id = $1`, issueID) })

	dbfx.Exec(t, `UPDATE comment SET parent_id=$2 WHERE issue_id=$1 AND id<>$2`, issueID, triggerCommentID)

	if w := completeTaskViaHandler(t, taskID, "done"); w.Code != http.StatusOK {
		t.Fatalf("CompleteTask: expected 200, got %d: %s", w.Code, w.Body.String())
	}

	// Exactly one follow-up, and it must be for the RACE comment — the
	// delivered coalesced comment must be excluded (not re-fired).
	if n := pendingTaskCountForAgentIssue(t, issueID, agentID); n != 1 {
		t.Fatalf("expected exactly 1 follow-up for the pre-dispatch race comment, got %d", n)
	}
	trigger, _, coalesced := taskTriggerOriginatorCoalesced(t, issueID, agentID)
	if trigger != raceCommentID {
		t.Errorf("follow-up trigger must be the race comment %s, got %s", raceCommentID, trigger)
	}
	if containsUUID(coalesced, deliveredCoalescedID) || trigger == deliveredCoalescedID {
		t.Errorf("the already-delivered coalesced comment %s must be excluded from the follow-up, got trigger=%s coalesced=%v",
			deliveredCoalescedID, trigger, coalesced)
	}
}

// TestCompleteTask_ReconcilesPlannedButUndeliveredComments pins the distinction
// between the enqueue plan and the claim receipt. The planned comments predate
// the task (as they do on an auto-retry), so the planned-id query branch is the
// only way completion can recover payload-overflow or legacy-undelivered input.
func TestCompleteTask_ReconcilesPlannedButUndeliveredComments(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database not available")
	}

	tests := []struct {
		name              string
		deliveredOldCount int
		wantFollowup      int
	}{
		{name: "partial receipt replays only omitted suffix", deliveredOldCount: 1, wantFollowup: 1},
		{name: "legacy trigger-only receipt replays whole coalesced batch", deliveredOldCount: 0, wantFollowup: 2},
	}
	for _, tc := range tests {
		t.Run(tc.name, func(t *testing.T) {
			ctx := context.Background()
			runtimeID := createClaimReclaimRuntime(t, ctx, "Planned undelivered runtime "+tc.name)
			agentID, issueID := createClaimReclaimAgentAndIssue(t, ctx, runtimeID, "Planned undelivered agent "+tc.name)
			dbfx.Exec(t, `
				UPDATE issue
				SET assignee_type = 'agent', assignee_id = $2
				WHERE id = $1
			`, issueID, agentID)

			insertComment := func(content, age string) string {
				t.Helper()
				var id string
				dbfx.QueryRow(t, `
					INSERT INTO comment (issue_id, workspace_id, author_type, author_id, content, type, created_at)
					VALUES ($1, $2, 'member', $3, $4, 'comment', now() - $5::interval)
					RETURNING id
				`, issueID, testWorkspaceID, testUserID, content, age).Scan(&id)
				return id
			}
			old1 := insertComment("planned old one", "10 minutes")
			old2 := insertComment("[@Agent](mention://agent/"+agentID+") planned old two", "9 minutes")
			trigger := insertComment("planned trigger", "8 minutes")
			dbfx.Exec(t, `UPDATE comment SET parent_id=$2 WHERE issue_id=$1 AND id<>$2`, issueID, old1)
			delivered := []string{trigger}
			if tc.deliveredOldCount > 0 {
				delivered = append([]string{old1}, delivered...)
			}

			var taskID string
			dbfx.QueryRow(t, `
				INSERT INTO agent_task_queue (
					agent_id, runtime_id, issue_id, trigger_comment_id,
					coalesced_comment_ids, delivered_comment_ids,
					status, priority, created_at, dispatched_at, started_at
				)
				VALUES (
					$1, $2, $3, $4,
					ARRAY[$5::uuid, $6::uuid], $7::uuid[],
					'running', 0, now() - interval '5 minutes', now() - interval '4 minutes', now() - interval '3 minutes'
				)
				RETURNING id
			`, agentID, runtimeID, issueID, trigger, old1, old2, delivered).Scan(&taskID)
			t.Cleanup(func() {
				testPool.Exec(context.Background(), `DELETE FROM agent_task_queue WHERE issue_id = $1`, issueID)
			})

			if w := completeTaskViaHandler(t, taskID, "done"); w.Code != http.StatusOK {
				t.Fatalf("CompleteTask: expected 200, got %d: %s", w.Code, w.Body.String())
			}
			if n := pendingTaskCountForAgentIssue(t, issueID, agentID); n != 1 {
				t.Fatalf("expected one bounded follow-up, got %d", n)
			}
			followupTrigger, _, followupCoalesced := taskTriggerOriginatorCoalesced(t, issueID, agentID)
			covered := append([]string{}, followupCoalesced...)
			covered = append(covered, followupTrigger)
			slices.Sort(covered)
			want := []string{old2}
			if tc.wantFollowup == 2 {
				want = []string{old1, old2}
			}
			slices.Sort(want)
			if !slices.Equal(covered, want) {
				t.Fatalf("follow-up coverage = %v, want %v", covered, want)
			}
		})
	}
}

// TestCompleteTask_ReconcilesExplicitMentionFromCompletedWorker pins Blocker 2
// (GH #8719 human spec): the completion-reconcile skip for other runs'
// comments applies ONLY to the exact recorded fallback id. A normal explicit
// @B mention authored by a completed non-leader worker (same shape, no
// record) must still be recovered when B completes.
//
// Setup mirrors TestCompleteTask_ReconcilesAgentAuthoredMentionToCompletedAgent
// (B dispatched, so creation-time routing drops the mention), except the
// mention carries the author's source task id and the author completes first:
// with the old completed-non-leader shape guard the mention would be skipped
// and no follow-up would ever appear.
func TestCompleteTask_ReconcilesExplicitMentionFromCompletedWorker(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database not available")
	}
	ctx := context.Background()

	var runtimeID string
	dbfx.QueryRow(t,
		`SELECT runtime_id FROM agent WHERE workspace_id = $1 AND runtime_id IS NOT NULL LIMIT 1`,
		testWorkspaceID).Scan(&runtimeID)
	workerW := createHandlerTestAgent(t, "Reconcile Completed Worker W", nil)
	agentB := createHandlerTestAgent(t, "Reconcile Explicit Target B", nil)

	issueID := dbfx.Issue(t, "reconcile-explicit-from-completed-worker fixture", testutil.Cols{
		"status":        "in_progress",
		"number":        999013,
		"assignee_type": "agent",
		"assignee_id":   agentB,
	})
	t.Cleanup(func() { testPool.Exec(ctx, `DELETE FROM comment WHERE issue_id = $1`, issueID) })

	issue, err := testHandler.Queries.GetIssue(ctx, util.MustParseUUID(issueID))
	if err != nil {
		t.Fatalf("setup: load issue: %v", err)
	}

	bTriggerCommentID := dbfx.Comment(t, issueID, "initial request for B", testutil.Cols{
		"created_at": testutil.Raw("now() - interval '10 minutes'"),
	})
	var bTaskID string
	dbfx.QueryRow(t, `
		INSERT INTO agent_task_queue (agent_id, runtime_id, issue_id, trigger_comment_id, delivered_comment_ids, status, priority, created_at, dispatched_at)
		VALUES ($1, $2, $3, $4, ARRAY[$4::uuid], 'dispatched', 0, now() - interval '10 minutes', now() - interval '5 minutes')
		RETURNING id
	`, agentB, runtimeID, issueID, bTriggerCommentID).Scan(&bTaskID)
	t.Cleanup(func() { testPool.Exec(ctx, `DELETE FROM agent_task_queue WHERE issue_id = $1`, issueID) })

	wTriggerCommentID := dbfx.Comment(t, issueID, "initial request for W", testutil.Cols{
		"created_at": testutil.Raw("now() - interval '10 minutes'"),
	})
	var wTaskID string
	dbfx.QueryRow(t, `
		INSERT INTO agent_task_queue (agent_id, runtime_id, issue_id, trigger_comment_id, delivered_comment_ids, status, priority, created_at, started_at, originator_user_id, accountable_user_id)
		VALUES ($1, $2, $3, $4, ARRAY[$4::uuid], 'running', 0, now() - interval '10 minutes', now() - interval '9 minutes', $5, $5)
		RETURNING id
	`, workerW, runtimeID, issueID, wTriggerCommentID, testUserID).Scan(&wTaskID)

	var mentionCommentID string
	mention := "[@B](mention://agent/" + agentB + ") please also handle this"
	dbfx.QueryRow(t, `
		INSERT INTO comment (issue_id, workspace_id, author_type, author_id, source_task_id, content, type)
		VALUES ($1, $2, 'agent', $3, $4, $5, 'comment')
		RETURNING id
	`, issueID, testWorkspaceID, workerW, wTaskID, mention).Scan(&mentionCommentID)
	dbfx.Exec(t, `UPDATE comment SET parent_id=$2 WHERE id=$1`, mentionCommentID, bTriggerCommentID)
	mentionComment, err := testHandler.Queries.GetComment(ctx, util.MustParseUUID(mentionCommentID))
	if err != nil {
		t.Fatalf("setup: load mention comment: %v", err)
	}
	testHandler.triggerTasksForComment(ctx, issue, mentionComment, nil, "agent", workerW, "", nil, nil)

	if n := queuedTaskCountForAgentIssue(t, issueID, agentB); n != 0 {
		t.Fatalf("expected the mention to be dropped at creation (0 queued follow-up), got %d", n)
	}

	if w := completeTaskViaHandler(t, wTaskID, "done"); w.Code != http.StatusOK {
		t.Fatalf("CompleteTask W: expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var wRecordID *string
	dbfx.QueryRow(t, `SELECT completion_fallback_comment_id::text FROM agent_task_queue WHERE id = $1`, wTaskID).Scan(&wRecordID)
	if wRecordID != nil {
		t.Fatalf("explicit reply was reclassified as a fallback: recorded %s", *wRecordID)
	}

	dbfx.Exec(t, `UPDATE agent_task_queue SET status = 'running', started_at = now() - interval '1 minute' WHERE id = $1`, bTaskID)
	if w := completeTaskViaHandler(t, bTaskID, "done"); w.Code != http.StatusOK {
		t.Fatalf("CompleteTask B: expected 200, got %d: %s", w.Code, w.Body.String())
	}

	if n := queuedTaskCountForAgentIssue(t, issueID, agentB); n != 1 {
		t.Fatalf("expected exactly 1 follow-up for B from the completed worker explicit mention after completion, got %d", n)
	}
	if n := pendingTaskCountForAgentIssue(t, issueID, workerW); n != 0 {
		t.Fatalf("comment author W must not be enqueued, got %d W task(s)", n)
	}
}

// TestCompletionReconcileSkipsRecordedFallback pins the other half of Blocker 2
// (GH #8719 human spec): a recorded fallback whose body carries a mention
// must never take generic routing in another run's completion reconcile.
// Removing the guard would wake B here; the exact-ID skip keeps it quiet.
func TestCompletionReconcileSkipsRecordedFallback(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database not available")
	}
	ctx := context.Background()

	var runtimeID string
	dbfx.QueryRow(t,
		`SELECT runtime_id FROM agent WHERE workspace_id = $1 AND runtime_id IS NOT NULL LIMIT 1`,
		testWorkspaceID).Scan(&runtimeID)
	workerW := createHandlerTestAgent(t, "Reconcile Fallback Author W", nil)
	agentB := createHandlerTestAgent(t, "Reconcile Fallback Bystander B", nil)

	issueID := dbfx.Issue(t, "reconcile-recorded-fallback fixture", testutil.Cols{
		"status":        "in_progress",
		"number":        999014,
		"assignee_type": "agent",
		"assignee_id":   agentB,
	})
	t.Cleanup(func() { testPool.Exec(ctx, `DELETE FROM comment WHERE issue_id = $1`, issueID) })

	// One shared trigger root so the synthesized fallback lands in B's
	// reconcilable thread; otherwise the test would pass vacuously.
	rootCommentID := dbfx.Comment(t, issueID, "initial request", testutil.Cols{
		"created_at": testutil.Raw("now() - interval '10 minutes'"),
	})
	var wTaskID string
	dbfx.QueryRow(t, `
		INSERT INTO agent_task_queue (agent_id, runtime_id, issue_id, trigger_comment_id, delivered_comment_ids, status, priority, created_at, started_at, originator_user_id, accountable_user_id)
		VALUES ($1, $2, $3, $4, ARRAY[$4::uuid], 'running', 0, now() - interval '10 minutes', now() - interval '9 minutes', $5, $5)
		RETURNING id
	`, workerW, runtimeID, issueID, rootCommentID, testUserID).Scan(&wTaskID)

	var bTaskID string
	dbfx.QueryRow(t, `
		INSERT INTO agent_task_queue (agent_id, runtime_id, issue_id, trigger_comment_id, delivered_comment_ids, status, priority, created_at, started_at)
		VALUES ($1, $2, $3, $4, ARRAY[$4::uuid], 'running', 0, now() - interval '10 minutes', now() - interval '9 minutes')
		RETURNING id
	`, agentB, runtimeID, issueID, rootCommentID).Scan(&bTaskID)
	t.Cleanup(func() { testPool.Exec(ctx, `DELETE FROM agent_task_queue WHERE issue_id = $1`, issueID) })

	if w := completeTaskViaHandler(t, wTaskID, "Processed the inputs delivered to this run"); w.Code != http.StatusOK {
		t.Fatalf("CompleteTask W: expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var fallbackID string
	dbfx.QueryRow(t, `SELECT id FROM comment WHERE source_task_id = $1 AND author_id = $2`, wTaskID, workerW).Scan(&fallbackID)
	if fallbackID == "" {
		t.Fatal("W fallback was not synthesized")
	}
	dbfx.Exec(t, `UPDATE comment SET content = $2 WHERE id = $1`, fallbackID,
		"Done. [@B](mention://agent/"+agentB+") please review as well")

	if w := completeTaskViaHandler(t, bTaskID, "done"); w.Code != http.StatusOK {
		t.Fatalf("CompleteTask B: expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if n := queuedTaskCountForAgentIssue(t, issueID, agentB); n != 0 {
		t.Fatalf("recorded fallback with a mention body woke B: got %d queued follow-up(s), want 0", n)
	}
}

// TestCompleteTask_SkipsExplicitMentionWhenSourceGone pins the permanent half
// of the reconcile lookup contract (GH #8719): an agent comment whose source
// run is provably gone (no-rows) stays skipped -- its lineage can never be
// proven, so only the transient path above recovers. Exact recorded fallbacks
// keep their own skip invariant regardless of this branch.
func TestCompleteTask_SkipsExplicitMentionWhenSourceGone(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database not available")
	}
	ctx := context.Background()

	var runtimeID string
	dbfx.QueryRow(t,
		`SELECT runtime_id FROM agent WHERE workspace_id = $1 AND runtime_id IS NOT NULL LIMIT 1`,
		testWorkspaceID).Scan(&runtimeID)
	workerW := createHandlerTestAgent(t, "Reconcile SourceGone Worker W", nil)
	agentB := createHandlerTestAgent(t, "Reconcile SourceGone Target B", nil)

	issueID := dbfx.Issue(t, "reconcile-explicit-source-gone fixture", testutil.Cols{
		"status":        "in_progress",
		"number":        999016,
		"assignee_type": "agent",
		"assignee_id":   agentB,
	})
	t.Cleanup(func() { testPool.Exec(ctx, `DELETE FROM comment WHERE issue_id = $1`, issueID) })

	bTriggerCommentID := dbfx.Comment(t, issueID, "initial request for B", testutil.Cols{
		"created_at": testutil.Raw("now() - interval '10 minutes'"),
	})
	var bTaskID string
	dbfx.QueryRow(t, `
		INSERT INTO agent_task_queue (agent_id, runtime_id, issue_id, trigger_comment_id, delivered_comment_ids, status, priority, created_at, dispatched_at)
		VALUES ($1, $2, $3, $4, ARRAY[$4::uuid], 'dispatched', 0, now() - interval '10 minutes', now() - interval '5 minutes')
		RETURNING id
	`, agentB, runtimeID, issueID, bTriggerCommentID).Scan(&bTaskID)
	t.Cleanup(func() { testPool.Exec(ctx, `DELETE FROM agent_task_queue WHERE issue_id = $1`, issueID) })

	wTriggerCommentID := dbfx.Comment(t, issueID, "initial request for W", testutil.Cols{
		"created_at": testutil.Raw("now() - interval '10 minutes'"),
	})
	var wTaskID string
	dbfx.QueryRow(t, `
		INSERT INTO agent_task_queue (agent_id, runtime_id, issue_id, trigger_comment_id, delivered_comment_ids, status, priority, created_at, started_at, originator_user_id, accountable_user_id)
		VALUES ($1, $2, $3, $4, ARRAY[$4::uuid], 'running', 0, now() - interval '10 minutes', now() - interval '9 minutes', $5, $5)
		RETURNING id
	`, workerW, runtimeID, issueID, wTriggerCommentID, testUserID).Scan(&wTaskID)

	var mentionCommentID string
	mention := "[@B](mention://agent/" + agentB + ") please also handle this"
	dbfx.QueryRow(t, `
		INSERT INTO comment (issue_id, workspace_id, author_type, author_id, source_task_id, content, type)
		VALUES ($1, $2, 'agent', $3, $4, $5, 'comment')
		RETURNING id
	`, issueID, testWorkspaceID, workerW, wTaskID, mention).Scan(&mentionCommentID)

	if w := completeTaskViaHandler(t, wTaskID, "done"); w.Code != http.StatusOK {
		t.Fatalf("CompleteTask W: expected 200, got %d: %s", w.Code, w.Body.String())
	}
	// The source run is provably gone: point the comment at a random id.
	dbfx.Exec(t, `UPDATE comment SET source_task_id = gen_random_uuid() WHERE id = $1`, mentionCommentID)

	dbfx.Exec(t, `UPDATE agent_task_queue SET status = 'running', started_at = now() - interval '1 minute' WHERE id = $1`, bTaskID)
	if w := completeTaskViaHandler(t, bTaskID, "done"); w.Code != http.StatusOK {
		t.Fatalf("CompleteTask B: expected 200, got %d: %s", w.Code, w.Body.String())
	}

	if n := queuedTaskCountForAgentIssue(t, issueID, agentB); n != 0 {
		t.Fatalf("source-gone comment must stay skipped, got %d queued follow-up(s)", n)
	}
}

// retryObligationCount reads how many durable completion-reconcile retry
// obligations a run currently carries (GH #8719).
func retryObligationCount(t *testing.T, taskID string) int {
	t.Helper()
	var n int
	dbfx.QueryRow(t,
		`SELECT COALESCE(jsonb_array_length(completion_reconcile_retry_obligations), 0) FROM agent_task_queue WHERE id = $1`,
		taskID).Scan(&n)
	return n
}

// fallbackCommentCountForTask counts the synthesized completion fallbacks a run
// produced; the completion contract allows exactly one per run.
func fallbackCommentCountForTask(t *testing.T, taskID string) int {
	t.Helper()
	var n int
	dbfx.QueryRow(t, `SELECT count(*) FROM comment WHERE source_task_id = $1`, taskID).Scan(&n)
	return n
}

// failQueriesDB injects read/write failures by query name below ONE database
// handle, standing in for a transient infrastructure outage.
//
// missingTasks fails the GetAgentTask read for those task ids; failQueryRow and
// failExec fail any statement whose text contains the substring. A failure that
// has to land below EVERY call site installs the same wrapper on both handles
// through withFailingQueries.
type failQueriesDB struct {
	db.DBTX
	missingTasks map[string]bool
	failQuery    string
	failQueryRow string
	failExec     string
}

func (d *failQueriesDB) Query(ctx context.Context, query string, args ...any) (pgx.Rows, error) {
	if d.failQuery != "" && strings.Contains(query, d.failQuery) {
		return nil, errors.New("injected query failure: " + d.failQuery)
	}
	return d.DBTX.Query(ctx, query, args...)
}

func (d *failQueriesDB) QueryRow(ctx context.Context, query string, args ...any) pgx.Row {
	if d.failQueryRow != "" && strings.Contains(query, d.failQueryRow) {
		return errRow{err: errors.New("injected query failure: " + d.failQueryRow)}
	}
	if strings.Contains(query, "-- name: GetAgentTask") && len(args) > 0 {
		if id, ok := args[0].(pgtype.UUID); ok && d.missingTasks[uuidToString(id)] {
			return errRow{err: errors.New("injected source task lookup failure")}
		}
	}
	return d.DBTX.QueryRow(ctx, query, args...)
}

func (d *failQueriesDB) Exec(ctx context.Context, query string, args ...any) (pgconn.CommandTag, error) {
	if d.failExec != "" && strings.Contains(query, d.failExec) {
		return pgconn.CommandTag{}, errors.New("injected exec failure: " + d.failExec)
	}
	return d.DBTX.Exec(ctx, query, args...)
}

// withFailingQueries swaps BOTH query handles for the duration of fn.
// Completion reconciliation reads source rows through h.Queries and resolves the
// originator chain through TaskService.Queries, so an outage that wraps only one
// of them does not reproduce the failure the retry contract is about (GH #8719
// review, Blocker 1).
func withFailingQueries(t *testing.T, wrapper *failQueriesDB, fn func()) {
	t.Helper()
	originalQueries := testHandler.Queries
	originalServiceQueries := testHandler.TaskService.Queries
	testHandler.Queries = db.New(wrapper)
	testHandler.TaskService.Queries = db.New(wrapper)
	defer func() {
		testHandler.Queries = originalQueries
		testHandler.TaskService.Queries = originalServiceQueries
	}()
	fn()
}

// mentionAgentBody is the explicit @agent mention the retry fixtures post.
func mentionAgentBody(agentID string) string {
	return "[@B](mention://agent/" + agentID + ") please also handle this"
}

// retryFixture is the shape every completion-reconcile retry test acts on: agent
// B has a running task on the issue, and worker agent W posted one comment while
// B was running, so B's completion reconcile owns it.
type retryFixture struct {
	issueID   string
	agentB    string
	workerW   string
	bTaskID   string
	wTaskID   string
	commentID string
}

// seedRetryFixture builds the fixture above. mentionFor receives the created
// agent ids and returns the comment body (an explicit @B / @S mention), which
// lets a test seed routing topology -- a squad led by B, say -- before the
// comment exists. The comment then goes through the production creation trigger,
// so the run starts exactly where a real comment starts.
func seedRetryFixture(t *testing.T, issueNumber int, label string, mentionFor func(agentB, workerW string) string) retryFixture {
	t.Helper()
	ctx := context.Background()

	var runtimeID string
	dbfx.QueryRow(t,
		`SELECT runtime_id FROM agent WHERE workspace_id = $1 AND runtime_id IS NOT NULL LIMIT 1`,
		testWorkspaceID).Scan(&runtimeID)
	workerW := createHandlerTestAgent(t, label+" Worker W", nil)
	agentB := createHandlerTestAgent(t, label+" Target B", nil)

	issueID := dbfx.Issue(t, label+" fixture", testutil.Cols{
		"status":        "in_progress",
		"number":        issueNumber,
		"assignee_type": "agent",
		"assignee_id":   agentB,
	})
	t.Cleanup(func() { testPool.Exec(ctx, `DELETE FROM comment WHERE issue_id = $1`, issueID) })
	t.Cleanup(func() { testPool.Exec(ctx, `DELETE FROM agent_task_queue WHERE issue_id = $1`, issueID) })
	issue, err := testHandler.Queries.GetIssue(ctx, util.MustParseUUID(issueID))
	if err != nil {
		t.Fatalf("setup: load issue: %v", err)
	}

	// B's run: a claim receipt (dispatched), so the creation path defers the
	// mention to it instead of enqueueing a second run. Both runs anchor ten
	// minutes back, so the comment under test lands inside the reconcile window.
	bTriggerCommentID := dbfx.Comment(t, issueID, "initial request for B", testutil.Cols{
		"created_at": testutil.Raw("now() - interval '10 minutes'"),
	})
	var bTaskID string
	dbfx.QueryRow(t, `
		INSERT INTO agent_task_queue (agent_id, runtime_id, issue_id, trigger_comment_id, delivered_comment_ids, status, priority, created_at, dispatched_at)
		VALUES ($1, $2, $3, $4, ARRAY[$4::uuid], 'dispatched', 0, now() - interval '10 minutes', now() - interval '9 minutes')
		RETURNING id
	`, agentB, runtimeID, issueID, bTriggerCommentID).Scan(&bTaskID)

	// W's run: the authoring run of the comment under test. Its originator is the
	// test member, so a healthy read admits member-scoped targets while an
	// unreadable one does not.
	wTriggerCommentID := dbfx.Comment(t, issueID, "initial request for W", testutil.Cols{
		"created_at": testutil.Raw("now() - interval '10 minutes'"),
	})
	var wTaskID string
	dbfx.QueryRow(t, `
		INSERT INTO agent_task_queue (agent_id, runtime_id, issue_id, trigger_comment_id, delivered_comment_ids, status, priority, created_at, started_at, originator_user_id, accountable_user_id)
		VALUES ($1, $2, $3, $4, ARRAY[$4::uuid], 'running', 0, now() - interval '10 minutes', now() - interval '9 minutes', $5, $5)
		RETURNING id
	`, workerW, runtimeID, issueID, wTriggerCommentID, testUserID).Scan(&wTaskID)

	var commentID string
	dbfx.QueryRow(t, `
		INSERT INTO comment (issue_id, workspace_id, author_type, author_id, source_task_id, parent_id, content, type)
		VALUES ($1, $2, 'agent', $3, $4, $5, $6, 'comment')
		RETURNING id
	`, issueID, testWorkspaceID, workerW, wTaskID, bTriggerCommentID, mentionFor(agentB, workerW)).Scan(&commentID)
	mentionComment, err := testHandler.Queries.GetComment(ctx, util.MustParseUUID(commentID))
	if err != nil {
		t.Fatalf("setup: load mention comment: %v", err)
	}
	testHandler.triggerTasksForComment(ctx, issue, mentionComment, nil, "agent", workerW, "", nil, nil)

	// The run then starts: the completion callback below has to win the
	// running -> completed CAS.
	dbfx.Exec(t, `UPDATE agent_task_queue SET status = 'running', started_at = now() - interval '9 minutes' WHERE id = $1`, bTaskID)

	return retryFixture{
		issueID:   issueID,
		agentB:    agentB,
		workerW:   workerW,
		bTaskID:   bTaskID,
		wTaskID:   wTaskID,
		commentID: commentID,
	}
}

// TestCompleteTask_RecordsRestrictedMentionDuringSourceOutage pins Blocker 1 of
// the GH #8719 review: the durable retry obligation is recorded BEFORE any
// source-dependent authorization runs. Target B here is reachable only through
// the delegation chain's human (a member allow-list entry), so an empty
// originator -- exactly what the same source-task outage collapses originator
// resolution to -- denies it. Asking routing first returned an empty answer,
// recorded nothing, and lost the mention permanently.
func TestCompleteTask_RecordsRestrictedMentionDuringSourceOutage(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database not available")
	}
	ctx := context.Background()
	fx := seedRetryFixture(t, 999020, "Reconcile Retry Restricted", func(agentB, _ string) string {
		return mentionAgentBody(agentB)
	})
	// B is invocable only by the human at the top of the comment's chain.
	dbfx.Exec(t, `DELETE FROM agent_invocation_target WHERE agent_id = $1`, fx.agentB)
	dbfx.Exec(t, `INSERT INTO agent_invocation_target (agent_id, target_type, target_id) VALUES ($1, 'member', $2)`, fx.agentB, testUserID)

	// The source read fails below BOTH call sites: the reconcile's own lookup and
	// the originator resolution the permission gate depends on.
	withFailingQueries(t, &failQueriesDB{DBTX: testPool, missingTasks: map[string]bool{fx.wTaskID: true}}, func() {
		if w := completeTaskViaHandler(t, fx.bTaskID, "done"); w.Code != http.StatusOK {
			t.Fatalf("CompleteTask under outage: expected 200, got %d: %s", w.Code, w.Body.String())
		}
	})
	if n := queuedTaskCountForAgentIssue(t, fx.issueID, fx.agentB); n != 0 {
		t.Fatalf("source outage woke %d B task(s), want 0 (fail closed)", n)
	}
	if n := retryObligationCount(t, fx.bTaskID); n != 1 {
		t.Fatalf("retry obligations after the outage = %d, want exactly 1", n)
	}

	// Reads recovered: the runtime sweep re-decides the owed comment through
	// CURRENT routing -- still admissible, so exactly one follow-up.
	result, err := testHandler.ReplayCompletionReconcileRetries(ctx, 10)
	if err != nil {
		t.Fatalf("ReplayCompletionReconcileRetries: %v", err)
	}
	if result.Scanned == 0 {
		t.Fatal("sweeper scanned no retry obligations")
	}
	if n := queuedTaskCountForAgentIssue(t, fx.issueID, fx.agentB); n != 1 {
		t.Fatalf("retry recovered %d B follow-up(s), want exactly 1", n)
	}
	var followTrigger string
	dbfx.QueryRow(t, `SELECT COALESCE(trigger_comment_id::text, '') FROM agent_task_queue WHERE issue_id = $1 AND agent_id = $2 AND status = 'queued'`, fx.issueID, fx.agentB).Scan(&followTrigger)
	if followTrigger != fx.commentID {
		t.Fatalf("recovered follow-up trigger = %q, want the explicit mention %q", followTrigger, fx.commentID)
	}
	if n := retryObligationCount(t, fx.bTaskID); n != 0 {
		t.Fatalf("sweeper left %d retry obligation(s), want 0", n)
	}
}

func TestCompletionReconcileRetryRetainsObligationOnRoutingReadFailure(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database not available")
	}
	for i, outage := range []struct {
		name         string
		failQuery    string
		failQueryRow string
	}{
		{name: "invocation target lookup", failQuery: "ListAgentInvocationTargets"},
		{name: "parent lookup", failQueryRow: "GetCommentInWorkspace"},
	} {
		t.Run(outage.name, func(t *testing.T) {
			ctx := context.Background()
			fx := seedRetryFixture(t, 999022+i, "Retry Read Outage "+outage.name, func(agentB, _ string) string {
				return mentionAgentBody(agentB)
			})
			dbfx.Exec(t, `DELETE FROM agent_invocation_target WHERE agent_id = $1`, fx.agentB)
			originatorID := dbfx.User(t, "Retry Read Outage Actor", "retry-read-outage-"+strings.ReplaceAll(outage.name, " ", "-")+"-"+time.Now().Format("20060102150405.000000000")+"@multica.test")
			dbfx.Member(t, testWorkspaceID, originatorID, "member")
			dbfx.Exec(t, `UPDATE agent_task_queue SET originator_user_id = $2, accountable_user_id = $2 WHERE id = $1`, fx.wTaskID, originatorID)
			dbfx.Exec(t, `INSERT INTO agent_invocation_target (agent_id, target_type, target_id) VALUES ($1, 'member', $2)`, fx.agentB, originatorID)
			agent, err := testHandler.Queries.GetAgentInWorkspace(ctx, db.GetAgentInWorkspaceParams{ID: util.MustParseUUID(fx.agentB), WorkspaceID: util.MustParseUUID(testWorkspaceID)})
			if err != nil {
				t.Fatalf("load target agent: %v", err)
			}
			if allowed, err := testHandler.invokeAgentDecisionChecked(ctx, agent, "agent", fx.workerW, originatorID, testWorkspaceID); err != nil || !allowed {
				t.Fatalf("seeded target permission is not invocable: allowed=%v err=%v", allowed, err)
			}
			withFailingQueries(t, &failQueriesDB{DBTX: testPool, missingTasks: map[string]bool{fx.wTaskID: true}}, func() {
				if w := completeTaskViaHandler(t, fx.bTaskID, "done"); w.Code != http.StatusOK {
					t.Fatalf("CompleteTask under source outage: expected 200, got %d: %s", w.Code, w.Body.String())
				}
			})
			if got := retryObligationCount(t, fx.bTaskID); got != 1 {
				t.Fatalf("retry obligations before replay = %d, want 1", got)
			}
			if got := queuedTaskCountForAgentIssue(t, fx.issueID, fx.agentB); got != 0 {
				t.Fatalf("source outage queued %d follow-up tasks, want 0", got)
			}
			withFailingQueries(t, &failQueriesDB{DBTX: testPool, failQuery: outage.failQuery, failQueryRow: outage.failQueryRow}, func() {
				if _, err := testHandler.ReplayCompletionReconcileRetries(ctx, 10); err == nil {
					t.Fatal("ReplayCompletionReconcileRetries succeeded during routing read outage")
				}
			})
			if got := retryObligationCount(t, fx.bTaskID); got != 1 {
				t.Fatalf("retry obligations after transient routing failure = %d, want 1", got)
			}
			if got := queuedTaskCountForAgentIssue(t, fx.issueID, fx.agentB); got != 0 {
				t.Fatalf("transient routing failure queued %d follow-up tasks, want 0", got)
			}
			if _, err := testHandler.ReplayCompletionReconcileRetries(ctx, 10); err != nil {
				t.Fatalf("healthy retry replay: %v", err)
			}
			if got := queuedTaskCountForAgentIssue(t, fx.issueID, fx.agentB); got != 1 {
				t.Fatalf("healthy replay queued %d follow-up tasks, want exactly 1", got)
			}
			if got := retryObligationCount(t, fx.bTaskID); got != 0 {
				t.Fatalf("retry obligations after healthy replay = %d, want 0", got)
			}
		})
	}
}

// TestCompletionReconcileRetryKeepsRecordedFallbackOut pins acceptance
// criterion 5: a synthesized completion fallback is identified by its EXACT
// recorded id, never by shape or body, on the retry path too. During the outage
// the fallback is unclassifiable (its own source run cannot be read), so it is
// obligated alongside the mention; at replay the exact-ID check keeps it out of
// generic routing while the mention is recovered through its own trigger.
func TestCompletionReconcileRetryKeepsRecordedFallbackOut(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database not available")
	}
	ctx := context.Background()
	var runtimeID string
	dbfx.QueryRow(t,
		`SELECT runtime_id FROM agent WHERE workspace_id = $1 AND runtime_id IS NOT NULL LIMIT 1`,
		testWorkspaceID).Scan(&runtimeID)
	fx := seedRetryFixture(t, 999021, "Reconcile Retry Fallback", func(agentB, _ string) string {
		return mentionAgentBody(agentB)
	})

	// A second worker's completion fallback, in B's reconcilable thread, whose
	// body mentions B: shape-shared with an explicit worker reply but recorded as
	// a fallback on its own run.
	workerW2 := createHandlerTestAgent(t, "Reconcile Retry Fallback Worker W2", nil)
	var w2TaskID string
	dbfx.QueryRow(t, `
		INSERT INTO agent_task_queue (agent_id, runtime_id, issue_id, trigger_comment_id, delivered_comment_ids, status, priority, created_at, started_at, originator_user_id, accountable_user_id)
		VALUES ($1, $2, $3, $4, ARRAY[$4::uuid], 'running', 0, now() - interval '10 minutes', now() - interval '9 minutes', $5, $5)
		RETURNING id
	`, workerW2, runtimeID, fx.issueID, fx.commentID, testUserID).Scan(&w2TaskID)
	if w := completeTaskViaHandler(t, w2TaskID, "Processed the inputs delivered to this run"); w.Code != http.StatusOK {
		t.Fatalf("CompleteTask W2: expected 200, got %d: %s", w.Code, w.Body.String())
	}
	var w2Fallbacks int
	dbfx.QueryRow(t, `SELECT count(*) FROM comment WHERE source_task_id = $1`, w2TaskID).Scan(&w2Fallbacks)
	if w2Fallbacks != 1 {
		t.Fatalf("W2 fallback count = %d, want exactly 1", w2Fallbacks)
	}

	dbfx.Exec(t, `
		UPDATE comment SET content = 'Done. [@B](mention://agent/' || $2 || ') please review as well'
		WHERE source_task_id = $1 AND author_id = $3
	`, w2TaskID, fx.agentB, workerW2)

	// The outage leaves both comments unclassifiable, so both are owed.
	withFailingQueries(t, &failQueriesDB{DBTX: testPool, missingTasks: map[string]bool{fx.wTaskID: true, w2TaskID: true}}, func() {
		if w := completeTaskViaHandler(t, fx.bTaskID, "done"); w.Code != http.StatusOK {
			t.Fatalf("CompleteTask under outage: expected 200, got %d: %s", w.Code, w.Body.String())
		}
	})
	if n := queuedTaskCountForAgentIssue(t, fx.issueID, fx.agentB); n != 0 {
		t.Fatalf("source outage woke %d B task(s), want 0 (fail closed)", n)
	}
	if n := retryObligationCount(t, fx.bTaskID); n != 2 {
		t.Fatalf("retry obligations = %d, want the mention and the recorded fallback", n)
	}

	if _, err := testHandler.ReplayCompletionReconcileRetries(ctx, 10); err != nil {
		t.Fatalf("ReplayCompletionReconcileRetries: %v", err)
	}
	if n := queuedTaskCountForAgentIssue(t, fx.issueID, fx.agentB); n != 1 {
		t.Fatalf("retry recovered %d B follow-up(s), want exactly 1", n)
	}
	var followTrigger string
	dbfx.QueryRow(t, `SELECT COALESCE(trigger_comment_id::text, '') FROM agent_task_queue WHERE issue_id = $1 AND agent_id = $2 AND status = 'queued'`, fx.issueID, fx.agentB).Scan(&followTrigger)
	if followTrigger != fx.commentID {
		t.Fatalf("recovered follow-up trigger = %q, want the explicit mention %q (the fallback body must never generic-enqueue)", followTrigger, fx.commentID)
	}
	if n := retryObligationCount(t, fx.bTaskID); n != 0 {
		t.Fatalf("sweeper left %d retry obligation(s), want 0", n)
	}
}

// TestCompletionReconcileRetryRechecksInvocationPermission pins Blocker 2: a
// permission change between the skip and the replay decides the replay. The
// obligation is recorded while B is workspace-invocable; B's allow-list is then
// replaced by a member target that does not name the delegation chain's human.
// Current comment routing denies that, so the retry must not enqueue B either.
func TestCompletionReconcileRetryRechecksInvocationPermission(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database not available")
	}
	ctx := context.Background()
	fx := seedRetryFixture(t, 999022, "Reconcile Retry Permission", func(agentB, _ string) string {
		return mentionAgentBody(agentB)
	})
	withFailingQueries(t, &failQueriesDB{DBTX: testPool, missingTasks: map[string]bool{fx.wTaskID: true}}, func() {
		if w := completeTaskViaHandler(t, fx.bTaskID, "done"); w.Code != http.StatusOK {
			t.Fatalf("CompleteTask under outage: expected 200, got %d: %s", w.Code, w.Body.String())
		}
	})
	if n := retryObligationCount(t, fx.bTaskID); n != 1 {
		t.Fatalf("retry obligations after the outage = %d, want exactly 1", n)
	}

	// Revoked: the comment's delegation chain now points at a human who is
	// neither B's owner nor on B's allow-list -- the shape a revoked mention
	// reaches.
	dbfx.Exec(t, `UPDATE agent_task_queue SET originator_user_id = u.id, accountable_user_id = u.id FROM (SELECT gen_random_uuid() AS id) u WHERE agent_task_queue.id = $1`, fx.wTaskID)
	dbfx.Exec(t, `DELETE FROM agent_invocation_target WHERE agent_id = $1`, fx.agentB)
	dbfx.Exec(t, `INSERT INTO agent_invocation_target (agent_id, target_type, target_id) VALUES ($1, 'member', $2)`, fx.agentB, testUserID)

	if _, err := testHandler.ReplayCompletionReconcileRetries(ctx, 10); err != nil {
		t.Fatalf("ReplayCompletionReconcileRetries: %v", err)
	}
	if n := queuedTaskCountForAgentIssue(t, fx.issueID, fx.agentB); n != 0 {
		t.Fatalf("retry bypassed the invoke gate: %d B task(s), want 0", n)
	}
	if n := retryObligationCount(t, fx.bTaskID); n != 0 {
		t.Fatalf("revoked obligation left %d element(s), want 0 (settled by current routing)", n)
	}
}

// TestCompletionReconcileRetryDoesNotResurrectSquadLeader pins Blocker 4 case 1:
// the stored obligation carries no role, so an @squad retry resolves the squad's
// CURRENT leader. Leadership moves from B to C before the replay, and current
// routing no longer addresses B at all -- and never fans out to C either.
func TestCompletionReconcileRetryDoesNotResurrectSquadLeader(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database not available")
	}
	ctx := context.Background()
	var squadID string
	fx := seedRetryFixture(t, 999023, "Reconcile Retry Squad", func(agentB, _ string) string {
		squadID = dbfx.Squad(t, "Reconcile Retry Squad", agentB)
		return "[@Squad](mention://squad/" + squadID + ") please coordinate this"
	})
	leaderC := createHandlerTestAgent(t, "Reconcile Retry Squad Leader C", nil)

	withFailingQueries(t, &failQueriesDB{DBTX: testPool, missingTasks: map[string]bool{fx.wTaskID: true}}, func() {
		if w := completeTaskViaHandler(t, fx.bTaskID, "done"); w.Code != http.StatusOK {
			t.Fatalf("CompleteTask under outage: expected 200, got %d: %s", w.Code, w.Body.String())
		}
	})
	if n := pendingTaskCountForAgentIssue(t, fx.issueID, fx.agentB); n != 0 {
		t.Fatalf("source outage woke %d B task(s), want 0 (fail closed)", n)
	}
	if n := retryObligationCount(t, fx.bTaskID); n != 1 {
		t.Fatalf("retry obligations after the outage = %d, want exactly 1", n)
	}

	// Leadership moves to C before the sweep runs.
	dbfx.Exec(t, `UPDATE squad SET leader_id = $2 WHERE id = $1`, squadID, leaderC)

	if _, err := testHandler.ReplayCompletionReconcileRetries(ctx, 10); err != nil {
		t.Fatalf("ReplayCompletionReconcileRetries: %v", err)
	}
	if n := pendingTaskCountForAgentIssue(t, fx.issueID, fx.agentB); n != 0 {
		t.Fatalf("obsolete squad leader resurrected: %d B task(s), want 0", n)
	}
	if n := pendingTaskCountForAgentIssue(t, fx.issueID, leaderC); n != 0 {
		t.Fatalf("retry fanned the owed comment out to the new leader: %d C task(s), want 0", n)
	}
	if n := retryObligationCount(t, fx.bTaskID); n != 0 {
		t.Fatalf("stale-role obligation left %d element(s), want 0", n)
	}
}

// TestCompletionReconcileRetryDemotesLostThreadParentLeader pins Blocker 4 case
// 2: a thread-parent retry reconstructs the CURRENT role. The replied-to comment
// was authored by B's leader run; by replay time leadership has moved to C, so
// current routing addresses B as an ordinary direct/thread-parent target. The
// retry must enqueue exactly that -- no is_leader_task, no stale squad_id.
func TestCompletionReconcileRetryDemotesLostThreadParentLeader(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database not available")
	}
	ctx := context.Background()
	var runtimeID string
	dbfx.QueryRow(t,
		`SELECT runtime_id FROM agent WHERE workspace_id = $1 AND runtime_id IS NOT NULL LIMIT 1`,
		testWorkspaceID).Scan(&runtimeID)
	agentB := createHandlerTestAgent(t, "Reconcile Retry Demote B", nil)
	leaderC := createHandlerTestAgent(t, "Reconcile Retry Demote C", nil)
	squadID := dbfx.Squad(t, "Reconcile Retry Demote Squad", agentB)

	issueID := dbfx.Issue(t, "reconcile-retry-demote fixture", testutil.Cols{
		"status":        "in_progress",
		"number":        999024,
		"assignee_type": "agent",
		"assignee_id":   agentB,
	})
	t.Cleanup(func() { testPool.Exec(ctx, `DELETE FROM comment WHERE issue_id = $1`, issueID) })
	t.Cleanup(func() { testPool.Exec(ctx, `DELETE FROM agent_task_queue WHERE issue_id = $1`, issueID) })

	rootID := dbfx.Comment(t, issueID, "initial request", testutil.Cols{
		"created_at": testutil.Raw("now() - interval '10 minutes'"),
	})
	// B's terminal leader run: it authored the comment the member replies to.
	var leaderTaskID string
	dbfx.QueryRow(t, `
		INSERT INTO agent_task_queue (agent_id, runtime_id, issue_id, trigger_comment_id, delivered_comment_ids, status, priority, created_at, completed_at, is_leader_task, squad_id)
		VALUES ($1, $2, $3, $4, ARRAY[$4::uuid], 'completed', 0, now() - interval '30 minutes', now() - interval '20 minutes', true, $5)
		RETURNING id
	`, agentB, runtimeID, issueID, rootID, squadID).Scan(&leaderTaskID)
	parentID := dbfx.Comment(t, issueID, "leader summary", testutil.Cols{
		"author_type":    "agent",
		"author_id":      agentB,
		"source_task_id": leaderTaskID,
		"parent_id":      rootID,
		"created_at":     testutil.Raw("now() - interval '20 minutes'"),
	})
	// B's running run, same thread.
	var bTaskID string
	dbfx.QueryRow(t, `
		INSERT INTO agent_task_queue (agent_id, runtime_id, issue_id, trigger_comment_id, delivered_comment_ids, status, priority, created_at, started_at)
		VALUES ($1, $2, $3, $4, ARRAY[$4::uuid], 'running', 0, now() - interval '10 minutes', now() - interval '9 minutes')
		RETURNING id
	`, agentB, runtimeID, issueID, rootID).Scan(&bTaskID)
	replyID := dbfx.Comment(t, issueID, "member reply to the leader", testutil.Cols{
		"parent_id": parentID,
	})

	// The dedup read fails, so the completing pass cannot prove the routing and
	// leaves the obligation owed instead of dropping it or guessing a role.
	withFailingQueries(t, &failQueriesDB{DBTX: testPool, failQueryRow: "-- name: HasPendingTaskForIssueAndAgentExcludingTriggerCommentInThread"}, func() {
		if w := completeTaskViaHandler(t, bTaskID, "done"); w.Code != http.StatusOK {
			t.Fatalf("CompleteTask with unprovable routing: expected 200, got %d: %s", w.Code, w.Body.String())
		}
	})
	if n := retryObligationCount(t, bTaskID); n != 1 {
		t.Fatalf("retry obligations = %d, want the owed thread-parent reply", n)
	}
	if n := pendingTaskCountForAgentIssue(t, issueID, agentB); n != 0 {
		t.Fatalf("unprovable routing still enqueued %d B task(s), want 0", n)
	}

	// Leadership moves before the sweep runs.
	dbfx.Exec(t, `UPDATE squad SET leader_id = $2 WHERE id = $1`, squadID, leaderC)
	if _, err := testHandler.ReplayCompletionReconcileRetries(ctx, 10); err != nil {
		t.Fatalf("ReplayCompletionReconcileRetries: %v", err)
	}
	if n := queuedTaskCountForAgentIssue(t, issueID, agentB); n != 1 {
		t.Fatalf("thread-parent retry enqueued %d B task(s), want exactly 1", n)
	}
	var trigger string
	var isLeader bool
	var squadText string
	dbfx.QueryRow(t, `SELECT COALESCE(trigger_comment_id::text, ''), is_leader_task, COALESCE(squad_id::text, '') FROM agent_task_queue WHERE issue_id = $1 AND agent_id = $2 AND status = 'queued'`, issueID, agentB).Scan(&trigger, &isLeader, &squadText)
	if trigger != replyID {
		t.Fatalf("retry follow-up trigger = %q, want the member reply %q", trigger, replyID)
	}
	if isLeader {
		t.Fatal("retry restored the obsolete squad-leader role (is_leader_task = true)")
	}
	if squadText != "" {
		t.Fatalf("retry restored the stale squad_id %q", squadText)
	}
	if n := queuedTaskCountForAgentIssue(t, issueID, leaderC); n != 0 {
		t.Fatalf("retry fanned the owed reply out to the new leader: %d C task(s), want 0", n)
	}
	if n := retryObligationCount(t, bTaskID); n != 0 {
		t.Fatalf("stale-role obligation left %d element(s), want 0", n)
	}
}

// TestCompletionReconcileRetryWriteFailureIsRetryable pins Blocker 3: the
// durable obligation is not best-effort. When even the obligation write fails,
// the terminal callback reports a retryable failure; the daemon's replay of that
// callback is what repairs it -- the existing idempotent terminal callback, no
// new watchdog. The transition is already committed by then, so the replay must
// reconcile WITHOUT repeating first-completion-only side effects, and every
// durable id (fallback, follow-up) stays unique.
func TestCompletionReconcileRetryWriteFailureIsRetryable(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database not available")
	}
	fx := seedRetryFixture(t, 999025, "Reconcile Retry WriteFail", func(agentB, _ string) string {
		return mentionAgentBody(agentB)
	})

	withFailingQueries(t, &failQueriesDB{
		DBTX:         testPool,
		missingTasks: map[string]bool{fx.wTaskID: true},
		failExec:     "-- name: RecordCompletionReconcileRetryComment",
	}, func() {
		if w := completeTaskViaHandler(t, fx.bTaskID, "Processed the inputs delivered to this run"); w.Code != http.StatusInternalServerError {
			t.Fatalf("CompleteTask with a failed obligation write: expected 500 (retryable), got %d: %s", w.Code, w.Body.String())
		}
	})
	// The terminal transition committed before the failure, and nothing was
	// durably recorded for the skipped comment.
	var status string
	dbfx.QueryRow(t, `SELECT status FROM agent_task_queue WHERE id = $1`, fx.bTaskID).Scan(&status)
	if status != "completed" {
		t.Fatalf("task status = %q, want the committed 'completed'", status)
	}
	if n := queuedTaskCountForAgentIssue(t, fx.issueID, fx.agentB); n != 0 {
		t.Fatalf("failed obligation write still enqueued %d B task(s), want 0", n)
	}
	if n := retryObligationCount(t, fx.bTaskID); n != 0 {
		t.Fatalf("failed obligation write left %d element(s), want 0", n)
	}

	// The daemon replays the terminal callback; the retry path re-runs the
	// reconciliation and durably covers the comment exactly once.
	if w := completeTaskViaHandler(t, fx.bTaskID, "Processed the inputs delivered to this run"); w.Code != http.StatusOK {
		t.Fatalf("replayed CompleteTask: expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if n := queuedTaskCountForAgentIssue(t, fx.issueID, fx.agentB); n != 1 {
		t.Fatalf("replay covered %d comments, want exactly 1 follow-up", n)
	}
	if n := fallbackCommentCountForTask(t, fx.bTaskID); n != 1 {
		t.Fatalf("replay synthesized %d fallback(s) for the run, want exactly 1", n)
	}
	if n := retryObligationCount(t, fx.bTaskID); n != 0 {
		t.Fatalf("replay left %d retry obligation(s), want 0", n)
	}

	// A third callback stays a no-op.
	if w := completeTaskViaHandler(t, fx.bTaskID, "Processed the inputs delivered to this run"); w.Code != http.StatusOK {
		t.Fatalf("second replay: expected 200, got %d: %s", w.Code, w.Body.String())
	}
	if n := queuedTaskCountForAgentIssue(t, fx.issueID, fx.agentB); n != 1 {
		t.Fatalf("second replay enqueued a duplicate follow-up: %d B task(s), want 1", n)
	}
	if n := fallbackCommentCountForTask(t, fx.bTaskID); n != 1 {
		t.Fatalf("second replay synthesized a duplicate fallback: %d, want 1", n)
	}
}

// TestCompletionReconcileRetryHonorsCurrentReadiness pins the readiness half of
// Blockers 2/4: a target that current comment routing refuses must be refused by
// the retry too. B is archived between the skip and the replay, and the retry
// must produce the same decision a fresh mention of B produces now.
func TestCompletionReconcileRetryHonorsCurrentReadiness(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database not available")
	}
	ctx := context.Background()
	fx := seedRetryFixture(t, 999026, "Reconcile Retry Readiness", func(agentB, _ string) string {
		return mentionAgentBody(agentB)
	})
	withFailingQueries(t, &failQueriesDB{DBTX: testPool, missingTasks: map[string]bool{fx.wTaskID: true}}, func() {
		if w := completeTaskViaHandler(t, fx.bTaskID, "done"); w.Code != http.StatusOK {
			t.Fatalf("CompleteTask under outage: expected 200, got %d: %s", w.Code, w.Body.String())
		}
	})
	if n := retryObligationCount(t, fx.bTaskID); n != 1 {
		t.Fatalf("retry obligations after the outage = %d, want exactly 1", n)
	}

	// The target becomes unready: its machine reports a CLI the OS refuses to
	// execute, which current comment routing refuses (ReasonRuntimeUnusable --
	// the verdict a human has to repair). The retry must refuse it too instead
	// of queueing a run that could never start.
	var runtimeID string
	dbfx.QueryRow(t, `SELECT COALESCE(runtime_id::text, '') FROM agent WHERE id = $1`, fx.agentB).Scan(&runtimeID)
	var prevStatus string
	var prevMetadata []byte
	if err := testPool.QueryRow(ctx, `SELECT status, metadata FROM agent_runtime WHERE id = $1`, runtimeID).Scan(&prevStatus, &prevMetadata); err != nil {
		t.Fatalf("setup: load runtime: %v", err)
	}
	t.Cleanup(func() {
		testPool.Exec(context.Background(), `UPDATE agent_runtime SET status = $2, metadata = $3 WHERE id = $1`, runtimeID, prevStatus, prevMetadata)
	})
	dbfx.Exec(t, `UPDATE agent_runtime SET status = 'offline', metadata = COALESCE(metadata, '{}'::jsonb) || '{"offline_reason": {"code": "not_executable", "detail": "injected by the reconcile retry test"}}'::jsonb WHERE id = $1`, runtimeID)
	issue, err := testHandler.Queries.GetIssue(ctx, util.MustParseUUID(fx.issueID))
	if err != nil {
		t.Fatalf("setup: load issue: %v", err)
	}
	fresh := dbfx.Comment(t, fx.issueID, mentionAgentBody(fx.agentB))
	freshComment, err := testHandler.Queries.GetComment(ctx, util.MustParseUUID(fresh))
	if err != nil {
		t.Fatalf("setup: load fresh comment: %v", err)
	}
	testHandler.triggerTasksForComment(ctx, issue, freshComment, nil, "member", testUserID, "", nil, nil)
	if n := queuedTaskCountForAgentIssue(t, fx.issueID, fx.agentB); n != 0 {
		t.Fatalf("fresh routing enqueued %d task(s) for an unusable target, want 0", n)
	}

	if _, err := testHandler.ReplayCompletionReconcileRetries(ctx, 10); err != nil {
		t.Fatalf("ReplayCompletionReconcileRetries: %v", err)
	}
	if n := queuedTaskCountForAgentIssue(t, fx.issueID, fx.agentB); n != 0 {
		t.Fatalf("retry enqueued %d task(s) for an unusable target, want 0", n)
	}
	if n := retryObligationCount(t, fx.bTaskID); n != 0 {
		t.Fatalf("unready obligation left %d element(s), want 0", n)
	}
}

// TestCompleteTask_SkipsExplicitMentionWhenSourceGone pins the permanent half
