package handler

import (
	"context"
	"encoding/json"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/multica-ai/multica/server/internal/testutil"
)

// blockedResumeCompleteTasks marks every task on the issue completed so the next
// status move starts from previous run ended with no pending run.
func blockedResumeCompleteTasks(t *testing.T, issueID string) {
	t.Helper()
	if _, err := testPool.Exec(context.Background(), "UPDATE agent_task_queue SET status = $2 WHERE issue_id = $1", issueID, "completed"); err != nil {
		t.Fatalf("complete tasks: %v", err)
	}
}

func blockedResumeUpdateStatus(t *testing.T, issueID string, body map[string]any, headers map[string]string) {
	t.Helper()
	w := httptest.NewRecorder()
	req := withURLParam(newRequest("PUT", "/api/issues/"+issueID, body), "id", issueID)
	for k, v := range headers {
		req.Header.Set(k, v)
	}
	testHandler.UpdateIssue(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("UpdateIssue %s: %d %s", issueID, w.Code, w.Body.String())
	}
}

func blockedResumeStatusOf(t *testing.T, issueID string) string {
	t.Helper()
	var status string
	if err := testPool.QueryRow(context.Background(), "SELECT status FROM issue WHERE id = $1", issueID).Scan(&status); err != nil {
		t.Fatalf("load status: %v", err)
	}
	return status
}

func TestBlockedToTodoResumesAgent(t *testing.T) {
	if testHandler == nil {
		t.Skip("database not available")
	}
	agentID := createHandlerTestAgent(t, "Blocked Resume Agent", nil)
	issue := createIssueForTest(t, map[string]any{"title": "blocked resume agent", "status": "todo"})
	blockedResumeUpdateStatus(t, issue.ID, map[string]any{"assignee_type": "agent", "assignee_id": agentID}, nil)
	blockedResumeCompleteTasks(t, issue.ID)
	blockedResumeUpdateStatus(t, issue.ID, map[string]any{"status": "blocked"}, nil)
	if got := queuedTaskCountFor(t, issue.ID, agentID); got != 0 {
		t.Fatalf("moving to blocked must not enqueue, got %d queued", got)
	}
	pv := previewIssueTrigger(t, map[string]any{"issue_ids": []string{issue.ID}, "status": "todo"})
	if pv.TotalCount != 1 || len(pv.Triggers) != 1 {
		t.Fatalf("preview blocked to todo: expected 1 trigger, got %+v", pv)
	}
	if pv.Triggers[0].AgentID != agentID || pv.Triggers[0].Source != "status" {
		t.Fatalf("preview blocked to todo: wrong trigger %+v", pv.Triggers[0])
	}
	blockedResumeUpdateStatus(t, issue.ID, map[string]any{"status": "todo"}, nil)
	if got := queuedTaskCountFor(t, issue.ID, agentID); got != 1 {
		t.Fatalf("blocked to todo must enqueue exactly 1 run, got %d", got)
	}
}

func TestBlockedToTodoResumesSquadLeader(t *testing.T) {
	if testHandler == nil {
		t.Skip("database not available")
	}
	ctx := context.Background()
	leaderID := createHandlerTestAgent(t, "Blocked Resume Leader", nil)
	var squadID string
	if err := testPool.QueryRow(ctx, "INSERT INTO squad (workspace_id, name, description, leader_id, creator_id) VALUES ($1, $2, $3, $4, $5) RETURNING id", testWorkspaceID, "Blocked Resume Squad", "", leaderID, testUserID).Scan(&squadID); err != nil {
		t.Fatalf("create squad: %v", err)
	}
	t.Cleanup(func() {
		testPool.Exec(context.Background(), "DELETE FROM squad WHERE id = $1", squadID)
	})
	issue := createIssueForTest(t, map[string]any{"title": "blocked resume squad", "status": "todo"})
	blockedResumeUpdateStatus(t, issue.ID, map[string]any{"assignee_type": "squad", "assignee_id": squadID}, nil)
	blockedResumeCompleteTasks(t, issue.ID)
	blockedResumeUpdateStatus(t, issue.ID, map[string]any{"status": "blocked"}, nil)
	if got := queuedTaskCountFor(t, issue.ID, leaderID); got != 0 {
		t.Fatalf("moving squad issue to blocked must not enqueue, got %d", got)
	}
	pv := previewIssueTrigger(t, map[string]any{"issue_ids": []string{issue.ID}, "status": "todo"})
	if pv.TotalCount != 1 || len(pv.Triggers) != 1 {
		t.Fatalf("preview squad blocked to todo: expected 1, got %+v", pv)
	}
	if pv.Triggers[0].AgentID != leaderID || pv.Triggers[0].Source != "status" {
		t.Fatalf("preview squad blocked to todo: wrong trigger %+v", pv.Triggers[0])
	}
	blockedResumeUpdateStatus(t, issue.ID, map[string]any{"status": "todo"}, nil)
	if got := queuedTaskCountFor(t, issue.ID, leaderID); got != 1 {
		t.Fatalf("squad blocked to todo must enqueue leader once, got %d", got)
	}
}

func TestBlockedToTodoPendingDedup(t *testing.T) {
	if testHandler == nil {
		t.Skip("database not available")
	}
	agentID := createHandlerTestAgent(t, "Blocked Resume Dedup", nil)
	issue := createIssueForTest(t, map[string]any{"title": "blocked resume dedup", "status": "todo"})
	blockedResumeUpdateStatus(t, issue.ID, map[string]any{"assignee_type": "agent", "assignee_id": agentID}, nil)
	blockedResumeUpdateStatus(t, issue.ID, map[string]any{"status": "blocked"}, nil)
	if got := queuedTaskCountFor(t, issue.ID, agentID); got != 1 {
		t.Fatalf("before: expected 1 pending task, got %d", got)
	}
	pv := previewIssueTrigger(t, map[string]any{"issue_ids": []string{issue.ID}, "status": "todo"})
	if pv.TotalCount != 0 {
		t.Fatalf("preview with pending run must be 0, got %+v", pv)
	}
	blockedResumeUpdateStatus(t, issue.ID, map[string]any{"status": "todo"}, nil)
	if got := queuedTaskCountFor(t, issue.ID, agentID); got != 1 {
		t.Fatalf("blocked to todo with pending run must not add a task, got %d", got)
	}
}

func TestBlockedToTodoSelfLoopSuppressed(t *testing.T) {
	if testHandler == nil {
		t.Skip("database not available")
	}
	agentID := createHandlerTestAgent(t, "Blocked Resume Self", nil)
	issue := createIssueForTest(t, map[string]any{"title": "blocked resume self", "status": "todo"})
	blockedResumeUpdateStatus(t, issue.ID, map[string]any{"assignee_type": "agent", "assignee_id": agentID}, nil)
	blockedResumeCompleteTasks(t, issue.ID)
	blockedResumeUpdateStatus(t, issue.ID, map[string]any{"status": "blocked"}, nil)
	selfTask := createHandlerTestTaskForAgentOnIssue(t, agentID, issue.ID)
	headers := map[string]string{"X-Agent-ID": agentID, "X-Task-ID": selfTask}
	previewReq := newRequest("POST", "/api/issues/preview-trigger?workspace_id="+testWorkspaceID, map[string]any{"issue_ids": []string{issue.ID}, "status": "todo"})
	for k, v := range headers {
		previewReq.Header.Set(k, v)
	}
	previewW := testutil.Call(t, testHandler.PreviewIssueTrigger, previewReq).Want(http.StatusOK)
	var preview IssueTriggerPreviewResponse
	if err := json.NewDecoder(previewW.Body).Decode(&preview); err != nil {
		t.Fatalf("decode preview: %v", err)
	}
	if preview.TotalCount != 0 {
		t.Fatalf("self-loop preview must be 0, got %+v", preview)
	}
	blockedResumeUpdateStatus(t, issue.ID, map[string]any{"status": "todo"}, headers)
	if got := queuedTaskCountFor(t, issue.ID, agentID); got != 0 {
		t.Fatalf("self-loop blocked to todo must not enqueue, got %d", got)
	}
}

func TestBlockedToTodoSuppressRun(t *testing.T) {
	if testHandler == nil {
		t.Skip("database not available")
	}
	agentID := createHandlerTestAgent(t, "Blocked Resume Suppress", nil)
	issue := createIssueForTest(t, map[string]any{"title": "blocked resume suppress", "status": "todo"})
	blockedResumeUpdateStatus(t, issue.ID, map[string]any{"assignee_type": "agent", "assignee_id": agentID}, nil)
	blockedResumeCompleteTasks(t, issue.ID)
	blockedResumeUpdateStatus(t, issue.ID, map[string]any{"status": "blocked"}, nil)
	blockedResumeUpdateStatus(t, issue.ID, map[string]any{"status": "todo", "suppress_run": true}, nil)
	if got := blockedResumeStatusOf(t, issue.ID); got != "todo" {
		t.Fatalf("suppress_run must still apply status, got %q", got)
	}
	if got := queuedTaskCountFor(t, issue.ID, agentID); got != 0 {
		t.Fatalf("suppress_run blocked to todo must not enqueue, got %d", got)
	}
}

func TestBlockedToTodoUnrelatedTransitionsStayQuiet(t *testing.T) {
	if testHandler == nil {
		t.Skip("database not available")
	}
	agentID := createHandlerTestAgent(t, "Blocked Resume Quiet", nil)
	cases := []struct {
		name string
		from string
		to   string
	}{
		{"blocked to done", "blocked", "done"},
		{"blocked to cancelled", "blocked", "cancelled"},
		{"todo to done", "todo", "done"},
	}
	for _, tc := range cases {
		t.Run(tc.name, func(t *testing.T) {
			issue := createIssueForTest(t, map[string]any{"title": "quiet " + tc.name, "status": tc.from, "assignee_type": "agent", "assignee_id": agentID})
			blockedResumeCompleteTasks(t, issue.ID)
			if _, err := testPool.Exec(context.Background(), "DELETE FROM agent_task_queue WHERE issue_id = $1 AND status = $2", issue.ID, "queued"); err != nil {
				t.Fatalf("reset queued: %v", err)
			}
			pv := previewIssueTrigger(t, map[string]any{"issue_ids": []string{issue.ID}, "status": tc.to})
			if pv.TotalCount != 0 {
				t.Fatalf("%s preview must be 0, got %+v", tc.name, pv)
			}
		})
	}
	blockedIssue := createIssueForTest(t, map[string]any{"title": "quiet blocked to blocked", "status": "blocked", "assignee_type": "agent", "assignee_id": agentID})
	blockedResumeCompleteTasks(t, blockedIssue.ID)
	if _, err := testPool.Exec(context.Background(), "DELETE FROM agent_task_queue WHERE issue_id = $1 AND status = $2", blockedIssue.ID, "queued"); err != nil {
		t.Fatalf("reset queued: %v", err)
	}
	if pv := previewIssueTrigger(t, map[string]any{"issue_ids": []string{blockedIssue.ID}, "status": "blocked"}); pv.TotalCount != 0 {
		t.Fatalf("blocked to blocked preview must be 0, got %+v", pv)
	}
	todoIssue := createIssueForTest(t, map[string]any{"title": "quiet todo to todo", "status": "todo", "assignee_type": "agent", "assignee_id": agentID})
	blockedResumeCompleteTasks(t, todoIssue.ID)
	if _, err := testPool.Exec(context.Background(), "DELETE FROM agent_task_queue WHERE issue_id = $1 AND status = $2", todoIssue.ID, "queued"); err != nil {
		t.Fatalf("reset queued: %v", err)
	}
	if pv := previewIssueTrigger(t, map[string]any{"issue_ids": []string{todoIssue.ID}, "status": "todo"}); pv.TotalCount != 0 {
		t.Fatalf("todo to todo preview must be 0, got %+v", pv)
	}
}
