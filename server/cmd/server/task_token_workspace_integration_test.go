package main

import (
	"context"
	"encoding/json"
	"io"
	"net/http"
	"testing"
	"time"

	"github.com/multica-ai/multica/server/internal/auth"
)

// TestTaskTokenTaskMessagesWorkspaceBoundary drives the real Auth and
// workspace middleware chain. A task token is workspace-scoped, not task-
// scoped, so another agent's task in that workspace remains readable while a
// task in another workspace is indistinguishable from a missing task.
func TestTaskTokenTaskMessagesWorkspaceBoundary(t *testing.T) {
	if testPool == nil {
		t.Skip("no database connection")
	}

	ctx := context.Background()
	agentA := getAgentID(t)
	agentB := createSecondAgent(t)
	_, taskA := seedTaskMessageTask(t, testWorkspaceID, agentA, "task-token owner task")
	_, taskB := seedTaskMessageTask(t, testWorkspaceID, agentB, "same workspace sibling task")
	seedTaskMessage(t, taskB, "sibling transcript")

	foreignWorkspace := seedTaskMessageWorkspace(t)
	foreignAgent := seedTaskMessageAgent(t, foreignWorkspace)
	_, foreignTask := seedTaskMessageTask(t, foreignWorkspace, foreignAgent, "foreign workspace task")
	seedTaskMessage(t, foreignTask, "foreign transcript")

	token, err := auth.GenerateAgentTaskToken()
	if err != nil {
		t.Fatalf("generate task token: %v", err)
	}
	if _, err := testPool.Exec(ctx, `
		INSERT INTO task_token (token_hash, task_id, agent_id, workspace_id, user_id, expires_at)
		VALUES ($1, $2, $3, $4, $5, $6)
	`, auth.HashToken(token), taskA, agentA, testWorkspaceID, testUserID, time.Now().Add(time.Hour)); err != nil {
		t.Fatalf("insert task token: %v", err)
	}
	t.Cleanup(func() {
		_, _ = testPool.Exec(context.Background(), `DELETE FROM task_token WHERE token_hash = $1`, auth.HashToken(token))
	})

	request := func(t *testing.T, taskID, spoofedWorkspace string) (int, []map[string]any) {
		t.Helper()
		req, err := http.NewRequest(http.MethodGet, testServer.URL+"/api/tasks/"+taskID+"/messages", nil)
		if err != nil {
			t.Fatalf("create request: %v", err)
		}
		req.Header.Set("Authorization", "Bearer "+token)
		// Both values are intentionally attacker-controlled. Auth must replace
		// them with the task token's authoritative source and workspace.
		req.Header.Set("X-Actor-Source", "member")
		req.Header.Set("X-Workspace-ID", spoofedWorkspace)
		resp, err := http.DefaultClient.Do(req)
		if err != nil {
			t.Fatalf("perform request: %v", err)
		}
		defer resp.Body.Close()
		var messages []map[string]any
		if resp.StatusCode == http.StatusOK {
			if err := json.NewDecoder(resp.Body).Decode(&messages); err != nil {
				t.Fatalf("decode messages: %v", err)
			}
		} else {
			_, _ = io.ReadAll(resp.Body)
		}
		return resp.StatusCode, messages
	}

	t.Run("same workspace sibling is readable", func(t *testing.T) {
		status, messages := request(t, taskB, foreignWorkspace)
		if status != http.StatusOK {
			t.Fatalf("status = %d, want 200", status)
		}
		if len(messages) != 1 || messages[0]["content"] != "sibling transcript" {
			t.Fatalf("messages = %#v, want sibling transcript", messages)
		}
	})

	t.Run("foreign workspace is hidden even with forged headers", func(t *testing.T) {
		status, _ := request(t, foreignTask, foreignWorkspace)
		if status != http.StatusNotFound {
			t.Fatalf("status = %d, want 404", status)
		}
	})
}

func seedTaskMessageWorkspace(t *testing.T) string {
	t.Helper()
	var workspaceID string
	if err := testPool.QueryRow(context.Background(), `
		INSERT INTO workspace (name, slug, description)
		VALUES ('Task message boundary fixture', $1, '')
		RETURNING id
	`, uniqueName("task-message-boundary")).Scan(&workspaceID); err != nil {
		t.Fatalf("seed workspace: %v", err)
	}
	t.Cleanup(func() {
		_, _ = testPool.Exec(context.Background(), `DELETE FROM workspace WHERE id = $1`, workspaceID)
	})
	return workspaceID
}

func seedTaskMessageTask(t *testing.T, workspaceID, agentID, title string) (string, string) {
	t.Helper()
	ctx := context.Background()
	var issueID, taskID string
	if err := testPool.QueryRow(ctx, `
		WITH next_number AS (
			SELECT COALESCE(MAX(number), 0) + 1 AS number
			FROM issue WHERE workspace_id = $1
		)
		INSERT INTO issue (workspace_id, title, status, priority, creator_type, creator_id, number)
		SELECT $1, $2, 'todo', 'none', 'member', $3, number
		FROM next_number
		RETURNING id
	`, workspaceID, title, testUserID).Scan(&issueID); err != nil {
		t.Fatalf("seed issue: %v", err)
	}
	if err := testPool.QueryRow(ctx, `
		INSERT INTO agent_task_queue (agent_id, runtime_id, issue_id, status, started_at)
		SELECT id, runtime_id, $2, 'running', now() FROM agent WHERE id = $1
		RETURNING id
	`, agentID, issueID).Scan(&taskID); err != nil {
		t.Fatalf("seed task: %v", err)
	}
	t.Cleanup(func() {
		_, _ = testPool.Exec(context.Background(), `DELETE FROM issue WHERE id = $1`, issueID)
	})
	return issueID, taskID
}

func seedTaskMessageAgent(t *testing.T, workspaceID string) string {
	t.Helper()
	ctx := context.Background()
	var runtimeID, agentID string
	if err := testPool.QueryRow(ctx, `
		INSERT INTO agent_runtime (workspace_id, name, runtime_mode, provider, status, device_info, metadata, owner_id, last_seen_at)
		VALUES ($1, $2, 'cloud', 'task-message-boundary', 'online', '', '{}'::jsonb, $3, now())
		RETURNING id
	`, workspaceID, uniqueName("task-message-boundary-runtime"), testUserID).Scan(&runtimeID); err != nil {
		t.Fatalf("seed runtime: %v", err)
	}
	if err := testPool.QueryRow(ctx, `
		INSERT INTO agent (workspace_id, name, description, runtime_mode, runtime_config, runtime_id, visibility, max_concurrent_tasks, owner_id)
		VALUES ($1, $2, '', 'cloud', '{}'::jsonb, $3, 'workspace', 1, $4)
		RETURNING id
	`, workspaceID, uniqueName("task-message-boundary-agent"), runtimeID, testUserID).Scan(&agentID); err != nil {
		t.Fatalf("seed agent: %v", err)
	}
	t.Cleanup(func() {
		_, _ = testPool.Exec(context.Background(), `DELETE FROM agent WHERE id = $1`, agentID)
		_, _ = testPool.Exec(context.Background(), `DELETE FROM agent_runtime WHERE id = $1`, runtimeID)
	})
	return agentID
}

func seedTaskMessage(t *testing.T, taskID, content string) {
	t.Helper()
	if _, err := testPool.Exec(context.Background(), `
		INSERT INTO task_message (task_id, seq, type, content)
		VALUES ($1, 1, 'text', $2)
	`, taskID, content); err != nil {
		t.Fatalf("seed task message: %v", err)
	}
}
