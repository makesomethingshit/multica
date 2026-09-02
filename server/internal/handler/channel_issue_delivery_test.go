package handler

import (
	"context"
	"net/http"
	"net/http/httptest"
	"testing"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/multica-ai/multica/server/internal/service"
	"github.com/multica-ai/multica/server/internal/util"
	"github.com/multica-ai/multica/server/pkg/dbid"
)

type channelIssueRouteFixture struct {
	issueID       string
	initialTaskID pgtype.UUID
	sessionID     string
	installID     string
}

func createChannelIssueRouteFixture(t *testing.T, agentID, title string) channelIssueRouteFixture {
	t.Helper()
	ctx := context.Background()
	var installID string
	appID := "web-update-" + util.UUIDToString(dbid.NewV7())
	if err := testPool.QueryRow(ctx, `
		INSERT INTO channel_installation (workspace_id, agent_id, channel_type, config, installer_user_id, status)
		VALUES ($1, $2, 'feishu', jsonb_build_object('app_id', $3::text), $4, 'active')
		RETURNING id`, testWorkspaceID, agentID, appID, testUserID).Scan(&installID); err != nil {
		t.Fatalf("create channel installation: %v", err)
	}
	sessionID := util.UUIDToString(dbid.NewV7())
	if _, err := testPool.Exec(ctx, `
		INSERT INTO chat_session (id, workspace_id, agent_id, creator_id, title)
		VALUES ($1, $2, $3, $4, '')`, sessionID, testWorkspaceID, agentID, testUserID); err != nil {
		t.Fatalf("create chat session: %v", err)
	}
	if _, err := testPool.Exec(ctx, `
		INSERT INTO channel_chat_session_binding (chat_session_id, installation_id, channel_type, channel_chat_id, chat_type, config, route_revision)
		VALUES ($1, $2, 'feishu', $3, 'group', '{}'::jsonb, 1)`, sessionID, installID, "oc_test_"+sessionID[:8]); err != nil {
		t.Fatalf("create channel binding: %v", err)
	}
	if _, err := testPool.Exec(ctx, `
		INSERT INTO channel_chat_context_generation (chat_session_id, revision) VALUES ($1, 1)`, sessionID); err != nil {
		t.Fatalf("create context generation: %v", err)
	}

	result, err := testHandler.IssueService.Create(ctx, service.IssueCreateParams{
		WorkspaceID:  util.MustParseUUID(testWorkspaceID),
		Title:        title,
		Status:       "todo",
		Priority:     "medium",
		AssigneeType: pgtype.Text{String: "agent", Valid: true},
		AssigneeID:   util.MustParseUUID(agentID),
		CreatorType:  "member",
		CreatorID:    util.MustParseUUID(testUserID),
		OriginType:   pgtype.Text{String: "lark_chat", Valid: true},
		OriginID:     util.MustParseUUID(sessionID),
	}, service.IssueCreateOpts{})
	if err != nil {
		t.Fatalf("create channel issue: %v", err)
	}
	if !result.AssignedTaskID.Valid {
		t.Fatalf("channel issue should have an initial assigned task")
	}
	if _, err := testHandler.Queries.GetChannelTaskDelivery(ctx, result.AssignedTaskID); err != nil {
		t.Fatalf("initial task delivery missing: %v", err)
	}

	fixture := channelIssueRouteFixture{
		issueID:       util.UUIDToString(result.Issue.ID),
		initialTaskID: result.AssignedTaskID,
		sessionID:     sessionID,
		installID:     installID,
	}
	t.Cleanup(func() {
		cleanupCtx := context.Background()
		_, _ = testPool.Exec(cleanupCtx, `DELETE FROM channel_task_delivery WHERE task_id IN (SELECT id FROM agent_task_queue WHERE issue_id = $1)`, fixture.issueID)
		_, _ = testPool.Exec(cleanupCtx, `DELETE FROM agent_task_queue WHERE issue_id = $1`, fixture.issueID)
		_, _ = testPool.Exec(cleanupCtx, `DELETE FROM issue WHERE id = $1`, fixture.issueID)
		_, _ = testPool.Exec(cleanupCtx, `DELETE FROM channel_chat_context_generation WHERE chat_session_id = $1`, fixture.sessionID)
		_, _ = testPool.Exec(cleanupCtx, `DELETE FROM channel_chat_session_binding WHERE chat_session_id = $1`, fixture.sessionID)
		_, _ = testPool.Exec(cleanupCtx, `DELETE FROM chat_session WHERE id = $1`, fixture.sessionID)
		_, _ = testPool.Exec(cleanupCtx, `DELETE FROM channel_installation WHERE id = $1`, fixture.installID)
	})
	return fixture
}

func completeInitialChannelIssueTask(t *testing.T, fixture channelIssueRouteFixture) {
	t.Helper()
	if _, err := testPool.Exec(context.Background(), `
		UPDATE agent_task_queue SET status = 'completed', completed_at = now() WHERE id = $1`, fixture.initialTaskID); err != nil {
		t.Fatalf("complete initial channel task: %v", err)
	}
	var deliveryCount int
	if err := testPool.QueryRow(context.Background(), `
		SELECT count(*) FROM channel_task_delivery WHERE task_id = $1`, fixture.initialTaskID).Scan(&deliveryCount); err != nil {
		t.Fatalf("count initial channel delivery: %v", err)
	}
	if deliveryCount != 1 {
		t.Fatalf("initial channel delivery count = %d, want 1", deliveryCount)
	}
}

func assertNoNewChannelDelivery(t *testing.T, fixture channelIssueRouteFixture, agentID string) {
	t.Helper()
	var taskID string
	if err := testPool.QueryRow(context.Background(), `
		SELECT id FROM agent_task_queue
		WHERE issue_id = $1 AND agent_id = $2 AND status IN ('queued', 'dispatched', 'running')
		ORDER BY created_at DESC LIMIT 1`, fixture.issueID, agentID).Scan(&taskID); err != nil {
		t.Fatalf("load follow-up task for %s: %v", agentID, err)
	}
	var deliveryCount int
	if err := testPool.QueryRow(context.Background(), `
		SELECT count(*) FROM channel_task_delivery WHERE task_id = $1`, taskID).Scan(&deliveryCount); err != nil {
		t.Fatalf("count follow-up delivery: %v", err)
	}
	if deliveryCount != 0 {
		t.Fatalf("follow-up task %s has %d channel deliveries, want 0", taskID, deliveryCount)
	}
	var initialDeliveryCount int
	if err := testPool.QueryRow(context.Background(), `
		SELECT count(*) FROM channel_task_delivery WHERE task_id = $1`, fixture.initialTaskID).Scan(&initialDeliveryCount); err != nil {
		t.Fatalf("count preserved initial delivery: %v", err)
	}
	if initialDeliveryCount != 1 {
		t.Fatalf("initial channel delivery after web update = %d, want 1", initialDeliveryCount)
	}
}

func TestChannelIssueDelivery_WebUpdatePathsDoNotSnapshot(t *testing.T) {
	if testHandler == nil {
		t.Skip("database not available")
	}
	initialAgentID := seededReadyAgentID(t)

	t.Run("update", func(t *testing.T) {
		newAgentID := createHandlerTestAgent(t, "ChannelWebUpdateAgent", []byte("[]"))
		fixture := createChannelIssueRouteFixture(t, initialAgentID, "channel web update")
		completeInitialChannelIssueTask(t, fixture)

		w := httptest.NewRecorder()
		req := withURLParam(newRequest("PUT", "/api/issues/"+fixture.issueID, map[string]any{
			"assignee_type": "agent",
			"assignee_id":   newAgentID,
		}), "id", fixture.issueID)
		testHandler.UpdateIssue(w, req)
		if w.Code != http.StatusOK {
			t.Fatalf("UpdateIssue: expected 200, got %d: %s", w.Code, w.Body.String())
		}
		assertNoNewChannelDelivery(t, fixture, newAgentID)
	})

	t.Run("batch_update", func(t *testing.T) {
		newAgentID := createHandlerTestAgent(t, "ChannelBatchUpdateAgent", []byte("[]"))
		fixture := createChannelIssueRouteFixture(t, initialAgentID, "channel batch update")
		completeInitialChannelIssueTask(t, fixture)

		w := httptest.NewRecorder()
		req := newRequest("POST", "/api/issues/batch-update", map[string]any{
			"issue_ids": []string{fixture.issueID},
			"updates": map[string]any{
				"assignee_type": "agent",
				"assignee_id":   newAgentID,
			},
		})
		testHandler.BatchUpdateIssues(w, req)
		if w.Code != http.StatusOK {
			t.Fatalf("BatchUpdateIssues: expected 200, got %d: %s", w.Code, w.Body.String())
		}
		assertNoNewChannelDelivery(t, fixture, newAgentID)
	})

	t.Run("comment", func(t *testing.T) {
		var hasRecoverySettledAt bool
		if err := testPool.QueryRow(context.Background(), `
			SELECT EXISTS (
				SELECT 1 FROM information_schema.columns
				WHERE table_schema = 'public'
				  AND table_name = 'delegated_failure_recovery'
				  AND column_name = 'recovery_settled_at')`).Scan(&hasRecoverySettledAt); err != nil {
			t.Fatalf("check comment trigger schema: %v", err)
		}
		if !hasRecoverySettledAt {
			t.Skip("comment trigger reconciliation migration is not applied")
		}
		fixture := createChannelIssueRouteFixture(t, initialAgentID, "channel comment trigger")
		completeInitialChannelIssueTask(t, fixture)

		w := httptest.NewRecorder()
		req := withURLParam(newRequest("POST", "/api/issues/"+fixture.issueID+"/comments", map[string]any{
			"content": "[@Agent](mention://agent/" + initialAgentID + ") follow-up",
		}), "id", fixture.issueID)
		testHandler.CreateComment(w, req)
		if w.Code != http.StatusCreated && w.Code != http.StatusOK {
			t.Fatalf("CreateComment: expected 200/201, got %d: %s", w.Code, w.Body.String())
		}
		assertNoNewChannelDelivery(t, fixture, initialAgentID)
	})
}
