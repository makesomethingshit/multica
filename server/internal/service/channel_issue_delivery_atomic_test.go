package service

import (
	"context"
	"testing"
	"time"

	"github.com/jackc/pgx/v5/pgtype"
	"github.com/multica-ai/multica/server/internal/events"
	"github.com/multica-ai/multica/server/internal/util"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
	"github.com/multica-ai/multica/server/pkg/dbid"
)

func TestChannelIssueDelivery_AtomicSnapshot_Success(t *testing.T) {
	pool := newResolveOriginatorPool(t)
	ctx := context.Background()
	q := db.New(pool)
	workspaceID, userID, agentID, _ := seedAttributionFixture(t, pool)
	workspaceUUID := util.MustParseUUID(workspaceID)
	userUUID := util.MustParseUUID(userID)
	agentUUID := util.MustParseUUID(agentID)

	// Create a feishu channel installation for this agent/workspace.
	var chatSessionID pgtype.UUID
	{
		// Use a unique app_id per test.
		appID := "test-app-" + workspaceID[:8]
		var instID string
		if err := pool.QueryRow(ctx, `
			INSERT INTO channel_installation (workspace_id, agent_id, channel_type, config, installer_user_id, status)
			VALUES ($1, $2, 'feishu', jsonb_build_object('app_id', $3::text), $4, 'active')
			RETURNING id`, workspaceID, agentID, appID, userID).Scan(&instID); err != nil {
			t.Fatalf("create channel installation: %v", err)
		}
		t.Cleanup(func() { pool.Exec(context.Background(), `DELETE FROM channel_installation WHERE id = $1`, instID) })

		var sessionID string
		if err := pool.QueryRow(ctx, `
			INSERT INTO chat_session (id, workspace_id, agent_id, creator_id, title)
			VALUES ($1, $2, $3, $4, '')
			RETURNING id`, dbid.NewV7(), workspaceID, agentID, userID).Scan(&sessionID); err != nil {
			t.Fatalf("create chat session: %v", err)
		}
		chatSessionID = util.MustParseUUID(sessionID)
		t.Cleanup(func() { pool.Exec(context.Background(), `DELETE FROM chat_session WHERE id = $1`, sessionID) })

		if _, err := pool.Exec(ctx, `
			INSERT INTO channel_chat_session_binding (chat_session_id, installation_id, channel_type, channel_chat_id, chat_type, config, route_revision)
			VALUES ($1, $2, 'feishu', 'oc_test_chat', 'group', '{}'::jsonb, 1)`, sessionID, instID); err != nil {
			t.Fatalf("create channel binding: %v", err)
		}
		if _, err := pool.Exec(ctx, `
			INSERT INTO channel_chat_context_generation (chat_session_id, revision) VALUES ($1, 1)`, sessionID); err != nil {
			t.Fatalf("create context generation: %v", err)
		}
	}

	bus := events.New()
	taskService := &TaskService{Queries: q, TxStarter: pool, Bus: bus}
	issueService := NewIssueService(q, pool, bus, nil, taskService)

	// Immediate channel issue (no media, no deferred).
	result, err := issueService.Create(ctx, IssueCreateParams{
		WorkspaceID:  workspaceUUID,
		Title:        "Channel issue immediate",
		Status:       "todo",
		Priority:     "medium",
		AssigneeType: pgtype.Text{String: "agent", Valid: true},
		AssigneeID:   agentUUID,
		CreatorType:  "member",
		CreatorID:    userUUID,
		OriginType:   pgtype.Text{String: "lark_chat", Valid: true},
		OriginID:     chatSessionID,
	}, IssueCreateOpts{})
	if err != nil {
		t.Fatalf("Create immediate channel issue: %v", err)
	}
	if !result.AssignedTaskID.Valid {
		t.Fatalf("immediate channel issue should have assigned task")
	}
	// Delivery must be atomically visible before any poll can claim the task.
	if _, err := q.GetChannelTaskDelivery(ctx, result.AssignedTaskID); err != nil {
		t.Fatalf("delivery missing for immediate channel issue task: %v", err)
	}
	// Quick sanity: task is claimable via poll path only after delivery exists.
	var deliveryCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM channel_task_delivery WHERE task_id = $1`, result.AssignedTaskID).Scan(&deliveryCount); err != nil {
		t.Fatalf("count delivery: %v", err)
	}
	if deliveryCount != 1 {
		t.Fatalf("delivery count = %d, want 1", deliveryCount)
	}
}

func TestChannelIssueDelivery_AtomicSnapshot_DeferredSuccess(t *testing.T) {
	pool := newResolveOriginatorPool(t)
	ctx := context.Background()
	q := db.New(pool)
	workspaceID, userID, agentID, _ := seedAttributionFixture(t, pool)
	workspaceUUID := util.MustParseUUID(workspaceID)
	userUUID := util.MustParseUUID(userID)
	agentUUID := util.MustParseUUID(agentID)

	var chatSessionID pgtype.UUID
	{
		appID := "test-app-" + workspaceID[:8]
		var instID string
		if err := pool.QueryRow(ctx, `
			INSERT INTO channel_installation (workspace_id, agent_id, channel_type, config, installer_user_id, status)
			VALUES ($1, $2, 'feishu', jsonb_build_object('app_id', $3::text), $4, 'active')
			RETURNING id`, workspaceID, agentID, appID, userID).Scan(&instID); err != nil {
			t.Fatalf("create channel installation: %v", err)
		}
		t.Cleanup(func() { pool.Exec(context.Background(), `DELETE FROM channel_installation WHERE id = $1`, instID) })
		var sessionID string
		if err := pool.QueryRow(ctx, `
			INSERT INTO chat_session (id, workspace_id, agent_id, creator_id, title)
			VALUES ($1, $2, $3, $4, '')
			RETURNING id`, dbid.NewV7(), workspaceID, agentID, userID).Scan(&sessionID); err != nil {
			t.Fatalf("create chat session: %v", err)
		}
		chatSessionID = util.MustParseUUID(sessionID)
		t.Cleanup(func() { pool.Exec(context.Background(), `DELETE FROM chat_session WHERE id = $1`, sessionID) })
		if _, err := pool.Exec(ctx, `
			INSERT INTO channel_chat_session_binding (chat_session_id, installation_id, channel_type, channel_chat_id, chat_type, config, route_revision)
			VALUES ($1, $2, 'feishu', 'oc_test_chat', 'group', '{}'::jsonb, 1)`, sessionID, instID); err != nil {
			t.Fatalf("create channel binding: %v", err)
		}
		if _, err := pool.Exec(ctx, `
			INSERT INTO channel_chat_context_generation (chat_session_id, revision) VALUES ($1, 1)`, sessionID); err != nil {
			t.Fatalf("create context generation: %v", err)
		}
	}

	bus := events.New()
	taskService := &TaskService{Queries: q, TxStarter: pool, Bus: bus}
	issueService := NewIssueService(q, pool, bus, nil, taskService)

	deferredResult, err := issueService.Create(ctx, IssueCreateParams{
		WorkspaceID:  workspaceUUID,
		Title:        "Channel issue deferred",
		Status:       "todo",
		Priority:     "medium",
		AssigneeType: pgtype.Text{String: "agent", Valid: true},
		AssigneeID:   agentUUID,
		CreatorType:  "member",
		CreatorID:    userUUID,
		OriginType:   pgtype.Text{String: "lark_chat", Valid: true},
		OriginID:     chatSessionID,
	}, IssueCreateOpts{AssignedAgentRunFireAt: time.Now().Add(time.Minute)})
	if err != nil {
		t.Fatalf("Create deferred channel issue: %v", err)
	}
	if !deferredResult.AssignedTaskID.Valid {
		t.Fatalf("deferred channel issue should have assigned task")
	}
	if _, err := q.GetChannelTaskDelivery(ctx, deferredResult.AssignedTaskID); err != nil {
		t.Fatalf("delivery missing for deferred channel issue task: %v", err)
	}
}

func TestChannelIssueDelivery_SnapshotFailureRollsBack(t *testing.T) {
	pool := newResolveOriginatorPool(t)
	ctx := context.Background()
	q := db.New(pool)
	workspaceID, userID, agentID, _ := seedAttributionFixture(t, pool)
	workspaceUUID := util.MustParseUUID(workspaceID)
	userUUID := util.MustParseUUID(userID)
	agentUUID := util.MustParseUUID(agentID)

	bus := events.New()
	taskService := &TaskService{Queries: q, TxStarter: pool, Bus: bus}
	issueService := NewIssueService(q, pool, bus, nil, taskService)

	// Use a random OriginID that has no channel_chat_session_binding, so
	// CreateChannelTaskDeliveryFromSession will find 0 rows and fail.
	bogusSessionID := dbid.NewV7()
	// Count issues before attempt.
	var beforeCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM issue WHERE workspace_id = $1`, workspaceID).Scan(&beforeCount); err != nil {
		t.Fatalf("count before: %v", err)
	}

	_, err := issueService.Create(ctx, IssueCreateParams{
		WorkspaceID:  workspaceUUID,
		Title:        "Channel issue bogus session",
		Status:       "todo",
		Priority:     "medium",
		AssigneeType: pgtype.Text{String: "agent", Valid: true},
		AssigneeID:   agentUUID,
		CreatorType:  "member",
		CreatorID:    userUUID,
		OriginType:   pgtype.Text{String: "lark_chat", Valid: true},
		OriginID:     bogusSessionID,
	}, IssueCreateOpts{AssignedAgentRunFireAt: time.Now().Add(time.Minute)})
	if err == nil {
		t.Fatalf("expected snapshot failure to abort issue creation, got nil")
	}

	var afterCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM issue WHERE workspace_id = $1`, workspaceID).Scan(&afterCount); err != nil {
		t.Fatalf("count after: %v", err)
	}
	if afterCount != beforeCount {
		t.Fatalf("issue count after failed snapshot = %d, want %d (rollback)", afterCount, beforeCount)
	}
	// No task should be visible for the bogus session.
	var taskCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM agent_task_queue WHERE issue_id IN (SELECT id FROM issue WHERE title = 'Channel issue bogus session')`).Scan(&taskCount); err != nil {
		t.Fatalf("count tasks: %v", err)
	}
	if taskCount != 0 {
		t.Fatalf("tasks for failed channel issue = %d, want 0", taskCount)
	}
}

func TestChannelIssueDelivery_NonChannelIssueDoesNotRequireDelivery(t *testing.T) {
	pool := newResolveOriginatorPool(t)
	ctx := context.Background()
	q := db.New(pool)
	workspaceID, userID, agentID, _ := seedAttributionFixture(t, pool)
	workspaceUUID := util.MustParseUUID(workspaceID)
	userUUID := util.MustParseUUID(userID)
	agentUUID := util.MustParseUUID(agentID)

	bus := events.New()
	taskService := &TaskService{Queries: q, TxStarter: pool, Bus: bus}
	issueService := NewIssueService(q, pool, bus, nil, taskService)

	// Web issue (no OriginType) should succeed without delivery.
	result, err := issueService.Create(ctx, IssueCreateParams{
		WorkspaceID:  workspaceUUID,
		Title:        "Web issue no origin",
		Status:       "todo",
		Priority:     "medium",
		AssigneeType: pgtype.Text{String: "agent", Valid: true},
		AssigneeID:   agentUUID,
		CreatorType:  "member",
		CreatorID:    userUUID,
	}, IssueCreateOpts{})
	if err != nil {
		t.Fatalf("Create web issue: %v", err)
	}
	if !result.AssignedTaskID.Valid {
		t.Fatalf("web issue should have assigned task")
	}
	// Web issue must NOT have a channel delivery row.
	if _, err := q.GetChannelTaskDelivery(ctx, result.AssignedTaskID); err == nil {
		t.Fatalf("web issue should not have delivery, but found one")
	}
}

func TestChannelIssueDelivery_ImmediateSnapshotFailureDoesNotCreateTask(t *testing.T) {
	pool := newResolveOriginatorPool(t)
	ctx := context.Background()
	q := db.New(pool)
	workspaceID, userID, agentID, _ := seedAttributionFixture(t, pool)
	workspaceUUID := util.MustParseUUID(workspaceID)
	userUUID := util.MustParseUUID(userID)
	agentUUID := util.MustParseUUID(agentID)

	bus := events.New()
	taskService := &TaskService{Queries: q, TxStarter: pool, Bus: bus}
	issueService := NewIssueService(q, pool, bus, nil, taskService)

	bogusSessionID := dbid.NewV7()
	result, err := issueService.Create(ctx, IssueCreateParams{
		WorkspaceID:  workspaceUUID,
		Title:        "Channel issue immediate bogus",
		Status:       "todo",
		Priority:     "medium",
		AssigneeType: pgtype.Text{String: "agent", Valid: true},
		AssigneeID:   agentUUID,
		CreatorType:  "member",
		CreatorID:    userUUID,
		OriginType:   pgtype.Text{String: "lark_chat", Valid: true},
		OriginID:     bogusSessionID,
	}, IssueCreateOpts{})
	if err != nil {
		t.Fatalf("Create immediate bogus channel issue should not fail the issue itself, got %v", err)
	}
	if result.AssignedTaskID.Valid {
		t.Fatalf("immediate bogus channel issue should have no assigned task due to snapshot failure, got %v", result.AssignedTaskID)
	}
	var issueCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM issue WHERE id = $1`, result.Issue.ID).Scan(&issueCount); err != nil {
		t.Fatalf("count issue: %v", err)
	}
	if issueCount != 1 {
		t.Fatalf("issue should exist despite task snapshot failure, count = %d", issueCount)
	}
	var taskCount int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM agent_task_queue WHERE issue_id = $1`, result.Issue.ID).Scan(&taskCount); err != nil {
		t.Fatalf("count tasks: %v", err)
	}
	if taskCount != 0 {
		t.Fatalf("tasks for immediate bogus channel issue = %d, want 0", taskCount)
	}
	var deliveryCountForIssue int
	if err := pool.QueryRow(ctx, `SELECT count(*) FROM channel_task_delivery WHERE task_id IN (SELECT id FROM agent_task_queue WHERE issue_id = $1)`, result.Issue.ID).Scan(&deliveryCountForIssue); err != nil {
		t.Fatalf("count deliveries for issue: %v", err)
	}
	if deliveryCountForIssue != 0 {
		t.Fatalf("deliveries for immediate bogus channel issue = %d, want 0", deliveryCountForIssue)
	}
}

func TestChannelIssueDelivery_SubsequentTasksDoNotInheritDelivery(t *testing.T) {
	pool := newResolveOriginatorPool(t)
	ctx := context.Background()
	q := db.New(pool)
	workspaceID, userID, agentID, _ := seedAttributionFixture(t, pool)
	workspaceUUID := util.MustParseUUID(workspaceID)
	userUUID := util.MustParseUUID(userID)
	agentUUID := util.MustParseUUID(agentID)

	var chatSessionID pgtype.UUID
	{
		appID := "test-app-" + workspaceID[:8]
		var instID string
		if err := pool.QueryRow(ctx, `
			INSERT INTO channel_installation (workspace_id, agent_id, channel_type, config, installer_user_id, status)
			VALUES ($1, $2, 'feishu', jsonb_build_object('app_id', $3::text), $4, 'active')
			RETURNING id`, workspaceID, agentID, appID, userID).Scan(&instID); err != nil {
			t.Fatalf("create channel installation: %v", err)
		}
		t.Cleanup(func() { pool.Exec(context.Background(), `DELETE FROM channel_installation WHERE id = $1`, instID) })
		var sessionID string
		if err := pool.QueryRow(ctx, `
			INSERT INTO chat_session (id, workspace_id, agent_id, creator_id, title)
			VALUES ($1, $2, $3, $4, '')
			RETURNING id`, dbid.NewV7(), workspaceID, agentID, userID).Scan(&sessionID); err != nil {
			t.Fatalf("create chat session: %v", err)
		}
		chatSessionID = util.MustParseUUID(sessionID)
		t.Cleanup(func() { pool.Exec(context.Background(), `DELETE FROM chat_session WHERE id = $1`, sessionID) })
		if _, err := pool.Exec(ctx, `
			INSERT INTO channel_chat_session_binding (chat_session_id, installation_id, channel_type, channel_chat_id, chat_type, config, route_revision)
			VALUES ($1, $2, 'feishu', 'oc_test_chat', 'group', '{}'::jsonb, 1)`, sessionID, instID); err != nil {
			t.Fatalf("create channel binding: %v", err)
		}
		if _, err := pool.Exec(ctx, `INSERT INTO channel_chat_context_generation (chat_session_id, revision) VALUES ($1, 1)`, sessionID); err != nil {
			t.Fatalf("create context generation: %v", err)
		}
	}

	bus := events.New()
	taskService := &TaskService{Queries: q, TxStarter: pool, Bus: bus}
	issueService := NewIssueService(q, pool, bus, nil, taskService)
	result, err := issueService.Create(ctx, IssueCreateParams{
		WorkspaceID:  workspaceUUID,
		Title:        "channel issue for subsequent",
		Status:       "todo",
		Priority:     "medium",
		AssigneeType: pgtype.Text{String: "agent", Valid: true},
		AssigneeID:   agentUUID,
		CreatorType:  "member",
		CreatorID:    userUUID,
		OriginType:   pgtype.Text{String: "lark_chat", Valid: true},
		OriginID:     chatSessionID,
	}, IssueCreateOpts{})
	if err != nil {
		t.Fatalf("create channel issue: %v", err)
	}
	issue, err := q.GetIssue(ctx, result.Issue.ID)
	if err != nil {
		t.Fatalf("load issue: %v", err)
	}
	firstTaskID := result.AssignedTaskID
	if !firstTaskID.Valid {
		t.Fatalf("initial channel issue should have assigned task")
	}
	if _, err := q.GetChannelTaskDelivery(ctx, firstTaskID); err != nil {
		t.Fatalf("initial channel issue task should have delivery: %v", err)
	}
	// Delete the first task so subsequent enqueues are not blocked by duplicate pending.
	pool.Exec(ctx, `DELETE FROM agent_task_queue WHERE id = $1`, util.UUIDToString(firstTaskID))
	pool.Exec(ctx, `DELETE FROM channel_task_delivery WHERE task_id = $1`, util.UUIDToString(firstTaskID))

	// Web task for same issue (no OriginType) should not inherit delivery.
	webIssue := issue
	webIssue.OriginType = pgtype.Text{}
	webIssue.OriginID = pgtype.UUID{}
	webTask, err := taskService.EnqueueTaskForIssue(ctx, webIssue)
	if err != nil {
		t.Logf("web task enqueue: %v", err)
	} else {
		if _, err := q.GetChannelTaskDelivery(ctx, webTask.ID); err == nil {
			t.Fatalf("web task should not have delivery, but found one for %v", webTask.ID)
		}
		pool.Exec(ctx, `DELETE FROM agent_task_queue WHERE id = $1`, util.UUIDToString(webTask.ID))
		pool.Exec(ctx, `DELETE FROM channel_task_delivery WHERE task_id = $1`, util.UUIDToString(webTask.ID))
	}

	// Comment-triggered task should not inherit delivery.
	var commentID string
	if err := pool.QueryRow(ctx, `
		INSERT INTO comment (issue_id, workspace_id, author_type, author_id, content)
		VALUES ($1, $2, 'member', $3, 'follow-up')
		RETURNING id`, util.UUIDToString(issue.ID), workspaceID, userID).Scan(&commentID); err != nil {
		t.Fatalf("create comment: %v", err)
	}
	commentUUID := util.MustParseUUID(commentID)
	commentTask, err := taskService.EnqueueTaskForIssue(ctx, issue, commentUUID)
	if err != nil {
		t.Logf("comment task enqueue: %v", err)
	} else {
		if _, err := q.GetChannelTaskDelivery(ctx, commentTask.ID); err == nil {
			t.Fatalf("comment-triggered task should not inherit delivery, but found one for %v", commentTask.ID)
		}
		pool.Exec(ctx, `DELETE FROM agent_task_queue WHERE id = $1`, util.UUIDToString(commentTask.ID))
		pool.Exec(ctx, `DELETE FROM channel_task_delivery WHERE task_id = $1`, util.UUIDToString(commentTask.ID))
	}

	// Rerun task should not inherit delivery.
	rerunSourceTask, err := taskService.EnqueueTaskForIssue(ctx, issue)
	if err != nil {
		t.Fatalf("create rerun source task: %v", err)
	}
	rerunTask, err := taskService.enqueueIssueTask(ctx, issue, pgtype.UUID{}, false, "", pgtype.UUID{}, rerunSourceTask.ID, pgtype.Timestamptz{})
	if err != nil {
		t.Logf("rerun task enqueue: %v", err)
	} else {
		if _, err := q.GetChannelTaskDelivery(ctx, rerunTask.ID); err == nil {
			t.Fatalf("rerun task should not inherit delivery, but found one for %v", rerunTask.ID)
		}
		pool.Exec(ctx, `DELETE FROM agent_task_queue WHERE id = $1`, util.UUIDToString(rerunTask.ID))
		pool.Exec(ctx, `DELETE FROM channel_task_delivery WHERE task_id = $1`, util.UUIDToString(rerunTask.ID))
	}
	pool.Exec(ctx, `DELETE FROM agent_task_queue WHERE id = $1`, util.UUIDToString(rerunSourceTask.ID))
	pool.Exec(ctx, `DELETE FROM channel_task_delivery WHERE task_id = $1`, util.UUIDToString(rerunSourceTask.ID))
}

func TestChannelIssueDelivery_BindingDeletedOrdinaryEnqueueStillSucceeds(t *testing.T) {
	pool := newResolveOriginatorPool(t)
	ctx := context.Background()
	q := db.New(pool)
	workspaceID, userID, agentID, _ := seedAttributionFixture(t, pool)
	workspaceUUID := util.MustParseUUID(workspaceID)
	userUUID := util.MustParseUUID(userID)
	agentUUID := util.MustParseUUID(agentID)

	var chatSessionID pgtype.UUID
	var instID string
	{
		appID := "test-app-" + workspaceID[:8]
		if err := pool.QueryRow(ctx, `
			INSERT INTO channel_installation (workspace_id, agent_id, channel_type, config, installer_user_id, status)
			VALUES ($1, $2, 'feishu', jsonb_build_object('app_id', $3::text), $4, 'active')
			RETURNING id`, workspaceID, agentID, appID, userID).Scan(&instID); err != nil {
			t.Fatalf("create channel installation: %v", err)
		}
		t.Cleanup(func() { pool.Exec(context.Background(), `DELETE FROM channel_installation WHERE id = $1`, instID) })
		var sessionID string
		if err := pool.QueryRow(ctx, `
			INSERT INTO chat_session (id, workspace_id, agent_id, creator_id, title)
			VALUES ($1, $2, $3, $4, '')
			RETURNING id`, dbid.NewV7(), workspaceID, agentID, userID).Scan(&sessionID); err != nil {
			t.Fatalf("create chat session: %v", err)
		}
		chatSessionID = util.MustParseUUID(sessionID)
		t.Cleanup(func() { pool.Exec(context.Background(), `DELETE FROM chat_session WHERE id = $1`, sessionID) })
		if _, err := pool.Exec(ctx, `
			INSERT INTO channel_chat_session_binding (chat_session_id, installation_id, channel_type, channel_chat_id, chat_type, config, route_revision)
			VALUES ($1, $2, 'feishu', 'oc_test_chat', 'group', '{}'::jsonb, 1)`, sessionID, instID); err != nil {
			t.Fatalf("create channel binding: %v", err)
		}
		if _, err := pool.Exec(ctx, `INSERT INTO channel_chat_context_generation (chat_session_id, revision) VALUES ($1, 1)`, sessionID); err != nil {
			t.Fatalf("create context generation: %v", err)
		}
	}

	bus := events.New()
	taskService := &TaskService{Queries: q, TxStarter: pool, Bus: bus}
	issueService := NewIssueService(q, pool, bus, nil, taskService)

	// First channel issue should have delivery.
	firstResult, err := issueService.Create(ctx, IssueCreateParams{
		WorkspaceID:  workspaceUUID,
		Title:        "Channel issue before delete",
		Status:       "todo",
		Priority:     "medium",
		AssigneeType: pgtype.Text{String: "agent", Valid: true},
		AssigneeID:   agentUUID,
		CreatorType:  "member",
		CreatorID:    userUUID,
		OriginType:   pgtype.Text{String: "lark_chat", Valid: true},
		OriginID:     chatSessionID,
	}, IssueCreateOpts{})
	if err != nil {
		t.Fatalf("Create first channel issue: %v", err)
	}
	if _, err := q.GetChannelTaskDelivery(ctx, firstResult.AssignedTaskID); err != nil {
		t.Fatalf("first channel issue should have delivery: %v", err)
	}

	// Delete the originating binding.
	if _, err := pool.Exec(ctx, `DELETE FROM channel_chat_session_binding WHERE chat_session_id = $1`, util.UUIDToString(chatSessionID)); err != nil {
		t.Fatalf("delete binding: %v", err)
	}
	// Delete the first task so the follow-up is not blocked by duplicate pending.
	pool.Exec(ctx, `DELETE FROM agent_task_queue WHERE id = $1`, util.UUIDToString(firstResult.AssignedTaskID))
	pool.Exec(ctx, `DELETE FROM channel_task_delivery WHERE task_id = $1`, util.UUIDToString(firstResult.AssignedTaskID))

	// Ordinary enqueue after binding delete should still succeed (without delivery) and not be blocked.
	ordinaryIssueID := firstResult.Issue.ID
	ordinaryIssue, err := q.GetIssue(ctx, ordinaryIssueID)
	if err != nil {
		t.Fatalf("load ordinary issue: %v", err)
	}
	var commentID string
	if err := pool.QueryRow(ctx, `
		INSERT INTO comment (issue_id, workspace_id, author_type, author_id, content)
		VALUES ($1, $2, 'member', $3, 'after delete')
		RETURNING id`, util.UUIDToString(ordinaryIssue.ID), workspaceID, userID).Scan(&commentID); err != nil {
		t.Fatalf("create comment after delete: %v", err)
	}
	commentUUID := util.MustParseUUID(commentID)
	followUpTask, err := taskService.EnqueueTaskForIssue(ctx, ordinaryIssue, commentUUID)
	if err != nil {
		t.Fatalf("ordinary enqueue after binding delete should succeed, got %v", err)
	}
	if _, err := q.GetChannelTaskDelivery(ctx, followUpTask.ID); err == nil {
		t.Fatalf("ordinary follow-up task after binding delete should not have delivery")
	}
	// First task's delivery should still be intact (already deleted, so check it was deleted).
	// The important part is that the first task's delivery was not affected by the binding delete
	// beyond the initial snapshot - it was already snapshotted.
}
