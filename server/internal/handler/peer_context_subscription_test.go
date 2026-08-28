package handler

import (
	"context"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"testing"

	"github.com/google/uuid"
	"github.com/jackc/pgx/v5"
	"github.com/multica-ai/multica/server/internal/middleware"
	"github.com/multica-ai/multica/server/internal/testutil"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

func TestIssueContextSubscriptionBoundaries(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database not available")
	}

	ctx := context.Background()
	runtimeID := handlerTestRuntimeID(t)
	var agentID string
	dbfx.QueryRow(t, `SELECT id FROM agent WHERE workspace_id = $1 ORDER BY created_at ASC LIMIT 1`, testWorkspaceID).Scan(&agentID)

	parentID := dbfx.Issue(t, "peer-context parent")
	peerIssueID := dbfx.Issue(t, "peer-context peer", testutil.Cols{"parent_issue_id": parentID})
	sourceTaskID := dbfx.Task(t, agentID, testutil.Cols{
		"issue_id":   dbfx.Issue(t, "peer-context source", testutil.Cols{"parent_issue_id": parentID}),
		"runtime_id": runtimeID,
		"status":     "running",
	})
	peerTaskID := dbfx.Task(t, agentID, testutil.Cols{
		"issue_id":   peerIssueID,
		"runtime_id": runtimeID,
		"status":     "running",
	})

	sourceTask := parseUUID(sourceTaskID)
	peerIssue := parseUUID(peerIssueID)
	if _, err := testHandler.Queries.CreateIssueContextSubscription(ctx, db.CreateIssueContextSubscriptionParams{
		TaskID: sourceTask, PeerIssueID: peerIssue,
	}); err != nil {
		t.Fatalf("create subscription: %v", err)
	}
	if rows, err := testHandler.Queries.ListIssueContextSubscriptions(ctx, sourceTask); err != nil || len(rows) != 1 || rows[0].PeerIssueID != peerIssue {
		t.Fatalf("list subscriptions = (%v, %v), want one peer row", rows, err)
	}

	t.Run("claim does not mark seen", func(t *testing.T) {
		rows, err := testHandler.Queries.ListActiveSiblingIssueTasks(ctx, db.ListActiveSiblingIssueTasksParams{
			TaskID: sourceTask, ParentIssueID: parseUUID(parentID), WorkspaceID: parseUUID(testWorkspaceID),
		})
		if err != nil {
			t.Fatalf("claim peer snapshot: %v", err)
		}
		if len(rows) != 1 || rows[0].TaskID != parseUUID(peerTaskID) {
			t.Fatalf("claim peers = %+v, want peer task %s", rows, peerTaskID)
		}
		if rows[0].SeenRevision != 0 {
			t.Fatalf("claim changed seen_revision to %d", rows[0].SeenRevision)
		}
		if !rows[0].Stale {
			t.Fatalf("claim did not mark unseen peer revision stale: %+v", rows[0])
		}
	})

	t.Run("explicit lookup marks seen", func(t *testing.T) {
		req := newRequest(http.MethodGet, "/api/tasks/"+peerTaskID+"/messages", nil)
		req.Header.Set("X-Actor-Source", "task_token")
		req.Header.Set("X-Task-ID", sourceTaskID)
		req = withURLParam(req, "taskId", peerTaskID)
		req = req.WithContext(middleware.SetMemberContext(req.Context(), testWorkspaceID, db.Member{}))
		w := httptest.NewRecorder()
		testHandler.ListTaskMessagesByUser(w, req)
		if w.Code != http.StatusOK {
			t.Fatalf("peer context lookup status = %d: %s", w.Code, w.Body.String())
		}

		var seen, revision int64
		dbfx.QueryRow(t, `SELECT s.seen_revision, i.revision FROM issue_context_subscription_task_seen s JOIN agent_task_queue pt ON pt.id = s.peer_task_id JOIN issue i ON i.id = pt.issue_id WHERE s.task_id = $1 AND s.peer_task_id = $2`, sourceTaskID, peerTaskID).Scan(&seen, &revision)
		if seen != revision || seen == 0 {
			t.Fatalf("seen_revision = %d, issue revision = %d", seen, revision)
		}
		rows, err := testHandler.Queries.ListActiveSiblingIssueTasks(ctx, db.ListActiveSiblingIssueTasksParams{
			TaskID: sourceTask, ParentIssueID: parseUUID(parentID), WorkspaceID: parseUUID(testWorkspaceID),
		})
		if err != nil || len(rows) != 1 || rows[0].Stale {
			t.Fatalf("seen peer still marked stale: rows=%+v err=%v", rows, err)
		}
	})

	t.Run("parallel peer tasks keep independent status cursors", func(t *testing.T) {
		peerTask2ID := dbfx.Task(t, agentID, testutil.Cols{
			"issue_id":   peerIssueID,
			"runtime_id": runtimeID,
			"status":     "running",
		})
		t.Cleanup(func() {
			dbfx.Exec(t, `UPDATE agent_task_queue SET status = 'completed' WHERE id = $1`, peerTask2ID)
		})
		// The first peer was already looked up above; only its task cursor is seen.
		rows, err := testHandler.Queries.ListActiveSiblingIssueTasks(ctx, db.ListActiveSiblingIssueTasksParams{
			TaskID: sourceTask, ParentIssueID: parseUUID(parentID), WorkspaceID: parseUUID(testWorkspaceID),
		})
		if err != nil {
			t.Fatalf("parallel peer snapshot after lookup: %v", err)
		}
		foundFirst, foundSecond := false, false
		for _, row := range rows {
			switch uuidToString(row.TaskID) {
			case peerTaskID:
				foundFirst = true
				if row.Stale {
					t.Fatalf("looked-up peer task remained stale: %+v", row)
				}
			case peerTask2ID:
				foundSecond = true
				if !row.Stale {
					t.Fatalf("unlooked-up parallel peer task was not stale: %+v", row)
				}
			}
		}
		if !foundFirst || !foundSecond {
			t.Fatalf("parallel peer tasks missing from snapshot: %+v", rows)
		}
	})

	t.Run("seen keeps the returned snapshot revision", func(t *testing.T) {
		suffix := strings.ReplaceAll(uuid.NewString(), "-", "")
		fn, trigger := "peer_seen_snapshot_"+suffix, "peer_seen_snapshot_trg_"+suffix
		dbfx.Exec(t, fmt.Sprintf(`CREATE FUNCTION %s() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN UPDATE issue SET revision = revision + 1 WHERE id = (SELECT issue_id FROM agent_task_queue WHERE id = NEW.peer_task_id); RETURN NEW; END $$`, fn))
		dbfx.Exec(t, fmt.Sprintf(`CREATE TRIGGER %s BEFORE INSERT OR UPDATE ON issue_context_subscription_task_seen FOR EACH ROW EXECUTE FUNCTION %s()`, trigger, fn))
		t.Cleanup(func() {
			dbfx.Exec(t, "DROP TRIGGER IF EXISTS "+trigger+" ON issue_context_subscription_task_seen")
			dbfx.Exec(t, "DROP FUNCTION IF EXISTS "+fn+"()")
		})

		req := newRequest(http.MethodGet, "/api/tasks/"+peerTaskID+"/messages", nil)
		req.Header.Set("X-Actor-Source", "task_token")
		req.Header.Set("X-Task-ID", sourceTaskID)
		req = withURLParam(req, "taskId", peerTaskID)
		req = req.WithContext(middleware.SetMemberContext(req.Context(), testWorkspaceID, db.Member{}))
		w := httptest.NewRecorder()
		testHandler.ListTaskMessagesByUser(w, req)
		if w.Code != http.StatusOK {
			t.Fatalf("snapshot lookup status = %d: %s", w.Code, w.Body.String())
		}

		var seen, revision int64
		dbfx.QueryRow(t, `SELECT s.seen_revision, i.revision FROM issue_context_subscription_task_seen s JOIN agent_task_queue pt ON pt.id = s.peer_task_id JOIN issue i ON i.id = pt.issue_id WHERE s.task_id = $1 AND s.peer_task_id = $2`, sourceTaskID, peerTaskID).Scan(&seen, &revision)
		if seen >= revision {
			t.Fatalf("seen_revision = %d, issue revision = %d; future revision was marked seen", seen, revision)
		}
	})

	t.Run("revision bump marks stale", func(t *testing.T) {
		dbfx.Exec(t, `UPDATE issue SET title = title || ' changed', revision = revision + 1 WHERE id = $1`, peerIssueID)
		rows, err := testHandler.Queries.ListActiveSiblingIssueTasks(ctx, db.ListActiveSiblingIssueTasksParams{
			TaskID: sourceTask, ParentIssueID: parseUUID(parentID), WorkspaceID: parseUUID(testWorkspaceID),
		})
		if err != nil || len(rows) != 1 || !rows[0].Stale || rows[0].IssueRevision <= rows[0].SeenRevision {
			t.Fatalf("revision bump did not mark peer stale: rows=%+v err=%v", rows, err)
		}
	})

	t.Run("task status bump marks stale", func(t *testing.T) {
		var revision int64
		dbfx.QueryRow(t, `SELECT revision FROM issue WHERE id = $1`, peerIssueID).Scan(&revision)
		dbfx.Exec(t, `UPDATE issue_context_subscription_task_seen SET seen_revision = $2, seen_task_status = 'running' WHERE task_id = $1 AND peer_task_id = $3`, sourceTaskID, revision, peerTaskID)
		dbfx.Exec(t, `UPDATE agent_task_queue SET status = 'waiting_local_directory' WHERE id = $1`, peerTaskID)
		rows, err := testHandler.Queries.ListActiveSiblingIssueTasks(ctx, db.ListActiveSiblingIssueTasksParams{
			TaskID: sourceTask, ParentIssueID: parseUUID(parentID), WorkspaceID: parseUUID(testWorkspaceID),
		})
		if err != nil || len(rows) != 1 || rows[0].IssueRevision != rows[0].SeenRevision || !rows[0].Stale {
			t.Fatalf("task status bump did not mark peer stale: rows=%+v err=%v", rows, err)
		}
	})

	t.Run("cross workspace create is 404-equivalent", func(t *testing.T) {
		foreignIssueID, _ := setupForeignWorkspaceFixture(t)
		_, err := testHandler.Queries.CreateIssueContextSubscription(ctx, db.CreateIssueContextSubscriptionParams{
			TaskID: sourceTask, PeerIssueID: parseUUID(foreignIssueID),
		})
		if err != pgx.ErrNoRows {
			t.Fatalf("cross-workspace create error = %v, want pgx.ErrNoRows", err)
		}
	})

	t.Run("seen write failure is not 200", func(t *testing.T) {
		// Force the Mark... statement to fail while leaving reads available.
		suffix := strings.ReplaceAll(uuid.NewString(), "-", "")
		fn, trigger := "peer_seen_fail_"+suffix, "peer_seen_fail_trg_"+suffix
		dbfx.Exec(t, fmt.Sprintf(`CREATE FUNCTION %s() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'forced seen write failure'; RETURN NEW; END $$`, fn))
		dbfx.Exec(t, fmt.Sprintf(`CREATE TRIGGER %s BEFORE INSERT OR UPDATE ON issue_context_subscription_task_seen FOR EACH ROW EXECUTE FUNCTION %s()`, trigger, fn))
		t.Cleanup(func() {
			dbfx.Exec(t, "DROP TRIGGER IF EXISTS "+trigger+" ON issue_context_subscription_task_seen")
			dbfx.Exec(t, "DROP FUNCTION IF EXISTS "+fn+"()")
		})

		req := newRequest(http.MethodGet, "/api/tasks/"+peerTaskID+"/messages", nil)
		req.Header.Set("X-Actor-Source", "task_token")
		req.Header.Set("X-Task-ID", sourceTaskID)
		req = withURLParam(req, "taskId", peerTaskID)
		req = req.WithContext(middleware.SetMemberContext(req.Context(), testWorkspaceID, db.Member{}))
		w := httptest.NewRecorder()
		testHandler.ListTaskMessagesByUser(w, req)
		if w.Code != http.StatusInternalServerError {
			t.Fatalf("seen write failure status = %d, want 500: %s", w.Code, w.Body.String())
		}
	})

	t.Run("delete removes subscription", func(t *testing.T) {
		deleted, err := testHandler.Queries.DeleteIssueContextSubscription(ctx, db.DeleteIssueContextSubscriptionParams{
			TaskID: sourceTask, PeerIssueID: peerIssue,
		})
		if err != nil || deleted != 1 {
			t.Fatalf("delete = (%d, %v), want (1, nil)", deleted, err)
		}
		if _, err := testHandler.Queries.GetIssueContextSubscription(ctx, db.GetIssueContextSubscriptionParams{
			TaskID: sourceTask, PeerIssueID: peerIssue,
		}); err != pgx.ErrNoRows {
			t.Fatalf("get after delete error = %v, want pgx.ErrNoRows", err)
		}
	})
}
