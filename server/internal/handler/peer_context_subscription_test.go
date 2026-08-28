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
		dbfx.QueryRow(t, `SELECT s.seen_revision, i.revision FROM issue_context_subscription s JOIN issue i ON i.id = s.peer_issue_id WHERE s.task_id = $1 AND s.peer_issue_id = $2`, sourceTaskID, peerIssueID).Scan(&seen, &revision)
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

	t.Run("revision bump marks stale", func(t *testing.T) {
		dbfx.Exec(t, `UPDATE issue SET title = title || ' changed', revision = revision + 1 WHERE id = $1`, peerIssueID)
		rows, err := testHandler.Queries.ListActiveSiblingIssueTasks(ctx, db.ListActiveSiblingIssueTasksParams{
			TaskID: sourceTask, ParentIssueID: parseUUID(parentID), WorkspaceID: parseUUID(testWorkspaceID),
		})
		if err != nil || len(rows) != 1 || !rows[0].Stale || rows[0].IssueRevision <= rows[0].SeenRevision {
			t.Fatalf("revision bump did not mark peer stale: rows=%+v err=%v", rows, err)
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
		dbfx.Exec(t, fmt.Sprintf(`CREATE TRIGGER %s BEFORE INSERT OR UPDATE ON issue_context_subscription FOR EACH ROW EXECUTE FUNCTION %s()`, trigger, fn))
		t.Cleanup(func() {
			dbfx.Exec(t, "DROP TRIGGER IF EXISTS "+trigger+" ON issue_context_subscription")
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
