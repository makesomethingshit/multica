package handler

import (
	"context"
	"encoding/json"
	"fmt"
	"net/http"
	"net/http/httptest"
	"strings"
	"sync"
	"testing"
	"time"

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
		subReq := newRequest(http.MethodGet, "/api/tasks/"+sourceTaskID+"/peer-context", nil)
		subReq = withURLParam(subReq, "taskId", sourceTaskID)
		subReq = subReq.WithContext(middleware.SetMemberContext(subReq.Context(), testWorkspaceID, db.Member{}))
		subW := httptest.NewRecorder()
		testHandler.ListIssueContextSubscriptions(subW, subReq)
		var subscriptions []issueContextSubscriptionResponse
		if subW.Code != http.StatusOK || json.Unmarshal(subW.Body.Bytes(), &subscriptions) != nil || len(subscriptions) != 1 || subscriptions[0].SeenRevision != revision {
			t.Fatalf("subscription API did not expose seen revision: status=%d body=%s", subW.Code, subW.Body.String())
		}
	})

	t.Run("stale lookup appends to the rebase audit log", func(t *testing.T) {
		dbfx.Exec(t, `UPDATE issue SET title = title || ' rebase', revision = revision + 1 WHERE id = $1`, peerIssueID)
		req := newRequest(http.MethodGet, "/api/tasks/"+peerTaskID+"/messages", nil)
		req.Header.Set("X-Actor-Source", "task_token")
		req.Header.Set("X-Task-ID", sourceTaskID)
		req = withURLParam(req, "taskId", peerTaskID)
		req = req.WithContext(middleware.SetMemberContext(req.Context(), testWorkspaceID, db.Member{}))
		w := httptest.NewRecorder()
		testHandler.ListTaskMessagesByUser(w, req)
		if w.Code != http.StatusOK {
			t.Fatalf("stale peer context lookup status = %d: %s", w.Code, w.Body.String())
		}
		var fromRev, toRev int64
		dbfx.QueryRow(t, `SELECT from_revision, to_revision FROM peer_context_rebase_log WHERE task_id = $1 AND peer_task_id = $2 ORDER BY seq DESC LIMIT 1`, sourceTaskID, peerTaskID).Scan(&fromRev, &toRev)
		if toRev <= fromRev {
			t.Fatalf("rebase audit row = (%d -> %d), want a revision advance", fromRev, toRev)
		}
		var taskMessageMarkers int
		dbfx.QueryRow(t, `SELECT count(*) FROM task_message WHERE task_id = $1 AND type = 'peer_context_rebase'`, sourceTaskID).Scan(&taskMessageMarkers)
		if taskMessageMarkers != 0 {
			t.Fatalf("rebase leaked %d rows into the daemon-owned task_message log", taskMessageMarkers)
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

	t.Run("second parallel task on a seen issue records status-only stale marker", func(t *testing.T) {
		// The subscription cursor already covers this issue's revision (both
		// earlier lookups marked it), so only the missing status cursor of the
		// new task can make it stale — the claim shows [stale] and the lookup
		// must record the rebase even though the revision never moved.
		var revisionBefore int64
		dbfx.QueryRow(t, `SELECT revision FROM issue WHERE id = $1`, peerIssueID).Scan(&revisionBefore)
		peerTask3ID := dbfx.Task(t, agentID, testutil.Cols{
			"issue_id":   peerIssueID,
			"runtime_id": runtimeID,
			"status":     "running",
		})
		t.Cleanup(func() {
			dbfx.Exec(t, `UPDATE agent_task_queue SET status = 'completed' WHERE id = $1`, peerTask3ID)
		})
		rows, err := testHandler.Queries.ListActiveSiblingIssueTasks(ctx, db.ListActiveSiblingIssueTasksParams{
			TaskID: sourceTask, ParentIssueID: parseUUID(parentID), WorkspaceID: parseUUID(testWorkspaceID),
		})
		if err != nil {
			t.Fatalf("claim snapshot with second parallel task: %v", err)
		}
		secondStale := false
		for _, row := range rows {
			if uuidToString(row.TaskID) == peerTask3ID {
				secondStale = row.Stale
			}
		}
		if !secondStale {
			t.Fatalf("claim did not mark new parallel task stale: %+v", rows)
		}

		req := newRequest(http.MethodGet, "/api/tasks/"+peerTask3ID+"/messages", nil)
		req.Header.Set("X-Actor-Source", "task_token")
		req.Header.Set("X-Task-ID", sourceTaskID)
		req = withURLParam(req, "taskId", peerTask3ID)
		req = req.WithContext(middleware.SetMemberContext(req.Context(), testWorkspaceID, db.Member{}))
		w := httptest.NewRecorder()
		testHandler.ListTaskMessagesByUser(w, req)
		if w.Code != http.StatusOK {
			t.Fatalf("second parallel task lookup status = %d: %s", w.Code, w.Body.String())
		}
		var fromRev, toRev int64
		var fromStatus, toStatus string
		dbfx.QueryRow(t, `SELECT from_revision, to_revision, from_status, to_status FROM peer_context_rebase_log WHERE task_id = $1 AND peer_task_id = $2 ORDER BY seq DESC LIMIT 1`, sourceTaskID, peerTask3ID).Scan(&fromRev, &toRev, &fromStatus, &toStatus)
		if fromRev != 0 || toRev != revisionBefore {
			t.Fatalf("status-only stale audit row = (rev %d -> %d), want a first read of the peer task at the unchanged revision %d", fromRev, toRev, revisionBefore)
		}
		if fromStatus != "" || toStatus != "running" {
			t.Fatalf("status-only stale audit row = (status %q -> %q), want \"\" -> \"running\"", fromStatus, toStatus)
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

	t.Run("audit write failure rolls back the seen cursor", func(t *testing.T) {
		// Force only the audit insert to fail while the seen write succeeds.
		suffix := strings.ReplaceAll(uuid.NewString(), "-", "")
		fn, trigger := "peer_rebase_fail_"+suffix, "peer_rebase_fail_trg_"+suffix
		dbfx.Exec(t, fmt.Sprintf(`CREATE FUNCTION %s() RETURNS trigger LANGUAGE plpgsql AS $$ BEGIN RAISE EXCEPTION 'forced audit write failure'; RETURN NEW; END $$`, fn))
		dbfx.Exec(t, fmt.Sprintf(`CREATE TRIGGER %s BEFORE INSERT ON peer_context_rebase_log FOR EACH ROW EXECUTE FUNCTION %s()`, trigger, fn))
		t.Cleanup(func() {
			dbfx.Exec(t, "DROP TRIGGER IF EXISTS "+trigger+" ON peer_context_rebase_log")
			dbfx.Exec(t, "DROP FUNCTION IF EXISTS "+fn+"()")
		})
		// Reset the cursor so the next lookup is stale and must record the event.
		dbfx.Exec(t, `DELETE FROM issue_context_subscription_task_seen WHERE task_id = $1 AND peer_task_id = $2`, sourceTaskID, peerTaskID)
		dbfx.Exec(t, `DELETE FROM peer_context_rebase_log WHERE task_id = $1 AND peer_task_id = $2`, sourceTaskID, peerTaskID)

		req := newRequest(http.MethodGet, "/api/tasks/"+peerTaskID+"/messages", nil)
		req.Header.Set("X-Actor-Source", "task_token")
		req.Header.Set("X-Task-ID", sourceTaskID)
		req = withURLParam(req, "taskId", peerTaskID)
		req = req.WithContext(middleware.SetMemberContext(req.Context(), testWorkspaceID, db.Member{}))
		w := httptest.NewRecorder()
		testHandler.ListTaskMessagesByUser(w, req)
		if w.Code != http.StatusInternalServerError {
			t.Fatalf("audit write failure status = %d, want 500: %s", w.Code, w.Body.String())
		}
		var seenRows int
		dbfx.QueryRow(t, `SELECT count(*) FROM issue_context_subscription_task_seen WHERE task_id = $1 AND peer_task_id = $2`, sourceTaskID, peerTaskID).Scan(&seenRows)
		if seenRows != 0 {
			t.Fatalf("seen cursor survived a failed audit write; retry would lose the rebase record")
		}

		dbfx.Exec(t, "DROP TRIGGER IF EXISTS "+trigger+" ON peer_context_rebase_log")
		dbfx.Exec(t, "DROP FUNCTION IF EXISTS "+fn+"()")
		w2 := httptest.NewRecorder()
		testHandler.ListTaskMessagesByUser(w2, req)
		if w2.Code != http.StatusOK {
			t.Fatalf("retry after audit failure status = %d: %s", w2.Code, w2.Body.String())
		}
		var auditRows, seenRows2 int
		dbfx.QueryRow(t, `SELECT count(*) FROM peer_context_rebase_log WHERE task_id = $1 AND peer_task_id = $2`, sourceTaskID, peerTaskID).Scan(&auditRows)
		dbfx.QueryRow(t, `SELECT count(*) FROM issue_context_subscription_task_seen WHERE task_id = $1 AND peer_task_id = $2`, sourceTaskID, peerTaskID).Scan(&seenRows2)
		if auditRows != 1 || seenRows2 != 1 {
			t.Fatalf("retry did not persist audit row (%d) and seen cursor (%d rows)", auditRows, seenRows2)
		}
	})

	t.Run("rebase audit keeps occurrence order and the daemon log stays untouched", func(t *testing.T) {
		// Daemon output before the first rebase, from the daemon's own counter.
		dbfx.Exec(t, `INSERT INTO task_message (task_id, seq, type, content) VALUES ($1, 1, 'text', 'daemon output one'), ($1, 2, 'text', 'daemon output two')`, sourceTaskID)

		// First stale event: a fresh cursor for the peer.
		dbfx.Exec(t, `DELETE FROM issue_context_subscription_task_seen WHERE task_id = $1 AND peer_task_id = $2`, sourceTaskID, peerTaskID)
		dbfx.Exec(t, `DELETE FROM peer_context_rebase_log WHERE task_id = $1 AND peer_task_id = $2`, sourceTaskID, peerTaskID)
		req := newRequest(http.MethodGet, "/api/tasks/"+peerTaskID+"/messages", nil)
		req.Header.Set("X-Actor-Source", "task_token")
		req.Header.Set("X-Task-ID", sourceTaskID)
		req = withURLParam(req, "taskId", peerTaskID)
		req = req.WithContext(middleware.SetMemberContext(req.Context(), testWorkspaceID, db.Member{}))
		w := httptest.NewRecorder()
		testHandler.ListTaskMessagesByUser(w, req)
		if w.Code != http.StatusOK {
			t.Fatalf("first stale lookup status = %d: %s", w.Code, w.Body.String())
		}

		// Second stale event, strictly later in time.
		dbfx.Exec(t, `UPDATE issue SET title = title || ' reordered', revision = revision + 1 WHERE id = $1`, peerIssueID)
		w2 := httptest.NewRecorder()
		testHandler.ListTaskMessagesByUser(w2, req)
		if w2.Code != http.StatusOK {
			t.Fatalf("second stale lookup status = %d: %s", w2.Code, w2.Body.String())
		}

		// The audit trail must read in true occurrence order: event 1 before event 2.
		var seq1, seq2 int64
		var created1, created2 time.Time
		rows, err := testHandler.Queries.ListPeerContextRebaseLog(ctx, sourceTask)
		if err != nil {
			t.Fatalf("list rebase audit log: %v", err)
		}
		matched := 0
		for _, row := range rows {
			if uuidToString(row.PeerTaskID) != peerTaskID {
				continue
			}
			switch matched {
			case 0:
				seq1, created1 = row.Seq, row.CreatedAt.Time
			case 1:
				seq2, created2 = row.Seq, row.CreatedAt.Time
			}
			matched++
		}
		if matched != 2 {
			t.Fatalf("rebase audit log has %d events for the peer, want 2", matched)
		}
		if seq1 >= seq2 || created2.Before(created1) {
			t.Fatalf("rebase audit events out of occurrence order: seq %d -> %d, created %v -> %v", seq1, seq2, created1, created2)
		}

		// The daemon-owned message log carries no server-written rows: the
		// daemon's counter never saw the audit events, so interleaving or
		// dedupe against them is impossible by construction.
		var messages []db.TaskMessage
		messages, err = testHandler.Queries.ListTaskMessages(ctx, sourceTask)
		if err != nil {
			t.Fatalf("list task messages: %v", err)
		}
		seqs := map[int32]bool{}
		for _, m := range messages {
			if m.Type == "peer_context_rebase" {
				t.Fatalf("task_message log contains a rebase marker at seq %d", m.Seq)
			}
			seqs[m.Seq] = true
		}
		for _, want := range []int32{1, 2} {
			if !seqs[want] {
				t.Fatalf("daemon message seq %d missing from the log; seqs = %v", want, seqs)
			}
		}
		if len(seqs) != 2 {
			t.Fatalf("unexpected extra rows in the daemon message log: %v", seqs)
		}

		// The daemon's next real message keeps counting from its own counter
		// and lands after the earlier output, unblocked by the audit events.
		dbfx.Exec(t, `INSERT INTO task_message (task_id, seq, type, content) VALUES ($1, 3, 'text', 'daemon output after rebases')`, sourceTaskID)
		messages, err = testHandler.Queries.ListTaskMessages(ctx, sourceTask)
		if err != nil {
			t.Fatalf("list task messages after daemon append: %v", err)
		}
		if len(messages) != 3 || messages[2].Seq != 3 {
			t.Fatalf("daemon append broke log ordering: %+v", messages)
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

	t.Run("list carries the peer revision from the join", func(t *testing.T) {
		if _, err := testHandler.Queries.CreateIssueContextSubscription(ctx, db.CreateIssueContextSubscriptionParams{
			TaskID: sourceTask, PeerIssueID: peerIssue,
		}); err != nil {
			t.Fatalf("create subscription: %v", err)
		}
		var revision int64
		dbfx.QueryRow(t, `SELECT revision FROM issue WHERE id = $1`, peerIssueID).Scan(&revision)
		req := newRequest(http.MethodGet, "/api/tasks/"+sourceTaskID+"/peer-context/subscriptions", nil)
		req = withURLParam(req, "taskId", sourceTaskID)
		req = req.WithContext(middleware.SetMemberContext(req.Context(), testWorkspaceID, db.Member{}))
		w := httptest.NewRecorder()
		testHandler.ListIssueContextSubscriptions(w, req)
		var subscriptions []issueContextSubscriptionResponse
		if w.Code != http.StatusOK || json.Unmarshal(w.Body.Bytes(), &subscriptions) != nil || len(subscriptions) != 1 {
			t.Fatalf("subscription list = status %d body %s", w.Code, w.Body.String())
		}
		if subscriptions[0].Revision != revision {
			t.Fatalf("subscription revision = %d, want the join-read issue revision %d", subscriptions[0].Revision, revision)
		}
	})
}

// TestIssueContextSubscriptionCapPinsResourceUse pins the per-task
// subscription cap (PUCK-58 review): once a task holds
// maxPeerContextSubscriptionsPerTask subscriptions, further creates are
// rejected instead of growing the set unboundedly.
func TestIssueContextSubscriptionCapPinsResourceUse(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database not available")
	}
	runtimeID := handlerTestRuntimeID(t)
	var agentID string
	dbfx.QueryRow(t, `SELECT id FROM agent WHERE workspace_id = $1 ORDER BY created_at ASC LIMIT 1`, testWorkspaceID).Scan(&agentID)

	parentID := dbfx.Issue(t, "cap parent")
	sourceTaskID := dbfx.Task(t, agentID, testutil.Cols{
		"issue_id":   dbfx.Issue(t, "cap source", testutil.Cols{"parent_issue_id": parentID}),
		"runtime_id": runtimeID,
		"status":     "running",
	})
	for i := 0; i < maxPeerContextSubscriptionsPerTask; i++ {
		dbfx.Exec(t, `INSERT INTO issue_context_subscription (task_id, peer_issue_id) VALUES ($1, $2)`,
			sourceTaskID, dbfx.Issue(t, fmt.Sprintf("cap peer %d", i), testutil.Cols{"parent_issue_id": parentID}))
	}

	overflowPeerID := dbfx.Issue(t, "cap overflow peer", testutil.Cols{"parent_issue_id": parentID})
	req := newRequest(http.MethodPost, "/api/tasks/"+sourceTaskID+"/peer-context/subscriptions", map[string]any{"peer_issue_id": overflowPeerID})
	req = withURLParam(req, "taskId", sourceTaskID)
	req = req.WithContext(middleware.SetMemberContext(req.Context(), testWorkspaceID, db.Member{}))
	w := httptest.NewRecorder()
	testHandler.CreateIssueContextSubscription(w, req)
	if w.Code != http.StatusBadRequest {
		t.Fatalf("over-cap create status = %d, want 400: %s", w.Code, w.Body.String())
	}
}

// TestDeleteIssueSweepsPeerContextRebaseLog pins the issue-delete sweep
// (PUCK-58 review): peer_context_rebase_log has no FK, so DeleteIssue must
// remove the rebase audit rows of the deleted issue's tasks on both the
// source and the peer side, or they strand forever.
func TestDeleteIssueSweepsPeerContextRebaseLog(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database not available")
	}
	ctx := context.Background()
	runtimeID := handlerTestRuntimeID(t)
	var agentID string
	dbfx.QueryRow(t, `SELECT id FROM agent WHERE workspace_id = $1 ORDER BY created_at ASC LIMIT 1`, testWorkspaceID).Scan(&agentID)

	parentID := dbfx.Issue(t, "rebase sweep parent")
	peerTaskID := dbfx.Task(t, agentID, testutil.Cols{
		"issue_id":   dbfx.Issue(t, "rebase sweep peer", testutil.Cols{"parent_issue_id": parentID}),
		"runtime_id": runtimeID,
		"status":     "running",
	})
	victimIssueID := dbfx.Issue(t, "rebase sweep victim", testutil.Cols{"parent_issue_id": parentID})
	victimTaskID := dbfx.Task(t, agentID, testutil.Cols{
		"issue_id":   victimIssueID,
		"runtime_id": runtimeID,
		"status":     "running",
	})
	// The deleted task appears on both sides of the audit log: as the source
	// of its own rebase and as the peer of a surviving task's rebase.
	// peer_context_rebase_log has no FK, so clean up explicitly on teardown.
	dbfx.Cleanup(t, `DELETE FROM peer_context_rebase_log WHERE task_id = $1 OR peer_task_id = $1 OR task_id = $2 OR peer_task_id = $2`, victimTaskID, peerTaskID)
	dbfx.Exec(t, `INSERT INTO peer_context_rebase_log (task_id, peer_task_id, to_revision, to_status) VALUES ($1, $2, 1, 'running')`, victimTaskID, peerTaskID)
	dbfx.Exec(t, `INSERT INTO peer_context_rebase_log (task_id, peer_task_id, to_revision, to_status) VALUES ($1, $2, 1, 'running')`, peerTaskID, victimTaskID)

	if err := testHandler.Queries.DeleteIssue(ctx, db.DeleteIssueParams{ID: parseUUID(victimIssueID), WorkspaceID: parseUUID(testWorkspaceID)}); err != nil {
		t.Fatalf("delete issue: %v", err)
	}
	if got := dbfx.Count(t, `SELECT count(*) FROM peer_context_rebase_log WHERE task_id = $1 OR peer_task_id = $2`, victimTaskID, victimTaskID); got != 0 {
		t.Fatalf("delete left %d rebase audit rows for the deleted issue's task", got)
	}
}

// TestIssueContextSubscriptionConcurrentLimit verifies the atomic admission
// (PUCK-58 review): concurrent creates for the same task cannot race past the
// per-task cap. Ten concurrent creates with only two slots left must leave the
// table at exactly the cap.
func TestIssueContextSubscriptionConcurrentLimit(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database not available")
	}
	runtimeID := handlerTestRuntimeID(t)
	var agentID string
	dbfx.QueryRow(t, `SELECT id FROM agent WHERE workspace_id = $1 ORDER BY created_at ASC LIMIT 1`, testWorkspaceID).Scan(&agentID)

	parentID := dbfx.Issue(t, "concurrent cap parent")
	sourceTaskID := dbfx.Task(t, agentID, testutil.Cols{
		"issue_id":   dbfx.Issue(t, "concurrent cap source", testutil.Cols{"parent_issue_id": parentID}),
		"runtime_id": runtimeID,
		"status":     "running",
	})

	// Pre-fill to 30 via the handler so the remaining capacity is 2.
	for i := 0; i < 30; i++ {
		peerID := dbfx.Issue(t, fmt.Sprintf("concurrent cap pre %d", i), testutil.Cols{"parent_issue_id": parentID})
		req := newRequest(http.MethodPost, "/api/tasks/"+sourceTaskID+"/peer-context/subscriptions", map[string]any{"peer_issue_id": peerID})
		req = withURLParam(req, "taskId", sourceTaskID)
		req = req.WithContext(middleware.SetMemberContext(req.Context(), testWorkspaceID, db.Member{}))
		w := httptest.NewRecorder()
		testHandler.CreateIssueContextSubscription(w, req)
		if w.Code != http.StatusCreated {
			t.Fatalf("pre-fill %d status = %d, want 201: %s", i, w.Code, w.Body.String())
		}
	}

	// Ten distinct new peers contend for the last two slots concurrently.
	peerIDs := make([]string, 10)
	for i := range peerIDs {
		peerIDs[i] = dbfx.Issue(t, fmt.Sprintf("concurrent cap contender %d", i), testutil.Cols{"parent_issue_id": parentID})
	}

	var wg sync.WaitGroup
	results := make([]int, len(peerIDs))
	for i, pid := range peerIDs {
		wg.Add(1)
		go func(idx int, peerID string) {
			defer wg.Done()
			req := newRequest(http.MethodPost, "/api/tasks/"+sourceTaskID+"/peer-context/subscriptions", map[string]any{"peer_issue_id": peerID})
			req = withURLParam(req, "taskId", sourceTaskID)
			req = req.WithContext(middleware.SetMemberContext(req.Context(), testWorkspaceID, db.Member{}))
			w := httptest.NewRecorder()
			testHandler.CreateIssueContextSubscription(w, req)
			results[idx] = w.Code
		}(i, pid)
	}
	wg.Wait()

	successes := 0
	for _, code := range results {
		if code == http.StatusCreated || code == http.StatusOK {
			successes++
		} else if code != http.StatusBadRequest {
			t.Fatalf("concurrent create returned %d, want 201/200 or 400", code)
		}
	}
	if successes != 2 {
		t.Fatalf("concurrent creates succeeded %d, want 2 to reach cap 32", successes)
	}
	if got := dbfx.Count(t, `SELECT count(*) FROM issue_context_subscription WHERE task_id = $1`, sourceTaskID); got != 32 {
		t.Fatalf("subscription count = %d, want exactly 32", got)
	}
}

// TestIssueContextSubscriptionDuplicateAtCapSucceeds verifies idempotency
// at the cap (PUCK-58 review): re-posting an already-subscribed peer when
// the table is at 32 must not be rejected with 400.
func TestIssueContextSubscriptionDuplicateAtCapSucceeds(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database not available")
	}
	runtimeID := handlerTestRuntimeID(t)
	var agentID string
	dbfx.QueryRow(t, `SELECT id FROM agent WHERE workspace_id = $1 ORDER BY created_at ASC LIMIT 1`, testWorkspaceID).Scan(&agentID)

	parentID := dbfx.Issue(t, "dup cap parent")
	sourceIssueID := dbfx.Issue(t, "dup cap source", testutil.Cols{"parent_issue_id": parentID})
	sourceTaskID := dbfx.Task(t, agentID, testutil.Cols{
		"issue_id":   sourceIssueID,
		"runtime_id": runtimeID,
		"status":     "running",
	})

	var firstPeerID string
	for i := 0; i < maxPeerContextSubscriptionsPerTask; i++ {
		peerID := dbfx.Issue(t, fmt.Sprintf("dup cap peer %d", i), testutil.Cols{"parent_issue_id": parentID})
		if i == 0 {
			firstPeerID = peerID
		}
		req := newRequest(http.MethodPost, "/api/tasks/"+sourceTaskID+"/peer-context/subscriptions", map[string]any{"peer_issue_id": peerID})
		req = withURLParam(req, "taskId", sourceTaskID)
		req = req.WithContext(middleware.SetMemberContext(req.Context(), testWorkspaceID, db.Member{}))
		w := httptest.NewRecorder()
		testHandler.CreateIssueContextSubscription(w, req)
		if w.Code != http.StatusCreated {
			t.Fatalf("fill %d status = %d, want 201: %s", i, w.Code, w.Body.String())
		}
	}

	// Duplicate of an already-subscribed peer must succeed even at cap.
	req := newRequest(http.MethodPost, "/api/tasks/"+sourceTaskID+"/peer-context/subscriptions", map[string]any{"peer_issue_id": firstPeerID})
	req = withURLParam(req, "taskId", sourceTaskID)
	req = req.WithContext(middleware.SetMemberContext(req.Context(), testWorkspaceID, db.Member{}))
	w := httptest.NewRecorder()
	testHandler.CreateIssueContextSubscription(w, req)
	if w.Code != http.StatusCreated && w.Code != http.StatusOK {
		t.Fatalf("duplicate at cap status = %d, want 200/201: %s", w.Code, w.Body.String())
	}
	if got := dbfx.Count(t, `SELECT count(*) FROM issue_context_subscription WHERE task_id = $1`, sourceTaskID); got != 32 {
		t.Fatalf("subscription count after duplicate = %d, want still 32", got)
	}

	// New peer beyond cap must still be rejected.
	overflowPeerID := dbfx.Issue(t, "dup cap overflow", testutil.Cols{"parent_issue_id": parentID})
	req2 := newRequest(http.MethodPost, "/api/tasks/"+sourceTaskID+"/peer-context/subscriptions", map[string]any{"peer_issue_id": overflowPeerID})
	req2 = withURLParam(req2, "taskId", sourceTaskID)
	req2 = req2.WithContext(middleware.SetMemberContext(req2.Context(), testWorkspaceID, db.Member{}))
	w2 := httptest.NewRecorder()
	testHandler.CreateIssueContextSubscription(w2, req2)
	if w2.Code != http.StatusBadRequest {
		t.Fatalf("overflow at cap status = %d, want 400: %s", w2.Code, w2.Body.String())
	}
}

// TestMarkIssueContextSubscriptionSeenRespectsCap verifies the Mark path
// also respects the cap (PUCK-58 review): when the subscription table is at
// 32, a ListTaskMessagesByUser that would implicitly create a 33rd
// subscription via Mark must not grow the table.
func TestMarkIssueContextSubscriptionSeenRespectsCap(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database not available")
	}
	ctx := context.Background()
	runtimeID := handlerTestRuntimeID(t)
	var agentID string
	dbfx.QueryRow(t, `SELECT id FROM agent WHERE workspace_id = $1 ORDER BY created_at ASC LIMIT 1`, testWorkspaceID).Scan(&agentID)

	parentID := dbfx.Issue(t, "mark cap parent")
	sourceTaskID := dbfx.Task(t, agentID, testutil.Cols{
		"issue_id":   dbfx.Issue(t, "mark cap source", testutil.Cols{"parent_issue_id": parentID}),
		"runtime_id": runtimeID,
		"status":     "running",
	})
	// Fill to cap via explicit creates
	for i := 0; i < maxPeerContextSubscriptionsPerTask; i++ {
		peerID := dbfx.Issue(t, fmt.Sprintf("mark cap peer %d", i), testutil.Cols{"parent_issue_id": parentID})
		req := newRequest(http.MethodPost, "/api/tasks/"+sourceTaskID+"/peer-context/subscriptions", map[string]any{"peer_issue_id": peerID})
		req = withURLParam(req, "taskId", sourceTaskID)
		req = req.WithContext(middleware.SetMemberContext(req.Context(), testWorkspaceID, db.Member{}))
		w := httptest.NewRecorder()
		testHandler.CreateIssueContextSubscription(w, req)
		if w.Code != http.StatusCreated {
			t.Fatalf("fill %d status = %d, want 201: %s", i, w.Code, w.Body.String())
		}
	}

	newPeerIssueID := dbfx.Issue(t, "mark cap new peer", testutil.Cols{"parent_issue_id": parentID})
	newPeerTaskID := dbfx.Task(t, agentID, testutil.Cols{
		"issue_id":   newPeerIssueID,
		"runtime_id": runtimeID,
		"status":     "running",
	})

	// Implicit subscription via task_token message lookup
	req := newRequest(http.MethodGet, "/api/tasks/"+newPeerTaskID+"/messages", nil)
	req.Header.Set("X-Actor-Source", "task_token")
	req.Header.Set("X-Task-ID", sourceTaskID)
	req = withURLParam(req, "taskId", newPeerTaskID)
	req = req.WithContext(middleware.SetMemberContext(req.Context(), testWorkspaceID, db.Member{}))
	w := httptest.NewRecorder()
	testHandler.ListTaskMessagesByUser(w, req)
	if w.Code != http.StatusOK {
		t.Fatalf("mark via message lookup status = %d, want 200: %s", w.Code, w.Body.String())
	}

	if got := dbfx.Count(t, `SELECT count(*) FROM issue_context_subscription WHERE task_id = $1`, sourceTaskID); got != 32 {
		t.Fatalf("subscription count after mark bypass attempt = %d, want still 32", got)
	}
	// Seen should still have been recorded for the peer_task
	var seenRows int
	dbfx.QueryRow(t, `SELECT count(*) FROM issue_context_subscription_task_seen WHERE task_id = $1 AND peer_task_id = $2`, sourceTaskID, newPeerTaskID).Scan(&seenRows)
	if seenRows != 1 {
		t.Fatalf("seen rows for new peer = %d, want 1 (mark's seen side should still succeed)", seenRows)
	}
	// No subscription row for the new peer_issue
	var subRows int
	dbfx.QueryRow(t, `SELECT count(*) FROM issue_context_subscription WHERE task_id = $1 AND peer_issue_id = $2`, sourceTaskID, newPeerIssueID).Scan(&subRows)
	if subRows != 0 {
		t.Fatalf("subscription unexpectedly created for new peer at cap: %d rows", subRows)
	}

	// Mark for an already-subscribed peer must still update seen even at cap
	// Pick an existing subscription's peer issue and create a fresh peer task on it
	var existingPeerIssueID string
	dbfx.QueryRow(t, `SELECT peer_issue_id FROM issue_context_subscription WHERE task_id = $1 LIMIT 1`, sourceTaskID).Scan(&existingPeerIssueID)
	existingPeerTaskID := dbfx.Task(t, agentID, testutil.Cols{
		"issue_id":   existingPeerIssueID,
		"runtime_id": runtimeID,
		"status":     "running",
	})
	req2 := newRequest(http.MethodGet, "/api/tasks/"+existingPeerTaskID+"/messages", nil)
	req2.Header.Set("X-Actor-Source", "task_token")
	req2.Header.Set("X-Task-ID", sourceTaskID)
	req2 = withURLParam(req2, "taskId", existingPeerTaskID)
	req2 = req2.WithContext(middleware.SetMemberContext(req2.Context(), testWorkspaceID, db.Member{}))
	w2 := httptest.NewRecorder()
	testHandler.ListTaskMessagesByUser(w2, req2)
	if w2.Code != http.StatusOK {
		t.Fatalf("mark existing peer at cap status = %d, want 200: %s", w2.Code, w2.Body.String())
	}
	_ = ctx
}
