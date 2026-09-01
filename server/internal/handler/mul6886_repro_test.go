package handler

import (
	"context"
	"net/url"
	"testing"

	"github.com/multica-ai/multica/server/internal/service"
	db "github.com/multica-ai/multica/server/pkg/db/generated"
)

// Baseline: upstream/main 2e297451001e65f78721efc24e36e7939e0f0ed6 (2026-09-01) + fork HEAD 15d904a99
// Repro command: go test ./internal/handler -run TestMUL6886 -count=1 -v
// Contract decisions for Step ① (no production code edit):
//   - Retry: intentional exclusion. ReanchorNextQueued keeps chat_input_task_id=id so
//     retry children (chat_input_task_id != id) are never the reanchor target. A
//     deferred A settlement must NOT move B's input when B is represented by a
//     running retry B2 whose owner is root B (same physical row). This test pins
//     that exclusion as GREEN baseline (and keeps it green after fix) to avoid
//     broad terminal-history rewrites. Owner-based precise targeting
//     (active head's chat_input_task_id) would be the alternative but is NOT chosen
//     here — would make retry another red baseline and is more invasive.
//   - Cursor: only STABLE FINAL SNAPSHOT is contract. After settlement, a fresh
//     pagination from empty cursor with small pages must reconstruct the same
//     order as ListChatMessages without duplicate/skip. cursor continuity across
//     settlement (reuse of a cursor obtained before settlement) is NOT supported;
//     if that were required, mutable created_at would need a data-model redesign
//     and this fix would be NO-GO. #6431's existing cancel tests do not assert the
//     latter, so we explicitly choose the former and pin it.
//   - Baseline colors: ONLY claimed follow-up is RED baseline (full list and its
//     pagination-correctness variant). Queued control, terminal negative, channel
//     isolation, retry exclusion, and pagination stability are GREEN baseline
//     boundary tests that must stay green after fix.

// TestMUL6886_DeferredCancelClaimedFollowUp_Repro is the deterministic
// regression required by the decision-gate. It reproduces the exact race
// left explicit in #7818.
//
// user A task A starts, A is cancelled (deferred), user B task B is queued
// and then CLAIMED, A's deferred assistant outcome settles late. Current
// main's ReanchorNextQueuedDirectChatInput only moves B while B is queued,
// so the persisted transcript becomes user A -> user B -> assistant A
// instead of user A -> assistant A -> user B.
// The test must FAIL on current main (showing the bug) and PASS after the
// minimal reanchor/settlement fix.
func TestMUL6886_DeferredCancelClaimedFollowUp_Repro(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database not available")
	}
	ctx := context.Background()
	agentID, sessionID, runtimeID, daemonID := setupDirectChatSession(t, ctx, "MUL-6886 repro")

	tA := sendDirectChat(t, ctx, agentID, sessionID, "user A")
	markTaskRunning(t, ctx, tA)

	if _, err := testHandler.TaskService.CancelTaskWithResult(ctx, parseUUID(tA), service.CancelTaskOptions{ClientSupportsDraftRestore: true}); err != nil {
		t.Fatalf("cancel task A: %v", err)
	}
	var deferredAt *string
	if err := testPool.QueryRow(ctx, `SELECT chat_finalize_deferred_at::text FROM agent_task_queue WHERE id = $1`, tA).Scan(&deferredAt); err != nil {
		t.Fatalf("read deferred marker: %v", err)
	}
	if deferredAt == nil {
		t.Fatalf("chat_finalize_deferred_at should be set after cancelling a started empty task")
	}

	tB := sendDirectChat(t, ctx, agentID, sessionID, "user B")
	var owner string
	if err := testPool.QueryRow(ctx, `SELECT chat_input_task_id::text FROM agent_task_queue WHERE id = $1`, tB).Scan(&owner); err != nil {
		t.Fatalf("read B owner: %v", err)
	}
	if owner != tB {
		t.Fatalf("B input owner = %s, want %s", owner, tB)
	}

	claimed := claimTaskForRuntimeGuard(t, runtimeID, daemonID)
	{
		var status string
		if err := testPool.QueryRow(ctx, `SELECT status FROM agent_task_queue WHERE id = $1`, tB).Scan(&status); err != nil {
			t.Fatalf("read B status after guard claim: %v", err)
		}
		if status == "queued" {
			t.Logf("B still queued after guard claim (%q), trying direct TaskService.ClaimTask; claimed=%+v", status, claimed)
			direct, err := testHandler.TaskService.ClaimTask(ctx, parseUUID(agentID))
			if err != nil {
				t.Fatalf("direct claim B: %v", err)
			}
			if direct == nil {
				t.Fatalf("claim B returned nil: B not claimable (status %q)", status)
			}
			if uuidToString(direct.ID) != tB {
				t.Logf("direct claim returned task %s, want B %s (checking B status)", uuidToString(direct.ID), tB)
			}
			if err := testPool.QueryRow(ctx, `SELECT status FROM agent_task_queue WHERE id = $1`, tB).Scan(&status); err != nil {
				t.Fatalf("read B status after direct claim: %v", err)
			}
			if status == "queued" {
				t.Fatalf("B still queued after both claim paths, status=%q", status)
			}
			t.Logf("B claimed via direct service, status=%q", status)
		} else {
			t.Logf("B claimed via runtime guard, status=%q chat_message=%q", status, claimed.ChatMessage)
			if claimed.ChatMessage != "user B" {
				t.Fatalf("claimed chat_message = %q, want user B (got B via dispatch)", claimed.ChatMessage)
			}
		}
	}

	var bStatus string
	if err := testPool.QueryRow(ctx, `SELECT status FROM agent_task_queue WHERE id = $1`, tB).Scan(&bStatus); err != nil {
		t.Fatalf("read B status: %v", err)
	}
	if bStatus == "queued" {
		t.Fatalf("B must be dispatched/running after claim, got queued")
	}
	if bStatus == "completed" || bStatus == "failed" || bStatus == "cancelled" {
		t.Fatalf("B must not be terminal before A settles, got %q", bStatus)
	}
	t.Logf("B status before A settle: %q", bStatus)

	var bCreatedBefore string
	if err := testPool.QueryRow(ctx, `SELECT created_at::text FROM chat_message WHERE task_id = $1 AND role='user'`, tB).Scan(&bCreatedBefore); err != nil {
		t.Fatalf("read B input created_at before settle: %v", err)
	}

	insertTaskTranscriptRow(t, ctx, tA)

	changed := testHandler.TaskService.FinalizeDeferredCancelledChat(ctx, parseUUID(tA))
	if !changed {
		t.Fatalf("FinalizeDeferredCancelledChat should have claimed deferred marker")
	}

	transcript, err := testHandler.Queries.ListChatMessages(ctx, parseUUID(sessionID))
	if err != nil {
		t.Fatalf("list chat messages: %v", err)
	}
	t.Logf("TRANSCRIPT actual: %v", msgContents(transcript))
	for i, m := range transcript {
		t.Logf("  [%d] role=%s content=%q created_at=%v id=%s task=%s", i, m.Role, m.Content, m.CreatedAt.Time, uuidToString(m.ID), uuidToString(m.TaskID))
	}
	var aAssistantCreated string
	if err := testPool.QueryRow(ctx, `SELECT created_at::text FROM chat_message WHERE task_id=$1 AND role='assistant'`, tA).Scan(&aAssistantCreated); err != nil {
		t.Fatalf("read assistant A created_at: %v", err)
	}
	var bUserCreatedAfter string
	if err := testPool.QueryRow(ctx, `SELECT created_at::text FROM chat_message WHERE task_id=$1 AND role='user'`, tB).Scan(&bUserCreatedAfter); err != nil {
		t.Fatalf("read B user created_at after settle: %v", err)
	}
	t.Logf("B input before settle: %s", bCreatedBefore)
	t.Logf("assistant A after settle: %s", aAssistantCreated)
	t.Logf("B input after settle: %s", bUserCreatedAfter)

	if len(transcript) != 3 {
		t.Fatalf("transcript len = %d, want 3 (user A, assistant A, user B)", len(transcript))
	}
	if transcript[0].Content != "user A" {
		t.Fatalf("transcript[0] = %q, want user A", transcript[0].Content)
	}
	// This assertion SHOULD fail on current main (proving the bug)
	if transcript[1].Content != "Stopped." && transcript[1].Content != "assistant A" {
		t.Fatalf("transcript[1] = %q, want assistant A / Stopped. (user A -> assistant A -> user B)", transcript[1].Content)
	}
	if transcript[2].Content != "user B" {
		t.Fatalf("transcript[2] = %q, want user B; actual order is %q (bug: user B before assistant A)", transcript[2].Content, msgContents(transcript))
	}
	if transcript[1].Content == "user B" {
		t.Fatalf("BUG REPRODUCED: transcript = user A -> user B -> assistant A (user B at %s before assistant A at %s)", bUserCreatedAfter, aAssistantCreated)
	}

	// Cursor: stable final snapshot must reconstruct the same order as full list
	// without duplicate/skip (performed entirely AFTER settlement, fresh cursor).
	// On current main this will be stable but wrong order (same bug) — we verify
	// stability here; correctness of order is already asserted above.
	var want = []string{"user A", "Stopped.", "user B"}
	var paged []string
	seen := map[string]bool{}
	var before *ChatMessagesCursorResponse
	for {
		params := url.Values{"limit": {"1"}}
		if before != nil {
			params.Set("before_created_at", before.CreatedAt)
			params.Set("before_id", before.ID)
		}
		page := fetchChatMessagesPageForTest(t, sessionID, params)
		for _, m := range page.Messages {
			if seen[m.ID] {
				t.Fatalf("cursor pagination returned duplicate %s", m.ID)
			}
			seen[m.ID] = true
			paged = append([]string{m.Content}, paged...)
		}
		if !page.HasMore {
			break
		}
		if page.NextCursor == nil {
			t.Fatal("page has_more without next_cursor")
		}
		before = page.NextCursor
	}
	t.Logf("PAGINATED reconstructed (limit=1): %v", paged)
	if len(paged) != len(want) {
		t.Fatalf("paged len = %d, want %d", len(paged), len(want))
	}
	for i := range want {
		if paged[i] != want[i] {
			t.Fatalf("paged[%d]=%q want %q (full=%v)", i, paged[i], want[i], msgContents(transcript))
		}
	}
}

// TestMUL6886_DeferredCancelQueuedFollowUp_Control proves the ordinary queued
// case still orders correctly on current main (GREEN baseline) — the bug is
// isolated to the claimed successor window.
func TestMUL6886_DeferredCancelQueuedFollowUp_Control(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database not available")
	}
	ctx := context.Background()
	agentID, sessionID, _, _ := setupDirectChatSession(t, ctx, "MUL-6886 control queued")

	tA := sendDirectChat(t, ctx, agentID, sessionID, "user A")
	markTaskRunning(t, ctx, tA)
	if _, err := testHandler.TaskService.CancelTaskWithResult(ctx, parseUUID(tA), service.CancelTaskOptions{ClientSupportsDraftRestore: true}); err != nil {
		t.Fatalf("cancel A: %v", err)
	}
	tB := sendDirectChat(t, ctx, agentID, sessionID, "user B")

	insertTaskTranscriptRow(t, ctx, tA)
	testHandler.TaskService.FinalizeDeferredCancelledChat(ctx, parseUUID(tA))

	transcript, err := testHandler.Queries.ListChatMessages(ctx, parseUUID(sessionID))
	if err != nil {
		t.Fatalf("list messages: %v", err)
	}
	t.Logf("CONTROL transcript (queued B): %v", msgContents(transcript))
	assertChatTranscriptContents(t, transcript, []string{"user A", "Stopped.", "user B"})

	var status string
	if err := testPool.QueryRow(ctx, `SELECT status FROM agent_task_queue WHERE id=$1`, tB).Scan(&status); err != nil {
		t.Fatalf("read B status: %v", err)
	}
	if status != "queued" {
		t.Fatalf("control B status = %q, want queued", status)
	}
}

// TestMUL6886_RetryExclusion_GREENPins that retry-owned input is intentionally
// NOT reanchored. When B has a running retry B2 (chat_input_task_id = B), the
// next head is B2 (dispatched) but the physical user row is still task_id=B
// with chat_input_task_id != id for the head. The current ReanchorNextQueued
// filter chat_input_task_id=id excludes it, and the minimal fix will keep that
// exclusion to avoid moving terminal-history rows via the retry child. Baseline
// is GREEN (no move, no duplicate) and stays green after fix; owner-based
// targeting was considered and rejected as too broad for this patch.
func TestMUL6886_RetryExclusion_GREEN(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database not available")
	}
	ctx := context.Background()
	agentID, sessionID, _, _ := setupDirectChatSession(t, ctx, "MUL-6886 retry exclusion")

	tA := sendDirectChat(t, ctx, agentID, sessionID, "user A")
	markTaskRunning(t, ctx, tA)
	if _, err := testHandler.TaskService.CancelTaskWithResult(ctx, parseUUID(tA), service.CancelTaskOptions{ClientSupportsDraftRestore: true}); err != nil {
		t.Fatalf("cancel A: %v", err)
	}
	tB := sendDirectChat(t, ctx, agentID, sessionID, "user B")

	// Make B's original terminal then create retry B2 dispatched.
	// We use direct SQL to force B=failed and then CreateRetryTask to get B2.
	if _, err := testPool.Exec(ctx, `UPDATE agent_task_queue SET status='failed', failure_reason='agent_error', completed_at=now() WHERE id=$1`, tB); err != nil {
		t.Fatalf("fail B: %v", err)
	}
	retry, err := testHandler.Queries.CreateRetryTask(ctx, db.CreateRetryTaskParams{ID: parseUUID(tB)})
	if err != nil {
		t.Fatalf("create retry B2: %v", err)
	}
	if !retry.ChatInputTaskID.Valid || uuidToString(retry.ChatInputTaskID) != tB {
		t.Fatalf("retry ChatInputTaskID=%s want %s", uuidToString(retry.ChatInputTaskID), tB)
	}
	// Move retry to dispatched (simulating claim) so head is B2 dispatched.
	if _, err := testPool.Exec(ctx, `UPDATE agent_task_queue SET status='dispatched', dispatched_at=now() WHERE id=$1`, uuidToString(retry.ID)); err != nil {
		t.Fatalf("dispatch B2: %v", err)
	}
	// Verify head is B2 dispatched (not B failed)
	var headID, headStatus, headOwner string
	if err := testPool.QueryRow(ctx,
		`SELECT id::text, status, chat_input_task_id::text FROM agent_task_queue WHERE chat_session_id=$1 AND status IN ('queued','dispatched','running','waiting_local_directory','deferred') AND regenerate_quick_actions_for IS NULL ORDER BY CASE WHEN status IN ('dispatched','running','waiting_local_directory') THEN 0 WHEN status='deferred' THEN 1 ELSE 2 END, priority DESC, created_at ASC, id ASC LIMIT 1`,
		sessionID).Scan(&headID, &headStatus, &headOwner); err != nil {
		t.Fatalf("read head: %v", err)
	}
	t.Logf("head id=%s status=%s owner=%s want B2 %s", headID, headStatus, headOwner, uuidToString(retry.ID))
	if headID != uuidToString(retry.ID) {
		t.Fatalf("head should be retry B2 %s, got %s", uuidToString(retry.ID), headID)
	}

	insertTaskTranscriptRow(t, ctx, tA)
	beforeCreated := ""
	testPool.QueryRow(ctx, `SELECT created_at::text FROM chat_message WHERE task_id=$1 AND role='user'`, tB).Scan(&beforeCreated)
	testHandler.TaskService.FinalizeDeferredCancelledChat(ctx, parseUUID(tA))
	afterCreated := ""
	testPool.QueryRow(ctx, `SELECT created_at::text FROM chat_message WHERE task_id=$1 AND role='user'`, tB).Scan(&afterCreated)
	t.Logf("retry case: user B before=%s after=%s", beforeCreated, afterCreated)
	if beforeCreated != afterCreated {
		t.Fatalf("retry exclusion: user B created_at moved from %s to %s — would imply retry was reanchored via owner, but contract is to exclude retries", beforeCreated, afterCreated)
	}
	// Also prove ListChatInputMessages still returns same batch via owner.
	batch, err := testHandler.Queries.ListChatInputMessages(ctx, parseUUID(tB))
	if err != nil || len(batch) != 1 || batch[0].Content != "user B" {
		t.Fatalf("ListChatInputMessages via owner B: %+v err %v", batch, err)
	}
	batch2, err := testHandler.Queries.ListChatInputMessages(ctx, retry.ChatInputTaskID)
	if err != nil || len(batch2) != 1 || batch2[0].Content != "user B" {
		t.Fatalf("ListChatInputMessages via retry owner: %+v err %v", batch2, err)
	}
}

// TestMUL6886_CursorStableFinalSnapshot_GREEN verifies pagination stability
// for the QUEUED case as GREEN baseline. It reconstructs the final transcript
// via small pages entirely AFTER settlement (fresh cursor) and checks no
// duplicate/skip and correct order. Pre-settlement cursor continuity is NOT
// asserted here — that is the explicitly excluded contract (see header). If
// that continuity were required, this mutable created_at fix would be NO-GO.
func TestMUL6886_CursorStableFinalSnapshot_GREEN(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database not available")
	}
	ctx := context.Background()
	agentID, sessionID, _, _ := setupDirectChatSession(t, ctx, "MUL-6886 cursor stable final")

	tA := sendDirectChat(t, ctx, agentID, sessionID, "user A")
	markTaskRunning(t, ctx, tA)
	if _, err := testHandler.TaskService.CancelTaskWithResult(ctx, parseUUID(tA), service.CancelTaskOptions{ClientSupportsDraftRestore: true}); err != nil {
		t.Fatalf("cancel A: %v", err)
	}
	sendDirectChat(t, ctx, agentID, sessionID, "user B")
	insertTaskTranscriptRow(t, ctx, tA)
	testHandler.TaskService.FinalizeDeferredCancelledChat(ctx, parseUUID(tA))

	full, err := testHandler.Queries.ListChatMessages(ctx, parseUUID(sessionID))
	if err != nil {
		t.Fatalf("full list: %v", err)
	}
	var want []string
	for _, m := range full {
		want = append(want, m.Content)
	}
	t.Logf("FULL after settle: %v", want)

	// Fresh pagination with limit=1 reconstructs final state
	var paged []string
	seen := map[string]bool{}
	var before *ChatMessagesCursorResponse
	for {
		params := url.Values{"limit": {"1"}}
		if before != nil {
			params.Set("before_created_at", before.CreatedAt)
			params.Set("before_id", before.ID)
		}
		page := fetchChatMessagesPageForTest(t, sessionID, params)
		for _, m := range page.Messages {
			if seen[m.ID] {
				t.Fatalf("duplicate %s", m.ID)
			}
			seen[m.ID] = true
			paged = append([]string{m.Content}, paged...)
		}
		if !page.HasMore {
			break
		}
		if page.NextCursor == nil {
			t.Fatal("has_more without next_cursor")
		}
		before = page.NextCursor
	}
	t.Logf("PAGED reconstructed: %v", paged)
	assertChatTranscriptContents(t, full, want) // trivially true, but pins API
	if len(paged) != len(want) {
		t.Fatalf("paged len %d != full len %d", len(paged), len(want))
	}
	for i := range want {
		if paged[i] != want[i] {
			t.Fatalf("paged[%d]=%q want %q", i, paged[i], want[i])
		}
	}
}

// TestMUL6886_CompletedTerminalNegative_GREEN pins that a completed follow-up
// is NOT reanchored. If B already has assistant B before A settles, A's late
// settlement must not rewrite B's input position. Baseline GREEN (no move) and
// stays green after fix because predicate excludes completed/failed/cancelled.
func TestMUL6886_CompletedTerminalNegative_GREEN(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database not available")
	}
	ctx := context.Background()
	agentID, sessionID, runtimeID, daemonID := setupDirectChatSession(t, ctx, "MUL-6886 completed negative")

	tA := sendDirectChat(t, ctx, agentID, sessionID, "user A")
	markTaskRunning(t, ctx, tA)
	if _, err := testHandler.TaskService.CancelTaskWithResult(ctx, parseUUID(tA), service.CancelTaskOptions{ClientSupportsDraftRestore: true}); err != nil {
		t.Fatalf("cancel A: %v", err)
	}
	tB := sendDirectChat(t, ctx, agentID, sessionID, "user B")
	claimed := claimTaskForRuntimeGuard(t, runtimeID, daemonID)
	var bStatus string
	testPool.QueryRow(ctx, `SELECT status FROM agent_task_queue WHERE id=$1`, tB).Scan(&bStatus)
	if bStatus == "queued" {
		// fallback via direct claim if guard missed due to cache
		testHandler.TaskService.ClaimTask(ctx, parseUUID(agentID))
		testPool.QueryRow(ctx, `SELECT status FROM agent_task_queue WHERE id=$1`, tB).Scan(&bStatus)
	}
	t.Logf("B claimed status before complete: %s chat=%q", bStatus, claimed.ChatMessage)
	markTaskRunning(t, ctx, tB)
	if _, err := testHandler.TaskService.CompleteTask(ctx, parseUUID(tB), completeResult(t, "assistant B"), "", "", "", false, "", ""); err != nil {
		t.Fatalf("complete B: %v", err)
	}
	fullBefore, _ := testHandler.Queries.ListChatMessages(ctx, parseUUID(sessionID))
	t.Logf("before A settle (B completed): %v", msgContents(fullBefore))
	var bCreatedBefore string
	testPool.QueryRow(ctx, `SELECT created_at::text FROM chat_message WHERE task_id=$1 AND role='user'`, tB).Scan(&bCreatedBefore)

	insertTaskTranscriptRow(t, ctx, tA)
	testHandler.TaskService.FinalizeDeferredCancelledChat(ctx, parseUUID(tA))

	var bCreatedAfter string
	testPool.QueryRow(ctx, `SELECT created_at::text FROM chat_message WHERE task_id=$1 AND role='user'`, tB).Scan(&bCreatedAfter)
	t.Logf("B created before=%s after=%s", bCreatedBefore, bCreatedAfter)
	if bCreatedBefore != bCreatedAfter {
		t.Fatalf("completed B was reanchored: %s -> %s", bCreatedBefore, bCreatedAfter)
	}
	fullAfter, _ := testHandler.Queries.ListChatMessages(ctx, parseUUID(sessionID))
	t.Logf("after A settle: %v", msgContents(fullAfter))
	// Completed history must stay as-is: user A, user B, assistant B, Stopped.(A)
	// The key assertion is B's input did not move; order check is pinned to that.
	assertChatTranscriptContents(t, fullAfter, []string{"user A", "user B", "assistant B", "Stopped."})
}

// TestMUL6886_SynchronousCancel_GREEN verifies the synchronous cancel path
// (non-deferred, already has transcript) still orders correctly without deferral.
// Baseline GREEN.
func TestMUL6886_SynchronousCancel_GREEN(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database not available")
	}
	ctx := context.Background()
	agentID, sessionID, _, _ := setupDirectChatSession(t, ctx, "MUL-6886 sync cancel")

	tA := sendDirectChat(t, ctx, agentID, sessionID, "user A")
	markTaskRunning(t, ctx, tA)
	insertTaskTranscriptRow(t, ctx, tA)
	// Now cancel with non-empty transcript → synchronous Stopped.
	res, err := testHandler.TaskService.CancelTaskWithResult(ctx, parseUUID(tA), service.CancelTaskOptions{ClientSupportsDraftRestore: true})
	if err != nil {
		t.Fatalf("cancel A sync: %v", err)
	}
	if res.CancelledChatMessage != nil {
		t.Logf("sync cancel produced restore? %+v", res.CancelledChatMessage)
	}
	var deferred *string
	testPool.QueryRow(ctx, `SELECT chat_finalize_deferred_at::text FROM agent_task_queue WHERE id=$1`, tA).Scan(&deferred)
	if deferred != nil {
		t.Fatalf("sync cancel should not defer, got %s", *deferred)
	}
	tB := sendDirectChat(t, ctx, agentID, sessionID, "user B")
	transcript, _ := testHandler.Queries.ListChatMessages(ctx, parseUUID(sessionID))
	t.Logf("sync cancel transcript: %v", msgContents(transcript))
	assertChatTranscriptContents(t, transcript, []string{"user A", "Stopped.", "user B"})
	var bStatus string
	testPool.QueryRow(ctx, `SELECT status FROM agent_task_queue WHERE id=$1`, tB).Scan(&bStatus)
	if bStatus != "queued" {
		t.Fatalf("B should be queued after sync cancel, got %s", bStatus)
	}
}

// TestMUL6886_ChannelIsolation_GREEN verifies channel-ingested batches are NOT moved.
// Baseline GREEN.
func TestMUL6886_ChannelIsolation_GREEN(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database not available")
	}
	ctx := context.Background()
	agentID, sessionID, _, _ := setupDirectChatSession(t, ctx, "MUL-6886 channel isolation")

	tA := sendDirectChat(t, ctx, agentID, sessionID, "user A")
	markTaskRunning(t, ctx, tA)
	if _, err := testHandler.TaskService.CancelTaskWithResult(ctx, parseUUID(tA), service.CancelTaskOptions{ClientSupportsDraftRestore: true}); err != nil {
		t.Fatalf("cancel A: %v", err)
	}
	// Create channel B: direct SQL with channel_ingested=true, owned batch.
	var runtimeID string
	testPool.QueryRow(ctx, `SELECT runtime_id::text FROM agent WHERE id=$1`, agentID).Scan(&runtimeID)
	var tB string
	if err := testPool.QueryRow(ctx, `
		INSERT INTO agent_task_queue (agent_id, runtime_id, chat_session_id, status, priority, chat_input_task_id)
		VALUES ($1, $2, $3, 'queued', 2, gen_random_uuid())
		RETURNING id::text
	`, agentID, runtimeID, sessionID).Scan(&tB); err != nil {
		t.Fatalf("insert channel B task: %v", err)
	}
	testPool.Exec(ctx, `UPDATE agent_task_queue SET chat_input_task_id=id WHERE id=$1`, tB)
	var msgID string
	testPool.QueryRow(ctx, `
		INSERT INTO chat_message (chat_session_id, role, content, task_id, channel_ingested)
		VALUES ($1, 'user', 'user channel B', $2, TRUE)
		RETURNING id::text
	`, sessionID, tB).Scan(&msgID)
	var before string
	testPool.QueryRow(ctx, `SELECT created_at::text FROM chat_message WHERE id=$1`, msgID).Scan(&before)

	insertTaskTranscriptRow(t, ctx, tA)
	testHandler.TaskService.FinalizeDeferredCancelledChat(ctx, parseUUID(tA))

	var after string
	testPool.QueryRow(ctx, `SELECT created_at::text FROM chat_message WHERE id=$1`, msgID).Scan(&after)
	t.Logf("channel B before=%s after=%s", before, after)
	if before != after {
		t.Fatalf("channel batch was reanchored: %s -> %s", before, after)
	}
	transcript, _ := testHandler.Queries.ListChatMessages(ctx, parseUUID(sessionID))
	t.Logf("channel transcript: %v", msgContents(transcript))
	// Channel message is visible? For this fixture it's channel_ingested true but still in transcript?
	// The key is it was NOT moved, so we only check timestamp stability.
}

// TestMUL6886_SameSessionScope_GREEN verifies tenancy isolation: settling
// session S1's A must not move session S2's B. Baseline GREEN.
func TestMUL6886_SameSessionScope_GREEN(t *testing.T) {
	if testHandler == nil || testPool == nil {
		t.Skip("database not available")
	}
	ctx := context.Background()
	agentID, session1, runtimeID, daemonID := setupDirectChatSession(t, ctx, "MUL-6886 session scope S1")
	// Create second session for same agent
	var session2 string
	testPool.QueryRow(ctx, `
		INSERT INTO chat_session (workspace_id, agent_id, creator_id, title, explicitly_created_at)
		VALUES ($1, $2, $3, 'MUL-6886 S2', now())
		RETURNING id::text
	`, testWorkspaceID, agentID, testUserID).Scan(&session2)
	t.Cleanup(func() { testPool.Exec(context.Background(), `DELETE FROM chat_session WHERE id=$1`, session2) })

	// S1: deferred A
	s1A := sendDirectChat(t, ctx, agentID, session1, "S1 user A")
	markTaskRunning(t, ctx, s1A)
	testHandler.TaskService.CancelTaskWithResult(ctx, parseUUID(s1A), service.CancelTaskOptions{ClientSupportsDraftRestore: true})
	// S1 B queued then claimed
	s1B := sendDirectChat(t, ctx, agentID, session1, "S1 user B")
	claimTaskForRuntimeGuard(t, runtimeID, daemonID)
	var s1BStatus string
	testPool.QueryRow(ctx, `SELECT status FROM agent_task_queue WHERE id=$1`, s1B).Scan(&s1BStatus)
	if s1BStatus == "queued" {
		testHandler.TaskService.ClaimTask(ctx, parseUUID(agentID))
	}

	// S2: B in other session, queued
	sess2, _ := testHandler.Queries.GetChatSession(ctx, parseUUID(session2))
	ag, _ := testHandler.Queries.GetAgent(ctx, parseUUID(agentID))
	res, _ := testHandler.TaskService.SendDirectChatMessage(ctx, sess2, ag, parseUUID(testUserID), "S2 user B", nil, "member", parseUUID(testUserID))
	s2B := uuidToString(res.Task.ID)
	var s2BBefore string
	testPool.QueryRow(ctx, `SELECT created_at::text FROM chat_message WHERE task_id=$1`, s2B).Scan(&s2BBefore)

	insertTaskTranscriptRow(t, ctx, s1A)
	testHandler.TaskService.FinalizeDeferredCancelledChat(ctx, parseUUID(s1A))

	var s2BAfter string
	testPool.QueryRow(ctx, `SELECT created_at::text FROM chat_message WHERE task_id=$1`, s2B).Scan(&s2BAfter)
	t.Logf("S2 B before=%s after=%s", s2BBefore, s2BAfter)
	if s2BBefore != s2BAfter {
		t.Fatalf("cross-session move: S2 B moved %s -> %s due to S1 settlement", s2BBefore, s2BAfter)
	}
	// S1 should be correctly ordered now
	s1Transcript, _ := testHandler.Queries.ListChatMessages(ctx, parseUUID(session1))
	t.Logf("S1 transcript: %v", msgContents(s1Transcript))
	assertChatTranscriptContents(t, s1Transcript, []string{"S1 user A", "Stopped.", "S1 user B"})
	s2Transcript, _ := testHandler.Queries.ListChatMessages(ctx, parseUUID(session2))
	t.Logf("S2 transcript: %v", msgContents(s2Transcript))
	assertChatTranscriptContents(t, s2Transcript, []string{"S2 user B"})
}
