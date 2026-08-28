-- name: CreateIssueContextSubscription :one
INSERT INTO issue_context_subscription (task_id, peer_issue_id, seen_revision)
SELECT @task_id, @peer_issue_id, 0
WHERE EXISTS (
    SELECT 1
    FROM agent_task_queue source_task
    JOIN issue source_issue ON source_issue.id = source_task.issue_id
    JOIN issue peer_issue ON peer_issue.id = @peer_issue_id
    WHERE source_task.id = @task_id
      AND source_issue.workspace_id = peer_issue.workspace_id
      AND source_issue.parent_issue_id IS NOT NULL
      AND source_issue.parent_issue_id = peer_issue.parent_issue_id
)
ON CONFLICT (task_id, peer_issue_id) DO UPDATE
SET updated_at = now()
RETURNING *;

-- name: GetIssueContextSubscription :one
SELECT * FROM issue_context_subscription
WHERE task_id = @task_id AND peer_issue_id = @peer_issue_id;

-- name: ListIssueContextSubscriptions :many
SELECT s.*
FROM issue_context_subscription s
JOIN issue i ON i.id = s.peer_issue_id
WHERE s.task_id = @task_id
ORDER BY s.created_at, s.peer_issue_id;

-- name: DeleteIssueContextSubscription :execrows
WITH deleted_seen AS (
    DELETE FROM issue_context_subscription_task_seen seen
    USING agent_task_queue peer_task
    WHERE seen.task_id = @task_id
      AND seen.peer_task_id = peer_task.id
      AND peer_task.issue_id = @peer_issue_id
)
DELETE FROM issue_context_subscription subscription
WHERE subscription.task_id = @task_id AND subscription.peer_issue_id = @peer_issue_id;

-- name: GetTaskPeerSnapshot :one
SELECT i.revision AS issue_revision, atq.status AS task_status
FROM agent_task_queue atq
JOIN issue i ON i.id = atq.issue_id
WHERE atq.id = @task_id;

-- name: GetIssueContextSubscriptionTaskSeen :one
SELECT task_id, peer_task_id, seen_revision, seen_task_status, created_at, updated_at
FROM issue_context_subscription_task_seen
WHERE task_id = @task_id AND peer_task_id = @peer_task_id;

-- name: LockPeerContextRebaseWriter :exec
-- Serialize the seen-cursor advance + audit-marker insert per source task so
-- two concurrent peer lookups cannot allocate the same marker seq.
SELECT pg_advisory_xact_lock(hashtextextended(@task_id::text || ':peer_context_rebase', 0));

-- name: CreatePeerContextRebaseMessage :one
-- Record the first-class rebase marker in the source task's execution log.
-- The lookup that triggered this write is authenticated and has already
-- proved the peer belongs to the same parent/workspace.
--
-- seq lives in a daemon-disjoint negative space counting down from
-- MIN(seq)-1: the daemon numbers its messages with a local positive counter
-- and never reads server rows, so MAX(seq)+1 would collide with the daemon's
-- next real message and the frontend's seq dedupe would drop one of the two.
-- Negative seqs also stay below every --since cursor, which tracks the
-- daemon's max seen seq, so markers can never poison incremental polls.
-- Callers hold LockPeerContextRebaseWriter, so concurrent markers on the same
-- task cannot allocate the same value.
INSERT INTO task_message (task_id, seq, type, tool, content)
VALUES (
    @task_id,
    COALESCE((SELECT MIN(seq) FROM task_message WHERE task_id = @task_id), 0) - 1,
    'peer_context_rebase',
    'peer-context',
    @content
)
RETURNING *;

-- name: MarkIssueContextSubscriptionSeen :one
WITH valid_peer AS (
    SELECT peer_task.issue_id
    FROM agent_task_queue source_task
    JOIN issue source_issue ON source_issue.id = source_task.issue_id
    JOIN agent_task_queue peer_task ON peer_task.id = @peer_task_id
    JOIN issue peer_issue ON peer_issue.id = peer_task.issue_id
    WHERE source_task.id = @task_id
      AND source_issue.workspace_id = peer_issue.workspace_id
      AND source_issue.parent_issue_id IS NOT NULL
      AND source_issue.parent_issue_id = peer_issue.parent_issue_id
), seen AS (
    INSERT INTO issue_context_subscription_task_seen (task_id, peer_task_id, seen_revision, seen_task_status)
    SELECT @task_id, @peer_task_id, @seen_revision, @seen_task_status
    FROM valid_peer
    ON CONFLICT (task_id, peer_task_id) DO UPDATE
    SET seen_revision = GREATEST(issue_context_subscription_task_seen.seen_revision, EXCLUDED.seen_revision),
        seen_task_status = CASE
            WHEN EXCLUDED.seen_revision >= issue_context_subscription_task_seen.seen_revision
            THEN EXCLUDED.seen_task_status
            ELSE issue_context_subscription_task_seen.seen_task_status
        END,
        updated_at = now()
    RETURNING *
), subscription AS (
    INSERT INTO issue_context_subscription (task_id, peer_issue_id, seen_revision)
    SELECT @task_id, issue_id, @seen_revision
    FROM valid_peer
    ON CONFLICT (task_id, peer_issue_id) DO UPDATE
    SET seen_revision = GREATEST(issue_context_subscription.seen_revision, EXCLUDED.seen_revision),
        updated_at = now()
)
SELECT * FROM seen;
