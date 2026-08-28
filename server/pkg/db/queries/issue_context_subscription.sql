-- name: CreateIssueContextSubscription :one
-- Atomic admission (PUCK-58 review): the per-task cap is enforced inside the
-- statement so concurrent creates cannot race past it, and an existing peer
-- remains updatable even at the cap. task_lock serializes concurrent
-- admissions for the same task via row-level lock on the parent task.
WITH task_lock AS (
  SELECT 1 FROM agent_task_queue WHERE id = @task_id FOR UPDATE
)
INSERT INTO issue_context_subscription (task_id, peer_issue_id, seen_revision)
SELECT @task_id, @peer_issue_id, 0
FROM task_lock
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
AND (
  EXISTS (SELECT 1 FROM issue_context_subscription WHERE task_id = @task_id AND peer_issue_id = @peer_issue_id)
  OR (SELECT COUNT(*) FROM issue_context_subscription WHERE task_id = @task_id) < 32
)
ON CONFLICT (task_id, peer_issue_id) DO UPDATE
SET updated_at = now()
RETURNING *;

-- name: GetIssueContextSubscription :one
SELECT * FROM issue_context_subscription
WHERE task_id = @task_id AND peer_issue_id = @peer_issue_id;

-- name: CountIssueContextSubscriptions :one
SELECT COUNT(*)::bigint FROM issue_context_subscription WHERE task_id = @task_id;

-- name: ValidatePeerIssueForSubscription :one
SELECT EXISTS (
    SELECT 1
    FROM agent_task_queue source_task
    JOIN issue source_issue ON source_issue.id = source_task.issue_id
    JOIN issue peer_issue ON peer_issue.id = @peer_issue_id
    WHERE source_task.id = @task_id
      AND source_issue.workspace_id = peer_issue.workspace_id
      AND source_issue.parent_issue_id IS NOT NULL
      AND source_issue.parent_issue_id = peer_issue.parent_issue_id
) AS valid;

-- name: ListIssueContextSubscriptions :many
-- The JOIN carries the peer issue's current revision in the same read so the
-- handler does not need a per-row GetIssue round trip (PUCK-58 review).
SELECT s.*, i.revision
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

-- name: CreatePeerContextRebaseLog :one
-- Append the rebase event to the source task's dedicated audit log. The
-- daemon-owned task_message seq space stays untouched (the daemon numbers
-- messages with a local counter the server never sees, so any server-side
-- task_message insert would either collide or sort out of order), and the
-- identity seq gives the trail a true occurrence order. No FK: task
-- teardown deletes these rows explicitly (DeleteUnstartedQuickCreateRetryTask,
-- DeleteTaskBatch), matching the issue_context_subscription_task_seen
-- cleanup pattern.
INSERT INTO peer_context_rebase_log (task_id, peer_task_id, from_revision, to_revision, from_status, to_status)
VALUES (@task_id, @peer_task_id, @from_revision, @to_revision, @from_status, @to_status)
RETURNING *;

-- name: ListPeerContextRebaseLog :many
SELECT * FROM peer_context_rebase_log
WHERE task_id = @task_id
ORDER BY seq ASC;

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
), task_lock AS (
  SELECT 1 FROM agent_task_queue WHERE id = @task_id FOR UPDATE
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
    FROM valid_peer, task_lock
    WHERE EXISTS (SELECT 1 FROM issue_context_subscription WHERE task_id = @task_id AND peer_issue_id = valid_peer.issue_id)
       OR (SELECT COUNT(*) FROM issue_context_subscription WHERE task_id = @task_id) < 32
    ON CONFLICT (task_id, peer_issue_id) DO UPDATE
    SET seen_revision = GREATEST(issue_context_subscription.seen_revision, EXCLUDED.seen_revision),
        updated_at = now()
)
SELECT * FROM seen;
