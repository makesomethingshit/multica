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
DELETE FROM issue_context_subscription
WHERE task_id = @task_id AND peer_issue_id = @peer_issue_id;

-- name: GetTaskPeerSnapshot :one
SELECT i.revision AS issue_revision, atq.status AS task_status
FROM agent_task_queue atq
JOIN issue i ON i.id = atq.issue_id
WHERE atq.id = @task_id;

-- name: MarkIssueContextSubscriptionSeen :one
INSERT INTO issue_context_subscription (task_id, peer_issue_id, seen_revision, seen_task_status)
SELECT @task_id, @peer_issue_id, @seen_revision, @seen_task_status
FROM issue i
JOIN agent_task_queue source_task ON source_task.id = @task_id
JOIN issue source_issue ON source_issue.id = source_task.issue_id
WHERE i.id = @peer_issue_id
  AND source_issue.workspace_id = i.workspace_id
  AND source_issue.parent_issue_id IS NOT NULL
  AND source_issue.parent_issue_id = i.parent_issue_id
ON CONFLICT (task_id, peer_issue_id) DO UPDATE
SET seen_revision = GREATEST(issue_context_subscription.seen_revision, EXCLUDED.seen_revision),
    seen_task_status = CASE
        WHEN EXCLUDED.seen_revision >= issue_context_subscription.seen_revision
        THEN EXCLUDED.seen_task_status
        ELSE issue_context_subscription.seen_task_status
    END,
    updated_at = now()
RETURNING *;
