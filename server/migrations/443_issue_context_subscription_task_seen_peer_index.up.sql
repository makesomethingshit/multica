CREATE INDEX CONCURRENTLY IF NOT EXISTS issue_context_subscription_task_seen_peer_idx
    ON issue_context_subscription_task_seen(peer_task_id);
