CREATE INDEX CONCURRENTLY IF NOT EXISTS peer_context_rebase_log_task_idx
    ON peer_context_rebase_log(task_id, seq);
