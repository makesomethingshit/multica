-- Durable retry obligations for completion-reconcile comments skipped on
-- transient source-task lookup failure (GH #8719). Each element records one
-- comment that would have been routed to the owning (completing) agent had
-- its source row been readable: {"comment_id", "is_leader", "squad_id"}.
-- The existing delegated-failure sweeper replays them: exact recorded
-- fallbacks and provably-gone sources are dropped, explicit replies are
-- routed through the normal mention enqueue. NULL (or an empty array) means
-- no retry is owed and never matches the sweeper scan.
--
-- Nullable JSONB with no default is a catalog-only change (no rewrite). Bound
-- lock acquisition so the ALTER fails fast and retries on the next run
-- instead of queueing an ACCESS EXCLUSIVE lock in front of every task query.
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '10s';

ALTER TABLE agent_task_queue ADD COLUMN IF NOT EXISTS completion_reconcile_retry_obligations JSONB;
