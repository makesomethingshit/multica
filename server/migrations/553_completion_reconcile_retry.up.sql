-- Durable retry obligations for completion-reconcile comments skipped on
-- transient source-task lookup failure (GH #8719). Each element records the
-- IDENTITY of one comment that still has to be reconsidered for the owning
-- (completing) agent: {"comment_id"}. Nothing else is durable here by design:
-- the element is an obligation, not a cached authorization or routing
-- decision, so source lineage, invocation permission, squad leadership and
-- readiness are all re-proven from current state when the existing runtime
-- sweep replays it. Exact recorded fallbacks and provably-gone sources are
-- dropped at replay; an explicit mention whose target is still admissible is
-- routed through the normal comment enqueue. NULL (or an empty array) means
-- no retry is owed and never matches the sweep scan.
--
-- Nullable JSONB with no default is a catalog-only change (no rewrite). Bound
-- lock acquisition so the ALTER fails fast and retries on the next run
-- instead of queueing an ACCESS EXCLUSIVE lock in front of every task query.
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '10s';

ALTER TABLE agent_task_queue ADD COLUMN IF NOT EXISTS completion_reconcile_retry_obligations JSONB;
