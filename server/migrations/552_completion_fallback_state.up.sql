-- Durable completion-fallback obligation state (GH #8719), companion to
-- 551_completion_fallback_comment. NULL means legacy or not-applicable and is
-- never a recovery obligation: only rows the new completion path explicitly
-- marks 'pending' are late-synthesis candidates, so pre-migration historical
-- completed runs can never be mistaken for new obligations. 'recorded' is set
-- atomically with the fallback id; 'settled' is terminal (delivered, invalid,
-- or not-needed) and leaves the bounded sweeper scans.
--
-- Nullable TEXT with no default is a catalog-only change (no rewrite). Bound
-- lock acquisition so the ALTER fails fast and retries on the next run
-- instead of queueing an ACCESS EXCLUSIVE lock in front of every task query.
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '10s';

ALTER TABLE agent_task_queue ADD COLUMN IF NOT EXISTS completion_fallback_state TEXT;
