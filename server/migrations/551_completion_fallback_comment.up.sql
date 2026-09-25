-- Record which comment a completed worker run synthesized as its completion
-- fallback (GH #8719), so completion reconcile and the sweeper replay can
-- identify it exactly instead of shape-matching every agent comment the run
-- authored (an explicit, possibly suppressed worker reply must never be
-- reclassified as a fallback).
--
-- No foreign key (repository rule). A hard-deleted fallback row simply stops
-- matching the sweeper join and the dispatch path treats the missing row as
-- permanently invalid lineage.
--
-- A nullable column with no default is a catalog-only change, so this does not
-- rewrite the table. Bound lock acquisition so the ALTER fails fast and retries
-- on the next run instead of queueing an ACCESS EXCLUSIVE lock in front of
-- every task query.
SET LOCAL lock_timeout = '2s';
SET LOCAL statement_timeout = '10s';

ALTER TABLE agent_task_queue ADD COLUMN IF NOT EXISTS completion_fallback_comment_id UUID;
