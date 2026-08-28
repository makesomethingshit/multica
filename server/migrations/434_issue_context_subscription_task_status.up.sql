CREATE TABLE issue_context_subscription_task_seen (
    task_id UUID NOT NULL REFERENCES agent_task_queue(id) ON DELETE CASCADE,
    peer_task_id UUID NOT NULL REFERENCES agent_task_queue(id) ON DELETE CASCADE,
    seen_revision BIGINT NOT NULL DEFAULT 0 CHECK (seen_revision >= 0),
    seen_task_status TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (task_id, peer_task_id)
);

CREATE INDEX issue_context_subscription_task_seen_peer_idx
    ON issue_context_subscription_task_seen(peer_task_id);
