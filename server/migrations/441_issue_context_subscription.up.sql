CREATE TABLE issue_context_subscription (
    task_id UUID NOT NULL REFERENCES agent_task_queue(id) ON DELETE CASCADE,
    peer_issue_id UUID NOT NULL REFERENCES issue(id) ON DELETE CASCADE,
    seen_revision BIGINT NOT NULL DEFAULT 0 CHECK (seen_revision >= 0),
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (task_id, peer_issue_id)
);

CREATE INDEX issue_context_subscription_peer_issue_idx
    ON issue_context_subscription(peer_issue_id);
