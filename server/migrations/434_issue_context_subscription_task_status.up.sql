CREATE TABLE issue_context_subscription_task_seen (
    task_id UUID NOT NULL,
    peer_task_id UUID NOT NULL,
    seen_revision BIGINT NOT NULL DEFAULT 0 CHECK (seen_revision >= 0),
    seen_task_status TEXT NOT NULL DEFAULT '',
    created_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    updated_at TIMESTAMPTZ NOT NULL DEFAULT now(),
    PRIMARY KEY (task_id, peer_task_id)
);
