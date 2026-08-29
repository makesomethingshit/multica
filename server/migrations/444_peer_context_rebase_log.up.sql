CREATE TABLE peer_context_rebase_log (
    seq BIGINT GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    task_id UUID NOT NULL,
    peer_task_id UUID NOT NULL,
    from_revision BIGINT NOT NULL DEFAULT 0,
    to_revision BIGINT NOT NULL,
    from_status TEXT NOT NULL DEFAULT '',
    to_status TEXT NOT NULL,
    created_at TIMESTAMPTZ NOT NULL DEFAULT now()
);
