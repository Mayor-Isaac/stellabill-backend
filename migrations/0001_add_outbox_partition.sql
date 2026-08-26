-- Add partition column to outbox_events
ALTER TABLE outbox_events
    ADD COLUMN IF NOT EXISTS partition INTEGER NOT NULL DEFAULT 0;

-- Index for dispatcher queries
CREATE INDEX IF NOT EXISTS idx_outbox_events_partition_status
    ON outbox_events (partition, status, id);

-- The publisher progress table now needs partition awareness.
DROP TABLE IF EXISTS outbox_publisher_progress;
CREATE TABLE outbox_publisher_progress (
    publisher   VARCHAR(255) NOT NULL,
    partition   INTEGER NOT NULL DEFAULT 0,
    last_event_id UUID,
    updated_at  TIMESTAMPTZ NOT NULL DEFAULT NOW(),
    PRIMARY KEY (publisher, partition)
);
