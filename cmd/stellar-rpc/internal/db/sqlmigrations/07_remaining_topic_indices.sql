-- +migrate Up

-- Add indexes for topic2, topic3, and topic4 to support efficient filtering
-- on any topic position. These indexes use (topic, id) composite structure
-- to enable efficient range queries when combined with cursor-based pagination.
CREATE INDEX idx_id_topic2 ON events (topic2, id);
CREATE INDEX idx_id_topic3 ON events (topic3, id);
CREATE INDEX idx_id_topic4 ON events (topic4, id);

-- +migrate Down
DROP INDEX idx_id_topic2;
DROP INDEX idx_id_topic3;
DROP INDEX idx_id_topic4;
