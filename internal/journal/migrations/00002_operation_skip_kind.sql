-- +goose Up
-- heos:min-runtime=1

ALTER TABLE heos.operations DROP CONSTRAINT operations_kind_check;
ALTER TABLE heos.operations ADD CONSTRAINT operations_kind_check
    CHECK (kind IN ('alarm', 'playback', 'volume', 'mute', 'transport', 'skip', 'stop', 'cancel'));
