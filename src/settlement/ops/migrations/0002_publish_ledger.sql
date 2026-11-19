-- Chunks the generator has committed to Kafka. See docs/adr/0009.
--
-- One row per (run_key, chunk_start), written only after the chunk's Kafka
-- transaction committed. A retried generate task reads this table and skips the
-- chunks already listed, so it publishes only what the failed attempt did not.

CREATE SCHEMA IF NOT EXISTS pipeline;

CREATE TABLE pipeline.publish_ledger (
    run_key        text        NOT NULL,
    chunk_start    integer     NOT NULL CHECK (chunk_start >= 0),
    transactions   integer     NOT NULL CHECK (transactions >= 0),
    settlements    integer     NOT NULL CHECK (settlements >= 0),
    committed_at   timestamptz NOT NULL DEFAULT now(),
    PRIMARY KEY (run_key, chunk_start)
);
