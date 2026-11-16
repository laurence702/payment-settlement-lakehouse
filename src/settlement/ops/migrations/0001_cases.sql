-- Exception cases and the analyst audit trail. See docs/adr/0007.
--
-- Two writers, two sets of columns. The pipeline sync writes the system_*
-- columns. Analyst actions write analyst_status and decided_fingerprint. Neither
-- touches the other's columns, so a pipeline run cannot overwrite a decision.

CREATE SCHEMA IF NOT EXISTS ops;

CREATE TABLE ops.cases (
    case_id               text        PRIMARY KEY CHECK (case_id ~ '^[0-9a-f]{64}$'),
    display_id            text        NOT NULL UNIQUE,
    transaction_ref       text        NOT NULL UNIQUE,
    merchant_id           text        NOT NULL,
    channel               text        NOT NULL,
    gateway               text        NOT NULL,
    bank_code             text        NOT NULL,
    currency              text        NOT NULL,
    succeeded_date        date        NOT NULL,
    amount_ngn_kobo       bigint      NOT NULL CHECK (amount_ngn_kobo >= 0),
    expected_net_kobo     bigint      NOT NULL,
    settled_net_kobo      bigint,

    break_type            text        NOT NULL CHECK (break_type IN (
        'missing_settlement', 'partial_settlement', 'short_paid',
        'overpaid', 'reversed_after_settlement'
    )),
    exposure_kobo         bigint      NOT NULL CHECK (exposure_kobo >= 0),
    system_status         text        NOT NULL CHECK (system_status IN ('OPEN', 'CLEARED')),
    system_fingerprint    text        NOT NULL,

    analyst_status        text        CHECK (analyst_status IN (
        'MANUAL_REVIEW', 'ESCALATED', 'EXPECTED_DELAY', 'CLOSED'
    )),
    decided_fingerprint   text,

    first_seen_at         timestamptz NOT NULL DEFAULT now(),
    last_seen_at          timestamptz NOT NULL DEFAULT now(),
    updated_at            timestamptz NOT NULL DEFAULT now(),

    CONSTRAINT decision_has_fingerprint
        CHECK ((analyst_status IS NULL) = (decided_fingerprint IS NULL))
);

CREATE INDEX cases_queue_idx
    ON ops.cases (system_status, exposure_kobo DESC, succeeded_date);

CREATE TABLE ops.case_actions (
    action_id             bigint      GENERATED ALWAYS AS IDENTITY PRIMARY KEY,
    case_id               text        NOT NULL REFERENCES ops.cases (case_id),
    analyst               text        NOT NULL CHECK (length(btrim(analyst)) > 0),
    action                text        NOT NULL CHECK (action IN (
        'START_REVIEW', 'ESCALATE', 'MARK_EXPECTED_DELAY', 'CLOSE', 'REOPEN'
    )),
    notes                 text        NOT NULL DEFAULT '',
    idempotency_key       text        NOT NULL UNIQUE CHECK (length(idempotency_key) BETWEEN 8 AND 128),
    resulting_status      text,
    fingerprint_at_action text        NOT NULL,
    created_at            timestamptz NOT NULL DEFAULT now()
);

CREATE INDEX case_actions_case_idx ON ops.case_actions (case_id, action_id);

-- The audit trail is append-only. A trigger enforces it for every role,
-- including the owner, which a REVOKE would not.
CREATE FUNCTION ops.reject_audit_mutation() RETURNS trigger
LANGUAGE plpgsql AS $$
BEGIN
    RAISE EXCEPTION 'ops.case_actions is append-only';
END;
$$;

CREATE TRIGGER case_actions_append_only
    BEFORE UPDATE OR DELETE ON ops.case_actions
    FOR EACH ROW EXECUTE FUNCTION ops.reject_audit_mutation();

CREATE TRIGGER case_actions_no_truncate
    BEFORE TRUNCATE ON ops.case_actions
    FOR EACH STATEMENT EXECUTE FUNCTION ops.reject_audit_mutation();

-- What a person sees. The status is derived, never stored, so there is no second
-- copy of it to fall out of step. Mirrors lifecycle.effective_status and
-- lifecycle.is_drifted.
CREATE VIEW ops.v_cases_effective AS
SELECT
    c.*,
    COALESCE(c.analyst_status, c.system_status) AS effective_status,
    (c.analyst_status IN ('CLOSED', 'EXPECTED_DELAY')
        AND c.decided_fingerprint IS DISTINCT FROM c.system_fingerprint) AS drifted
FROM ops.cases AS c;
