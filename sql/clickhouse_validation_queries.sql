-- Settlement Lakehouse ClickHouse Validation Queries
-- Run these in the ClickHouse Play UI at http://localhost:8123/play
-- or via: docker exec np_clickhouse clickhouse-client --user dataeng --password dataeng_local_only
--
-- NOTE: The UI syntax requires explicit column lists or SELECT *. Backtick-quoting
-- is not needed; just avoid the bare "select from table" form shown in the screenshot.

-- ─────────────────────────────────────────────────────────────────────────────
-- 0.  ORIENTATION — what tables exist and how many rows each has
-- ─────────────────────────────────────────────────────────────────────────────

SHOW TABLES FROM settlement;

SELECT
    table,
    formatReadableQuantity(total_rows) AS rows,
    formatReadableSize(total_bytes)    AS size
FROM system.tables
WHERE database = 'settlement'
ORDER BY table;


-- ─────────────────────────────────────────────────────────────────────────────
-- 1.  fct_transactions — understand the transaction population
-- ─────────────────────────────────────────────────────────────────────────────

-- 1a. Row count and status split
SELECT
    status,
    count()                              AS tx_count,
    round(count() * 100.0 / sum(count()) OVER (), 1) AS pct
FROM settlement.fct_transactions
GROUP BY status
ORDER BY tx_count DESC;

-- 1b. Settlement-eligible transactions by currency
SELECT
    currency,
    count()                   AS eligible_count,
    sum(amount_ngn_kobo) / 100 AS total_ngn  -- convert kobo → naira for readability
FROM settlement.fct_transactions
WHERE is_settlement_eligible
GROUP BY currency;

-- 1c. Merchant SLA distribution (T+1 vs T+2)
SELECT
    settlement_lag_days,
    count() AS merchants_tx_count
FROM settlement.fct_transactions
WHERE is_settlement_eligible
GROUP BY settlement_lag_days
ORDER BY settlement_lag_days;

-- 1d. Check grain: each transaction_ref must appear exactly once
SELECT
    count()                           AS total_rows,
    countDistinct(transaction_ref)    AS distinct_refs,
    total_rows - distinct_refs        AS duplicate_refs  -- must be 0
FROM settlement.fct_transactions;


-- ─────────────────────────────────────────────────────────────────────────────
-- 2.  fct_settlements — understand the settlement population
-- ─────────────────────────────────────────────────────────────────────────────

-- 2a. Row counts and payout batch sizes
SELECT
    count()                     AS settlement_lines,
    countDistinct(payout_id)    AS payout_batches,
    round(count() / countDistinct(payout_id), 1) AS avg_lines_per_batch
FROM settlement.fct_settlements;

-- 2b. Orphan settlements — settlement lines with no matching transaction
--     Non-zero here means an ingestion gap or upstream data-quality problem.
SELECT count() AS orphan_settlement_lines
FROM settlement.fct_settlements
WHERE is_orphan_settlement;

-- 2c. Settlement lag distribution (days from success to settlement)
SELECT
    settlement_lag_days,
    count() AS lines
FROM settlement.fct_settlements
WHERE settlement_lag_days IS NOT NULL
GROUP BY settlement_lag_days
ORDER BY settlement_lag_days;

-- 2d. Sample a payout batch to see multiple transactions in one batch
SELECT
    payout_id,
    count() AS lines_in_batch,
    sum(net_kobo) / 100 AS batch_net_ngn
FROM settlement.fct_settlements
GROUP BY payout_id
HAVING lines_in_batch > 1
ORDER BY lines_in_batch DESC
LIMIT 10;


-- ─────────────────────────────────────────────────────────────────────────────
-- 3.  mart_settlement_reconciliation — the core product
-- ─────────────────────────────────────────────────────────────────────────────

-- 3a. Reconciliation status breakdown — the headline number
SELECT
    reconciliation_status,
    count()                              AS row_count,
    round(count() * 100.0 / sum(count()) OVER (), 2) AS pct
FROM settlement.mart_settlement_reconciliation
GROUP BY reconciliation_status
ORDER BY row_count DESC;

-- 3b. Source freshness status
SELECT
    source_freshness_status,
    count() AS rows
FROM settlement.mart_settlement_reconciliation
GROUP BY source_freshness_status;

-- 3c. Amount-match check by currency
SELECT
    currency,
    countIf(is_amount_matched)         AS amount_matched,
    countIf(NOT is_amount_matched AND is_settled) AS amount_mismatched,
    sum(settlement_variance_kobo) / 100 AS total_variance_ngn
FROM settlement.mart_settlement_reconciliation
WHERE is_settled
GROUP BY currency;

-- 3d. The key business question the mart answers:
--     Which eligible charges have no settlement, how old, and what is outstanding?
SELECT
    transaction_ref,
    merchant_id,
    currency,
    expected_net_kobo / 100 AS expected_ngn,
    expected_settlement_date,
    days_since_success,
    reconciliation_status,
    reconciliation_bucket
FROM settlement.mart_settlement_reconciliation
WHERE reconciliation_status IN ('missing_settlement', 'unverified_source')
ORDER BY days_since_success DESC
LIMIT 20;

-- 3e. Short-paid and partial settlements — money came but not enough
SELECT
    transaction_ref,
    merchant_id,
    currency,
    expected_net_kobo / 100    AS expected_ngn,
    settled_net_kobo / 100     AS settled_ngn,
    settlement_variance_kobo / 100 AS variance_ngn,
    outstanding_net_kobo / 100 AS outstanding_ngn,
    reconciliation_status,
    payout_id
FROM settlement.mart_settlement_reconciliation
WHERE reconciliation_status IN ('short_paid', 'partial_settlement', 'overpaid')
ORDER BY abs(settlement_variance_kobo) DESC
LIMIT 20;

-- 3f. Reversed-after-settlement rows
SELECT
    transaction_ref,
    merchant_id,
    settled_at,
    succeeded_at,
    outstanding_net_kobo / 100 AS outstanding_ngn
FROM settlement.mart_settlement_reconciliation
WHERE reconciliation_status = 'reversed_after_settlement'
LIMIT 10;

-- 3g. SLA breach summary (legacy buckets)
SELECT
    reconciliation_bucket,
    count()                              AS rows,
    round(count() * 100.0 / sum(count()) OVER (), 2) AS pct
FROM settlement.mart_settlement_reconciliation
GROUP BY reconciliation_bucket
ORDER BY rows DESC;


-- ─────────────────────────────────────────────────────────────────────────────
-- 4.  CROSS-TABLE VALIDATION — prove the pipeline is internally consistent
-- ─────────────────────────────────────────────────────────────────────────────

-- 4a. Grain check: mart row count must equal eligible + reversed-with-settlement
--     If this returns non-zero, a load is stale.
SELECT
    (SELECT count() FROM settlement.mart_settlement_reconciliation)
    -
    (SELECT count() FROM settlement.fct_transactions
     WHERE is_settlement_eligible
        OR (is_reversed AND transaction_ref IN
            (SELECT transaction_ref FROM settlement.fct_settlements)))
    AS row_delta;  -- must be 0

-- 4b. Integrity: matched rows must have zero outstanding balance
SELECT count() AS violations
FROM settlement.mart_settlement_reconciliation
WHERE is_amount_matched AND outstanding_net_kobo != 0;  -- must be 0

-- 4c. Matched rows must not carry a variance
SELECT count() AS inconsistent_matched_rows
FROM settlement.mart_settlement_reconciliation
WHERE reconciliation_status = 'matched'
  AND (settlement_variance_kobo != 0
    OR outstanding_net_kobo != 0
    OR NOT is_amount_matched);  -- must be 0

-- 4d. Every settled row in the mart should have a matching settlement line
SELECT count() AS mart_settled_rows_without_settlement_line
FROM settlement.mart_settlement_reconciliation r
WHERE r.is_settled
  AND NOT EXISTS (
      SELECT 1 FROM settlement.fct_settlements s
      WHERE s.transaction_ref = r.transaction_ref
  );  -- should be 0


-- ─────────────────────────────────────────────────────────────────────────────
-- 5.  OPERATIONAL SPOT CHECKS
-- ─────────────────────────────────────────────────────────────────────────────

-- 5a. Total outstanding balance across all unsettled eligible charges
SELECT
    currency,
    sum(outstanding_net_kobo) / 100 AS total_outstanding_ngn,
    count()                          AS unsettled_tx_count
FROM settlement.mart_settlement_reconciliation
WHERE reconciliation_status IN ('missing_settlement', 'short_paid',
                                 'partial_settlement', 'unverified_source')
GROUP BY currency;

-- 5b. Per-merchant reconciliation health
SELECT
    merchant_id,
    countIf(reconciliation_status = 'matched')             AS matched,
    countIf(reconciliation_status = 'missing_settlement')  AS missing,
    countIf(reconciliation_status = 'short_paid')          AS short_paid,
    sum(outstanding_net_kobo) / 100                        AS total_outstanding_ngn
FROM settlement.mart_settlement_reconciliation
GROUP BY merchant_id
ORDER BY total_outstanding_ngn DESC
LIMIT 15;

-- 5c. Daily settlement throughput
SELECT
    toDate(settled_at)   AS settlement_day,
    count()              AS lines_settled,
    sum(net_kobo) / 100  AS total_settled_ngn
FROM settlement.fct_settlements
GROUP BY settlement_day
ORDER BY settlement_day;
