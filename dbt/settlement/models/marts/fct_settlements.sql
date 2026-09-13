{{ config(
    materialized = 'external',
    location     = "{{ var('marts_location') }}/fct_settlements.parquet",
    format       = 'parquet'
) }}

-- Grain: one row per settlement_id.

select
    s.settlement_id,
    s.transaction_ref,
    s.merchant_id,
    s.gross_kobo,
    s.fee_kobo,
    s.net_kobo,
    s.currency,
    s.settlement_date,
    s.settled_at,
    t.created_at as transaction_created_at,
    t.channel,
    t.gateway,
    -- Whole days between the charge succeeding and the money settling. This is
    -- the number a merchant actually complains about.
    date_diff('day', cast(t.updated_at as date), s.settlement_date) as settlement_lag_days,
    -- A settlement whose transaction we have never seen. Non-zero here means
    -- either an ingestion gap or a genuine upstream data-quality problem, and
    -- the two need different responses.
    t.transaction_ref is null as is_orphan_settlement
from {{ ref('stg_settlements') }} s
left join {{ ref('stg_transactions') }} t
    on s.transaction_ref = t.transaction_ref
