{{ config(
    materialized = 'external',
    location     = "{{ var('marts_location') }}/fct_transactions.parquet",
    format       = 'parquet'
) }}

-- Grain: one row per transaction_ref.

select
    transaction_ref,
    merchant_id,
    customer_id,
    gateway,
    channel,
    bank_code,
    status,
    currency,
    failure_reason,
    amount_kobo,
    fee_kobo,
    amount_ngn_kobo,
    fee_ngn_kobo,
    amount_ngn_kobo - fee_ngn_kobo as net_ngn_kobo,
    fx_rate_to_ngn,
    created_at,
    updated_at,
    ingested_at,
    event_date,
    arrival_lag_seconds,
    is_success,
    is_reversed,
    is_settlement_eligible
from {{ ref('stg_transactions') }}
