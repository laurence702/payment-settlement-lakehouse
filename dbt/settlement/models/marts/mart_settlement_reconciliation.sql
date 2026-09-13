{{ config(
    materialized = 'external',
    location     = "{{ var('marts_location') }}/mart_settlement_reconciliation.parquet",
    format       = 'parquet'
) }}

-- The model this whole pipeline exists to produce.
--
-- Question: which successful charges have not been settled, and how overdue
-- are they? In a real PSP integration this is the difference between "we are
-- owed money and nobody noticed" and "we caught it the next morning".
--
-- Reproducibility note: "today" is derived from the data (the latest
-- ingested_at), NOT from current_date. A mart whose output changes because you
-- reran it on a different day cannot be tested, and cannot be diffed against a
-- previous run to see what actually changed.

with as_of as (

    select max(ingested_at)::date as as_of_date
    from {{ ref('stg_transactions') }}

),

eligible as (

    select
        t.transaction_ref,
        t.merchant_id,
        t.channel,
        t.gateway,
        t.bank_code,
        t.currency,
        t.amount_ngn_kobo,
        t.fee_ngn_kobo,
        t.updated_at as succeeded_at,
        cast(t.updated_at as date) as succeeded_date
    from {{ ref('stg_transactions') }} t
    where t.is_settlement_eligible

),

joined as (

    select
        e.*,
        s.settlement_id,
        s.settlement_date,
        s.net_kobo as settled_net_kobo,
        a.as_of_date,
        date_diff('day', e.succeeded_date, a.as_of_date) as days_since_success
    from eligible e
    cross join as_of a
    left join {{ ref('stg_settlements') }} s
        on e.transaction_ref = s.transaction_ref

)

select
    transaction_ref,
    merchant_id,
    channel,
    gateway,
    bank_code,
    currency,
    amount_ngn_kobo,
    fee_ngn_kobo,
    succeeded_at,
    succeeded_date,
    settlement_id,
    settlement_date,
    settled_net_kobo,
    as_of_date,
    days_since_success,

    settlement_id is not null as is_settled,

    case
        when settlement_id is not null then 'settled'
        when days_since_success <= 1 then 'pending_t1'
        when days_since_success = 2  then 'pending_t2'
        when days_since_success <= {{ var('settlement_sla_days') }} then 'pending_within_sla'
        else 'breached_sla'
    end as reconciliation_bucket,

    -- Money still owed to the merchant. Zero once settled, so this column sums
    -- straight to total exposure without a filter.
    case
        when settlement_id is not null then 0
        else amount_ngn_kobo - fee_ngn_kobo
    end as outstanding_net_kobo,

    -- Did the settled amount match what we expected? A non-zero variance on a
    -- settled row is a pricing or FX bug, and is a different alert from a
    -- missing settlement.
    case
        when settlement_id is null then null
        else settled_net_kobo - (amount_ngn_kobo - fee_ngn_kobo)
    end as settlement_variance_kobo

from joined
