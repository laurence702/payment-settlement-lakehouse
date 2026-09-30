{{ config(
    materialized = 'external',
    location     = "{{ var('marts_location') }}/mart_settlement_reconciliation.parquet",
    format       = 'parquet'
) }}

-- Grain: one successful charge, plus a reversed charge with a prior settlement.
-- The as-of date is derived from staged input so identical input is reproducible.

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
        t.successful_at as succeeded_at,
        cast(t.successful_at as date) as succeeded_date,
        t.expected_settlement_date,
        t.settlement_source_watermark_at,
        t.status
    from {{ ref('stg_transactions') }} t
    where t.is_settlement_eligible
       or (
           t.is_reversed
           and exists (
               select 1
               from {{ ref('stg_settlements') }} s
               where s.transaction_ref = t.transaction_ref
           )
       )

),

joined as (

    select
        e.*,
        s.settlement_id,
        s.payout_id,
        s.settlement_date,
        s.settled_at,
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
    expected_settlement_date,
    settlement_source_watermark_at,
    settlement_id,
    payout_id,
    settlement_date,
    settled_at,
    settled_net_kobo,
    as_of_date,
    days_since_success,

    settlement_id is not null as is_settled,
    settlement_id is not null
        and settled_net_kobo = amount_ngn_kobo - fee_ngn_kobo as is_amount_matched,
    amount_ngn_kobo - fee_ngn_kobo as expected_net_kobo,
    case
        when settlement_id is null then null
        else settled_net_kobo - (amount_ngn_kobo - fee_ngn_kobo)
    end as settlement_variance_kobo,

    case
        when settlement_source_watermark_at::date >= expected_settlement_date then 'fresh'
        else 'stale_or_incomplete'
    end as source_freshness_status,

    case
        when status = 'reversed' and settlement_id is not null and settled_at < succeeded_at
            then 'reversed_after_settlement'
        when settlement_id is null
             and settlement_source_watermark_at::date < expected_settlement_date
            then 'unverified_source'
        when settlement_id is null then 'missing_settlement'
        when settled_net_kobo = amount_ngn_kobo - fee_ngn_kobo then 'matched'
        when settled_net_kobo < (amount_ngn_kobo - fee_ngn_kobo) * 9 / 10 then 'partial_settlement'
        when settled_net_kobo < amount_ngn_kobo - fee_ngn_kobo then 'short_paid'
        else 'overpaid'
    end as reconciliation_status,

    case
        when settlement_id is not null then 'settled'
        when settlement_source_watermark_at::date < expected_settlement_date then 'pending_within_sla'
        when days_since_success <= 1 then 'pending_t1'
        when days_since_success = 2  then 'pending_t2'
        when as_of_date <= expected_settlement_date then 'pending_within_sla'
        else 'breached_sla'
    end as reconciliation_bucket,

    case
        when settlement_id is null then amount_ngn_kobo - fee_ngn_kobo
        when status = 'reversed' and settled_at < succeeded_at then 0
        else greatest(0, amount_ngn_kobo - fee_ngn_kobo - settled_net_kobo)
    end as outstanding_net_kobo

from joined
