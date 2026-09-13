{{ config(
    materialized = 'external',
    location     = "{{ var('marts_location') }}/mart_merchant_daily.parquet",
    format       = 'parquet'
) }}

-- Grain: one row per merchant per day.

select
    merchant_id,
    event_date,
    count(*)                                            as attempts,
    count(*) filter (where is_success)                  as successes,
    count(*) filter (where status = 'failed')           as failures,
    count(*) filter (where is_reversed)                 as reversals,

    -- Guarded division: a merchant with zero attempts on a day should not make
    -- the whole model fail, and should not silently become 0% either.
    case when count(*) = 0 then null
         else round(count(*) filter (where is_success) * 1.0 / count(*), 4)
    end                                                 as success_rate,

    sum(amount_ngn_kobo)                                as gross_attempted_kobo,
    sum(amount_ngn_kobo) filter (where is_success)      as gross_successful_kobo,
    sum(fee_ngn_kobo)    filter (where is_success)      as fees_kobo,
    sum(amount_ngn_kobo - fee_ngn_kobo)
        filter (where is_success)                       as net_successful_kobo,

    count(distinct customer_id)                         as unique_customers,
    round(avg(arrival_lag_seconds), 1)                  as avg_arrival_lag_seconds,
    max(arrival_lag_seconds)                            as max_arrival_lag_seconds

from {{ ref('stg_transactions') }}
group by 1, 2
