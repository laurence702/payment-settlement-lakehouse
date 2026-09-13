{{ config(
    materialized = 'external',
    location     = "{{ var('marts_location') }}/mart_channel_performance.parquet",
    format       = 'parquet'
) }}

-- Grain: one row per channel per bank per day.
-- Answers "is this a us problem or a bank problem", which is the first question
-- asked whenever a success rate drops.

select
    event_date,
    channel,
    coalesce(bank_code, 'n/a') as bank_code,
    gateway,
    count(*)                                    as attempts,
    count(*) filter (where is_success)          as successes,
    case when count(*) = 0 then null
         else round(count(*) filter (where is_success) * 1.0 / count(*), 4)
    end                                         as success_rate,
    sum(amount_ngn_kobo)                        as gross_attempted_kobo,

    -- Failure breakdown, so a drop can be attributed without another query.
    count(*) filter (where failure_reason = 'insufficient_funds') as fail_insufficient_funds,
    count(*) filter (where failure_reason = 'do_not_honour')      as fail_do_not_honour,
    count(*) filter (where failure_reason = 'invalid_pin')        as fail_invalid_pin,
    count(*) filter (where failure_reason = 'timeout')            as fail_timeout,
    count(*) filter (where failure_reason = 'limit_exceeded')     as fail_limit_exceeded,
    count(*) filter (where failure_reason = 'suspected_fraud')    as fail_suspected_fraud

from {{ ref('stg_transactions') }}
group by 1, 2, 3, 4
