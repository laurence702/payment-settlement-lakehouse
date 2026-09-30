-- Replaces dbt_utils.accepted_range. Hand-written on purpose: pulling the whole
-- dbt_utils package for one bounds check would mean the Airflow image needs
-- `dbt deps`, and therefore outbound internet, on every DAG run. Ten lines of
-- SQL is cheaper than a network dependency in a scheduled job.
select 'mart_merchant_daily' as model, merchant_id as grain, success_rate
from {{ ref('mart_merchant_daily') }}
where success_rate is not null and (success_rate < 0 or success_rate > 1)

union all

select 'mart_channel_performance', channel, success_rate
from {{ ref('mart_channel_performance') }}
where success_rate is not null and (success_rate < 0 or success_rate > 1)
