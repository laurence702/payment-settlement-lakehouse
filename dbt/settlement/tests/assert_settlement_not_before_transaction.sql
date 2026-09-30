-- Money cannot settle before the charge that produced it. A negative lag means
-- clocks or joins are wrong, and either way the aging buckets are lies.
select settlement_id, transaction_ref, settlement_lag_days
from {{ ref('fct_settlements') }}
where settlement_lag_days < 0
  and not is_orphan_settlement
