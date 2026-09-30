-- An amount-matched settlement must carry zero outstanding balance. Partial and
-- short payments remain outstanding even though a settlement line exists.
select transaction_ref, outstanding_net_kobo
from {{ ref('mart_settlement_reconciliation') }}
where is_amount_matched
  and outstanding_net_kobo <> 0
