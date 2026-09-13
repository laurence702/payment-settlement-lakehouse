-- A settled transaction must carry zero outstanding balance. If this fails,
-- total exposure is being overstated and someone will chase money that already
-- arrived.
select transaction_ref, outstanding_net_kobo
from {{ ref('mart_settlement_reconciliation') }}
where is_settled
  and outstanding_net_kobo <> 0
