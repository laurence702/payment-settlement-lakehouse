-- The break flag, its type and its exposure must agree, and the classification
-- must not leak: a matched row is never a break, and a settlement that has
-- breached its SLA with no money received always is.
select transaction_ref, reconciliation_status, is_break, break_type, exposure_kobo
from {{ ref('mart_settlement_reconciliation') }}
where (is_break and break_type is null)
   or (not is_break and break_type is not null)
   or (not is_break and exposure_kobo <> 0)
   or (is_break and exposure_kobo < 0)
   or (reconciliation_status = 'matched' and is_break)
   or (reconciliation_status = 'missing_settlement'
       and reconciliation_bucket = 'breached_sla'
       and not is_break)
   or length(case_id) <> 64
