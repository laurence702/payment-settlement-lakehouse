-- Fees can never exceed the amount charged, so net is never negative. Catches
-- the classic bug where a fee cap is applied to the wrong currency.
select transaction_ref, amount_ngn_kobo, fee_ngn_kobo, net_ngn_kobo
from {{ ref('fct_transactions') }}
where net_ngn_kobo < 0
   or fee_ngn_kobo < 0
   or amount_ngn_kobo < 0
