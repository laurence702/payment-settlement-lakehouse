-- A USD transaction without an FX rate silently converts to zero naira, which
-- understates revenue instead of failing. Make it fail.
select transaction_ref, currency, fx_rate_to_ngn, amount_ngn_kobo
from {{ ref('fct_transactions') }}
where currency = 'USD'
  and (fx_rate_to_ngn is null or fx_rate_to_ngn <= 0)
