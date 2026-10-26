-- Assertion: in int_customer_mrr_movements, for every customer and month,
--   last_month_mrr_cents + movement_amount_cents = mrr_cents
-- i.e. the classified movement fully explains the change in MRR from the previous month.
--
-- Why it matters: the monthly MRR bridge sums movement_amount_cents by type. If a movement amount
-- is wrong (for example churn recorded as 0 instead of minus the previous MRR), the bridge no longer
-- adds up, and nothing else would flag it.
--
-- The test passes when the query returns zero rows. Each returned row is a customer-month
-- where the movement does not explain the change in MRR.
select 
    *
from {{ ref('int_customer_mrr_movements') }}
where last_month_mrr_cents + movement_amount_cents <> mrr_cents
