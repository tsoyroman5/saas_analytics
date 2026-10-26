-- Assertion: in every month, starting_mrr in mrr_monthly equals the sum of last_month_mrr_cents
-- over all customers in int_customer_mrr_movements.
--
-- Why it matters: starting_mrr is computed as the previous month's ending_mrr (a lag over monthly rows).
-- The customer-level sum comes from a lag over each customer's own rows. Two independent routes to
-- the same number must agree. A disagreement means a customer-month is missing, a customer's rows
-- have a gap, or the monthly chain is broken somewhere.
--
-- The test passes when the query returns zero rows. Each returned row is a month where the two
-- values differ, or where a month is missing on one side.
with last_month_mrr_sum as (
    select month, sum(last_month_mrr_cents) as last_month_mrr_cents_sum
    from {{ ref('int_customer_mrr_movements') }}
    group by month
)

select 
    lmms.month,
from last_month_mrr_sum as lmms
left join {{ ref('mrr_monthly') }} as mm on mm.month = lmms.month
where lmms.last_month_mrr_cents_sum <> mm.starting_mrr