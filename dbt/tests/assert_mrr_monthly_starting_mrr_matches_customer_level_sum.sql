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
with customer_level as (
    select
        month,
        sum(last_month_mrr_cents) as customer_level_starting_mrr
    from {{ ref('int_customer_mrr_movements') }}
    group by month
)

select
    coalesce(cl.month, mm.month) as month,
    cl.customer_level_starting_mrr,
    mm.starting_mrr,
    mm.starting_mrr - cl.customer_level_starting_mrr as difference
from customer_level as cl
full outer join {{ ref('mrr_monthly') }} as mm
    on cl.month = mm.month
where cl.customer_level_starting_mrr is distinct from mm.starting_mrr