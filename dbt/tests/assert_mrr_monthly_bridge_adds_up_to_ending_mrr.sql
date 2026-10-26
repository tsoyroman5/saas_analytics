-- Assertion: in mrr_monthly, for every month,
--   starting_mrr + new_mrr + reactivation_mrr + expansion_mrr - contraction_mrr - churned_mrr = ending_mrr
--
-- Why it matters: this proves the classified movements fully explain the change in total MRR.
-- A missed case, a double-counted case or a sign error in the movement model breaks the equation.
--
-- The test passes when the query returns zero rows. Each returned row is a month where the bridge
-- does not add up. A null in any of these columns would not be caught here; not_null tests on the
-- columns cover that.
select *
from {{ ref('mrr_monthly') }}
where starting_mrr + new_mrr + reactivation_mrr + expansion_mrr - contraction_mrr - churned_mrr <> ending_mrr