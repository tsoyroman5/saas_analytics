-- Assertion: mrr_monthly equals the generator's ground truth (truth_mrr_monthly) in every month and
-- in every column: the five MRR movements, starting and ending MRR, and the three customer counts.
--
-- Why it matters: the bridge, the movement rules and the MRR calculation were all written by the same
-- person; agreement with an independently computed truth is the evidence that they are right,
-- not just consistent with each other.
--
-- The test passes when the query returns zero rows. Each returned row is a month that is missing on
-- one side or that differs in at least one column (the *_mismatch columns show which).
-- Tagged `truth`: it needs the generator's truth tables, so it is not part of the production path.
{{
    config(
        tags=["truth"]
    )
}}

select
    coalesce(mm.month, parse_date('%F', concat(tmm.month, '-', '01'))) as month,
    coalesce(mm.starting_mrr <> tmm.starting_mrr, true)
        as starting_mrr_mismatch,
    coalesce(mm.new_mrr <> tmm.new_mrr, true) as new_mrr_mismatch,
    coalesce(mm.reactivation_mrr <> tmm.reactivation_mrr, true)
        as reactivation_mrr_mismatch,
    coalesce(mm.expansion_mrr <> tmm.expansion_mrr, true)
        as expansion_mrr_mismatch,
    coalesce(mm.contraction_mrr <> tmm.contraction_mrr, true)
        as contraction_mrr_mismatch,
    coalesce(mm.churned_mrr <> tmm.churned_mrr, true) as churned_mrr_mismatch,
    coalesce(mm.ending_mrr <> tmm.ending_mrr, true) as ending_mrr_mismatch,
    coalesce(mm.active_customers <> tmm.active_customers, true)
        as active_customers_mismatch,
    coalesce(mm.new_customers <> tmm.new_customers, true)
        as new_customers_mismatch,
    coalesce(mm.churned_customers <> tmm.churned_customers, true)
        as churned_customers_mismatch
from {{ ref('mrr_monthly') }} as mm
full outer join {{ source('raw_saas', 'truth_mrr_monthly') }} as tmm
    on mm.month = parse_date('%F', concat(tmm.month, '-', '01'))
where
    coalesce(mm.starting_mrr <> tmm.starting_mrr, true)
    or coalesce(mm.new_mrr <> tmm.new_mrr, true)
    or coalesce(mm.reactivation_mrr <> tmm.reactivation_mrr, true)
    or coalesce(mm.expansion_mrr <> tmm.expansion_mrr, true)
    or coalesce(mm.contraction_mrr <> tmm.contraction_mrr, true)
    or coalesce(mm.churned_mrr <> tmm.churned_mrr, true)
    or coalesce(mm.ending_mrr <> tmm.ending_mrr, true)
    or coalesce(mm.active_customers <> tmm.active_customers, true)
    or coalesce(mm.new_customers <> tmm.new_customers, true)
    or coalesce(mm.churned_customers <> tmm.churned_customers, true)
