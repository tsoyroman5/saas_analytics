-- Assertion: for every customer and month in which the true MRR is above 0, int_customer_month_mrr has
-- the same MRR, and it has no other customer-months with MRR above 0.
--
-- Why it matters: this checks the base of every metric (the month-end state, the cancel rule and the
-- price join) at the finest grain, so an error cannot hide inside a correct monthly total.
--
-- The truth lists only months with MRR above 0, so model rows with MRR 0 (canceled customers) are
-- filtered out before the comparison. The test passes when the query returns zero rows; each returned
-- row is a customer-month missing on one side or with a different MRR.
-- Tagged `truth`: it needs the generator's truth tables, so it is not part of the production path.
{{
    config(
        tags=["truth"]
    )
}}

with filtered_customer_month_mrr as (
    select *
    from
        {{ ref('int_customer_month_mrr') }}
    where
        mrr_cents > 0
)

select
    coalesce(icmm.customer_id, tcmm.customer_id) as customer_id,
    coalesce(icmm.month, parse_date('%F', concat(tcmm.month, '-', '01')))
        as month,
    coalesce(icmm.mrr_cents <> tcmm.mrr_cents, true) as mrr_mismatch
from filtered_customer_month_mrr as icmm
full outer join {{ source('raw_saas', 'truth_customer_month_mrr') }} as tcmm
    on
        icmm.customer_id = tcmm.customer_id
        and icmm.month = parse_date('%F', concat(tcmm.month, '-', '01'))
where coalesce(icmm.mrr_cents <> tcmm.mrr_cents, true)
