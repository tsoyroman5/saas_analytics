with mrr_sums_by_type as (
    select
        month,
        ifnull(lag(sum(mrr_cents)) over (order by month), 0) as starting_mrr,
        sum(
            case
                when movement_type = 'new'
                    then movement_amount_cents
                else 0
            end
        ) as new_mrr,
        sum(
            case
                when movement_type = 'reactivation'
                    then movement_amount_cents
                else 0
            end
        ) as reactivation_mrr,
        sum(
            case
                when movement_type = 'expansion'
                    then movement_amount_cents
                else 0
            end
        ) as expansion_mrr,
        -sum(
            case
                when movement_type = 'contraction'
                    then movement_amount_cents
                else 0
            end
        ) as contraction_mrr,
        -sum(
            case
                when movement_type = 'churn'
                    then movement_amount_cents
                else 0
            end
        ) as churned_mrr,
        sum(mrr_cents) as ending_mrr,
        countif(mrr_cents > 0) as active_customers,
        countif(movement_type = 'new') as new_customers,
        countif(movement_type = 'churn') as churned_customers
    from {{ ref('int_customer_mrr_movements') }}
    group by month
)

select * from mrr_sums_by_type
