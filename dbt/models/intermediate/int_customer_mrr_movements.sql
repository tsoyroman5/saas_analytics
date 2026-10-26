with last_month_mrr as (
    select
        customer_id,
        month,
        event_type,
        plan,
        seats,
        billing_interval,
        reason,
        price_per_seat_month_cents,
        mrr_cents,
        ifnull(
            lag(mrr_cents) over (partition by customer_id order by month), 0
        ) as last_month_mrr_cents,
        ifnull(max(mrr_cents)
                    over (
                        partition by customer_id
                        order by month
                        rows between unbounded preceding and 1 preceding
                    ), 0) as max_history_amount_cents
    from {{ ref('int_customer_month_mrr') }}
),

movement_type_col as (
    select
        customer_id,
        month,
        event_type,
        plan,
        seats,
        billing_interval,
        reason,
        price_per_seat_month_cents,
        mrr_cents,
        last_month_mrr_cents,
        max_history_amount_cents,
        case
            when
                last_month_mrr_cents = 0
                and mrr_cents > 0
                and max_history_amount_cents = 0
                then 'new'
            when
                last_month_mrr_cents = 0
                and mrr_cents > 0
                and max_history_amount_cents > 0
                then 'reactivation'
            when mrr_cents > last_month_mrr_cents and mrr_cents > 0
                then 'expansion'
            when mrr_cents < last_month_mrr_cents and mrr_cents > 0
                then 'contraction'
            when last_month_mrr_cents > 0 and mrr_cents = 0
                then 'churn'
            else 'nothing'
        end as movement_type
    from
        last_month_mrr
),

amount_col as (
    select
        customer_id,
        month,
        event_type,
        plan,
        seats,
        billing_interval,
        price_per_seat_month_cents,
        mrr_cents,
        last_month_mrr_cents,
        reason,
        movement_type,
        max_history_amount_cents,
        case
            when
                (movement_type = 'new' or movement_type = 'reactivation')
                then mrr_cents
            when
                (movement_type = 'expansion' or movement_type = 'contraction')
                then mrr_cents - last_month_mrr_cents
            when movement_type = 'churn' then -last_month_mrr_cents
            else 0
        end as movement_amount_cents
    from
        movement_type_col
)

select * from amount_col
