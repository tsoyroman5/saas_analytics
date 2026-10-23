with cs_first_month as (
    select
        customer_id,
        date_trunc(event_ts, month) as first_month
    from
        {{ ref('stg_subscription_events') }}
    where
        event_type = 'started'
),

date_spine as (
    select month
    from
        unnest(generate_date_array(
            (
                select date_trunc(min(_batch_date), month) from {{ ref('stg_subscription_events') }}
            ),
            (
                select date_trunc(max(_batch_date), month) from {{ ref('stg_subscription_events') }}
            ),
            interval 1 month
        )) as month
),

customer_months as (
    select 
        cfm.customer_id, 
        ds.month
    from
        cs_first_month as cfm
    cross join 
        date_spine as ds
    where ds.month >= cfm.first_month
),

joined_subscription_events as (
    select
        cms.customer_id,
        cms.month,
        row_number() over(partition by cms.customer_id, cms.month order by event_ts desc) as month_row_num,
        se.event_id,
        se.event_type,
        se.event_ts,
        se.plan,
        se.seats,
        se.billing_interval,
        se.reason
    from 
        customer_months as cms
    left join 
        {{ ref('stg_subscription_events') }} as se
    on 
        cms.customer_id = se.customer_id and
        se.event_ts < date_add(cms.month, interval 1 month)
),

month_end_state as (
    select
        customer_id,
        month,
        event_id,
        event_type,
        plan,
        seats,
        billing_interval,
        reason
    from
        joined_subscription_events
    where
        month_row_num = 1
),

join_plan_prices as (
    select
        mes.customer_id,
        mes.month,
        mes.event_id,
        mes.event_type,
        mes.plan,
        mes.seats,
        mes.billing_interval,
        mes.reason,
        pp.price_per_seat_month_cents,
        case
            when mes.event_type <> 'canceled' then mes.seats * pp.price_per_seat_month_cents
            else 0
        end as mrr_cents
    from
        month_end_state as mes
    left join
        {{ ref('plan_prices') }} as pp 
        on mes.plan = pp.plan and
        mes.billing_interval = pp.billing_interval

)

select * from join_plan_prices
