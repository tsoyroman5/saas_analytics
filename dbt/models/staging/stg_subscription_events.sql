with source as (

    select * from {{ source('raw_saas', 'subscription_events') }}

),

renamed as (

    select
        event_id,
        customer_id,
        event_type,
        cast(event_ts as datetime) as event_ts,
        plan,
        cast(seats as int64) as seats,
        billing_interval,
        reason,
        cast(_batch_date as date) as _batch_date

    from source

),

deduped as (
    select
        event_id,
        customer_id,
        event_type,
        event_ts,
        plan,
        seats,
        billing_interval,
        reason,
        _batch_date
    from (
        select
            *,
            row_number()
                over (partition by event_id order by _batch_date)
                as row_num
        from renamed
    )
    where row_num = 1
)

select * from deduped
