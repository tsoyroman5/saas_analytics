with source as (

    select * from {{ source('raw_saas', 'product_events') }}

),

renamed as (

    select
        event_id,
        customer_id,
        user_id,
        event_name,
        cast(event_ts as datetime) as event_ts,
        cast(_batch_date as date) as _batch_date

    from source

),

deduped as (
    select
        event_id,
        customer_id,
        user_id,
        event_name,
        event_ts,
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
