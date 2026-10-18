with source as (

    select * from {{ source('raw_saas', 'payment_attempts') }}

),

renamed as (

    select
        attempt_id,
        invoice_id,
        customer_id,
        cast(attempted_at as datetime) as attempted_at,
        lower(status) as status,
        failure_reason,
        cast(_batch_date as date) as _batch_date

    from source

)

select * from renamed
