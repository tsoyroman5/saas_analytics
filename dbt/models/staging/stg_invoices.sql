with source as (

    select * from {{ source('raw_saas', 'invoices') }}

),

renamed as (

    select
        invoice_id,
        customer_id,
        cast(issued_date as date) as issued_date,
        lower(plan) as plan,
        cast(seats as int) as seats,
        lower(billing_interval) as billing_interval,
        cast(amount_cents as int) as amount_cents,
        upper(currency) as currency,
        cast(_batch_date as date) as _batch_date

    from source

)

select * from renamed

