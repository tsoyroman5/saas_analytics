with source as (

    select * from {{ source('raw_saas', 'customers') }}

),

renamed as (

    select
        customer_id,
        company_name,
        upper(country) as country,
        cast(signup_date as date) as signup_date,
        lower(plan) as plan,
        cast(seats as int) as seats,
        lower(billing_interval) as billing_interval,
        lower(status) as status,
        cast(_batch_date as date) as _batch_date

    from source

)

select * from renamed