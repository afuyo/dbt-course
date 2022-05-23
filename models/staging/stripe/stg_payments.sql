with source as (

    select * from {{ source('stripe', 'payment') }}

),

renamed as (

    select
         ID as payment_id, 
        ORDERID as order_id, 
        PAYMENTMETHOD as payment_method,
        STATUS, 
        AMOUNT, 
        CREATED,
        _batched_at

    from source

)

select * from renamed
