with payments as (
    select 
        ID as payment_id, 
        ORDERID as order_id, 
        PAYMENTMETHOD as payment_method,
        STATUS, 
        AMOUNT, 
        CREATED
     from raw.stripe.payment
)  
select * from payments