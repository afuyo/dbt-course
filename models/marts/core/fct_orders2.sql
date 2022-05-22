with orders as  (
    select * from {{ ref('stg_orders' )}}
),

payments as (
    select * from {{ref('stg_payments')}}
),

order_payments as 
    (
    select 
        payments.amount,
        payments.status,
        orders.order_id,
        orders.customer_id
        from orders 
            left join payments 
            on payments.order_id=orders.order_id
),

final as (
    select * from order_payments
)
select * from final
       