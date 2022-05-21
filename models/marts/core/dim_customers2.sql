with customers as (

    select * 

    from {{ref('stg_customers')}}

),

orders as (

    select * 

    from {{ref('stg_orders')}}

),

fct_orders as (
    select * 
    from {{ref('fct_orders2')}}
),

customer_orders as (

    select
        customer_id,

        min(order_date) as first_order_date,
        max(order_date) as most_recent_order_date,
        count(order_id) as number_of_orders

    from orders

    group by 1

),


final as (

    select
        customers.customer_id,
        customers.first_name,
        customers.last_name,
        customer_orders.first_order_date,
        customer_orders.most_recent_order_date,
        coalesce(customer_orders.number_of_orders, 0) as number_of_orders

    from customers

    left join customer_orders using (customer_id)

),
final2 as (
    select final.*, 
    fct_orders.amount as life_time_value 
    from final 
        left join fct_orders 
        on final.customer_id=fct_orders.customer_id
        where fct_orders.status= 'success'
)

select * from final2