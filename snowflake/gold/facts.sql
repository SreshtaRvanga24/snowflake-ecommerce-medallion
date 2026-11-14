-- fact tables
-- fact_orders (order_id, customer_id, order_date_key, orderd_time, order_status, channel, subtotal, discount_amount, order_amount, curreny)
-- fact_order_events (event_id, order_id, status, event_date_key, time, event_source)
-- fact_payments (payment_id, order_id, customer_id, payment_datekey, time, payment_method, payment_status, amount, currency)
-- fact_order_items (order_item_id, order_id, product_id, quantity, unit_price, line_amount)

select * from silver.orders;
select * from silver.order_status_events;
select * from silver.payments;
select * from silver.order_items;



create or replace table fact_order_items as (
    select 
        order_item_id
        , order_id
        , product_id
        , quantity
        , unit_price
        , line_amount
    from 
        silver.order_items o
);


create or replace table fact_payments as (
    select 
        payment_id
        , order_id
        , customer_id
        , date_key as payment_date_key
        , (payment_ts) as payment_time
        , payment_method
        , payment_status
        , amount
        , currency
    from 
        silver.payments o
    left join dim_date dd on 
        dd.full_date = o.payment_ts::date
);



create or replace table fact_order_status_events as (
    select 
        event_id
        , order_id
        , status
        , date_key as order_event_date_key
        , (event_ts) as event_time
        , event_source
    from 
        silver.order_status_events o
    left join dim_date dd on 
        dd.full_date = o.event_ts::date
);


create or replace table fact_orders as(
    select
        o.order_id,
        c.customer_sk,
        dd.date_key                 as order_date_key,
        o.order_ts,
        o.order_status, o.sales_channel,
        o.subtotal, o.discount_amount, o.order_amount, o.currency
    from 
        silver.orders o
    left join dim_customers c
        on c.customer_id = o.customer_id
        and o.order_ts >= c.valid_from
        and o.order_ts <  c.valid_to
    left join dim_date dd on dd.full_date = o.order_date
    
);
