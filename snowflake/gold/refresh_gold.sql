use role transformer_role;
use warehouse compute_wh;

create or replace procedure retail_db.gold.refresh_gold()
returns varchar
language SQL
execute as caller 
as 
$$
begin 

    -- refresh of dim tables
    -- dim_date is a static table
    -- dim customers is a view
    create or replace table retail_db.gold.dim_products as(
        select product_id,
               any_value(product_name)                  as product_name,
               coalesce(any_value(category), 'Unknown') as category
        from 
            retail_db.silver.order_items
        group by 
            product_id
    );

    -- refresh of fact tables
    
    create or replace table retail_db.gold.fact_orders as(
        select
            o.order_id,
            c.customer_sk,
            dd.date_key                 as order_date_key,
            o.order_ts,
            o.order_status, o.sales_channel,
            o.subtotal, o.discount_amount, o.order_amount, o.currency
        from 
            retail_db.silver.orders o
        left join retail_db.gold.dim_customers c
            on c.customer_id = o.customer_id
            and o.order_ts >= c.valid_from
            and o.order_ts <  c.valid_to
        left join retail_db.gold.dim_date dd 
            on dd.full_date = o.order_date
        
    );
    
    create or replace table retail_db.gold.fact_order_items as(
        select
            i.order_item_id,
            i.order_id,
            i.product_id,
            fo.customer_sk,
            fo.order_date_key,
            i.quantity,
            i.unit_price,
            i.line_amount
        from 
            retail_db.silver.order_items i
        left join retail_db.gold.fact_orders fo 
            on fo.order_id = i.order_id
    );
    
    create or replace table retail_db.gold.fact_payments as (
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
            retail_db.silver.payments o
        left join retail_db.gold.dim_date dd on 
            dd.full_date = o.payment_ts::date
    );
    
    create or replace table retail_db.gold.fact_order_status_events as (
        select 
            event_id
            , order_id
            , status
            , date_key as order_event_date_key
            , (event_ts) as event_time
            , event_source
        from 
            retail_db.silver.order_status_events o
        left join retail_db.gold.dim_date dd on 
            dd.full_date = o.event_ts::date
    );

    -- force refresh of aggregates just in case

    alter dynamic table retail_db.gold.agg_daily_revenue refresh;
    alter dynamic table retail_db.gold.agg_customer_summary refresh;


    RETURN 'All DIM and FACT tables refreshed successfully';

end;
$$; 


create or replace task retail_db.gold.REFRESH_GOLD
    warehouse = 'COMPUTE_WH'
as 
    call retail_db.gold.refresh_gold(); 


execute task retail_db.gold.REFRESH_GOLD; 



    
    