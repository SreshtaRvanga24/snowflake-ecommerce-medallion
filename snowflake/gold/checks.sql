-- Row counts match Silver: 1000 / 2445 / 1441 / 3520
select 'orders', count(*) from fact_orders union all
select 'order_items', count(*) from fact_order_items union all
select 'payments', count(*) from fact_payments union all
select 'events', count(*) from fact_order_status_events;

-- Primary keys are unique (expect 0 rows)
select order_id from fact_orders group by 1 having count(*) > 1;
select product_id from dim_products group by 1 having count(*) > 1;

-- Every fact row found its dimensions (expect all zeros)
select count_if(customer_sk is null)    as no_customer,
       count_if(order_date_key is null) as no_date
from fact_orders;
select count(*) as items_without_product
from fact_order_items i left join dim_products p using (product_id)
where p.product_id is null;

-- Revenue reconciles with Silver: the two numbers must be equal
select (select sum(order_amount) from fact_orders)          as gold_revenue,
       (select sum(order_amount) from retail_db.silver.orders) as silver_revenue;


