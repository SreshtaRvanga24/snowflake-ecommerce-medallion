create or replace dynamic table agg_daily_revenue
  target_lag = '1 hour'
  warehouse = compute_wh
as
select 
    full_date as order_date
    , sales_channel
    , count(order_id) as total_orders
    , sum(subtotal) as gross_sales
    , sum(discount_amount) as discounts
    , sum(order_amount) as net_sales
    , round(net_sales/nullif(total_orders, 0), 2 )as avg_order_val

from 
    fact_orders fo
join dim_date dd
    on dd.date_key = order_date_key
group by 1, 2;

create or replace view vw_monthly_sales_trend as 
with monthly_vals as (
    select 
        year(order_date) as year
        , month(order_date) as month
        , sum(total_orders) as monthly_orders
        , sum(gross_sales) as monthly_gross_sales
        , sum(discounts) as discounts
        , sum(net_sales) as monthly_net_sales

    from 
        agg_daily_revenue
    group by 1, 2
)
select *
    , lag(monthly_net_sales) over (order by year, month) as prev_month_net_sales
    , round(100 * (monthly_net_sales - prev_month_net_sales)/ nullif(prev_month_net_sales, 0), 2) as mom_growth_pct
    , round(monthly_net_sales / nullif(monthly_orders, 0), 2) as avg_order_value
from 
    monthly_vals ; 



create or replace view VW_CATEGORY_PERFORMANCE as
select 
    year, 
    month, 
    month_name, 
    category, 
    count(distinct foi.order_id) as orders, 
    sum(quantity) as units_sold, sum(line_amount) as category_gross_sales, 
    sum(category_gross_sales) over(partition by year, month) as monthly_gross_sales, 
    round(100 * (category_gross_sales)/ nullif(monthly_gross_sales, 0), 2) as pct_of_gross
from 
    fact_order_items foi
left join dim_products dp 
    on dp.product_id = foi.product_id
-- left join fact_orders fo on fo.order_id = foi.order_id
left join dim_date dd 
    on dd.date_key = foi.order_date_key
group by 1, 2, 3, 4
; 



create or replace dynamic table agg_customer_summary
  target_lag = '1 hour'
  warehouse = compute_wh
as
with current_customers as (
    select customer_id, customer_name, segment, city
    from dim_customers
    where is_current = true
),
order_history as (
    select
        dc.customer_id,
        min(dd.full_date)       as first_order_date,
        max(dd.full_date)       as last_order_date,
        count(fo.order_id)      as total_orders,
        sum(fo.order_amount)    as lifetime_value
    from fact_orders fo
    join dim_customers dc on dc.customer_sk = fo.customer_sk     -- all versions
    join dim_date dd      on dd.date_key = fo.order_date_key
    group by dc.customer_id
)
select
    cc.customer_id,
    cc.customer_name,
    cc.segment,
    cc.city,
    oh.first_order_date,
    oh.last_order_date,
    coalesce(oh.total_orders, 0)                                  as total_orders,
    coalesce(oh.lifetime_value, 0)                                as lifetime_value,
    round(oh.lifetime_value / nullif(oh.total_orders, 0), 2)      as avg_order_value
from current_customers cc
left join order_history oh on oh.customer_id = cc.customer_id;



create or replace view vw_revenue_by_segment as
with current_customers as (
    select customer_id, customer_name, segment, city
    from dim_customers
    where is_current = true
),
order_history as (
    select
        dc.customer_id,
        dd.full_date as ordered_date, 
        sum(fo.order_amount)    as revenue
    from fact_orders fo
    join dim_customers dc on dc.customer_sk = fo.customer_sk     
    join dim_date dd      on dd.date_key = fo.order_date_key
    group by dc.customer_id, ordered_date
)
select
    cc.segment, oh.ordered_date, 
    sum(oh.revenue) as rev
from current_customers cc
join order_history oh on oh.customer_id = cc.customer_id group by all;


create or replace view VW_PAYMENT_METHOD_HEALTH as 
select 
    payment_method, year, month
    , count(order_id) total_attempts
    , sum(case when payment_status = 'SUCCESS' then 1 else 0 end) as successful_attempts
    , sum(case when payment_status = 'FAILED' then 1 else 0 end) as failed_attempts
    , round(100*(successful_attempts/nullif(total_attempts, 0)), 2) as success_rate 

from fact_payments fp
left join dim_date dd on dd.date_key = fp.payment_date_key
group by payment_method, year, month;

create or replace view vw_order_payment_status as
with payments as (
    select 
        order_id
        , sum(amount) as amount 
    from 
        fact_payments 
    where 
        payment_status = 'SUCCESS' 
    group by 
        order_id

)
, orders as (
    select  
        order_id
        , order_amount 
    from 
        fact_orders
)

select 
    o.order_id, 
    case when abs(amount - order_amount) < 0.01 then 'Fully Paid' 
         when amount > 0 and amount < order_amount then 'Underpaid'
         when amount > order_amount then 'Overpaid'
         else 'Unpaid'
    end as status

from 
    orders o  
left join payments p 
    on o.order_id = p.order_id

    ;


create or replace view vw_order_fulfillment as
with order_details as (
    select
        fo.order_id, 
        fo.sales_channel, year, month, 
        min(case when status = 'CREATED'    then event_time end) as created,
        min(case when status = 'PROCESSING' then event_time end) as processing,
        min(case when status = 'SHIPPED'    then event_time end) as shipped,
        min(case when status = 'DELIVERED'  then event_time end) as delivered,
        min(case when status = 'CANCELLED'  then event_time end) as cancelled,
        min(case when status = 'RETURNED'   then event_time end) as returned, 
        case
            when returned  is not null then 'RETURNED'
            when cancelled is not null then 'CANCELLED'
            when delivered is not null then 'DELIVERED'
            else 'IN_PROGRESS' 
        end as fulfillment_state
    from fact_orders fo 
    left join fact_order_status_events fose on fo.order_id = fose.order_id
    left join dim_date dd on dd.date_key = fo.order_date_key
    -- from (select order_id, status, event_time from fact_order_status_events)
    -- pivot (min(event_time) for status in (select distinct status from fact_order_status_events)) -- shortcut which works only in snowflake
    group by 
        fo.order_id,
        fo.sales_channel, 
        year, 
        month
    
)
    
    select *, 
        round(datediff(minute, created, shipped)   / 60, 1) as hours_to_ship,
        round(datediff(minute, created, delivered) / 60, 1) as hours_to_deliver
    from order_details
;


create or replace view vw_fulfillment_summary as 
select 
    fo.sales_channel
    , year
    , month
    , count(*) as total_orders
    , count(delivered) as delivered_orders
    , round(avg(vof.hours_to_deliver), 2) as avg_hours_to_deliver
    , max(vof.hours_to_deliver) as max_hours_to_deliver
    , round(100 * count_if(hours_to_deliver <= 72)/ nullif(count(delivered), 0), 2) as pct_delivered_within_72h
from fact_orders fo 
left join vw_order_fulfillment vof 
    on vof.order_id = fo.order_id
group by 
    fo.sales_channel
    , year
    , month
;


create or replace view vw_executive_kpis as -- old no month as granularity
select 
    (select sum(order_amount) from fact_orders) as total_revenue
    , ((select count(distinct dc.customer_id) from fact_orders fo join dim_customers dc on dc.customer_sk = fo.customer_sk)) as active_customers
    , (select count(distinct order_id) from fact_orders) as total_orders
    , round(total_revenue/ nullif(total_orders, 0), 2) as avg_order_value
    , (select round(100 * (sum(case when payment_status = 'SUCCESS' then 1 else 0 end))/nullif(count(payment_id),0), 2) from fact_payments) as success_payments_rate
    , (select round(100 * count_if(status = 'Fully Paid') / nullif(count(*), 0), 2) from vw_order_payment_status) as pct_orders_fully_paid
    , (select round(avg(hours_to_deliver), 1) from vw_order_fulfillment) as avg_hours_to_deliver;


create or replace view vw_executive_kpis_monthly as
with payments_per_order as (
    select order_id,
           count(*)                             as attempts,
           count_if(payment_status = 'SUCCESS') as successes
    from fact_payments
    group by order_id
),
order_level as (                                  
    select
        fo.order_id,
        dd.year, dd.month,
        dc.region, dc.city,                       
        dc.customer_id,
        fo.order_amount,
        p.attempts, p.successes,
        ops.status = 'Fully Paid'                  as is_fully_paid,
        f.delivered, f.hours_to_deliver
    from fact_orders fo
    join dim_date dd                     on dd.date_key = fo.order_date_key
    left join dim_customers dc           on dc.customer_sk = fo.customer_sk
    left join payments_per_order p       on p.order_id = fo.order_id
    left join vw_order_payment_status ops on ops.order_id = fo.order_id
    left join vw_order_fulfillment f     on f.order_id = fo.order_id
)
select
    year, month, region, city,
    sum(order_amount)                                       as net_revenue,
    count(*)                                                as total_orders,
    coalesce(sum(attempts), 0)                              as payment_attempts,
    coalesce(sum(successes), 0)                             as successful_attempts,
    count_if(is_fully_paid)                                 as fully_paid_orders,
    count(delivered)                                        as delivered_orders,
    sum(hours_to_deliver)                                   as total_delivery_hours,
    count(distinct customer_id)                             as ordering_customers,
    round(sum(order_amount) / nullif(count(*), 0), 2)       as avg_order_value,
    round(100 * sum(successes) / nullif(sum(attempts), 0), 2) as payment_success_rate_pct,
    round(100 * count_if(is_fully_paid) / nullif(count(*), 0), 2) as pct_orders_fully_paid,
    round(sum(hours_to_deliver) / nullif(count(delivered), 0), 1) as avg_hours_to_deliver
from order_level
group by year, month, region, city;


