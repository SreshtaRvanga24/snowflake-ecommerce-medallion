-- Reconciliation: every distinct business key in Bronze must exist exactly
-- once in Silver. Returns one row per table where the counts disagree.
with checks as (
    select 'customers' as model,
           (select count(distinct upper(trim(customer_id))) from {{ source('bronze', 'customers') }} where customer_id is not null) as bronze_keys,
           (select count(*) from {{ ref('customers') }}) as silver_rows
    union all
    select 'orders',
           (select count(distinct upper(trim(order_id))) from {{ source('bronze', 'orders') }} where order_id is not null),
           (select count(*) from {{ ref('orders') }})
    union all
    select 'order_items',
           (select count(distinct upper(trim(order_item_id))) from {{ source('bronze', 'order_items') }} where order_item_id is not null),
           (select count(*) from {{ ref('order_items') }})
    union all
    select 'payments',
           (select count(distinct upper(trim(payment_id))) from {{ source('bronze', 'payments') }} where payment_id is not null),
           (select count(*) from {{ ref('payments') }})
    union all
    select 'order_status_events',
           (select count(distinct upper(trim(event_id))) from {{ source('bronze', 'order_status_events') }} where event_id is not null),
           (select count(*) from {{ ref('order_status_events') }})
)
select * from checks where bronze_keys <> silver_rows
