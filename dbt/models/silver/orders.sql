{{ config(unique_key='order_id') }}

with orders_staging as (

    select
        upper(trim(order_id))               as order_id,
        upper(trim(customer_id))            as customer_id,
        try_to_date(order_date)             as order_date,
        try_to_timestamp_ntz(order_ts)      as order_ts,
        upper(trim(order_status))           as order_status,
        upper(trim(sales_channel))          as sales_channel,
        try_to_number(subtotal, 12, 2)      as subtotal,
        try_to_number(discount_amount, 12, 2) as discount_amount,
        try_to_number(order_amount, 12, 2)  as order_amount,
        upper(trim(currency))               as currency,
        try_to_timestamp_ntz(updated_at)    as updated_at,
        source_system                       as _source_system,
        source_file_name                    as _source_file,
        source_file_row_number              as _source_file_row,
        load_id                             as _load_id,
        ingestion_timestamp                 as _ingested_at
    from {{ source('bronze', 'orders') }}

),

new_rows as (

    select *
    from orders_staging
    {% if is_incremental() %}
    where _ingested_at > (
        select coalesce(max(_ingested_at), '1900-01-01'::timestamp_ltz) from {{ this }}
    )
    {% endif %}

),

unique_rows as (

    select *
    from new_rows
    where order_id is not null
    qualify row_number() over (
        partition by order_id
        order by updated_at desc nulls last, _ingested_at desc, _source_file desc, _source_file_row desc
    ) = 1

)

select u.*
from unique_rows u
{% if is_incremental() %}
left join {{ this }} t on t.order_id = u.order_id
where t.order_id is null
   or u.updated_at > t.updated_at
{% endif %}