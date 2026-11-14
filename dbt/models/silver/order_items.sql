{{ config(unique_key='order_item_id') }}

with order_items_staging as (

    select
        upper(trim(order_item_id))          as order_item_id,
        upper(trim(order_id))               as order_id,
        upper(trim(product_id))             as product_id,
        trim(product_name)                  as product_name,
        initcap(trim(category))             as category,
        try_to_number(quantity)             as quantity,
        try_to_number(unit_price, 12, 2)    as unit_price,
        try_to_number(line_amount, 12, 2)   as line_amount,
        try_to_timestamp_ntz(updated_at)    as updated_at,
        source_system                       as _source_system,
        source_file_name                    as _source_file,
        source_file_row_number              as _source_file_row,
        load_id                             as _load_id,
        ingestion_timestamp                 as _ingested_at
    from {{ source('bronze', 'order_items') }}

),

new_rows as (

    select *
    from order_items_staging
    {% if is_incremental() %}
    where _ingested_at > (
        select coalesce(max(_ingested_at), '1900-01-01'::timestamp_ltz) from {{ this }}
    )
    {% endif %}

),

unique_rows as (

    select *
    from new_rows
    where order_item_id is not null
    qualify row_number() over (
        partition by order_item_id
        order by updated_at desc nulls last, _ingested_at desc, _source_file desc, _source_file_row desc
    ) = 1

)

select u.*
from unique_rows u
{% if is_incremental() %}
left join {{ this }} t on t.order_item_id = u.order_item_id
where t.order_item_id is null
   or u.updated_at > t.updated_at
{% endif %}