{{ config(unique_key='payment_id') }}

with payments_staging as (

    select
        upper(trim(payment_id))             as payment_id,
        upper(trim(order_id))               as order_id,
        upper(trim(customer_id))            as customer_id,
        try_to_timestamp_ntz(payment_ts)    as payment_ts,
        upper(trim(payment_method))         as payment_method,
        upper(trim(payment_status))         as payment_status,
        try_to_number(amount, 12, 2)        as amount,
        upper(trim(currency))               as currency,
        try_to_timestamp_ntz(updated_at)    as updated_at,
        source_system                       as _source_system,
        source_file_name                    as _source_file,
        source_file_row_number              as _source_file_row,
        load_id                             as _load_id,
        ingestion_timestamp                 as _ingested_at
    from {{ source('bronze', 'payments') }}

),

new_rows as (

    select *
    from payments_staging
    {% if is_incremental() %}
    where _ingested_at > (
        select coalesce(max(_ingested_at), '1900-01-01'::timestamp_ltz) from {{ this }}
    )
    {% endif %}

),

unique_rows as (

    select *
    from new_rows
    where payment_id is not null
    qualify row_number() over (
        partition by payment_id
        order by updated_at desc nulls last, _ingested_at desc, _source_file desc, _source_file_row desc
    ) = 1

)

select u.*
from unique_rows u
{% if is_incremental() %}
left join {{ this }} t on t.payment_id = u.payment_id
where t.payment_id is null
   or u.updated_at > t.updated_at
{% endif %}