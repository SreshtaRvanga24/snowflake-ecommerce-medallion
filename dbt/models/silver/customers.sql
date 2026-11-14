{{ config(unique_key='customer_id') }}

with customer_staging as (

    select
        upper(trim(customer_id))            as customer_id,
        trim(customer_name)                 as customer_name,
        lower(trim(email))                  as email,
        initcap(trim(region))               as region,
        initcap(trim(city))                 as city,
        initcap(trim(segment))              as segment,
        try_to_date(signup_date)            as signup_date,
        try_to_boolean(is_active)           as is_active,
        try_to_timestamp_ntz(updated_at)    as updated_at,
        source_system                       as _source_system,
        source_file_name                    as _source_file,
        source_file_row_number              as _source_file_row,
        load_id                             as _load_id,
        ingestion_timestamp                 as _ingested_at
    from {{ source('bronze', 'customers') }}

),

new_rows as (

    select *
    from customer_staging
    {% if is_incremental() %}
    where _ingested_at > (
        select coalesce(max(_ingested_at), '1900-01-01'::timestamp_ltz) from {{ this }}
    )
    {% endif %}

),

unique_rows as (

    select *
    from new_rows
    where customer_id is not null
    qualify row_number() over (
        partition by customer_id
        order by updated_at desc nulls last, _ingested_at desc, _source_file desc, _source_file_row desc
    ) = 1

)

select u.*
from unique_rows u
{% if is_incremental() %}
left join {{ this }} t on t.customer_id = u.customer_id
where t.customer_id is null
   or u.updated_at > t.updated_at
{% endif %}