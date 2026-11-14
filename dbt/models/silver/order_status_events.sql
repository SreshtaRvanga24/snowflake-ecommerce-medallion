{{ config(unique_key='event_id') }}

with order_status_events_staging as (

    select
        upper(trim(event_id))               as event_id,
        upper(trim(order_id))               as order_id,
        upper(trim(status))                 as status,
        try_to_timestamp_ntz(event_ts)      as event_ts,
        upper(trim(source_system_code))     as event_source,
        try_to_timestamp_ntz(updated_at)    as updated_at,
        source_system                       as _source_system,
        source_file_name                    as _source_file,
        source_file_row_number              as _source_file_row,
        load_id                             as _load_id,
        ingestion_timestamp                 as _ingested_at
    from {{ source('bronze', 'order_status_events') }}

),

new_rows as (

    select *
    from order_status_events_staging
    {% if is_incremental() %}
    where _ingested_at > (
        select coalesce(max(_ingested_at), '1900-01-01'::timestamp_ltz) from {{ this }}
    )
    {% endif %}

),

unique_rows as (

    select *
    from new_rows
    where event_id is not null
    qualify row_number() over (
        partition by event_id
        order by updated_at desc nulls last, _ingested_at desc, _source_file desc, _source_file_row desc
    ) = 1

)

select u.*
from unique_rows u
{% if is_incremental() %}
left join {{ this }} t on t.event_id = u.event_id
where t.event_id is null
   or u.updated_at > t.updated_at
{% endif %}