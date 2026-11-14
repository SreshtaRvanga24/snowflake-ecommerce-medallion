-- SCD2 integrity: every customer has exactly one current version in the snapshot.
select customer_id, count_if(dbt_valid_to = '9999-12-31'::timestamp_ntz) as current_rows
from {{ ref('customers_snapshot') }}
group by customer_id
having count_if(dbt_valid_to = '9999-12-31'::timestamp_ntz) <> 1