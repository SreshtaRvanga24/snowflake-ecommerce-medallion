select a.customer_id, a.dbt_scd_id, b.dbt_scd_id as overlapping_scd_id
from {{ ref('customers_snapshot') }} a
join {{ ref('customers_snapshot') }} b
  on a.customer_id = b.customer_id
 and a.dbt_scd_id <> b.dbt_scd_id
 and a.dbt_valid_from < b.dbt_valid_to
 and b.dbt_valid_from < a.dbt_valid_to