-- Business rule: an order's status events should not happen before the order.
-- Warn only: this documents a data finding rather than blocking the pipeline.
{{ config(severity='warn') }}

select e.event_id, e.order_id, e.status, e.event_ts, o.order_ts
from {{ ref('order_status_events') }} e
join {{ ref('orders') }} o on o.order_id = e.order_id
where e.event_ts < o.order_ts
