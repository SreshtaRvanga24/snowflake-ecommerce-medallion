# Snowflake Hands-on Dataset

Synthetic e-commerce order and payment dataset for SQL/Snowflake interview practice.

## Files and suggested grain
- CUSTOMERS.csv: one intended current row per customer (CUSTOMER_ID).
- ORDERS_RAW.csv: order extract with intentional duplicate ORDER_ID versions. Keep the latest UPDATED_AT per ORDER_ID when building a current-state table.
- ORDER_ITEMS.csv: one row per order line (ORDER_ITEM_ID).
- PAYMENTS_RAW.csv: payment attempts; includes a small number of exact duplicate rows. PAYMENT_ID is the intended business key.
- ORDER_STATUS_EVENTS.csv: one row per order-status event (EVENT_ID); multiple events per order are expected.

## Important dataset characteristics
- 200 customers and 1,000 distinct orders.
- Duplicate order versions and duplicate payment rows are intentional.
- Optional fields contain some NULL values.
- Orders have multiple items and can have multiple payment attempts.
- Dates span 2025-01-01 through late 2026.
- Amounts are INR.
- All records are synthetic; no personal data.

## Suggested Snowflake setup
Create a database and schema, then upload the five CSV files through Snowsight > Data > Add Data > Load files into a table.
Suggested database/schema: `HANDS_ON_DB.PUBLIC`.
Use the file names as table names, or choose your own.

Recommended initial table names:
`CUSTOMERS`, `ORDERS_RAW`, `ORDER_ITEMS`, `PAYMENTS_RAW`, `ORDER_STATUS_EVENTS`.

For a first load, allow Snowflake to infer column types, then inspect the resulting types. If you want to practice robust ingestion, initially load into VARCHAR columns and cast in staging SQL.

## Exercises we will do after loading
1. Profile row counts, NULLs, duplicate business keys and date ranges.
2. Deduplicate ORDERS_RAW by ORDER_ID using UPDATED_AT.
3. Deduplicate PAYMENTS_RAW by PAYMENT_ID.
4. Build an order-level fact dataset without join fan-out.
5. Revenue, payment success rate, customer ranking and rolling-window queries.
6. Latest status per order using window functions and QUALIFY.
7. Incremental MERGE patterns and late-arriving updates.
8. Query Profile, pruning and warehouse sizing concepts.

## Grain reminders
- Orders: one row per ORDER_ID after deduplication.
- Order items: one row per ORDER_ITEM_ID.
- Payments: one row per payment attempt (PAYMENT_ID).
- Status events: one row per EVENT_ID.
Do not join all four detail tables together and sum amounts without pre-aggregating; that can multiply rows and inflate metrics.
