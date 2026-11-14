-- dim tables
-- dim_date
-- dim_products (product_id, product, category)
-- dim_customers (customer_id, name, email, region, city, segment, signup_date_id, is_active)




create or replace view retail_db.gold.dim_customers as
select
    dbt_scd_id as customer_sk
    , customer_id
    , customer_name
    , email
    , region
    , city
    , segment
    , signup_date
    , is_active
    , iff(row_number() over (partition by customer_id order by dbt_valid_from) = 1,
        '1900-01-01'::timestamp_ntz, dbt_valid_from) as valid_from,
    dbt_valid_to as valid_to,
    dbt_valid_to = '9999-12-31'::timestamp_ntz      as is_current
from retail_db.silver.customers_snapshot;


create or replace table dim_products as (
    select 
        distinct product_id
        , product_name
        , category 
    from 
        silver.order_items 
    where 
        category is not NULL 
    order by 
        product_id
        
);


CREATE OR REPLACE TABLE DIM_DATE AS (

    WITH DATE_RANGE AS (
        SELECT
            DATEADD(
                DAY,
                row_number() over (order by seq4()) - 1,
                '2000-01-01'::DATE
            ) AS CALENDAR_DATE
        FROM TABLE(GENERATOR(ROWCOUNT => 18628))
    )

    SELECT
        TO_NUMBER(TO_CHAR(CALENDAR_DATE, 'YYYYMMDD')) AS DATE_KEY,
        CALENDAR_DATE AS FULL_DATE,

        DAY(CALENDAR_DATE) AS DAY,
        DAYNAME(CALENDAR_DATE) AS DAY_NAME,
        DAYOFWEEKISO(CALENDAR_DATE) AS DAY_OF_WEEK,
        DAYOFYEAR(CALENDAR_DATE) AS DAY_OF_YEAR,

        WEEKISO(CALENDAR_DATE) AS WEEK_OF_YEAR,
        MONTH(CALENDAR_DATE) AS MONTH,
        MONTHNAME(CALENDAR_DATE) AS MONTH_NAME,
        QUARTER(CALENDAR_DATE) AS QUARTER,

        YEAR(CALENDAR_DATE) AS YEAR,
        TO_NUMBER(TO_CHAR(CALENDAR_DATE, 'YYYYMM')) AS YEAR_MONTH,

        DAYOFWEEKISO(CALENDAR_DATE) IN (6, 7) AS IS_WEEKEND,

        DAY(CALENDAR_DATE) = 1 AS IS_MONTH_START,

        LAST_DAY(CALENDAR_DATE) = CALENDAR_DATE AS IS_MONTH_END,

        MONTH(CALENDAR_DATE) IN (1, 4, 7, 10)
            AND DAY(CALENDAR_DATE) = 1 AS IS_QUARTER_START,

        MONTH(CALENDAR_DATE) IN (3, 6, 9, 12)
            AND CALENDAR_DATE = LAST_DAY(CALENDAR_DATE) AS IS_QUARTER_END,

        MONTH(CALENDAR_DATE) = 1
            AND DAY(CALENDAR_DATE) = 1 AS IS_YEAR_START,

        MONTH(CALENDAR_DATE) = 12
            AND DAY(CALENDAR_DATE) = 31 AS IS_YEAR_END

    FROM DATE_RANGE
);


select * from dim_date;