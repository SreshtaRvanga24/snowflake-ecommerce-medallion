USE ROLE LOADER_ROLE;
USE WAREHOUSE COMPUTE_WH;
USE SCHEMA RETAIL_DB.BRONZE;
 
/* ---------------------------------------------------------------------
   1. Load procedure
   EXECUTE AS OWNER: whoever calls it (you, or the task) runs with
   LOADER_ROLE's privileges and nothing more.
   --------------------------------------------------------------------- */
CREATE OR REPLACE PROCEDURE SP_LOAD_S3()
RETURNS STRING
LANGUAGE SQL
EXECUTE AS OWNER
AS
$$
DECLARE
  load_id  STRING;
  started  TIMESTAMP_LTZ;
  copy_sql STRING;
  logged   INTEGER;
BEGIN
  started := CURRENT_TIMESTAMP();
  load_id := 'S3_' || TO_CHAR(started, 'YYYYMMDD_HH24MISS');
 
  -- CUSTOMERS
  copy_sql := 'COPY INTO RETAIL_DB.BRONZE.CUSTOMERS
      (CUSTOMER_ID, CUSTOMER_NAME, EMAIL, REGION, CITY, SEGMENT, SIGNUP_DATE, IS_ACTIVE, UPDATED_AT,
       SOURCE_SYSTEM, SOURCE_FILE_NAME, SOURCE_FILE_ROW_NUMBER, SOURCE_FILE_LAST_MODIFIED, LOAD_ID)
    FROM (SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9,
                 ''S3'', METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, METADATA$FILE_LAST_MODIFIED, ''' || load_id || '''
          FROM @RETAIL_DB.BRONZE.STG_S3_LANDING/customers/)
    PATTERN = ''.*[.]csv''
    ON_ERROR = ''CONTINUE''';
  EXECUTE IMMEDIATE :copy_sql;
 
  -- ORDERS
  copy_sql := 'COPY INTO RETAIL_DB.BRONZE.ORDERS
      (ORDER_ID, CUSTOMER_ID, ORDER_DATE, ORDER_TS, ORDER_STATUS, SALES_CHANNEL, SUBTOTAL,
       DISCOUNT_AMOUNT, ORDER_AMOUNT, CURRENCY, UPDATED_AT,
       SOURCE_SYSTEM, SOURCE_FILE_NAME, SOURCE_FILE_ROW_NUMBER, SOURCE_FILE_LAST_MODIFIED, LOAD_ID)
    FROM (SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9, $10, $11,
                 ''S3'', METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, METADATA$FILE_LAST_MODIFIED, ''' || load_id || '''
          FROM @RETAIL_DB.BRONZE.STG_S3_LANDING/orders/)
    PATTERN = ''.*[.]csv''
    ON_ERROR = ''CONTINUE''';
  EXECUTE IMMEDIATE :copy_sql;
 
  -- ORDER_ITEMS
  copy_sql := 'COPY INTO RETAIL_DB.BRONZE.ORDER_ITEMS
      (ORDER_ITEM_ID, ORDER_ID, PRODUCT_ID, PRODUCT_NAME, CATEGORY, QUANTITY, UNIT_PRICE, LINE_AMOUNT, UPDATED_AT,
       SOURCE_SYSTEM, SOURCE_FILE_NAME, SOURCE_FILE_ROW_NUMBER, SOURCE_FILE_LAST_MODIFIED, LOAD_ID)
    FROM (SELECT $1, $2, $3, $4, $5, $6, $7, $8, $9,
                 ''S3'', METADATA$FILENAME, METADATA$FILE_ROW_NUMBER, METADATA$FILE_LAST_MODIFIED, ''' || load_id || '''
          FROM @RETAIL_DB.BRONZE.STG_S3_LANDING/order_items/)
    PATTERN = ''.*[.]csv''
    ON_ERROR = ''CONTINUE''';
  EXECUTE IMMEDIATE :copy_sql;
 
  -- Audit: one row per file loaded by this call (skipped files don't appear)
  INSERT INTO RETAIL_DB.AUDIT.FILE_INGESTION_LOG
    (LOAD_ID, SOURCE_SYSTEM, SOURCE_FILE_NAME, TARGET_TABLE, STATUS,
     ROWS_PARSED, ROWS_LOADED, ERRORS_SEEN, FIRST_ERROR_MESSAGE, STARTED_AT, FINISHED_AT)
  SELECT :load_id, 'S3', FILE_NAME, 'RETAIL_DB.BRONZE.' || TABLE_NAME,
         IFF(STATUS = 'LOAD_FAILED', 'FAILED', STATUS),
         ROW_PARSED, ROW_COUNT, ERROR_COUNT, FIRST_ERROR_MESSAGE,
         :started, LAST_LOAD_TIME
  FROM RETAIL_DB.INFORMATION_SCHEMA.LOAD_HISTORY
  WHERE SCHEMA_NAME = 'BRONZE'
    AND TABLE_NAME IN ('CUSTOMERS', 'ORDERS', 'ORDER_ITEMS')
    AND LAST_LOAD_TIME >= :started;
 
  logged := SQLROWCOUNT;
  RETURN load_id || ': ' || logged || ' file(s) loaded';
END;
$$;
 
/* ---------------------------------------------------------------------
   2. Test by hand before scheduling anything
   --------------------------------------------------------------------- */
CALL SP_LOAD_S3();        -- with files uploaded: "S3_...: 3 file(s) loaded"
CALL SP_LOAD_S3();        -- again: "0 file(s) loaded" (COPY skipped them)
 
/* ---------------------------------------------------------------------
   3. Schedule
   Every 6 hours. Each run wakes the XS warehouse for about a minute
   (60-second minimum billing). Tasks are created SUSPENDED.
   --------------------------------------------------------------------- */
CREATE TASK IF NOT EXISTS TASK_S3_INGEST
  WAREHOUSE = COMPUTE_WH
  SCHEDULE = 'USING CRON 0 */6 * * * UTC'
  COMMENT = 'Loads new S3 files into Bronze'
AS
  CALL RETAIL_DB.BRONZE.SP_LOAD_S3();
 
ALTER TASK TASK_S3_INGEST RESUME;
 
-- Run it now instead of waiting for the schedule
EXECUTE TASK TASK_S3_INGEST;
 
-- Check runs (allow ~1 minute after EXECUTE TASK)
SELECT NAME, STATE, ERROR_MESSAGE, SCHEDULED_TIME, COMPLETED_TIME
FROM TABLE(RETAIL_DB.INFORMATION_SCHEMA.TASK_HISTORY(TASK_NAME => 'TASK_S3_INGEST'))
ORDER BY SCHEDULED_TIME DESC;
 
-- To stop the schedule (e.g. when not working on the project):
-- ALTER TASK TASK_S3_INGEST SUSPEND;
