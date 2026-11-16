"""
daily_retail_pipeline
Runs the whole medallion pipeline every evening at 21:00 IST.

    batch_date
      -> generate_batch
           -> upload_azurite -> load_azure_to_bronze (triggers the azure_to_bronze DAG)
           -> upload_s3      -> load_s3_to_bronze    (CALL SP_LOAD_S3 in Snowflake)
      -> dbt_build                                   (Silver + customer SCD2 snapshot)
      -> refresh_gold                                (EXECUTE TASK, then wait for the result)
      -> gold_checks                                 (reconciliation; fails the run if Gold is wrong)

Every step is safe to rerun: generation skips an existing batch, uploads skip
existing files, Bronze loads skip already-loaded files, Silver merges only
real changes, and Gold is rebuilt from Silver.

Container requirements (see docker-compose changes):
  /opt/airflow/project       the repo (read-write: batches, generator state, dbt target)
  /opt/airflow/keys          Snowflake private key
  /home/airflow/.dbt         dbt profiles.yml (reads SNOWFLAKE_* env vars)
  /opt/airflow/dbt_venv      dbt installed in its own virtualenv (Dockerfile)
"""
from __future__ import annotations

import os
import time
from datetime import timedelta

import pendulum

try:  # Airflow 3
    from airflow.sdk import dag, task
except ImportError:  # Airflow 2
    from airflow.decorators import dag, task

try:  # Airflow 3
    from airflow.providers.standard.operators.trigger_dagrun import TriggerDagRunOperator
except ImportError:  # Airflow 2
    from airflow.operators.trigger_dagrun import TriggerDagRunOperator

PROJECT = os.environ.get("PROJECT_DIR", "/opt/airflow/project")
DBT = "/opt/airflow/dbt_venv/bin/dbt"
LOCAL_TZ = "Asia/Kolkata"


def snowflake_conn(role: str):
    """Key-pair connection as PIPELINE_SVC, using the given role."""
    import snowflake.connector
    from cryptography.hazmat.primitives import serialization

    with open(os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"], "rb") as fh:
        key = serialization.load_pem_private_key(fh.read(), password=None)
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        private_key=key.private_bytes(
            encoding=serialization.Encoding.DER,
            format=serialization.PrivateFormat.PKCS8,
            encryption_algorithm=serialization.NoEncryption(),
        ),
        role=role,
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        database=os.environ.get("SNOWFLAKE_DATABASE", "RETAIL_DB"),
    )


@dag(
    dag_id="daily_retail_pipeline",
    schedule="0 21 * * *",                                   # 21:00 every day...
    start_date=pendulum.datetime(2026, 11, 1, tz=LOCAL_TZ),  # ...in IST
    catchup=False,
    max_active_runs=1,
    default_args={"retries": 1, "retry_delay": timedelta(minutes=2)},
    tags=["pipeline", "daily"],
)
def daily_retail_pipeline():

    @task
    def batch_date(**context) -> str:
        """The day this run produces data for, in IST."""
        when = (context.get("logical_date")
                or getattr(context["dag_run"], "run_after", None)
                or pendulum.now())
        return pendulum.instance(when).in_timezone(LOCAL_TZ).to_date_string()

    @task.bash(cwd=PROJECT)
    def generate_batch(day: str) -> str:
        # Skip if the batch already exists, so a rerun never generates twice
        return (f'if [ -d "datasets/batches/{day}" ]; then echo "Batch {day} exists, skipping"; '
                f'else python ingestion/generator/generate_daily_batch.py --batch-date {day}; fi')

    @task.bash(cwd=PROJECT)
    def upload_azurite(day: str) -> str:
        return f"python ingestion/azure/azurite_upload.py --src datasets/batches/{day} --batch-date {day}"

    @task.bash(cwd=PROJECT)
    def upload_s3(day: str) -> str:
        return f"python ingestion/s3/s3_upload.py --src datasets/batches/{day} --batch-date {day}"

    load_azure_to_bronze = TriggerDagRunOperator(
        task_id="load_azure_to_bronze",
        trigger_dag_id="azure_to_bronze",
        wait_for_completion=True,          # this DAG waits until the Azure load finishes
        poke_interval=20,
        allowed_states=["success"],
        failed_states=["failed"],
    )

    @task
    def load_s3_to_bronze() -> str:
        with snowflake_conn("LOADER_ROLE") as conn:
            result = conn.cursor().execute("CALL RETAIL_DB.BRONZE.SP_LOAD_S3()").fetchone()[0]
        print(result)
        return result

    @task.bash(cwd=f"{PROJECT}/dbt")
    def dbt_build() -> str:
        return f"{DBT} deps && {DBT} build"

    @task(execution_timeout=timedelta(minutes=30))
    def refresh_gold() -> str:
        """Start the Gold task in Snowflake, then poll its history until it finishes."""
        with snowflake_conn("TRANSFORMER_ROLE") as conn:
            cur = conn.cursor()
            started = cur.execute("SELECT CURRENT_TIMESTAMP()").fetchone()[0]
            cur.execute("EXECUTE TASK RETAIL_DB.GOLD.REFRESH_GOLD")

            while True:
                time.sleep(15)
                row = cur.execute(
                    """
                    SELECT state, error_message
                    FROM TABLE(RETAIL_DB.INFORMATION_SCHEMA.TASK_HISTORY(
                        TASK_NAME => 'REFRESH_GOLD',
                        SCHEDULED_TIME_RANGE_START => DATEADD(minute, -1, %s::TIMESTAMP_LTZ)))
                    ORDER BY scheduled_time DESC
                    LIMIT 1
                    """,
                    (started,),
                ).fetchone()
                state = row[0] if row else "NOT_YET_VISIBLE"
                print(f"REFRESH_GOLD state: {state}")
                if state == "SUCCEEDED":
                    return state
                if state in ("FAILED", "CANCELLED"):
                    raise RuntimeError(f"REFRESH_GOLD {state}: {row[1]}")

    @task
    def gold_checks() -> None:
        """Fail the run if Gold doesn't reconcile with Silver."""
        checks = {
            "revenue difference Gold vs Silver":
                "SELECT ABS((SELECT SUM(order_amount) FROM RETAIL_DB.GOLD.FACT_ORDERS) - "
                "(SELECT SUM(order_amount) FROM RETAIL_DB.SILVER.ORDERS))",
            "order count difference Gold vs Silver":
                "SELECT ABS((SELECT COUNT(*) FROM RETAIL_DB.GOLD.FACT_ORDERS) - "
                "(SELECT COUNT(*) FROM RETAIL_DB.SILVER.ORDERS))",
            "duplicate order_ids in FACT_ORDERS":
                "SELECT COUNT(*) FROM (SELECT order_id FROM RETAIL_DB.GOLD.FACT_ORDERS "
                "GROUP BY 1 HAVING COUNT(*) > 1)",
            "orders without a customer version":
                "SELECT COUNT(*) FROM RETAIL_DB.GOLD.FACT_ORDERS WHERE customer_sk IS NULL",
            "customers without exactly one current version":
                "SELECT COUNT(*) FROM (SELECT customer_id FROM RETAIL_DB.GOLD.DIM_CUSTOMERS "
                "GROUP BY 1 HAVING COUNT_IF(is_current) <> 1)",
        }
        failures = []
        with snowflake_conn("TRANSFORMER_ROLE") as conn:
            cur = conn.cursor()
            for name, sql in checks.items():
                value = cur.execute(sql).fetchone()[0] or 0
                status = "OK  " if value < 0.01 else "FAIL"
                print(f"{status} {name}: {value}")
                if value >= 0.01:
                    failures.append(f"{name} = {value}")
        if failures:
            raise ValueError("Gold checks failed: " + "; ".join(failures))

    day = batch_date()
    generated = generate_batch(day)

    azure = upload_azurite(day)
    s3 = upload_s3(day)
    generated >> [azure, s3]

    azure >> load_azure_to_bronze
    s3_loaded = load_s3_to_bronze()
    s3 >> s3_loaded

    silver = dbt_build()
    [load_azure_to_bronze, s3_loaded] >> silver

    gold = refresh_gold()
    silver >> gold >> gold_checks()


daily_retail_pipeline()