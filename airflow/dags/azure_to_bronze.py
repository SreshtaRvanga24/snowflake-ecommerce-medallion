from __future__ import annotations

import os
import re
import tempfile
from pathlib import Path

import pendulum

try:  # Airflow 3
    from airflow.sdk import dag, task
except ImportError:  # Airflow 2
    from airflow.decorators import dag, task


SOURCE_SYSTEM = "AZURE"
STAGE = "RETAIL_DB.BRONZE.STG_AZURE_INTERNAL"

# Landing folder -> Bronze table and its source columns, in CSV order.
TABLES = {
    "payments": {
        "table": "RETAIL_DB.BRONZE.PAYMENTS",
        "columns": [
            "PAYMENT_ID", "ORDER_ID", "CUSTOMER_ID", "PAYMENT_TS", "PAYMENT_METHOD",
            "PAYMENT_STATUS", "AMOUNT", "CURRENCY", "UPDATED_AT",
        ],
    },
    "order_status_events": {
        "table": "RETAIL_DB.BRONZE.ORDER_STATUS_EVENTS",
        "columns": [
            "EVENT_ID", "ORDER_ID", "STATUS", "EVENT_TS",
            "SOURCE_SYSTEM_CODE",  # CSV column SOURCE_SYSTEM
            "UPDATED_AT",
        ],
    },
}

LINEAGE_COLUMNS = [
    "SOURCE_SYSTEM", "SOURCE_FILE_NAME", "SOURCE_FILE_ROW_NUMBER",
    "SOURCE_FILE_LAST_MODIFIED", "LOAD_ID",
]


# helpers
def snowflake_conn():
    import snowflake.connector
    from cryptography.hazmat.primitives import serialization

    key_path = os.environ["SNOWFLAKE_PRIVATE_KEY_PATH"]
    with open(key_path, "rb") as fh:
        private_key = serialization.load_pem_private_key(fh.read(), password=None)
    private_key_der = private_key.private_bytes(
        encoding=serialization.Encoding.DER,
        format=serialization.PrivateFormat.PKCS8,
        encryption_algorithm=serialization.NoEncryption(),
    )
    return snowflake.connector.connect(
        account=os.environ["SNOWFLAKE_ACCOUNT"],
        user=os.environ["SNOWFLAKE_USER"],
        private_key=private_key_der,
        role=os.environ.get("SNOWFLAKE_ROLE", "LOADER_ROLE"),
        warehouse=os.environ.get("SNOWFLAKE_WAREHOUSE", "COMPUTE_WH"),
        database=os.environ.get("SNOWFLAKE_DATABASE", "RETAIL_DB"),
        schema="BRONZE",
    )


def blob_container():
    from azure.storage.blob import BlobServiceClient

    service = BlobServiceClient.from_connection_string(
        os.environ["AZURE_STORAGE_CONNECTION_STRING"]
    )
    return service.get_container_client(os.environ.get("AZURE_CONTAINER", "retail-landing"))


def safe_id(value: str) -> str:
    """Make a run id safe to inline into SQL."""
    return re.sub(r"[^A-Za-z0-9_.:+-]", "_", value)


def build_copy_sql(folder: str, staged_file: str, load_id: str) -> str:
    cfg = TABLES[folder]
    n = len(cfg["columns"])
    target_cols = ", ".join(cfg["columns"] + LINEAGE_COLUMNS)
    source_cols = ", ".join(f"${i}" for i in range(1, n + 1))
    return f"""
        COPY INTO {cfg['table']} ({target_cols})
        FROM (
          SELECT {source_cols},
                 '{SOURCE_SYSTEM}', METADATA$FILENAME, METADATA$FILE_ROW_NUMBER,
                 METADATA$FILE_LAST_MODIFIED, '{load_id}'
          FROM @{STAGE}/{folder}/
        )
        FILES = ('{staged_file}')
        FILE_FORMAT = (FORMAT_NAME = 'RETAIL_DB.BRONZE.FF_CSV')
        ON_ERROR = 'CONTINUE'
    """


# DAG
@dag(
    dag_id="azure_to_bronze",
    schedule="@daily",
    start_date=pendulum.datetime(2026, 11, 1, tz="UTC"),
    catchup=False,
    max_active_runs=1,
    tags=["ingestion", "bronze", "azure"],
)
def azure_to_bronze():

    @task
    def list_new_files() -> list[dict]:
        """Blobs in the known folders that are not yet logged as LOADED."""
        container = blob_container()
        candidates = [
            {"blob": b.name, "folder": b.name.split("/", 1)[0]}
            for b in container.list_blobs()
            if "/" in b.name
            and b.name.split("/", 1)[0] in TABLES
            and b.name.lower().endswith(".csv")
        ]

        with snowflake_conn() as conn:
            rows = conn.cursor().execute(
                "SELECT DISTINCT SOURCE_FILE_NAME FROM RETAIL_DB.AUDIT.FILE_INGESTION_LOG "
                "WHERE SOURCE_SYSTEM = %s AND STATUS = 'LOADED'",
                (SOURCE_SYSTEM,),
            ).fetchall()
        loaded = {r[0] for r in rows}

        new_files = [c for c in candidates if c["blob"] not in loaded]
        print(f"{len(candidates)} blobs found, {len(new_files)} new")
        return new_files

    @task
    def load_file(file: dict, **context) -> dict:
        blob_name, folder = file["blob"], file["folder"]
        table = TABLES[folder]["table"]
        load_id = safe_id(context["run_id"])
        file_name = Path(blob_name).name

        with snowflake_conn() as conn:
            cur = conn.cursor()
            cur.execute(
                "INSERT INTO RETAIL_DB.AUDIT.FILE_INGESTION_LOG "
                "(LOAD_ID, SOURCE_SYSTEM, SOURCE_FILE_NAME, TARGET_TABLE, STATUS) "
                "VALUES (%s, %s, %s, %s, 'STARTED')",
                (load_id, SOURCE_SYSTEM, blob_name, table),
            )

            def finish(status, parsed=None, loaded=None, errors=None, first_error=None):
                cur.execute(
                    "UPDATE RETAIL_DB.AUDIT.FILE_INGESTION_LOG "
                    "SET STATUS = %s, ROWS_PARSED = %s, ROWS_LOADED = %s, ERRORS_SEEN = %s, "
                    "    FIRST_ERROR_MESSAGE = %s, FINISHED_AT = CURRENT_TIMESTAMP() "
                    "WHERE LOAD_ID = %s AND SOURCE_FILE_NAME = %s AND STATUS = 'STARTED'",
                    (status, parsed, loaded, errors, first_error, load_id, blob_name),
                )

            try:
                with tempfile.TemporaryDirectory() as tmp:
                    local_path = Path(tmp) / file_name
                    with open(local_path, "wb") as fh:
                        blob_container().download_blob(blob_name).readinto(fh)

                    cur.execute(
                        f"PUT 'file://{local_path}' @{STAGE}/{folder}/ "
                        "AUTO_COMPRESS = TRUE OVERWRITE = TRUE"
                    )

                staged_file = f"{file_name}.gz"
                cur.execute(build_copy_sql(folder, staged_file, load_id))
                cols = [d[0].lower() for d in cur.description]
                results = [dict(zip(cols, row)) for row in cur.fetchall()]

                if not results or "rows_loaded" not in results[0]:
                    # COPY returns a single status message when it skips a file
                    # that its load history says was already loaded.
                    finish("SKIPPED", first_error="Already in COPY load history")
                    status = "SKIPPED"
                else:
                    r = results[0]
                    status = {
                        "LOADED": "LOADED",
                        "PARTIALLY_LOADED": "PARTIALLY_LOADED",
                    }.get(r["status"], "FAILED")
                    finish(status, r.get("rows_parsed"), r.get("rows_loaded"),
                           r.get("errors_seen"), r.get("first_error"))

                cur.execute(f"REMOVE @{STAGE}/{folder}/{staged_file}")

            except Exception as exc:
                finish("FAILED", first_error=str(exc)[:1000])
                raise

        print(f"{blob_name} -> {table}: {status}")
        if status == "FAILED":
            raise RuntimeError(f"COPY failed for {blob_name}; see AUDIT.FILE_INGESTION_LOG")
        return {"blob": blob_name, "status": status}

    load_file.expand(file=list_new_files())


azure_to_bronze()