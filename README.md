# E-commerce Analytics Platform on Snowflake

End-to-end medallion pipeline: two ingestion paths into Bronze, dbt to Silver,
Streams/Tasks and Dynamic Tables to Gold, Streamlit and BI on top.

## Architecture
_Diagram: docs/images/architecture.png_

| Path | Flow | Pattern |
|---|---|---|
| Azure | Azure Blob (Azurite emulator) -> Airflow/Python -> internal stage -> COPY INTO | Orchestrated batch |
| S3 | S3 -> external stage -> Snowpipe / Task -> COPY INTO | Snowflake-native |

## Scope decisions
- Azure Blob is emulated locally with Azurite (no Azure account available).
  The ingestion code is identical against real Azure; only the connection string changes.

## Setup
_TODO_

## Repository layout
_TODO_
