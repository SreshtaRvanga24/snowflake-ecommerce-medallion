FROM apache/airflow:3.1.5

# Switch to root to install OS deps if needed
USER root

# (Optional but safe for dbt / snowflake)
RUN apt-get update && apt-get install -y \
    git \
    build-essential \
    && apt-get clean

# Back to airflow user
USER airflow

# Install Python packages
RUN pip install --no-cache-dir \
    pandas \
    numpy \
    snowflake-connector-python \
    apache-airflow-providers-snowflake \
    azure-storage-blob \
    boto3 \
    python-dotenv


RUN python -m venv /opt/airflow/dbt_venv && \
    PIP_USER=false /opt/airflow/dbt_venv/bin/pip install --no-cache-dir \
        dbt-core==1.12.7 dbt-snowflake==1.12.5
