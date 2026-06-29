FROM apache/airflow:2.9.3-python3.11

USER root
RUN apt-get update \
 && apt-get install -y --no-install-recommends git \
 && apt-get clean && rm -rf /var/lib/apt/lists/*
USER airflow

# Provider installed against Airflow's official constraints so pip cannot
# alter airflow core itself (unconstrained installs silently break the image)
RUN pip install --no-cache-dir \
    "apache-airflow-providers-databricks" \
    --constraint "https://raw.githubusercontent.com/apache/airflow/constraints-2.9.3/constraints-3.11.txt"

# dbt gets its OWN venv so its pins never collide with Airflow's.
# Pinned to MATCH the host version so "works on host, breaks in container" can't happen.
RUN python -m venv /home/airflow/dbt-venv \
 && /home/airflow/dbt-venv/bin/pip install --no-cache-dir "dbt-databricks==1.12.1"
