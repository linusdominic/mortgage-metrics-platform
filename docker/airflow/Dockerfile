# Airflow 3 image with the pipeline package, dbt and Cosmos installed.
#
# The project is copied in, not mounted: the image is the deployable unit, and
# the dbt manifest Cosmos renders the task group from is baked at build time.
FROM apache/airflow:3.4.0-python3.12

USER airflow

# Exact versions exported from uv.lock (`make airflow-requirements`), so the
# container runs the same dbt, DuckDB, MetricFlow and Cosmos as local runs.
COPY --chown=airflow:0 docker/airflow/requirements.txt /tmp/requirements.txt
RUN pip install --no-cache-dir -r /tmp/requirements.txt

COPY --chown=airflow:0 pyproject.toml README.md LICENSE /opt/airflow/project/
COPY --chown=airflow:0 src /opt/airflow/project/src
COPY --chown=airflow:0 config /opt/airflow/project/config
COPY --chown=airflow:0 dbt /opt/airflow/project/dbt
COPY --chown=airflow:0 dags /opt/airflow/project/dags
COPY --chown=airflow:0 evals /opt/airflow/project/evals
RUN pip install --no-cache-dir --no-deps /opt/airflow/project

ENV MMP_REPO_ROOT=/opt/airflow/project \
    MMP_DATA_DIR=/opt/airflow/data \
    DBT_PROFILES_DIR=/opt/airflow/project/dbt \
    DO_NOT_TRACK=1 \
    DBT_SEND_ANONYMOUS_USAGE_STATS=false

# Parse once so target/manifest.json exists for Cosmos (LoadMode.DBT_MANIFEST).
RUN cd /opt/airflow/project/dbt \
    && MMP_DATA_DIR=/tmp/parse dbt parse --target-path /opt/airflow/project/dbt/target \
    && rm -rf /tmp/parse /opt/airflow/project/dbt/logs
