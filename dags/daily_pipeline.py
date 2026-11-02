"""Airflow-style DAG definition (declarative task graph).

In production this would be an Airflow DAG with PythonOperators / dbt
operators scheduled daily; here the same graph — task_id, callable,
depends_on — is executed by src/run_pipeline.py, so the orchestration
logic (ordering, retries, failure propagation) is real and testable
without an Airflow server.
"""
DAG = {
    "dag_id": "order_events_daily",
    "schedule": "daily",
    "tasks": [
        {"task_id": "ingest", "depends_on": []},
        {"task_id": "build_staging", "depends_on": ["ingest"]},
        {"task_id": "build_intermediate", "depends_on": ["build_staging"]},
        {"task_id": "build_marts", "depends_on": ["build_intermediate"]},
        {"task_id": "run_tests", "depends_on": ["build_marts"]},
        {"task_id": "publish", "depends_on": ["run_tests"]},
    ],
}
