"""
DAG: ежедневный пайплайн интернет-магазина.

generate_data -> data_quality_check -> spark_transform -> spark_build_marts

Идемпотентность: batch_date = logical_date DAG-рана. Генератор и Spark-джобы
пишут/перезаписывают данные конкретно за эту дату, поэтому повторный запуск
за тот же день не создаёт дублей (используется overwrite/ON CONFLICT).

Credentials are injected via environment variables from docker-compose.yml
(which reads from .env). Do NOT hardcode secrets in this file.
"""

from datetime import datetime, timedelta

from airflow import DAG
from airflow.operators.bash import BashOperator

default_args = {
    "owner": "data-eng",
    "retries": 2,
    "retry_delay": timedelta(minutes=2),
}

with DAG(
    dag_id="ecommerce_pipeline",
    description="Ежедневный ETL пайплайн интернет-магазина: Postgres -> Spark -> ClickHouse",
    default_args=default_args,
    schedule_interval="@daily",
    start_date=datetime(2024, 1, 1),
    catchup=False,
    max_active_runs=1,
    tags=["portfolio", "ecommerce", "spark", "clickhouse"],
) as dag:

    generate_data = BashOperator(
        task_id="generate_data",
        bash_command=(
            "python /opt/airflow/generator/generate_data.py "
            "--batch-date {{ ds }} --n-users-new 20 --n-events 1500"
        ),
    )

    data_quality_check = BashOperator(
        task_id="data_quality_check",
        bash_command=(
            "python /opt/airflow/quality_checks/validate.py --batch-date {{ ds }}"
        ),
    )

    spark_transform = BashOperator(
        task_id="spark_transform_orders",
        bash_command=(
            "spark-submit --master local[*] "
            "/opt/airflow/spark_jobs/transform_orders.py --batch-date {{ ds }}"
        ),
    )

    spark_build_marts = BashOperator(
        task_id="spark_build_marts",
        bash_command=(
            "spark-submit --master local[*] "
            "/opt/airflow/spark_jobs/build_marts.py --batch-date {{ ds }}"
        ),
    )

    generate_data >> data_quality_check >> spark_transform >> spark_build_marts
