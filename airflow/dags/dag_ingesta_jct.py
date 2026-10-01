from airflow import DAG
from airflow.operators.bash import BashOperator
from datetime import datetime, timedelta

with DAG(
    dag_id="ingesta_job_control_tower",
    description="Extrae métricas de ejecución desde Airflow hacia Postgres para el JCT",
    schedule=timedelta(minutes=5),
    start_date=datetime(2026, 9, 1),
    catchup=False,
    tags=["jct", "infraestructura"],
) as dag:

    ejecutar_ingesta = BashOperator(
        task_id="extraer_metricas",
        bash_command="pip install requests psycopg2-binary --quiet && python /opt/airflow/scripts/motor_ingesta.py",
    )