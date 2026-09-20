from airflow import DAG
from airflow.operators.bash import BashOperator
from airflow.operators.python import PythonOperator
from datetime import datetime
import random

def tarea_con_fallo_aleatorio(nombre_tarea, probabilidad_fallo=0.3):
    print(f"Ejecutando {nombre_tarea}...")
    numero_aleatorio = random.random()
    print(f"Número aleatorio generado: {numero_aleatorio:.2f} (falla si es menor a {probabilidad_fallo})")
    
    if numero_aleatorio < probabilidad_fallo:
        raise Exception(f"Fallo simulado en '{nombre_tarea}': timeout al conectar con la fuente de datos")
    
    print(f"{nombre_tarea} completada con éxito")

with DAG(
    dag_id="mi_primer_dag",
    description="DAG de práctica para explorar la interfaz de Airflow",
    schedule="@daily",
    start_date=datetime(2026, 9, 1),
    catchup=False,
    tags=["practica", "jct", "area_finanzas"],
) as dag:

    tarea_1 = BashOperator(
        task_id="extraccion",
        bash_command="echo 'Simulando extracción de datos desde Oracle...' && sleep 5",
    )

    tarea_2 = PythonOperator(
        task_id="transformacion",
        python_callable=tarea_con_fallo_aleatorio,
        op_kwargs={"nombre_tarea": "transformacion", "probabilidad_fallo": 0.2},
    )

    tarea_3 = BashOperator(
        task_id="carga",
        bash_command="echo 'Simulando carga a destino cloud...' && sleep 5",
    )

    tarea_1 >> tarea_2 >> tarea_3