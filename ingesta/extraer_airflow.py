"""
Script de ingesta: extrae ejecuciones de Airflow (dag runs + task instances)
vía su API REST y las guarda en Postgres (base de datos jct).
Para tareas fallidas, además extrae el mensaje de error desde los logs.
"""

import re
import requests
from requests.auth import HTTPBasicAuth
import psycopg2
from datetime import datetime

AIRFLOW_URL = "http://airflow-webserver:8080/api/v1"
AIRFLOW_USER = "admin"
AIRFLOW_PASSWORD = "admin"
TENANT_ID = "5f0af1ee-3d72-4f37-8aed-b5fd5a1903eb"  # Práctica Armando

DB_CONFIG = {
    "host": "postgres",
    "port": 5432,
    "dbname": "jct",
    "user": "airflow",
    "password": "airflow",
}


def obtener_dag_runs(dag_id):
    url = f"{AIRFLOW_URL}/dags/{dag_id}/dagRuns"
    respuesta = requests.get(url, auth=HTTPBasicAuth(AIRFLOW_USER, AIRFLOW_PASSWORD))
    respuesta.raise_for_status()
    return respuesta.json()["dag_runs"]


def obtener_task_instances(dag_id, run_id):
    url = f"{AIRFLOW_URL}/dags/{dag_id}/dagRuns/{run_id}/taskInstances"
    respuesta = requests.get(url, auth=HTTPBasicAuth(AIRFLOW_USER, AIRFLOW_PASSWORD))
    respuesta.raise_for_status()
    return respuesta.json()["task_instances"]


def obtener_tags_dag(dag_id):
    url = f"{AIRFLOW_URL}/dags/{dag_id}"
    respuesta = requests.get(url, auth=HTTPBasicAuth(AIRFLOW_USER, AIRFLOW_PASSWORD))
    respuesta.raise_for_status()
    tags = [t["name"] for t in respuesta.json().get("tags", [])]
    return tags


def obtener_mensaje_error(dag_id, run_id, task_id, intento):
    """
    Descarga el log de una tarea fallida y extrae la línea de la excepción.
    Devuelve None si no encuentra un mensaje claro.
    """
    url = f"{AIRFLOW_URL}/dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}/logs/{intento}"
    headers = {"Accept": "text/plain"}
    respuesta = requests.get(
        url, auth=HTTPBasicAuth(AIRFLOW_USER, AIRFLOW_PASSWORD), headers=headers
    )
    if respuesta.status_code != 200:
        return None

    log_texto = respuesta.text

    # Buscamos específicamente nuestra excepción personalizada
    match = re.search(r"Exception: (.+)", log_texto)
    if match:
        return match.group(1).strip()

    # Si no la encuentra, busca cualquier línea que empiece con "Error"
    match = re.search(r"(Error:.+)", log_texto)
    if match:
        return match.group(1).strip()[:500]  # limitamos longitud

    return None


def calcular_duracion(inicio, fin):
    if not inicio or not fin:
        return None
    formato = "%Y-%m-%dT%H:%M:%S.%f%z" if "." in inicio else "%Y-%m-%dT%H:%M:%S%z"
    try:
        t_inicio = datetime.strptime(inicio, formato)
        t_fin = datetime.strptime(fin, formato)
        return (t_fin - t_inicio).total_seconds()
    except ValueError:
        return None


def guardar_en_postgres(registros):
    conexion = psycopg2.connect(**DB_CONFIG)
    cursor = conexion.cursor()

    for r in registros:
        cursor.execute(
            """
            INSERT INTO job_executions
                (tenant_id, dag_id, run_id, task_id, estado, fecha_inicio, fecha_fin,
                 duracion_segundos, area, proyecto, mensaje_error)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (dag_id, run_id, task_id)
            DO UPDATE SET
                estado = EXCLUDED.estado,
                fecha_inicio = EXCLUDED.fecha_inicio,
                fecha_fin = EXCLUDED.fecha_fin,
                duracion_segundos = EXCLUDED.duracion_segundos,
                mensaje_error = EXCLUDED.mensaje_error,
                extraido_en = NOW();
            """,
            (
                TENANT_ID, r["dag_id"], r["run_id"], r["task_id"], r["estado"],
                r["fecha_inicio"], r["fecha_fin"], r["duracion_segundos"],
                r["area"], r["proyecto"], r["mensaje_error"],
            ),
        )

    conexion.commit()
    cursor.close()
    conexion.close()
    print(f"{len(registros)} registros guardados/actualizados en Postgres.")


def main():
    dag_id = "mi_primer_dag"
    tags = obtener_tags_dag(dag_id)
    area = tags[-1] if tags else "sin_area"
    proyecto = dag_id

    dag_runs = obtener_dag_runs(dag_id)
    print(f"Encontradas {len(dag_runs)} corridas del DAG '{dag_id}'.")

    registros = []
    fallos_procesados = 0

    for run in dag_runs:
        run_id = run["dag_run_id"]
        tareas = obtener_task_instances(dag_id, run_id)

        for tarea in tareas:
            mensaje_error = None

            if tarea["state"] == "failed":
                intento = tarea.get("try_number", 1)
                mensaje_error = obtener_mensaje_error(dag_id, run_id, tarea["task_id"], intento)
                fallos_procesados += 1

            registros.append({
                "dag_id": dag_id,
                "run_id": run_id,
                "task_id": tarea["task_id"],
                "estado": tarea["state"],
                "fecha_inicio": tarea["start_date"],
                "fecha_fin": tarea["end_date"],
                "duracion_segundos": calcular_duracion(tarea["start_date"], tarea["end_date"]),
                "area": area,
                "proyecto": proyecto,
                "mensaje_error": mensaje_error,
            })

    print(f"Procesados {fallos_procesados} logs de tareas fallidas.")
    guardar_en_postgres(registros)


if __name__ == "__main__":
    main()