"""
Motor de ingesta multi-tenant: para cada tenant y cada fuente de datos
configurada, usa el conector correspondiente (via ConnectorFactory) y
guarda las ejecuciones en Postgres.
"""

import sys
import os
sys.path.insert(0, "/opt/airflow/scripts" if os.path.exists("/opt/airflow/scripts") else ".")

import psycopg2
from datetime import datetime
from core.connectors.factory import ConnectorFactory

DB_CONFIG = {
    "host": "postgres",
    "port": 5432,
    "dbname": "jct",
    "user": "airflow",
    "password": "airflow",
}

DAG_ID_PRACTICA = "mi_primer_dag"  # luego esto vendra de tenant_data_sources


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


def obtener_fuentes_activas(conexion):
    """Trae todas las fuentes de datos activas, de todos los tenants."""
    cursor = conexion.cursor()
    cursor.execute("""
        SELECT ds.id, ds.tenant_id, ds.tipo_orquestador, ds.config_conexion, ds.nombre
        FROM tenant_data_sources ds
        WHERE ds.activo = TRUE
    """)
    columnas = ["id", "tenant_id", "tipo_orquestador", "config_conexion", "nombre"]
    filas = [dict(zip(columnas, fila)) for fila in cursor.fetchall()]
    cursor.close()
    return filas


def guardar_en_postgres(conexion, tenant_id, data_source_id, dag_id, proyecto, area, registros):
    cursor = conexion.cursor()
    for r in registros:
        cursor.execute(
            """
            INSERT INTO job_executions
                (tenant_id, data_source_id, dag_id, run_id, task_id, estado,
                 fecha_inicio, fecha_fin, duracion_segundos, area, proyecto, mensaje_error)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
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
                tenant_id, data_source_id, dag_id, r["run_id"], r["task_id"], r["estado"],
                r["fecha_inicio"], r["fecha_fin"], r["duracion_segundos"],
                area, proyecto, r.get("mensaje_error"),
            ),
        )
    conexion.commit()
    cursor.close()


def procesar_fuente(conexion, fuente):
    print(f"Procesando fuente '{fuente['nombre']}' (tipo: {fuente['tipo_orquestador']})...")

    config = fuente["config_conexion"]
    conector = ConnectorFactory.crear(fuente["tipo_orquestador"], config)

    dag_id = DAG_ID_PRACTICA  # fase siguiente: esto vendra de config
    tags = conector.obtener_tags(dag_id)
    area = tags[-1] if tags else "sin_area"

    ejecuciones = conector.obtener_ejecuciones(dag_id)
    print(f"  {len(ejecuciones)} registros de ejecucion encontrados.")

    for ejecucion in ejecuciones:
        if ejecucion["estado"] == "failed":
            ejecucion["mensaje_error"] = conector.obtener_mensaje_error(
                dag_id, ejecucion["run_id"], ejecucion["task_id"], ejecucion["intento"]
            )
        ejecucion["duracion_segundos"] = calcular_duracion(
            ejecucion["fecha_inicio"], ejecucion["fecha_fin"]
        )

    guardar_en_postgres(
        conexion, fuente["tenant_id"], fuente["id"], dag_id, dag_id, area, ejecuciones
    )
    print(f"  {len(ejecuciones)} registros guardados para tenant {fuente['tenant_id']}.")


def main():
    conexion = psycopg2.connect(**DB_CONFIG)
    fuentes = obtener_fuentes_activas(conexion)
    print(f"Encontradas {len(fuentes)} fuentes de datos activas.")

    for fuente in fuentes:
        try:
            procesar_fuente(conexion, fuente)
        except Exception as e:
            print(f"  ERROR procesando fuente '{fuente['nombre']}': {e}")
            # el patron Circuit Breaker en miniatura: un error aqui no detiene a las demas fuentes

    conexion.close()


if __name__ == "__main__":
    main()