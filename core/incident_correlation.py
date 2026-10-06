"""
Motor de correlacion de incidentes: agrupa fallos relacionados de una
misma corrida en UN incidente, con causa raiz y tareas afectadas,
en vez de mostrar N alarmas sueltas.
"""

import psycopg2
import psycopg2.extras

DB_CONFIG = {
    "host": "postgres",
    "port": 5432,
    "dbname": "jct",
    "user": "airflow",
    "password": "airflow",
}


def calcular_severidad(cantidad_afectados: int) -> str:
    if cantidad_afectados >= 3:
        return "ALTA"
    if cantidad_afectados >= 1:
        return "MEDIA"
    return "BAJA"


def correlacionar_incidentes(conexion):
    cursor = conexion.cursor(cursor_factory=psycopg2.extras.RealDictCursor)

    # Trae todas las corridas que tienen AL MENOS una tarea en estado 'failed'
    cursor.execute("""
        SELECT DISTINCT tenant_id, data_source_id, dag_id, run_id
        FROM job_executions
        WHERE estado = 'failed'
    """)
    corridas_con_falla = cursor.fetchall()

    print(f"Analizando {len(corridas_con_falla)} corridas con al menos una falla...")

    for corrida in corridas_con_falla:
        cursor.execute("""
            SELECT task_id, estado, mensaje_error, area, proyecto
            FROM job_executions
            WHERE tenant_id = %s AND dag_id = %s AND run_id = %s
        """, (corrida["tenant_id"], corrida["dag_id"], corrida["run_id"]))
        tareas = cursor.fetchall()

        causa_raiz = next((t for t in tareas if t["estado"] == "failed"), None)
        afectados = [t["task_id"] for t in tareas if t["estado"] == "upstream_failed"]

        if not causa_raiz:
            continue

        severidad = calcular_severidad(len(afectados))

        cursor.execute("""
            INSERT INTO incidents
                (tenant_id, data_source_id, dag_id, run_id, causa_raiz_task_id,
                 causa_raiz_mensaje, tareas_afectadas, cantidad_afectados,
                 area, proyecto, severidad)
            VALUES (%s, %s, %s, %s, %s, %s, %s, %s, %s, %s, %s)
            ON CONFLICT (tenant_id, dag_id, run_id)
            DO UPDATE SET
                causa_raiz_task_id = EXCLUDED.causa_raiz_task_id,
                causa_raiz_mensaje = EXCLUDED.causa_raiz_mensaje,
                tareas_afectadas = EXCLUDED.tareas_afectadas,
                cantidad_afectados = EXCLUDED.cantidad_afectados,
                severidad = EXCLUDED.severidad,
                actualizado_en = NOW();
        """, (
            corrida["tenant_id"], corrida["data_source_id"], corrida["dag_id"], corrida["run_id"],
            causa_raiz["task_id"], causa_raiz["mensaje_error"], afectados, len(afectados),
            causa_raiz["area"], causa_raiz["proyecto"], severidad,
        ))

    conexion.commit()
    cursor.close()
    print(f"Correlacion completada: {len(corridas_con_falla)} incidentes procesados.")


def main():
    conexion = psycopg2.connect(**DB_CONFIG)
    correlacionar_incidentes(conexion)
    conexion.close()


if __name__ == "__main__":
    main()