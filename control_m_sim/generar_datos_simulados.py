"""
Genera un archivo CSV que simula el reporte de ejecuciones que Control-M
exporta periodicamente (patron real de integracion para ambientes legacy
sin API directa disponible).
"""

import csv
import random
from datetime import datetime, timedelta

JOB_NAME = "BCP_ETL_CIERRE_DIARIO"
APPLICATION = "FINANZAS"
SUB_APPLICATION = "CIERRE_CONTABLE"

MENSAJES_ERROR = [
    "NOTOK: Return code 8 - Timeout esperando archivo de entrada en servidor FTP",
    "NOTOK: Return code 12 - Conexion rechazada a base de datos origen (Oracle)",
    "NOTOK: Return code 4 - Espacio insuficiente en disco destino",
]

filas = []
ahora = datetime.now()

for i in range(12):
    inicio = ahora - timedelta(hours=(12 - i) * 2)
    duracion_minutos = random.randint(3, 15)
    fin = inicio + timedelta(minutes=duracion_minutos)
    es_fallo = random.random() < 0.6

    filas.append({
        "job_name": JOB_NAME,
        "run_id": f"CTM{inicio.strftime('%Y%m%d%H%M%S')}",
        "application": APPLICATION,
        "sub_application": SUB_APPLICATION,
        "status": "NOTOK" if es_fallo else "OK",
        "start_time": inicio.strftime("%Y-%m-%dT%H:%M:%S"),
        "end_time": fin.strftime("%Y-%m-%dT%H:%M:%S"),
        "return_code": random.choice([4, 8, 12]) if es_fallo else 0,
        "error_message": random.choice(MENSAJES_ERROR) if es_fallo else "",
    })

with open("control_m_sim/control_m_jobs.csv", "w", newline="", encoding="utf-8") as f:
    campos = ["job_name", "run_id", "application", "sub_application", "status",
              "start_time", "end_time", "return_code", "error_message"]
    writer = csv.DictWriter(f, fieldnames=campos)
    writer.writeheader()
    writer.writerows(filas)

print(f"Generadas {len(filas)} filas simuladas en control_m_sim/control_m_jobs.csv")