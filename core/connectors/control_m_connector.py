"""
Implementacion del conector para Control-M, basada en lectura de archivos
de reporte exportados (patron de integracion real para ambientes legacy
on-premise sin API directa disponible, comun en banca).
"""

import csv
from core.connectors.base import OrchestratorConnector


class ControlMConnector(OrchestratorConnector):
    def __init__(self, config: dict):
        super().__init__(config)
        self.ruta_archivo = config["ruta_archivo"]

    def obtener_ejecuciones(self, job_name: str) -> list[dict]:
        filas = self._leer_archivo()
        resultado = []

        for fila in filas:
            if fila["job_name"] != job_name:
                continue
            resultado.append({
                "run_id": fila["run_id"],
                "task_id": fila["job_name"],  # Control-M no subdivide en tareas como Airflow
                "estado": "success" if fila["status"] == "OK" else "failed",
                "fecha_inicio": self._a_iso(fila["start_time"]),
                "fecha_fin": self._a_iso(fila["end_time"]),
                "intento": 1,
                "_mensaje_error": fila.get("error_message") or None,
            })
        return resultado

    def obtener_mensaje_error(self, job_name: str, run_id: str, task_id: str, intento: int) -> str | None:
        filas = self._leer_archivo()
        for fila in filas:
            if fila["job_name"] == job_name and fila["run_id"] == run_id:
                return fila.get("error_message") or None
        return None

    def obtener_tags(self, job_name: str) -> list[str]:
        filas = self._leer_archivo()
        for fila in filas:
            if fila["job_name"] == job_name:
                return [fila["application"], fila["sub_application"]]
        return []

    def _leer_archivo(self) -> list[dict]:
        with open(self.ruta_archivo, "r", encoding="utf-8") as f:
            return list(csv.DictReader(f))

    @staticmethod
    def _a_iso(fecha_str: str) -> str:
        # El CSV ya viene en formato ISO sin timezone; agregamos +00:00 para mantener consistencia
        return fecha_str + ".000000+0000"