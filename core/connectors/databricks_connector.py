"""
Implementacion del conector para Databricks Jobs API.
Traduce las llamadas a la API REST de Databricks al formato estandar
que el resto del sistema espera (el mismo contrato que AirflowConnector).
"""

import requests
from datetime import datetime, timezone
from core.connectors.base import OrchestratorConnector


class DatabricksConnector(OrchestratorConnector):
    def __init__(self, config: dict):
        super().__init__(config)
        self.base_url = f"https://{config['host']}/api/2.1"
        self.headers = {"Authorization": f"Bearer {config['token']}"}

    def obtener_ejecuciones(self, job_id: str) -> list[dict]:
        url = f"{self.base_url}/jobs/runs/list"
        params = {"job_id": job_id, "limit": 25}
        respuesta = requests.get(url, headers=self.headers, params=params)
        respuesta.raise_for_status()
        runs = respuesta.json().get("runs", [])

        resultado = []
        for run in runs:
            run_id = str(run["run_id"])
            tareas = run.get("tasks", [])

            if not tareas:
                # Jobs simples de una sola tarea pueden no traer sub-tareas separadas
                tareas = [{
                    "task_key": "ejecucion",
                    "run_id": run["run_id"],
                    "state": run.get("state", {}),
                    "start_time": run.get("start_time"),
                    "end_time": run.get("end_time"),
                }]

            for tarea in tareas:
                estado_raw = tarea.get("state", {})
                resultado.append({
                    "run_id": run_id,
                    "task_id": tarea.get("task_key", "ejecucion"),
                    "estado": self._mapear_estado(estado_raw),
                    "fecha_inicio": self._epoch_a_iso(tarea.get("start_time")),
                    "fecha_fin": self._epoch_a_iso(tarea.get("end_time")),
                    "intento": 1,
                    "_run_id_tarea": tarea.get("run_id", run["run_id"]),  # usado internamente para pedir el log
                })
        return resultado

    def obtener_mensaje_error(self, job_id: str, run_id: str, task_id: str, intento: int) -> str | None:
        # Para Databricks usamos directamente el run_id de la tarea (ya viene calculado en obtener_ejecuciones)
        url = f"{self.base_url}/jobs/runs/get-output"
        respuesta = requests.get(url, headers=self.headers, params={"run_id": run_id})
        if respuesta.status_code != 200:
            return None
        datos = respuesta.json()
        error = datos.get("error")
        if error:
            return error[:500]
        return None

    def obtener_tags(self, job_id: str) -> list[str]:
        url = f"{self.base_url}/jobs/get"
        respuesta = requests.get(url, headers=self.headers, params={"job_id": job_id})
        respuesta.raise_for_status()
        tags = respuesta.json().get("settings", {}).get("tags", {})
        return list(tags.values()) if tags else []

    @staticmethod
    def _mapear_estado(estado_raw: dict) -> str:
        life_cycle = estado_raw.get("life_cycle_state", "")
        result = estado_raw.get("result_state", "")
        if result == "SUCCESS":
            return "success"
        if result == "FAILED":
            return "failed"
        if life_cycle in ("PENDING", "RUNNING"):
            return "running"
        return (result or life_cycle or "unknown").lower()

    @staticmethod
    def _epoch_a_iso(epoch_ms) -> str | None:
        if not epoch_ms:
            return None
        return datetime.fromtimestamp(epoch_ms / 1000, tz=timezone.utc).strftime("%Y-%m-%dT%H:%M:%S.%f%z")