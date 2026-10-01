"""
Implementacion del conector para Apache Airflow.
Traduce las llamadas a la API REST de Airflow al formato estandar
que el resto del sistema espera.
"""

import re
import requests
from requests.auth import HTTPBasicAuth
from core.connectors.base import OrchestratorConnector


class AirflowConnector(OrchestratorConnector):
    def __init__(self, config: dict):
        super().__init__(config)
        self.base_url = config["url"]
        self.auth = HTTPBasicAuth(config["usuario"], config["password"])

    def obtener_ejecuciones(self, dag_id: str) -> list[dict]:
        dag_runs = self._obtener_dag_runs(dag_id)
        resultado = []

        for run in dag_runs:
            run_id = run["dag_run_id"]
            tareas = self._obtener_task_instances(dag_id, run_id)
            for tarea in tareas:
                resultado.append({
                    "run_id": run_id,
                    "task_id": tarea["task_id"],
                    "estado": tarea["state"],
                    "fecha_inicio": tarea["start_date"],
                    "fecha_fin": tarea["end_date"],
                    "intento": tarea.get("try_number", 1),
                })
        return resultado

    def obtener_mensaje_error(self, dag_id: str, run_id: str, task_id: str, intento: int) -> str | None:
        url = f"{self.base_url}/dags/{dag_id}/dagRuns/{run_id}/taskInstances/{task_id}/logs/{intento}"
        respuesta = requests.get(url, auth=self.auth, headers={"Accept": "text/plain"})
        if respuesta.status_code != 200:
            return None

        log_texto = respuesta.text
        match = re.search(r"Exception: (.+)", log_texto)
        if match:
            return match.group(1).strip()
        match = re.search(r"(Error:.+)", log_texto)
        if match:
            return match.group(1).strip()[:500]
        return None

    def obtener_tags(self, dag_id: str) -> list[str]:
        url = f"{self.base_url}/dags/{dag_id}"
        respuesta = requests.get(url, auth=self.auth)
        respuesta.raise_for_status()
        return [t["name"] for t in respuesta.json().get("tags", [])]

    def _obtener_dag_runs(self, dag_id):
        url = f"{self.base_url}/dags/{dag_id}/dagRuns"
        respuesta = requests.get(url, auth=self.auth)
        respuesta.raise_for_status()
        return respuesta.json()["dag_runs"]

    def _obtener_task_instances(self, dag_id, run_id):
        url = f"{self.base_url}/dags/{dag_id}/dagRuns/{run_id}/taskInstances"
        respuesta = requests.get(url, auth=self.auth)
        respuesta.raise_for_status()
        return respuesta.json()["task_instances"]