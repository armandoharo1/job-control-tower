"""
Interfaz base que todo conector de orquestador debe implementar.
Este es el patron Adapter: el resto del sistema solo conoce estos metodos,
sin importar si por detras hay Airflow, Control-M, Databricks, etc.
"""

from abc import ABC, abstractmethod


class OrchestratorConnector(ABC):
    def __init__(self, config: dict):
        self.config = config

    @abstractmethod
    def obtener_ejecuciones(self, dag_id: str) -> list[dict]:
        """
        Debe devolver una lista de diccionarios con esta forma estandar:
        {
            "run_id": str,
            "task_id": str,
            "estado": str,
            "fecha_inicio": str,
            "fecha_fin": str,
        }
        """
        ...

    @abstractmethod
    def obtener_mensaje_error(self, dag_id: str, run_id: str, task_id: str, intento: int) -> str | None:
        """Debe devolver el texto del error, o None si no lo encuentra."""
        ...

    @abstractmethod
    def obtener_tags(self, dag_id: str) -> list[str]:
        """Debe devolver etiquetas/area asociadas al pipeline."""
        ...