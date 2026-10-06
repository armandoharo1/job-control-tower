"""
Fabrica de conectores: dado un tipo de orquestador, devuelve la
implementacion correcta. Este es el UNICO lugar del sistema que sabe
que tipos de orquestador existen.
"""

from core.connectors.base import OrchestratorConnector
from core.connectors.airflow_connector import AirflowConnector
from core.connectors.databricks_connector import DatabricksConnector
from core.connectors.control_m_connector import ControlMConnector


class ConnectorFactory:
    _registro: dict[str, type[OrchestratorConnector]] = {}

    @classmethod
    def registrar(cls, tipo: str, clase: type[OrchestratorConnector]):
        cls._registro[tipo] = clase

    @classmethod
    def crear(cls, tipo_orquestador: str, config: dict) -> OrchestratorConnector:
        clase = cls._registro.get(tipo_orquestador)
        if not clase:
            disponibles = ", ".join(cls._registro.keys())
            raise ValueError(
                f"Conector no soportado: '{tipo_orquestador}'. Disponibles: {disponibles}"
            )
        return clase(config)


ConnectorFactory.registrar("airflow", AirflowConnector)
ConnectorFactory.registrar("databricks", DatabricksConnector)
ConnectorFactory.registrar("control_m", ControlMConnector)