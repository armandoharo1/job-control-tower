"""
Utilidades para enviar alertas a Slack cuando una tarea de Airflow falla.
"""

import requests
from airflow.models import Variable


def notificar_fallo_slack(context):
    """
    Callback que se ejecuta automáticamente cuando una tarea falla.
    Envía un mensaje formateado a Slack con los detalles del fallo.
    """
    try:
        webhook_url = Variable.get("slack_webhook_jct")
    except KeyError:
        print("No se encontró la variable 'slack_webhook_jct' en Airflow. Alerta no enviada.")
        return

    dag_id = context["dag"].dag_id
    task_id = context["task_instance"].task_id
    run_id = context["run_id"]
    log_url = context["task_instance"].log_url
    excepcion = context.get("exception", "Sin detalle de excepción")

    mensaje = {
        "blocks": [
            {
                "type": "header",
                "text": {"type": "plain_text", "text": "🔴 Job fallido en Job Control Tower"},
            },
            {
                "type": "section",
                "fields": [
                    {"type": "mrkdwn", "text": f"*DAG:*\n{dag_id}"},
                    {"type": "mrkdwn", "text": f"*Tarea:*\n{task_id}"},
                    {"type": "mrkdwn", "text": f"*Run ID:*\n{run_id}"},
                    {"type": "mrkdwn", "text": f"*Error:*\n{str(excepcion)[:200]}"},
                ],
            },
            {
                "type": "section",
                "text": {"type": "mrkdwn", "text": f"<{log_url}|Ver log completo en Airflow>"},
            },
        ]
    }

    respuesta = requests.post(webhook_url, json=mensaje)
    if respuesta.status_code != 200:
        print(f"Error al enviar alerta a Slack: {respuesta.status_code} - {respuesta.text}")
    else:
        print("Alerta enviada a Slack correctamente.")