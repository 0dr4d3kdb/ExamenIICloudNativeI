
import json
import os
import uuid
from datetime import datetime, timezone
from urllib.parse import quote

import pika
import requests

from messaging_config import (
    get_route,
    EVENTS_EXCHANGE,
    MESSAGE_DELIVERY_MODE,
    PUBLISH_MANDATORY,
    MAX_BATCH_SIZE,
    MAX_MESSAGE_LENGTH,
    RABBITMQ_USER,
    RABBITMQ_PASSWORD,
    RABBITMQ_VHOST,
)
from rabbitmq import get_connection


# ============================================================
# SERVICIO DE PUBLICACION DE MENSAJES
# ============================================================

class MessagingService:
    """Servicio de publicación AMQP."""

    def publish_batch(self, messages, message_type):
        route = get_route(message_type)

        if not isinstance(messages, list):
            raise ValueError("messages debe ser una lista")

        if not 1 <= len(messages) <= MAX_BATCH_SIZE:
            raise ValueError(
                f"Se permiten entre 1 y {MAX_BATCH_SIZE} mensajes"
            )

        for message in messages:
            if not isinstance(message, str):
                raise ValueError("Cada mensaje debe ser texto")

            if not 1 <= len(message.strip()) <= MAX_MESSAGE_LENGTH:
                raise ValueError("Longitud de mensaje inválida")

        connection = get_connection()
        results = []

        try:
            channel = connection.channel()
            channel.confirm_delivery()

            for message in messages:
                payload = {
                    "id": str(uuid.uuid4()),
                    "type": route.message_type,
                    "message": message.strip(),
                    "created_at": datetime.now(
                        timezone.utc
                    ).isoformat()
                }

                try:
                    channel.basic_publish(
                        exchange=EVENTS_EXCHANGE,
                        routing_key=route.routing_key,
                        body=json.dumps(payload),
                        mandatory=PUBLISH_MANDATORY,
                        properties=pika.BasicProperties(
                            delivery_mode=MESSAGE_DELIVERY_MODE,
                            content_type="application/json",
                            message_id=payload["id"]
                        )
                    )

                    results.append({
                        "id": payload["id"],
                        "status": "queued"
                    })

                except (
                    pika.exceptions.AMQPError,
                    OSError
                ) as exc:
                    results.append({
                        "id": payload["id"],
                        "status": "failed",
                        "error": str(exc)
                    })
                    break

            return {
                "requested": len(messages),
                "confirmed": sum(
                    x["status"] == "queued"
                    for x in results
                ),
                "results": results
            }

        finally:
            if connection.is_open:
                connection.close()


# ============================================================
# SERVICIO DE ADMINISTRACION Y MONITOREO
# ============================================================

class AdminService:
    """
    Administra recursos y consulta estadísticas
    mediante RabbitMQ Management API.
    """

    QUEUE_NAMES = {
        "normal": "mensajes.normal",
        "prioritario": "mensajes.prioritarios",
        "normalDlq": "mensajes.normal.dlq",
        "prioritarioDlq": "mensajes.prioritarios.dlq"
    }

    def __init__(self):
        base = os.environ["RABBITMQ_MANAGEMENT_URL"]

        self.base_url = base.rstrip("/") + "/api"
        self.auth = (RABBITMQ_USER, RABBITMQ_PASSWORD)
        self.vhost = quote(RABBITMQ_VHOST, safe="")

    # --------------------------------------------------------
    # PETICIONES A RABBITMQ MANAGEMENT API
    # --------------------------------------------------------

    def request(self, method, path, payload=None):
        response = requests.request(
            method,
            self.base_url + path,
            auth=self.auth,
            json=payload,
            timeout=10
        )

        if response.status_code >= 400:
            raise RuntimeError(
                f"RabbitMQ HTTP {response.status_code}: "
                f"{response.text[:300]}"
            )

        if response.content:
            return response.json()

        return {"success": True}

    # --------------------------------------------------------
    # ADMINISTRACION DE COLAS
    # --------------------------------------------------------

    def list_queues(self):
        return self.request(
            "GET",
            f"/queues/{self.vhost}"
        )

    def create_queue(self, name, durable=True):
        return self.request(
            "PUT",
            f"/queues/{self.vhost}/{quote(name, safe='')}",
            {
                "durable": durable,
                "auto_delete": False,
                "arguments": {}
            }
        )

    def delete_queue(self, name):
        return self.request(
            "DELETE",
            f"/queues/{self.vhost}/{quote(name, safe='')}"
        )

    # --------------------------------------------------------
    # ADMINISTRACION DE EXCHANGES
    # --------------------------------------------------------

    def list_exchanges(self):
        return self.request(
            "GET",
            f"/exchanges/{self.vhost}"
        )

    def create_exchange(self, name, exchange_type):
        return self.request(
            "PUT",
            f"/exchanges/{self.vhost}/{quote(name, safe='')}",
            {
                "type": exchange_type,
                "durable": True,
                "auto_delete": False,
                "internal": False,
                "arguments": {}
            }
        )

    def delete_exchange(self, name):
        return self.request(
            "DELETE",
            f"/exchanges/{self.vhost}/{quote(name, safe='')}"
        )

    # --------------------------------------------------------
    # ADMINISTRACION DE BINDINGS
    # --------------------------------------------------------

    def create_binding(self, exchange, queue, key):
        return self.request(
            "POST",
            f"/bindings/{self.vhost}/e/"
            f"{quote(exchange, safe='')}/q/"
            f"{quote(queue, safe='')}",
            {
                "routing_key": key,
                "arguments": {}
            }
        )

    def delete_binding(self, exchange, queue, key):
        bindings = self.request(
            "GET",
            f"/bindings/{self.vhost}/e/"
            f"{quote(exchange, safe='')}/q/"
            f"{quote(queue, safe='')}"
        )

        matching = [
            binding
            for binding in bindings
            if binding["routing_key"] == key
        ]

        if not matching:
            raise ValueError("Binding no encontrado")

        for binding in matching:
            self.request(
                "DELETE",
                f"/bindings/{self.vhost}/e/"
                f"{quote(exchange, safe='')}/q/"
                f"{quote(queue, safe='')}/"
                f"{quote(binding['properties_key'], safe='')}"
            )

        return {"success": True}

    # ========================================================
    # MONITOREO DE RABBITMQ
    # ========================================================

    def get_queue_details(self):
        """
        Consulta las colas del virtual host configurado
        y devuelve sus estadísticas detalladas.

        Se realiza una sola petición HTTP para obtener
        las métricas de todas las colas.
        """

        queues = self.list_queues()

        if not isinstance(queues, list):
            raise ValueError(
                "RabbitMQ devolvió una lista de colas inválida"
            )

        queue_index = {
            queue["name"]: queue
            for queue in queues
            if (
                isinstance(queue, dict)
                and isinstance(queue.get("name"), str)
            )
        }

        details = {}

        for key, queue_name in self.QUEUE_NAMES.items():
            queue = queue_index.get(queue_name)

            if queue is None:
                details[key] = {
                    "name": queue_name,
                    "exists": False,
                    "messages": None,
                    "messages_ready": None,
                    "messages_unacknowledged": None,
                    "consumers": None,
                    "state": None
                }
                continue

            details[key] = {
                "name": queue_name,
                "exists": True,
                "messages": queue.get("messages", 0),
                "messages_ready": queue.get(
                    "messages_ready", 0
                ),
                "messages_unacknowledged": queue.get(
                    "messages_unacknowledged", 0
                ),
                "consumers": queue.get("consumers", 0),
                "state": queue.get("state", "unknown")
            }

        return details

    def get_stats(self):
        """
        Devuelve un resumen compatible con el
        dashboard React existente.

        Incluye también estadísticas detalladas.
        """

        details = self.get_queue_details()

        stats = {
            key: value["messages"]
            for key, value in details.items()
        }

        stats["details"] = details

        stats["totals"] = {
            "messages": sum(
                value["messages"] or 0
                for value in details.values()
            ),
            "messages_ready": sum(
                value["messages_ready"] or 0
                for value in details.values()
            ),
            "messages_unacknowledged": sum(
                value["messages_unacknowledged"] or 0
                for value in details.values()
            ),
            "consumers": sum(
                value["consumers"] or 0
                for value in details.values()
            )
        }

        return stats

    def get_overview(self):
        """
        Obtiene estadísticas generales del broker,
        como cantidades de colas, conexiones y canales.
        """

        overview = self.request(
            "GET",
            "/overview"
        )

        object_totals = overview.get(
            "object_totals", {}
        )

        queue_totals = overview.get(
            "queue_totals", {}
        )

        return {
            "rabbitmq_version": overview.get(
                "rabbitmq_version"
            ),
            "cluster_name": overview.get(
                "cluster_name"
            ),
            "queues": object_totals.get(
                "queues", 0
            ),
            "connections": object_totals.get(
                "connections", 0
            ),
            "channels": object_totals.get(
                "channels", 0
            ),
            "consumers": object_totals.get(
                "consumers", 0
            ),
            "messages": queue_totals.get(
                "messages", 0
            ),
            "messages_ready": queue_totals.get(
                "messages_ready", 0
            ),
            "messages_unacknowledged": queue_totals.get(
                "messages_unacknowledged", 0
            )
        }

