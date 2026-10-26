
"""
Configuración centralizada de mensajería RabbitMQ.

Topología:
    app.events
      normal       -> mensajes.normal
      prioritario  -> mensajes.prioritarios

    app.dlx
      normal.failed      -> mensajes.normal.dlq
      prioritario.failed -> mensajes.prioritarios.dlq

Todas las colas son durables.
Los mensajes fallidos se enrutan mediante DLX.
"""

import os
from dataclasses import dataclass


# ==========================================
# CONEXIÓN RABBITMQ
# ==========================================

RABBITMQ_HOST = os.getenv(
    "RABBITMQ_HOST",
    "localhost"
)

RABBITMQ_PORTS = (5672, 5673)

RABBITMQ_USER = os.getenv(
    "RABBITMQ_USER",
    "admin"
)

RABBITMQ_PASSWORD = os.getenv(
    "RABBITMQ_PASSWORD",
    ""
)

RABBITMQ_VHOST = os.getenv(
    "RABBITMQ_VHOST",
    "/"
)


# ==========================================
# EXCHANGES
# ==========================================

EVENTS_EXCHANGE = "app.events"
DEAD_LETTER_EXCHANGE = "app.dlx"

EXCHANGE_TYPE = "direct"


# ==========================================
# DEFINICIÓN DE RUTAS
# ==========================================

@dataclass(frozen=True)
class MessageRoute:
    message_type: str
    queue: str
    routing_key: str
    dlq: str
    dead_letter_routing_key: str


NORMAL_ROUTE = MessageRoute(
    message_type="normal",
    queue="mensajes.normal",
    routing_key="normal",
    dlq="mensajes.normal.dlq",
    dead_letter_routing_key="normal.failed"
)

PRIORITY_ROUTE = MessageRoute(
    message_type="prioritario",
    queue="mensajes.prioritarios",
    routing_key="prioritario",
    dlq="mensajes.prioritarios.dlq",
    dead_letter_routing_key="prioritario.failed"
)


ROUTES = {
    NORMAL_ROUTE.message_type: NORMAL_ROUTE,
    PRIORITY_ROUTE.message_type: PRIORITY_ROUTE
}


# ==========================================
# COMPORTAMIENTO DE LOS CONSUMIDORES
# ==========================================

MAX_RETRIES = 3

PROCESSING_DELAY_SECONDS = 2

PREFETCH_COUNT = 1


# ==========================================
# PUBLICACIÓN MASIVA
# ==========================================

MAX_BATCH_SIZE = 100

MAX_MESSAGE_LENGTH = 2000


# ==========================================
# CONFIGURACIÓN DE COLAS
# ==========================================

QUEUE_DURABLE = True

QUEUE_AUTO_DELETE = False

QUEUE_EXCLUSIVE = False


# ==========================================
# CONFIGURACIÓN DE MENSAJES
# ==========================================

MESSAGE_DELIVERY_MODE = 2

PUBLISH_MANDATORY = True


# ==========================================
# HELPERS
# ==========================================

def get_route(message_type: str) -> MessageRoute:
    """
    Obtiene una ruta de mensajería válida.

    Evita que los servicios utilicen
    nombres de colas escritos directamente.
    """

    if not isinstance(message_type, str):
        raise ValueError(
            "El tipo de mensaje debe ser texto"
        )

    route = ROUTES.get(message_type.lower())

    if route is None:
        raise ValueError(
            "Tipo de mensaje inválido. "
            "Valores permitidos: "
            + ", ".join(ROUTES.keys())
        )

    return route

