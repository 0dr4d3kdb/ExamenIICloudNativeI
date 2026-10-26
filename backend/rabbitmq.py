
import random
import pika

from messaging_config import (
    RABBITMQ_HOST,
    RABBITMQ_PORTS,
    RABBITMQ_USER,
    RABBITMQ_PASSWORD,
    RABBITMQ_VHOST,
    EVENTS_EXCHANGE,
    DEAD_LETTER_EXCHANGE,
    EXCHANGE_TYPE,
    ROUTES,
    QUEUE_DURABLE,
    QUEUE_EXCLUSIVE,
    QUEUE_AUTO_DELETE,
)


def get_connection():
    """Conecta a uno de los dos nodos RabbitMQ."""

    credentials = pika.PlainCredentials(
        RABBITMQ_USER,
        RABBITMQ_PASSWORD
    )

    ports = list(RABBITMQ_PORTS)
    random.shuffle(ports)

    parameters = [
        pika.ConnectionParameters(
            host=RABBITMQ_HOST,
            port=port,
            virtual_host=RABBITMQ_VHOST,
            credentials=credentials,
            heartbeat=60,
            blocked_connection_timeout=15,
            socket_timeout=5,
            connection_attempts=2,
            retry_delay=2
        )
        for port in ports
    ]

    return pika.BlockingConnection(parameters)


def declare_topology(channel):
    """
    Declara toda la topología de mensajería.

    Por cada ruta:
      - Cola principal
      - Cola de mensajes fallidos
      - Binding de publicación
      - Binding de Dead Letter

    Las DLQ se crean antes que las colas principales.
    """

    channel.exchange_declare(
        exchange=EVENTS_EXCHANGE,
        exchange_type=EXCHANGE_TYPE,
        durable=True
    )

    channel.exchange_declare(
        exchange=DEAD_LETTER_EXCHANGE,
        exchange_type=EXCHANGE_TYPE,
        durable=True
    )

    for route in ROUTES.values():

        # Cola de mensajes fallidos
        channel.queue_declare(
            queue=route.dlq,
            durable=QUEUE_DURABLE,
            exclusive=QUEUE_EXCLUSIVE,
            auto_delete=QUEUE_AUTO_DELETE
        )

        channel.queue_bind(
            exchange=DEAD_LETTER_EXCHANGE,
            queue=route.dlq,
            routing_key=route.dead_letter_routing_key
        )

        # Cola principal con configuración DLX
        channel.queue_declare(
            queue=route.queue,
            durable=QUEUE_DURABLE,
            exclusive=QUEUE_EXCLUSIVE,
            auto_delete=QUEUE_AUTO_DELETE,
            arguments={
                "x-queue-type": "classic",
                "x-dead-letter-exchange":
                    DEAD_LETTER_EXCHANGE,
                "x-dead-letter-routing-key":
                    route.dead_letter_routing_key
            }
        )

        channel.queue_bind(
            exchange=EVENTS_EXCHANGE,
            queue=route.queue,
            routing_key=route.routing_key
        )


def initialize_topology():
    """Inicializa los recursos y cierra la conexión."""

    connection = get_connection()

    try:
        channel = connection.channel()
        declare_topology(channel)

        print(
            "[RABBITMQ] Topología declarada",
            flush=True
        )

    finally:
        if connection.is_open:
            connection.close()


if __name__ == "__main__":
    initialize_topology()

