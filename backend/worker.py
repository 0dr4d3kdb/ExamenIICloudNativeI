
import json
import os
import time

import pika

from messaging_config import (
    get_route,
    MAX_RETRIES,
    PROCESSING_DELAY_SECONDS,
    PREFETCH_COUNT
)
from rabbitmq import get_connection


MESSAGE_TYPE = os.getenv("MESSAGE_TYPE", "normal")
ROUTE = get_route(MESSAGE_TYPE)


def process_message(ch, method, properties, body):
    """
    ACK: procesamiento exitoso.
    NACK sin requeue: fallo definitivo -> DLQ.
    Reintentos locales: errores de procesamiento.

    El consumidor es independiente del productor.
    """

    try:
        data = json.loads(body)
        message = data["message"]

        if not isinstance(message, str):
            raise ValueError("Mensaje inválido")

    except Exception as exc:
        print(f"[INVALIDO] {exc}", flush=True)
        ch.basic_nack(
            delivery_tag=method.delivery_tag,
            requeue=False
        )
        return

    for attempt in range(1, MAX_RETRIES + 1):
        try:
            print(
                f"[{MESSAGE_TYPE}] Recibido: "
                f"{data['id']} | intento {attempt}",
                flush=True
            )

            time.sleep(PROCESSING_DELAY_SECONDS)

            if "[ERROR]" in message:
                raise RuntimeError(
                    "Error simulado de procesamiento"
                )

            print(
                f"[{MESSAGE_TYPE}] Procesado: {message}",
                flush=True
            )

            ch.basic_ack(
                delivery_tag=method.delivery_tag
            )
            return

        except Exception as exc:
            print(
                f"[{MESSAGE_TYPE}] Error: {exc}",
                flush=True
            )

            if attempt < MAX_RETRIES:
                time.sleep(1)

    print(
        f"[{MESSAGE_TYPE}] Enviando a DLQ: {message}",
        flush=True
    )

    ch.basic_nack(
        delivery_tag=method.delivery_tag,
        requeue=False
    )


def start_worker():
    while True:
        connection = None

        try:
            connection = get_connection()
            channel = connection.channel()

            channel.basic_qos(
                prefetch_count=PREFETCH_COUNT
            )

            channel.basic_consume(
                queue=ROUTE.queue,
                on_message_callback=process_message,
                auto_ack=False
            )

            print(
                f"[WORKER] Escuchando {ROUTE.queue}",
                flush=True
            )

            channel.start_consuming()

        except (
            pika.exceptions.AMQPError,
            OSError
        ) as exc:
            print(
                f"[WORKER] Reconectando: {exc}",
                flush=True
            )
            time.sleep(5)

        finally:
            if connection and connection.is_open:
                connection.close()


if __name__ == "__main__":
    start_worker()

