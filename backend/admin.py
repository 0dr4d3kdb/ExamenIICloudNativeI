
import os
import re

from flask import Flask, jsonify, request
from flask_cors import CORS

from messaging_service import AdminService
from messaging_config import (
    ROUTES,
    EVENTS_EXCHANGE,
    DEAD_LETTER_EXCHANGE
)

app = Flask(__name__)
CORS(
    app,
    resources={
        r"/api/*": {
            "origins": os.environ["FRONTEND_ORIGIN"]
        }
    }
)

service = AdminService()

NAME_PATTERN = re.compile(r"^[a-zA-Z0-9._-]{1,100}$")

PROTECTED = {
    EVENTS_EXCHANGE,
    DEAD_LETTER_EXCHANGE,
    *(
        name
        for route in ROUTES.values()
        for name in (route.queue, route.dlq)
    )
}


def validate_name(name):
    if not isinstance(name, str):
        raise ValueError("El nombre debe ser texto")

    if not NAME_PATTERN.fullmatch(name):
        raise ValueError(
            "Nombre inválido. Usa 1 a 100 caracteres: "
            "letras, números, puntos, guiones o guion bajo"
        )

    return name


def validate_custom_name(name):
    name = validate_name(name)

    if name in PROTECTED or name.startswith("amq."):
        raise ValueError(
            "No se puede modificar un recurso reservado"
        )

    return name


def body():
    data = request.get_json(silent=True)
    if not isinstance(data, dict):
        raise ValueError("Se requiere un objeto JSON")
    return data


@app.errorhandler(ValueError)
def validation_error(error):
    return jsonify({"error": str(error)}), 400


@app.errorhandler(RuntimeError)
def rabbitmq_error(error):
    app.logger.error("%s", error)
    return jsonify({
        "error": "Error al administrar RabbitMQ",
        "detail": str(error)
    }), 502


@app.get("/health")
def health():
    return jsonify({
        "status": "ok",
        "service": "rabbitmq-admin"
    })


@app.get("/api/admin/queues")
def list_queues():
    return jsonify(service.list_queues())


@app.post("/api/admin/queues")
def create_queue():
    data = body()
    name = validate_custom_name(data.get("name"))
    durable = data.get("durable", True)

    if type(durable) is not bool:
        raise ValueError("durable debe ser booleano")

    result = service.create_queue(name, durable)
    return jsonify(result), 201


@app.delete("/api/admin/queues/<name>")
def delete_queue(name):
    name = validate_custom_name(name)
    return jsonify(service.delete_queue(name))


@app.get("/api/admin/exchanges")
def list_exchanges():
    return jsonify(service.list_exchanges())


@app.post("/api/admin/exchanges")
def create_exchange():
    data = body()
    name = validate_custom_name(data.get("name"))
    exchange_type = data.get("type", "direct")

    if exchange_type not in ("direct", "topic", "fanout"):
        raise ValueError("Tipo de exchange inválido")

    result = service.create_exchange(name, exchange_type)
    return jsonify(result), 201


@app.delete("/api/admin/exchanges/<name>")
def delete_exchange(name):
    name = validate_custom_name(name)
    return jsonify(service.delete_exchange(name))


@app.post("/api/admin/bindings")
def create_binding():
    data = body()

    exchange = validate_custom_name(
        data.get("exchange")
    )
    queue = validate_custom_name(
        data.get("queue")
    )
    key = validate_name(
        data.get("routing_key")
    )

    result = service.create_binding(
        exchange, queue, key
    )

    return jsonify(result), 201


@app.delete("/api/admin/bindings")
def delete_binding():
    data = body()

    exchange = validate_custom_name(
        data.get("exchange")
    )
    queue = validate_custom_name(
        data.get("queue")
    )
    key = validate_name(
        data.get("routing_key")
    )

    return jsonify(
        service.delete_binding(exchange, queue, key)
    )


if __name__ == "__main__":
    app.run(host="0.0.0.0", port=5001)

