
import os
import logging
from datetime import datetime, timezone

import requests
from flask import Flask, jsonify, request
from flask_cors import CORS

from messaging_config import (
    ROUTES,
    MAX_BATCH_SIZE,
    MAX_MESSAGE_LENGTH
)

from messaging_service import (
    MessagingService,
    AdminService
)


# ============================================================
# 1. CONFIGURACION GENERAL DE FLASK
# ============================================================

app = Flask(__name__)

app.config["MAX_CONTENT_LENGTH"] = 256 * 1024
app.config["JSON_SORT_KEYS"] = False

logging.basicConfig(
    level=logging.INFO,
    format="%(asctime)s %(levelname)s %(message)s"
)

logger = logging.getLogger(__name__)


# ============================================================
# 2. CONFIGURACION CORS
# ============================================================

# Se permite exclusivamente el origen del frontend.
# Debe coincidir con la URL usada en el navegador.

FRONTEND_ORIGIN = os.environ["FRONTEND_ORIGIN"]

CORS(
    app,
    resources={
        r"/*": {
            "origins": [FRONTEND_ORIGIN]
        }
    },
    methods=[
        "GET",
        "POST",
        "PUT",
        "DELETE",
        "OPTIONS"
    ],
    allow_headers=[
        "Content-Type",
        "Authorization"
    ]
)


# ============================================================
# 3. INICIALIZACION DE SERVICIOS
# ============================================================

# Servicio de publicacion AMQP.
messaging_service = MessagingService()

# Servicio de administracion y monitoreo RabbitMQ.
admin_service = AdminService()


# ============================================================
# 4. FUNCIONES AUXILIARES
# ============================================================

def utc_now():
    """
    Retorna la fecha y hora actual en UTC.
    """
    return datetime.now(
        timezone.utc
    ).isoformat()


def error_response(message, status_code):
    """
    Genera una respuesta JSON uniforme.
    """
    return jsonify({
        "error": message,
        "timestamp": utc_now()
    }), status_code


def publish(messages, message_type):
    """
    Publica mensajes mediante MessagingService.

    Codigos:
    202: todos los mensajes confirmados.
    207: publicacion parcialmente confirmada.
    503: ningun mensaje confirmado.
    400: datos invalidos.
    """

    try:
        result = messaging_service.publish_batch(
            messages,
            message_type
        )

        requested = result["requested"]
        confirmed = result["confirmed"]

        if confirmed == requested:
            status_code = 202

        elif confirmed > 0:
            status_code = 207

        else:
            status_code = 503

        return jsonify(result), status_code

    except ValueError as exc:
        return error_response(
            str(exc),
            400
        )

    except Exception:
        logger.exception(
            "Error publicando mensajes en RabbitMQ"
        )

        return error_response(
            "No fue posible publicar los mensajes",
            503
        )


def monitoring_error(exc):
    """
    Maneja errores del sistema de monitoreo
    sin exponer credenciales ni detalles internos.
    """

    logger.exception(
        "Error consultando RabbitMQ: %s",
        type(exc).__name__
    )

    return error_response(
        "No fue posible consultar las estadisticas de RabbitMQ",
        503
    )


# ============================================================
# 5. HEALTH CHECK
# GET /health
# ============================================================

@app.get("/health")
def health():
    """
    Comprueba que Flask esta operativo.

    No depende de la disponibilidad de RabbitMQ.
    """

    return jsonify({
        "status": "ok",
        "service": "producer-api"
    }), 200


# ============================================================
# 6. TIPOS DE MENSAJES
# GET /api/message-types
# ============================================================

@app.get("/api/message-types")
def message_types():
    """
    Devuelve los tipos de mensajes disponibles.
    """

    return jsonify({
        "types": list(ROUTES.keys()),
        "max_batch_size": MAX_BATCH_SIZE,
        "max_message_length": MAX_MESSAGE_LENGTH
    }), 200


# ============================================================
# 7. PUBLICACION INDIVIDUAL
# POST /api/messages
# ============================================================

@app.post("/api/messages")
def publish_one():
    """
    Publica un mensaje individual.

    Ejemplo:
    {
        "message": "Hola RabbitMQ",
        "type": "normal"
    }
    """

    data = request.get_json(silent=True)

    if not isinstance(data, dict):
        return error_response(
            "JSON invalido",
            400
        )

    return publish(
        [data.get("message")],
        data.get("type", "normal")
    )


# ============================================================
# 8. PUBLICACION POR LOTES
# POST /api/messages/batch
# ============================================================

@app.post("/api/messages/batch")
def publish_many():
    """
    Publica varios mensajes.

    Ejemplo:
    {
        "messages": [
            "Mensaje 1",
            "Mensaje 2"
        ],
        "type": "prioritario"
    }
    """

    data = request.get_json(silent=True)

    if not isinstance(data, dict):
        return error_response(
            "JSON invalido",
            400
        )

    return publish(
        data.get("messages"),
        data.get("type", "normal")
    )


# ============================================================
# 9. GENERACION AUTOMATICA DE MENSAJES
# POST /api/messages/generate
# ============================================================

@app.post("/api/messages/generate")
def generate_messages():
    """
    Genera mensajes de prueba.

    Ejemplo:
    {
        "quantity": 10,
        "type": "normal",
        "prefix": "Prueba",
        "simulate_error": false
    }

    Si simulate_error es true, se agrega
    el marcador [ERROR] a cada mensaje.
    """

    data = request.get_json(silent=True)

    if not isinstance(data, dict):
        return error_response(
            "JSON invalido",
            400
        )

    quantity = data.get(
        "quantity",
        10
    )

    message_type = data.get(
        "type",
        "normal"
    )

    prefix = data.get(
        "prefix",
        "Mensaje de prueba"
    )

    simulate_error = data.get(
        "simulate_error",
        False
    )

    # Validacion de cantidad.
    if type(quantity) is not int:
        return error_response(
            "quantity debe ser entero",
            400
        )

    if not 1 <= quantity <= MAX_BATCH_SIZE:
        return error_response(
            f"quantity debe estar entre 1 y {MAX_BATCH_SIZE}",
            400
        )

    # Validacion del prefijo.
    if not isinstance(prefix, str):
        return error_response(
            "prefix debe ser texto",
            400
        )

    prefix = prefix.strip()

    if not prefix:
        return error_response(
            "prefix no puede estar vacio",
            400
        )

    # Validacion de simulacion de errores.
    if type(simulate_error) is not bool:
        return error_response(
            "simulate_error debe ser booleano",
            400
        )

    # Validacion del tipo de mensaje.
    if not isinstance(message_type, str):
        return error_response(
            "type debe ser texto",
            400
        )

    if message_type not in ROUTES:
        return error_response(
            "Tipo de mensaje no permitido",
            400
        )

    suffix = (
        " [ERROR]"
        if simulate_error
        else ""
    )

    # Se calcula el largo maximo del mensaje generado.
    longest_message = (
        f"{prefix} #{quantity}{suffix}"
    )

    if len(longest_message) > MAX_MESSAGE_LENGTH:
        return error_response(
            "Los mensajes generados superan la longitud maxima",
            400
        )

    messages = [
        f"{prefix} #{i}{suffix}"
        for i in range(1, quantity + 1)
    ]

    return publish(
        messages,
        message_type
    )


# ============================================================
# 10. ESTADISTICAS PRINCIPALES
# GET /api/stats
# ============================================================

@app.get("/api/stats")
def get_stats():
    """
    Obtiene las estadisticas de las cuatro colas.

    Mantiene compatibilidad con React:

    normal
    prioritario
    normalDlq
    prioritarioDlq

    Tambien incluye:
    details
    totals
    """

    try:
        stats = admin_service.get_stats()

        return jsonify(stats), 200

    except (
        requests.RequestException,
        RuntimeError,
        ValueError,
        TypeError,
        KeyError
    ) as exc:
        return monitoring_error(exc)

    except Exception as exc:
        return monitoring_error(exc)


# ============================================================
# 11. ESTADISTICAS DETALLADAS DE COLAS
# GET /api/stats/queues
# ============================================================

@app.get("/api/stats/queues")
def get_queue_stats():
    """
    Devuelve informacion detallada de cada cola.

    Incluye:
    - name
    - exists
    - messages
    - messages_ready
    - messages_unacknowledged
    - consumers
    - state
    """

    try:
        details = admin_service.get_queue_details()

        return jsonify({
            "timestamp": utc_now(),
            "queues": details
        }), 200

    except Exception as exc:
        return monitoring_error(exc)


# ============================================================
# 12. ESTADISTICAS GENERALES DEL BROKER
# GET /api/stats/overview
# ============================================================

@app.get("/api/stats/overview")
def get_broker_overview():
    """
    Consulta el estado general de RabbitMQ.

    Incluye:
    - Version RabbitMQ
    - Nombre del cluster
    - Cantidad de colas
    - Conexiones
    - Canales
    - Consumidores
    - Mensajes
    """

    try:
        overview = admin_service.get_overview()

        return jsonify({
            "timestamp": utc_now(),
            "overview": overview
        }), 200

    except Exception as exc:
        return monitoring_error(exc)


# ============================================================
# 13. DASHBOARD CONSOLIDADO
# GET /api/stats/dashboard
# ============================================================

@app.get("/api/stats/dashboard")
def get_dashboard():
    """
    Entrega un resumen consolidado para React.

    Combina:
    - Estado de la API
    - Estadisticas de las colas
    - Estadisticas generales del broker
    """

    try:
        stats = admin_service.get_stats()
        overview = admin_service.get_overview()

        return jsonify({
            "timestamp": utc_now(),

            "api": {
                "status": "ok",
                "service": "producer-api"
            },

            "queues": {
                "normal": stats.get("normal"),
                "prioritario": stats.get("prioritario"),
                "normalDlq": stats.get("normalDlq"),
                "prioritarioDlq": stats.get("prioritarioDlq")
            },

            "details": stats.get(
                "details",
                {}
            ),

            "totals": stats.get(
                "totals",
                {}
            ),

            "broker": overview

        }), 200

    except Exception as exc:
        return monitoring_error(exc)


# ============================================================
# 14. MANEJO DE ERRORES HTTP
# ============================================================

@app.errorhandler(404)
def not_found(error):
    return error_response(
        "Endpoint no encontrado",
        404
    )


@app.errorhandler(405)
def method_not_allowed(error):
    return error_response(
        "Metodo HTTP no permitido",
        405
    )


@app.errorhandler(413)
def payload_too_large(error):
    return error_response(
        "El contenido enviado supera el limite permitido",
        413
    )


@app.errorhandler(500)
def internal_server_error(error):
    logger.exception(
        "Error interno del servidor"
    )

    return error_response(
        "Error interno del servidor",
        500
    )


# ============================================================
# 15. INICIO DE FLASK
# ============================================================

if __name__ == "__main__":
    app.run(
        host="0.0.0.0",
        port=5000
    )

