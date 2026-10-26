
import { useCallback, useEffect, useState } from "react";
import {
  Activity,
  AlertTriangle,
  CheckCircle2,
  Database,
  Inbox,
  Layers,
  RefreshCw,
  Send,
  Server,
  Users,
  Zap,
} from "lucide-react";

import { messagingApi } from "./services/api";
import "./App.css";

const EMPTY_STATS = {
  normal: null,
  prioritario: null,
  normalDlq: null,
  prioritarioDlq: null,
};

const QUEUE_CARDS = [
  {
    key: "normal",
    label: "Mensajes normales",
    Icon: Inbox,
  },
  {
    key: "prioritario",
    label: "Mensajes prioritarios",
    Icon: Zap,
  },
  {
    key: "normalDlq",
    label: "DLQ normal",
    Icon: AlertTriangle,
  },
  {
    key: "prioritarioDlq",
    label: "DLQ prioritaria",
    Icon: AlertTriangle,
  },
];

function formatValue(value) {
  return value ?? "—";
}

function App() {
  const [connected, setConnected] = useState(false);
  const [dashboard, setDashboard] = useState(null);
  const [stats, setStats] = useState(EMPTY_STATS);

  const [message, setMessage] = useState("");
  const [messageType, setMessageType] = useState("normal");

  const [quantity, setQuantity] = useState(10);
  const [prefix, setPrefix] = useState("Mensaje de prueba");
  const [simulateError, setSimulateError] = useState(false);

  const [loading, setLoading] = useState(false);
  const [refreshing, setRefreshing] = useState(false);
  const [error, setError] = useState("");
  const [result, setResult] = useState(null);
  const [lastUpdated, setLastUpdated] = useState(null);

  const refreshDashboard = useCallback(async () => {
    setRefreshing(true);

    try {
      const [health, data] = await Promise.all([
        messagingApi.health(),
        messagingApi.getDashboard(),
      ]);

      setConnected(health.status === "ok");

      setDashboard(data);

      setStats({
        normal: data.queues?.normal ?? null,
        prioritario: data.queues?.prioritario ?? null,
        normalDlq: data.queues?.normalDlq ?? null,
        prioritarioDlq: data.queues?.prioritarioDlq ?? null,
      });

      setLastUpdated(new Date());
    } catch (err) {
      console.error("Error actualizando dashboard:", err);
      setError(err.message);
    } finally {
      setRefreshing(false);
    }
  }, []);

  useEffect(() => {
    refreshDashboard();

    const interval = setInterval(
      refreshDashboard,
      5000
    );

    return () => clearInterval(interval);
  }, [refreshDashboard]);

  async function handleSendMessage(event) {
    event.preventDefault();

    if (!message.trim()) {
      setError("Debes escribir un mensaje");
      return;
    }

    setLoading(true);
    setError("");
    setResult(null);

    try {
      const response = await messagingApi.sendMessage(
        message,
        messageType
      );

      setResult(response);
      setMessage("");

      await refreshDashboard();
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  }

  async function handleGenerateMessages(event) {
    event.preventDefault();

    setLoading(true);
    setError("");
    setResult(null);

    try {
      const response = await messagingApi.generateMessages({
        quantity: Number(quantity),
        type: messageType,
        prefix,
        simulate_error: simulateError,
      });

      setResult(response);

      await refreshDashboard();
    } catch (err) {
      setError(err.message);
    } finally {
      setLoading(false);
    }
  }

  const broker = dashboard?.broker || {};
  const totals = dashboard?.totals || {};
  const details = dashboard?.details || {};

  return (
    <div className="app">
      <header className="header">
        <div>
          <div className="eyebrow">
            SISTEMA DE MENSAJERÍA DISTRIBUIDA
          </div>

          <h1>RabbitMQ Dashboard</h1>

          <p>
            Publicación de mensajes y monitoreo
            de colas en tiempo real.
          </p>
        </div>

        <div className="header-actions">
          <div
            className={`connection-status ${
              connected ? "connected" : "disconnected"
            }`}
          >
            {connected ? (
              <CheckCircle2 size={18} />
            ) : (
              <AlertTriangle size={18} />
            )}

            {connected
              ? "Backend conectado"
              : "Backend desconectado"}
          </div>

          <button
            type="button"
            className="refresh-button"
            onClick={refreshDashboard}
            disabled={refreshing}
          >
            <RefreshCw size={17} />
            Actualizar
          </button>
        </div>
      </header>

      <main>
        <section className="section">
          <div className="section-heading">
            <h2>
              <Activity size={21} />
              Estado de las colas
            </h2>

            <span>
              Actualización automática cada 5 segundos
            </span>
          </div>

          <div className="stats-grid">
            {QUEUE_CARDS.map(({ key, label, Icon }) => (
              <div className="stat-card" key={key}>
                <div className="stat-card-top">
                  <span>{label}</span>
                  <Icon size={21} />
                </div>

                <div className="stat-value">
                  {formatValue(stats[key])}
                </div>

                <div className="stat-description">
                  Mensajes actualmente en cola
                </div>
              </div>
            ))}
          </div>
        </section>

        <section className="section">
          <div className="section-heading">
            <h2>
              <Server size={21} />
              Estado general de RabbitMQ
            </h2>
          </div>

          <div className="overview-grid">
            <div className="overview-card">
              <Database size={23} />
              <div>
                <span>Colas del broker</span>
                <strong>{formatValue(broker.queues)}</strong>
              </div>
            </div>

            <div className="overview-card">
              <Users size={23} />
              <div>
                <span>Consumidores</span>
                <strong>
                  {formatValue(broker.consumers)}
                </strong>
              </div>
            </div>

            <div className="overview-card">
              <Layers size={23} />
              <div>
                <span>Conexiones</span>
                <strong>
                  {formatValue(broker.connections)}
                </strong>
              </div>
            </div>

            <div className="overview-card">
              <Activity size={23} />
              <div>
                <span>Mensajes pendientes</span>
                <strong>
                  {formatValue(totals.messages_ready)}
                </strong>
              </div>
            </div>
          </div>

          <div className="broker-info">
            <span>
              Cluster: {broker.cluster_name || "—"}
            </span>

            <span>
              RabbitMQ: {broker.rabbitmq_version || "—"}
            </span>

            <span>
              Última actualización:{" "}
              {lastUpdated
                ? lastUpdated.toLocaleTimeString()
                : "—"}
            </span>
          </div>
        </section>

        <section className="section">
          <div className="section-heading">
            <h2>
              <Send size={21} />
              Publicación de mensajes
            </h2>
          </div>

          <div className="forms-grid">
            <form
              className="panel"
              onSubmit={handleSendMessage}
            >
              <h3>Enviar mensaje individual</h3>

              <label htmlFor="message">
                Contenido del mensaje
              </label>

              <textarea
                id="message"
                value={message}
                onChange={(event) =>
                  setMessage(event.target.value)
                }
                placeholder="Escribe un mensaje..."
                rows={4}
                required
              />

              <label htmlFor="single-type">
                Tipo de mensaje
              </label>

              <select
                id="single-type"
                value={messageType}
                onChange={(event) =>
                  setMessageType(event.target.value)
                }
              >
                <option value="normal">Normal</option>
                <option value="prioritario">
                  Prioritario
                </option>
              </select>

              <button
                className="primary-button"
                type="submit"
                disabled={loading}
              >
                <Send size={17} />
                Enviar mensaje
              </button>
            </form>

            <form
              className="panel"
              onSubmit={handleGenerateMessages}
            >
              <h3>Generar mensajes masivos</h3>

              <label htmlFor="quantity">
                Cantidad de mensajes
              </label>

              <input
                id="quantity"
                type="number"
                min="1"
                max="100"
                value={quantity}
                onChange={(event) =>
                  setQuantity(event.target.value)
                }
                required
              />

              <label htmlFor="prefix">
                Prefijo
              </label>

              <input
                id="prefix"
                type="text"
                value={prefix}
                onChange={(event) =>
                  setPrefix(event.target.value)
                }
                required
              />

              <label className="checkbox-label">
                <input
                  type="checkbox"
                  checked={simulateError}
                  onChange={(event) =>
                    setSimulateError(event.target.checked)
                  }
                />

                Simular errores para probar DLQ
              </label>

              <button
                className="primary-button"
                type="submit"
                disabled={loading}
              >
                <Zap size={17} />
                Generar mensajes
              </button>
            </form>
          </div>
        </section>

        <section className="section">
          <div className="section-heading">
            <h2>
              <Database size={21} />
              Detalle de colas
            </h2>
          </div>

          <div className="table-container">
            <table>
              <thead>
                <tr>
                  <th>Cola</th>
                  <th>Pendientes</th>
                  <th>Sin confirmar</th>
                  <th>Consumidores</th>
                  <th>Estado</th>
                </tr>
              </thead>

              <tbody>
                {QUEUE_CARDS.map(({ key, label }) => {
                  const queue = details[key];

                  return (
                    <tr key={key}>
                      <td>{queue?.name || label}</td>

                      <td>
                        {formatValue(
                          queue?.messages_ready
                        )}
                      </td>

                      <td>
                        {formatValue(
                          queue?.messages_unacknowledged
                        )}
                      </td>

                      <td>
                        {formatValue(
                          queue?.consumers
                        )}
                      </td>

                      <td>
                        {queue?.exists === false
                          ? "No existe"
                          : queue?.state || "—"}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
          </div>
        </section>

        {error && (
          <div className="alert error-alert" role="alert">
            <AlertTriangle size={19} />
            {error}
            <button
              type="button"
              onClick={() => setError("")}
              aria-label="Cerrar error"
            >
              ×
            </button>
          </div>
        )}

        {result && (
          <div className="alert success-alert">
            <CheckCircle2 size={20} />

            <div>
              <strong>Resultado de publicación</strong>

              <p>
                Solicitados: {result.requested} |
                Confirmados: {result.confirmed}
              </p>
            </div>
          </div>
        )}
      </main>
    </div>
  );
}

export default App;

