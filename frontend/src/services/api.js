
const API_URL = import.meta.env.VITE_API_URL;

async function request(path, options = {}) {
  const headers = {
    ...options.headers,
  };

  if (options.body !== undefined) {
    headers["Content-Type"] = "application/json";
  }

  const response = await fetch(`${API_URL}${path}`, {
    ...options,
    headers,
  });

  let data;

  try {
    data = await response.json();
  } catch {
    throw new Error("El backend devolvió una respuesta inválida");
  }

  if (!response.ok) {
    throw new Error(
      data.error || `Error HTTP ${response.status}`
    );
  }

  return data;
}

export const messagingApi = {
  health() {
    return request("/health");
  },

  getMessageTypes() {
    return request("/api/message-types");
  },

  sendMessage(message, type) {
    return request("/api/messages", {
      method: "POST",
      body: JSON.stringify({ message, type }),
    });
  },

  sendBatch(messages, type) {
    return request("/api/messages/batch", {
      method: "POST",
      body: JSON.stringify({ messages, type }),
    });
  },

  generateMessages({
    quantity,
    type,
    prefix,
    simulate_error,
  }) {
    return request("/api/messages/generate", {
      method: "POST",
      body: JSON.stringify({
        quantity,
        type,
        prefix,
        simulate_error,
      }),
    });
  },

  getStats() {
    return request("/api/stats");
  },

  getQueueStats() {
    return request("/api/stats/queues");
  },

  getOverview() {
    return request("/api/stats/overview");
  },

  getDashboard() {
    return request("/api/stats/dashboard");
  },
};

