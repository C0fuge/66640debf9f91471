import type { CoreChange, CoreClient } from "../index";
import { createClient, failure, isChange, isObject } from "./client";

export const browserOrigin = "http://127.0.0.1:5173";
const endpoint = "ws://127.0.0.1:43127/core";
const protocol = "ch-core-v1";
const maxPending = 16;
const requestTimeout = 30_000;
type Listener = (change: CoreChange) => void;
interface Pending {
  resolve: (value: unknown) => void;
  reject: (reason: unknown) => void;
  timer: ReturnType<typeof setTimeout>;
}

export function takeToken(location: Location, history: History): string | null {
  const fragment = new URLSearchParams(location.hash.slice(1));
  if (!fragment.has("token")) return null;
  const token = fragment.get("token");
  fragment.delete("token");
  const rest = fragment.toString();
  history.replaceState(history.state, "", location.pathname + location.search + (rest ? "#" + rest : ""));
  return location.origin === browserOrigin && token && token.length <= 4096 ? token : null;
}

export function createBrowserClient(token: string | null, openSocket = () => new WebSocket(endpoint, protocol)): CoreClient {
  const listeners = new Set<Listener>();
  const pending = new Map<string, Pending | null>();
  let socket: WebSocket | undefined;
  let ready = false;
  let connecting: Promise<void> | undefined;
  let reconnect: ReturnType<typeof setTimeout> | undefined;
  let delay = 350;
  let id = 0;

  function notify(change: CoreChange) {
    for (const listener of listeners) listener(change);
  }

  function scheduleReconnect() {
    if (!listeners.size || !token || reconnect !== undefined) return;
    reconnect = setTimeout(() => {
      reconnect = undefined;
      void connect().catch(scheduleReconnect);
    }, delay);
    delay = Math.min(delay * 2, 8_000);
  }

  function connect(): Promise<void> {
    if (ready) return Promise.resolve();
    if (connecting) return connecting;
    if (!token) return Promise.reject(failure("unavailable"));
    let ws: WebSocket;
    try {
      ws = openSocket();
    } catch {
      return Promise.reject(failure("unavailable"));
    }
    socket = ws;
    connecting = new Promise<void>((resolve, reject) => {
      const handshake = setTimeout(disconnect, 5_000);
      function disconnect() {
        if (socket !== ws) return;
        const wasReady = ready;
        socket = undefined;
        ready = false;
        connecting = undefined;
        clearTimeout(handshake);
        for (const item of pending.values()) {
          if (!item) continue;
          clearTimeout(item.timer);
          item.reject(failure("unavailable"));
        }
        pending.clear();
        reject(failure("unavailable"));
        ws.onclose = ws.onerror = ws.onmessage = ws.onopen = null;
        ws.close();
        if (wasReady) notify({ kind: "reset" });
        scheduleReconnect();
      }
      ws.onclose = ws.onerror = disconnect;
      ws.onopen = () => {
        if (ws.protocol !== protocol) return disconnect();
        try {
          ws.send(JSON.stringify({ token }));
        } catch {
          disconnect();
        }
      };
      ws.onmessage = ({ data }) => {
        if (typeof data !== "string") return disconnect();
        let frame: unknown;
        try {
          frame = JSON.parse(data);
        } catch {
          return disconnect();
        }
        if (!isObject(frame)) return disconnect();
        if (!ready) {
          if (frame.ready !== true) return disconnect();
          clearTimeout(handshake);
          delay = 350;
          ready = true;
          connecting = undefined;
          resolve();
          notify({ kind: "reset" });
        } else if (typeof frame.id === "string" && Object.hasOwn(frame, "reply")) {
          const item = pending.get(frame.id);
          pending.delete(frame.id);
          if (!item) return;
          clearTimeout(item.timer);
          item.resolve(frame.reply);
        } else if (isChange(frame.change)) {
          notify(frame.change);
        } else {
          disconnect();
        }
      };
    });
    return connecting;
  }

  return createClient(request => {
    if (!ready || !socket || pending.size >= maxPending) return Promise.reject(failure("unavailable"));
    const ws = socket;
    const key = String(++id);
    return new Promise((resolve, reject) => {
      const timer = setTimeout(() => {
        // The server still counts the request until it replies or the socket closes.
        pending.set(key, null);
        reject(failure("unavailable"));
      }, requestTimeout);
      pending.set(key, { resolve, reject, timer });
      try {
        ws.send(JSON.stringify({ id: key, request }));
      } catch {
        pending.delete(key);
        clearTimeout(timer);
        reject(failure("unavailable"));
        ws.close();
      }
    });
  }, async listener => {
    const callback: Listener = change => listener(change);
    listeners.add(callback);
    const stop = () => {
      listeners.delete(callback);
      if (!listeners.size) {
        clearTimeout(reconnect);
        reconnect = undefined;
      }
    };
    try {
      await connect();
      return stop;
    } catch {
      stop();
      throw failure("unavailable");
    }
  });
}
