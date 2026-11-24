import { StrictMode } from "react";
import { createRoot } from "react-dom/client";
import { isTauri } from "@tauri-apps/api/core";
import { App } from "./App";
import { createAppClient } from "./core/adapters";
import "./styles.css";

const missingToken = !isTauri() && !new URLSearchParams(window.location.hash.slice(1)).get("token");
const client = createAppClient();
createRoot(document.getElementById("root")!).render(
  <StrictMode><App client={client} missingToken={missingToken} /></StrictMode>,
);
