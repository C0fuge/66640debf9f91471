import { isTauri } from "@tauri-apps/api/core";
import { createBrowserClient, takeToken } from "./browser";
import { createNativeClient } from "./native";

export function createAppClient() {
  if (isTauri()) return createNativeClient();
  return createBrowserClient(takeToken(window.location, window.history));
}
