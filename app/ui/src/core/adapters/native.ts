import { invoke } from "@tauri-apps/api/core";
import { listen } from "@tauri-apps/api/event";
import { createClient, failure, isChange } from "./client";

export function createNativeClient() {
  return createClient(request => invoke("core_request", { request }), async listener => {
    try {
      const stop = await listen<unknown>("core-change", ({ payload }) => {
        listener(isChange(payload) ? payload : { kind: "reset" });
      });
      let active = true;
      return () => {
        if (active) {
          active = false;
          stop();
        }
      };
    } catch {
      throw failure("unavailable");
    }
  });
}
