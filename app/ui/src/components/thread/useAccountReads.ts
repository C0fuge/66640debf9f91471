import { useCallback, useEffect, useLayoutEffect, useRef, useState } from "react";
import type { CoreClient } from "../../core";
import { failureText } from "./shared";

type Task = (active: () => boolean) => Promise<void>;
interface Controller {
  reload: () => void;
  run: (task: Task) => boolean;
}

// Each mounted selection has one subscription and one queue for snapshot reads.
// A dirty flag survives a read/action in flight, including the initial read.
export function useAccountReads(client: CoreClient, accountId: string, read: Task) {
  const reader = useRef(read);
  useLayoutEffect(() => { reader.current = read; }, [read]);
  const controller = useRef<Controller | null>(null);
  const [attempt, setAttempt] = useState(0);
  const [subscribed, setSubscribed] = useState(false);
  const [busy, setBusy] = useState(true);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let active = true;
    let watching = false;
    let running = false;
    let dirty = false;
    let unsubscribe: (() => void) | undefined;
    const isActive = () => active;

    function run(task: Task) {
      if (!active || !watching || running) return false;
      running = true;
      setBusy(true);
      setError(null);
      void (async () => {
        try {
          await task(isActive);
        } catch (cause) {
          if (active) setError(failureText(cause));
        } finally {
          running = false;
          if (active) {
            if (dirty) drain();
            else setBusy(false);
          }
        }
      })();
      return true;
    }

    function drain() {
      if (!active || !watching || running || !dirty) return;
      dirty = false;
      run(reader.current);
    }

    function reload() {
      dirty = true;
      queueMicrotask(drain);
    }

    controller.current = { reload, run };
    setSubscribed(false);
    setBusy(true);
    setError(null);
    void (async () => {
      try {
        const stop = await client.subscribe(change => {
          if (change.kind === "reset" || (change.kind === "account" && change.accountId === accountId)) reload();
        });
        if (!active) { stop(); return; }
        unsubscribe = stop;
        watching = true;
        setSubscribed(true);
        reload();
      } catch (cause) {
        if (!active) return;
        controller.current = null;
        setError(failureText(cause));
        setBusy(false);
      }
    })();

    return () => {
      active = false;
      controller.current = null;
      unsubscribe?.();
    };
  }, [client, accountId, attempt]);

  const reload = useCallback(() => {
    if (controller.current) controller.current.reload();
    else setAttempt(value => value + 1);
  }, []);
  return { busy, error, subscribed, reload, run: (task: Task) => controller.current?.run(task) ?? false };
}
