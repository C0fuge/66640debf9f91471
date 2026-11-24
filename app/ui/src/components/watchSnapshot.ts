import type { CoreChange, CoreClient } from "../core";

export function watchSnapshot<T>(
  client: Pick<CoreClient, "subscribe">,
  relevant: (change: CoreChange) => boolean,
  read: () => Promise<T>,
  accept: (value: T) => void,
  failed: () => void,
) {
  let stopped = false;
  let listening = false;
  let scheduled = false;
  let reading = false;
  let dirty = false;
  let unsubscribe: (() => void) | undefined;

  async function run() {
    scheduled = false;
    if (stopped) return;
    reading = true;
    dirty = false;
    try {
      const value = await read();
      if (!stopped) accept(value);
    } catch {
      if (!stopped) failed();
    } finally {
      reading = false;
      if (dirty) invalidate();
    }
  }

  function invalidate() {
    dirty = true;
    if (stopped || !listening || scheduled || reading) return;
    scheduled = true;
    queueMicrotask(run);
  }

  void client.subscribe(change => {
    if (relevant(change)) invalidate();
  }).then(stop => {
    if (stopped) return stop();
    unsubscribe = stop;
    listening = true;
    invalidate();
  }, () => {
    if (!stopped) failed();
  });

  return () => {
    stopped = true;
    unsubscribe?.();
  };
}
