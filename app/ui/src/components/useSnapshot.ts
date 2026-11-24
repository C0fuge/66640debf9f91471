import { useEffect, useState } from "react";
import type { CoreChange, CoreClient } from "../core";
import { watchSnapshot } from "./watchSnapshot";

export function useSnapshot<T>(client: CoreClient, read: () => Promise<T>, relevant: (event: CoreChange) => boolean) {
  const [data, setData] = useState<T>();
  const [error, setError] = useState(false);
  const [attempt, setAttempt] = useState(0);
  useEffect(() => watchSnapshot(client, relevant, read, value => {
    setData(value);
    setError(false);
  }, () => setError(true)), [client, read, relevant, attempt]);
  return { data, error, retry: () => { setError(false); setAttempt(n => n + 1); } };
}
