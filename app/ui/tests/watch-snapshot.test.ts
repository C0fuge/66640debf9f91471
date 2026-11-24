import assert from "node:assert/strict";
import test from "node:test";
import { setImmediate } from "node:timers/promises";
import type { CoreChange } from "../src/core";
import { watchSnapshot } from "../src/components/watchSnapshot";

function deferred<T>() {
  let resolve!: (value: T) => void;
  const promise = new Promise<T>(done => { resolve = done; });
  return { promise, resolve };
}

test("waits for active subscription, batches invalidations and ignores stale reads after disposal", async () => {
  const active = deferred<() => void>();
  let notify!: (event: CoreChange) => void;
  let unlistens = 0;
  const reads: ReturnType<typeof deferred<number>>[] = [];
  const values: number[] = [];
  const stop = watchSnapshot({
    subscribe: callback => { notify = callback; return active.promise; },
  }, change => change.kind === "reset", () => {
    const read = deferred<number>();
    reads.push(read);
    return read.promise;
  }, value => values.push(value), () => assert.fail("unexpected read failure"));
  notify({ kind: "reset" });
  await setImmediate();
  assert.equal(reads.length, 0);
  active.resolve(() => { unlistens++; });
  await setImmediate();
  assert.equal(reads.length, 1);
  notify({ kind: "reset" });
  notify({ kind: "reset" });
  reads[0].resolve(1);
  await setImmediate();
  assert.equal(reads.length, 2);
  reads[1].resolve(2);
  await setImmediate();
  notify({ kind: "inventory" });
  await setImmediate();
  assert.equal(reads.length, 2);
  notify({ kind: "reset" });
  await setImmediate();
  stop();
  reads[2].resolve(3);
  await setImmediate();
  assert.deepEqual(values, [1, 2]);
  assert.equal(unlistens, 1);
});

test("disposal during subscribe immediately releases the later listener", async () => {
  const active = deferred<() => void>();
  let unlistens = 0;
  const stop = watchSnapshot({ subscribe: () => active.promise }, () => true,
    async () => assert.fail("stale read"), () => assert.fail("stale value"), () => assert.fail("stale failure"));
  stop();
  active.resolve(() => { unlistens++; });
  await setImmediate();
  assert.equal(unlistens, 1);
});
