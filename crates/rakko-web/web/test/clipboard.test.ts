import { test } from "node:test";
import assert from "node:assert/strict";
import { createCopyFeedback, type CopyStatus } from "../src/clipboard.ts";

function harness(write: ((t: string) => Promise<void>) | undefined) {
  const seen: CopyStatus[] = [];
  const timers: { fn: () => void; ms: number }[] = [];
  const fb = createCopyFeedback({
    write,
    onChange: (s) => seen.push(s),
    schedule: (fn, ms) => {
      const t = { fn, ms };
      timers.push(t);
      return t;
    },
    cancel: (t) => {
      timers.splice(timers.indexOf(t as (typeof timers)[number]), 1);
    },
  });
  return { fb, seen, timers };
}

test("successful copy writes the text, shows copied, and reverts after 1.5s", async () => {
  const written: string[] = [];
  const { fb, seen, timers } = harness(async (t) => void written.push(t));
  await fb.copy("hello");
  assert.deepEqual(written, ["hello"]);
  assert.deepEqual(seen, ["copied"]);
  assert.equal(timers.length, 1);
  assert.equal(timers[0].ms, 1500);
  timers[0].fn();
  assert.deepEqual(seen, ["copied", "idle"]);
});

test("rejected clipboard write shows failed, then reverts", async () => {
  const { fb, seen, timers } = harness(() => Promise.reject(new Error("denied")));
  await fb.copy("x");
  assert.deepEqual(seen, ["failed"]);
  timers[0].fn();
  assert.deepEqual(seen, ["failed", "idle"]);
});

test("unavailable clipboard shows failed", async () => {
  const { fb, seen } = harness(undefined);
  await fb.copy("x");
  assert.deepEqual(seen, ["failed"]);
});

test("synchronous throw from the clipboard shows failed", async () => {
  const { fb, seen } = harness(() => {
    throw new Error("insecure context");
  });
  await fb.copy("x");
  assert.deepEqual(seen, ["failed"]);
});

test("a second click restarts the revert timer", async () => {
  const { fb, timers } = harness(async () => {});
  await fb.copy("a");
  await fb.copy("b");
  assert.equal(timers.length, 1);
});
