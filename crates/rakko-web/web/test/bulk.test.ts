import { test } from "node:test";
import assert from "node:assert/strict";
import { confirmMatches, filterTopics, runBulk } from "../src/bulk.ts";

const topics = [{ name: "orders" }, { name: "Orders-dlq" }, { name: "users" }];

test("filterTopics matches case-insensitively and an empty filter matches nothing for bulk", () => {
  assert.deepEqual(filterTopics(topics, "ORDERS").map((t) => t.name), ["orders", "Orders-dlq"]);
  assert.deepEqual(filterTopics(topics, "   "), []);
  assert.deepEqual(filterTopics(topics, "zzz"), []);
});

test("confirmMatches requires the exact count", () => {
  assert.equal(confirmMatches("2", 2), true);
  assert.equal(confirmMatches(" 2 ", 2), true);
  assert.equal(confirmMatches("3", 2), false);
  assert.equal(confirmMatches("", 2), false);
  assert.equal(confirmMatches("02", 2), false);
  assert.equal(confirmMatches("0", 0), false);
});

test("runBulk reports every topic and one failure does not stop the rest", async () => {
  const calls: string[] = [];
  const results = await runBulk(["a", "b", "c"], async (t) => {
    calls.push(t);
    if (t === "b") throw new Error("boom");
    return t === "c" ? { removed: 5 } : undefined;
  });
  assert.deepEqual(calls, ["a", "b", "c"]);
  assert.deepEqual(results, [
    { topic: "a", ok: true },
    { topic: "b", ok: false, error: "boom" },
    { topic: "c", ok: true },
  ]);
});
