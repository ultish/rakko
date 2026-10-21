import { test } from "node:test";
import assert from "node:assert/strict";
import { liveView, prependLive } from "../src/liveRows.ts";
import type { Summary } from "../src/api.ts";

const row = (partition: number, offset: number): Summary =>
  ({ partition, offset, timestamp_ms: null, key: null, value: null, header_count: 0 }) as unknown as Summary;
const ids = (rows: Summary[]) => rows.map((r) => `${r.partition}:${r.offset}`);

test("keep on: new live messages come first, already-loaded messages stay below", () => {
  const loaded = [row(0, 5), row(0, 4), row(0, 3)];
  const live = [row(0, 7), row(0, 6)];
  assert.deepEqual(ids(liveView(live, loaded, true, 100)), ["0:7", "0:6", "0:5", "0:4", "0:3"]);
});

test("keep on with nothing live yet shows the loaded messages", () => {
  const loaded = [row(0, 1), row(1, 9)];
  assert.deepEqual(ids(liveView([], loaded, true, 100)), ["0:1", "1:9"]);
});

test("keep off: only messages that arrived since switching", () => {
  const loaded = [row(0, 5)];
  assert.deepEqual(ids(liveView([], loaded, false, 100)), []);
  assert.deepEqual(ids(liveView([row(0, 6)], loaded, false, 100)), ["0:6"]);
});

test("a message present in both lists appears once, in its live position", () => {
  const loaded = [row(0, 5), row(0, 4)];
  const live = [row(0, 6), row(0, 5)];
  assert.deepEqual(ids(liveView(live, loaded, true, 100)), ["0:6", "0:5", "0:4"]);
});

test("same offset on different partitions are different messages", () => {
  assert.deepEqual(ids(liveView([row(1, 5)], [row(0, 5)], true, 100)), ["1:5", "0:5"]);
});

test("the view is capped, dropping the oldest", () => {
  const loaded = [row(0, 3), row(0, 2), row(0, 1)];
  assert.deepEqual(ids(liveView([row(0, 4)], loaded, true, 2)), ["0:4", "0:3"]);
});

test("prependLive puts a batch (oldest first) ahead of earlier rows, newest first, capped", () => {
  const prev = [row(0, 2), row(0, 1)];
  assert.deepEqual(ids(prependLive(prev, [row(0, 3), row(0, 4)], 3)), ["0:4", "0:3", "0:2"]);
});
