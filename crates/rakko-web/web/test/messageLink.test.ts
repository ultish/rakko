import { test } from "node:test";
import assert from "node:assert/strict";
import { isPlainLeftClick, messageHref, parseMsgParam } from "../src/messageLink.ts";

test("href points at the message under the topic route", () => {
  assert.equal(messageHref("local", "orders", 2, 15), "#/local/topics/orders?msg=2%3A15");
});

test("href encodes profile and topic names", () => {
  assert.equal(messageHref("my prof", "a/b", 0, 1), "#/my%20prof/topics/a%2Fb?msg=0%3A1");
});

test("href round-trips through the msg param parser", () => {
  const href = messageHref("local", "orders", 3, 42);
  const msg = new URLSearchParams(href.slice(href.indexOf("?") + 1)).get("msg");
  assert.deepEqual(parseMsgParam(msg), { partition: 3, offset: 42 });
});

test("parseMsgParam rejects junk", () => {
  assert.equal(parseMsgParam(null), null);
  assert.equal(parseMsgParam("x:1"), null);
});

test("only an unmodified primary click is handled in-page", () => {
  const base = { button: 0, metaKey: false, ctrlKey: false, shiftKey: false, altKey: false };
  assert.equal(isPlainLeftClick(base), true);
  assert.equal(isPlainLeftClick({ ...base, button: 1 }), false);
  assert.equal(isPlainLeftClick({ ...base, ctrlKey: true }), false);
  assert.equal(isPlainLeftClick({ ...base, metaKey: true }), false);
  assert.equal(isPlainLeftClick({ ...base, shiftKey: true }), false);
});
