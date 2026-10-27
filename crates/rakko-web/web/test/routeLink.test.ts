import { test } from "node:test";
import assert from "node:assert/strict";
import { routeHref } from "../src/routeLink.ts";

test("topic name links to its topic route", () => {
  assert.equal(routeHref("local", "topics", "demo-events"), "#/local/topics/demo-events");
});

test("consumer group name links to its group route", () => {
  assert.equal(routeHref("local", "groups", "billing-svc"), "#/local/groups/billing-svc");
});

test("schema subject links to its schemas route", () => {
  assert.equal(routeHref("local", "schemas", "demo-orders-avro-value"), "#/local/schemas/demo-orders-avro-value");
});

test("names are percent-encoded so they survive a fresh page load", () => {
  assert.equal(routeHref("my prof", "topics", "a/b?c#d"), "#/my%20prof/topics/a%2Fb%3Fc%23d");
});

test("no item yields the view root", () => {
  assert.equal(routeHref("local", "groups", ""), "#/local/groups");
});
