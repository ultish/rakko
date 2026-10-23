import { test } from "node:test";
import assert from "node:assert/strict";
import { COLLAPSED_BY_DEFAULT, consumersHeading, consumerRows, toggle } from "../src/consumerList.ts";

test("the panel starts collapsed and each click flips it", () => {
  assert.equal(COLLAPSED_BY_DEFAULT, true);
  const expanded = toggle(!COLLAPSED_BY_DEFAULT);
  assert.equal(expanded, true);
  assert.equal(toggle(expanded), false);
});

test("the heading shows the group count once loaded", () => {
  assert.equal(consumersHeading(null), "Consumers");
  assert.equal(consumersHeading(0), "Consumers (0)");
  assert.equal(consumersHeading(3), "Consumers (3)");
});

test("rows carry group, state, members and formatted lag as separate cells", () => {
  const rows = consumerRows([{ name: "billing", state: "Stable", total_lag: 1234, members: 2 }]);
  assert.deepEqual(rows, [{ name: "billing", state: "Stable", members: "2", lag: "1,234", lagging: true }]);
  assert.equal(consumerRows([{ name: "g", state: "Empty", total_lag: 0, members: 0 }])[0].lagging, false);
});
