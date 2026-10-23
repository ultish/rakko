import type { TopicConsumer } from "./api";

const count = (n: number) => n.toLocaleString("en-US");

export const COLLAPSED_BY_DEFAULT = true;

export const toggle = (expanded: boolean) => !expanded;

export const consumersHeading = (count: number | null) => (count === null ? "Consumers" : `Consumers (${count})`);

export type ConsumerRow = { name: string; state: string; members: string; lag: string; lagging: boolean };

export const consumerRows = (groups: TopicConsumer[]): ConsumerRow[] =>
  groups.map((g) => ({ name: g.name, state: g.state, members: count(g.members), lag: count(g.total_lag), lagging: g.total_lag > 0 }));
