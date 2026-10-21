import type { Summary } from "./api";

const id = (r: Summary) => `${r.partition}:${r.offset}`;

/** Prepends a batch received oldest-first to rows kept newest-first, capped at `max`. */
export function prependLive(prev: Summary[], batch: Summary[], max: number): Summary[] {
  return batch.slice().reverse().concat(prev).slice(0, max);
}

/**
 * What the live view shows: messages that arrived live (newest first), then, when
 * `keep` is on, the messages loaded before switching to live. A message in both lists
 * appears once.
 */
export function liveView(live: Summary[], loaded: Summary[], keep: boolean, max: number): Summary[] {
  if (!keep) return live;
  const seen = new Set(live.map(id));
  return live.concat(loaded.filter((r) => !seen.has(id(r)))).slice(0, max);
}
