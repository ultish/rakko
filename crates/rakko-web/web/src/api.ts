export type Payload = { format: string; preview: string; size: number | null };

export type Summary = {
  partition: number;
  offset: number;
  timestamp_ms: number | null;
  key: Payload;
  value: Payload;
  header_count: number;
};

export type MatchesPage = {
  matches: Summary[];
  found: number;
  scanned_offsets: number;
  total_offsets: number;
  done: boolean;
  error: string | null;
  skipped_undecodable: number;
};

export type Topic = {
  name: string;
  partitions: number;
  replication_factor: number;
  compression: string;
  messages: number;
};

export type Profile = { name: string; has_schema_registry: boolean };

export type Detail = {
  partition: number;
  offset: number;
  timestamp_ms: number | null;
  headers: [string, string][];
  part: "key" | "value";
  present: boolean;
  format: string;
  size: number;
  text: string;
  start: number;
  end: number;
  total: number;
};

export type Group = { name: string; state: string; members: number; protocol: string };

export type GroupDetail = {
  name: string;
  state: string;
  total_lag: number;
  members: { id: string; client_id: string; host: string }[];
  lags: { topic: string; partition: number; committed: number | null; low: number; high: number; lag: number | null }[];
};

export type ResetTarget =
  | { kind: "earliest" }
  | { kind: "latest" }
  | { kind: "offset"; value: number }
  | { kind: "timestamp"; value: number };

export type Brokers = {
  under_replicated: number;
  offline: number;
  brokers: { id: number; host: string; port: number; leader_partitions: number; replica_partitions: number }[];
};

export type Field = { text?: string; base64?: string };
export type ProduceBody = { key?: Field; value?: Field; headers: { key: string; value: string }[] };

export type Completion = { prefix: string; candidates: string[] };

// Scans belong to this browser's session on the server, so concurrent users never share a filter.
const session = (() => {
  try {
    let id = localStorage.getItem("rakko-session");
    if (!id) {
      id = crypto.randomUUID();
      localStorage.setItem("rakko-session", id);
    }
    return id;
  } catch {
    return crypto.randomUUID();
  }
})();

async function request<T>(path: string, init: RequestInit = {}): Promise<T> {
  const response = await fetch(`/api${path}`, {
    ...init,
    headers: { "x-rakko-session": session, "content-type": "application/json", ...init.headers },
  });
  const text = await response.text();
  if (!response.ok) {
    let message = response.statusText;
    try {
      message = JSON.parse(text).error ?? message;
    } catch {
      /* non-JSON error body */
    }
    throw new Error(message);
  }
  return (text ? JSON.parse(text) : undefined) as T;
}

const enc = encodeURIComponent;

const base = (profile: string, topic: string) => `/p/${enc(profile)}/topics/${enc(topic)}`;

function filterParams(text: string, query: string): string {
  const p = new URLSearchParams();
  if (text.trim()) p.set("text", text);
  if (query.trim()) p.set("query", query);
  return p.toString();
}

/** URLs the browser opens directly (download / event stream) rather than via fetch. */
export const urls = {
  export: (profile: string, topic: string, text: string, query: string) =>
    `/api${base(profile, topic)}/export?${filterParams(text, query)}`,
  tail: (profile: string, topic: string, text: string, query: string) =>
    `/api${base(profile, topic)}/tail?${filterParams(text, query)}`,
};

export const api = {
  groups: (profile: string) => request<Group[]>(`/p/${enc(profile)}/groups`),
  group: (profile: string, group: string) => request<GroupDetail>(`/p/${enc(profile)}/groups/${enc(group)}`),
  resetGroup: (profile: string, group: string, target: ResetTarget, partitions?: { topic: string; partition: number }[]) =>
    request<{ reset: number }>(`/p/${enc(profile)}/groups/${enc(group)}/reset`, {
      method: "POST",
      body: JSON.stringify({ target, partitions, confirm: group }),
    }),
  brokers: (profile: string) => request<Brokers>(`/p/${enc(profile)}/brokers`),
  brokerConfigs: (profile: string, id: number) =>
    request<{ name: string; value: string }[]>(`/p/${enc(profile)}/brokers/${id}/configs`),
  produce: (profile: string, topic: string, body: ProduceBody) =>
    request<{ partition: number; offset: number }>(`${base(profile, topic)}/produce`, {
      method: "POST",
      body: JSON.stringify(body),
    }),
  replay: (profile: string, topic: string, s: Summary) =>
    request<{ partition: number; offset: number }>(`${base(profile, topic)}/messages/${s.partition}/${s.offset}/replay`, {
      method: "POST",
    }),
  importFile: (profile: string, topic: string, file: File) =>
    request<{ sent: number; failed: number; errors: string[] }>(`${base(profile, topic)}/import`, {
      method: "POST",
      body: file,
      headers: { "content-type": "application/x-ndjson" },
    }),
  profiles: () => request<Profile[]>("/profiles"),
  topics: (profile: string) => request<Topic[]>(`/p/${enc(profile)}/topics`),
  createScan: (profile: string, topic: string, text: string, query: string) =>
    request<{ scan_id: string; total_offsets: number }>(`/p/${enc(profile)}/topics/${enc(topic)}/scans`, {
      method: "POST",
      body: JSON.stringify({ text, query }),
    }),
  matches: (scanId: string, from: number, limit: number, signal?: AbortSignal) =>
    request<MatchesPage>(`/scans/${enc(scanId)}/matches?from=${from}&limit=${limit}&wait_ms=2500`, { signal }),
  deleteScan: (scanId: string) => request<void>(`/scans/${enc(scanId)}`, { method: "DELETE" }),
  complete: (profile: string, topic: string, input: string, signal?: AbortSignal) =>
    request<Completion>(`/p/${enc(profile)}/topics/${enc(topic)}/complete`, {
      method: "POST",
      body: JSON.stringify({ input }),
      signal,
    }),
  detail: (profile: string, topic: string, s: Summary, part: "key" | "value", start = 0) =>
    request<Detail>(
      `/p/${enc(profile)}/topics/${enc(topic)}/messages/${s.partition}/${s.offset}?part=${part}&start=${start}`,
    ),
};
