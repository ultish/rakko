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

export type Field = { text?: string; base64?: string; avro?: { subject: string; json: string } };

export type TopicConfig = { name: string; value: string | null; overridden: boolean; read_only: boolean; sensitive: boolean; source: string };

export type AvroTemplate = { subject: string; version: number; id: number; template: unknown };
export type ProduceBody = { key?: Field; value?: Field; headers: { key: string; value: string }[] };

export type SchemaReference = { name: string; subject: string; version: number };

export type SubjectVersion = {
  subject: string;
  version: number;
  id: number;
  schemaType: string;
  schema: string;
  references: SchemaReference[];
};

export type SchemaDraft = { schema: string; schema_type: string; references: SchemaReference[] };

/** Restricts a scan to a time window (epoch ms; from inclusive, to exclusive) or one partition's offsets. */
export type Range = { fromMs?: number; toMs?: number; partition?: number; fromOffset?: number; toOffset?: number };

export type PartitionInfo = { partition: number; low: number; high: number; messages: number };

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

function filterParams(text: string, query: string, range: Range = {}): string {
  const p = new URLSearchParams();
  if (text.trim()) p.set("text", text);
  if (query.trim()) p.set("query", query);
  if (range.fromMs !== undefined) p.set("from_ms", String(range.fromMs));
  if (range.toMs !== undefined) p.set("to_ms", String(range.toMs));
  if (range.partition !== undefined) p.set("partition", String(range.partition));
  if (range.fromOffset !== undefined) p.set("from_offset", String(range.fromOffset));
  if (range.toOffset !== undefined) p.set("to_offset", String(range.toOffset));
  return p.toString();
}

const rangeBody = (r: Range) => ({
  from_ms: r.fromMs,
  to_ms: r.toMs,
  partition: r.partition,
  from_offset: r.fromOffset,
  to_offset: r.toOffset,
});

/** URLs the browser opens directly (download / event stream) rather than via fetch. */
export const urls = {
  export: (profile: string, topic: string, text: string, query: string, range: Range = {}) =>
    `/api${base(profile, topic)}/export?${filterParams(text, query, range)}`,
  /** `part`: key | value | message; `format`: raw | decoded (ignored for message). */
  download: (profile: string, topic: string, s: Summary, part: "key" | "value" | "message", format: "raw" | "decoded" = "raw") =>
    `/api${base(profile, topic)}/messages/${s.partition}/${s.offset}/download?part=${part}&format=${format}`,
  tail: (profile: string, topic: string, text: string, query: string) =>
    `/api${base(profile, topic)}/tail?${filterParams(text, query)}`,
};

export const api = {
  createTopic: (profile: string, body: { name: string; partitions: number; replication_factor: number; configs: Record<string, string> }) =>
    request<{ name: string }>(`/p/${enc(profile)}/topics`, { method: "POST", body: JSON.stringify(body) }),
  deleteTopic: (profile: string, topic: string) =>
    request<unknown>(`/p/${enc(profile)}/topics/${enc(topic)}?confirm=${enc(topic)}`, { method: "DELETE" }),
  topicConfigsAll: (profile: string, topic: string) => request<TopicConfig[]>(`/p/${enc(profile)}/topics/${enc(topic)}/configs/all`),
  alterTopicConfigs: (profile: string, topic: string, set: Record<string, string>, unset: string[]) =>
    request<{ ok: boolean }>(`/p/${enc(profile)}/topics/${enc(topic)}/configs`, { method: "PUT", body: JSON.stringify({ set, unset }) }),
  avroTemplate: (profile: string, subject: string) => request<AvroTemplate>(`/p/${enc(profile)}/schemas/${enc(subject)}/template`),
  topicConfigs: (profile: string, topic: string) =>
    request<{ name: string; value: string }[]>(`/p/${enc(profile)}/topics/${enc(topic)}/configs`),
  tombstone: (profile: string, topic: string, s: Summary) =>
    request<{ partition: number; offset: number }>(`${base(profile, topic)}/messages/${s.partition}/${s.offset}/tombstone`, {
      method: "POST",
    }),
  deleteRecords: (profile: string, topic: string, partition: number, beforeOffset: number) =>
    request<{ low: number }>(`${base(profile, topic)}/delete-records`, {
      method: "POST",
      body: JSON.stringify({ partition, before_offset: beforeOffset, confirm: topic }),
    }),
  subjects: (profile: string) => request<string[]>(`/p/${enc(profile)}/schemas`),
  versions: (profile: string, subject: string) => request<number[]>(`/p/${enc(profile)}/schemas/${enc(subject)}`),
  schemaVersion: (profile: string, subject: string, version: number | "latest") =>
    request<SubjectVersion>(`/p/${enc(profile)}/schemas/${enc(subject)}/versions/${version}`),
  checkSchema: (profile: string, subject: string, d: SchemaDraft) =>
    request<{ compatible: boolean }>(`/p/${enc(profile)}/schemas/${enc(subject)}/check`, { method: "POST", body: JSON.stringify(d) }),
  registerSchema: (profile: string, subject: string, d: SchemaDraft) =>
    request<{ id: number }>(`/p/${enc(profile)}/schemas/${enc(subject)}`, { method: "POST", body: JSON.stringify(d) }),
  deleteSchemaVersion: (profile: string, subject: string, version: number, permanent: boolean) =>
    request<unknown>(`/p/${enc(profile)}/schemas/${enc(subject)}/versions/${version}?permanent=${permanent}&confirm=${enc(subject)}`, {
      method: "DELETE",
    }),
  deleteSubject: (profile: string, subject: string, permanent: boolean) =>
    request<unknown>(`/p/${enc(profile)}/schemas/${enc(subject)}?permanent=${permanent}&confirm=${enc(subject)}`, { method: "DELETE" }),
  compatibility: (profile: string, subject?: string) =>
    request<{ level: string }>(`/p/${enc(profile)}/${subject ? `schemas/${enc(subject)}/config` : "schema-config"}`),
  setCompatibility: (profile: string, level: string, subject?: string) =>
    request<{ level: string }>(`/p/${enc(profile)}/${subject ? `schemas/${enc(subject)}/config` : "schema-config"}`, {
      method: "PUT",
      body: JSON.stringify({ level }),
    }),
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
  partitions: (profile: string, topic: string) => request<PartitionInfo[]>(`${base(profile, topic)}/partitions`),
  createScan: (profile: string, topic: string, text: string, query: string, range: Range = {}, table = false) =>
    request<{ scan_id: string; total_offsets: number }>(`/p/${enc(profile)}/topics/${enc(topic)}/scans`, {
      method: "POST",
      body: JSON.stringify({ text, query, range: rangeBody(range), table }),
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
