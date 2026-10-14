import { useEffect, useState } from "react";
import { api, type PartitionInfo, type Range } from "./api";

/** The form's raw field values; [`toRange`] turns them into a validated [`Range`]. */
export type RangeDraft = { from: string; to: string; partition: string; fromOffset: string; toOffset: string };

export const emptyDraft: RangeDraft = { from: "", to: "", partition: "", fromOffset: "", toOffset: "" };

const pad = (n: number) => String(n).padStart(2, "0");

/** `datetime-local` value (local time, minute precision). */
function local(ms: number): string {
  const d = new Date(ms);
  return `${d.getFullYear()}-${pad(d.getMonth() + 1)}-${pad(d.getDate())}T${pad(d.getHours())}:${pad(d.getMinutes())}`;
}

const PRESETS: { label: string; ms: number }[] = [
  { label: "15 min", ms: 15 * 60_000 },
  { label: "1 h", ms: 60 * 60_000 },
  { label: "24 h", ms: 24 * 60 * 60_000 },
  { label: "7 d", ms: 7 * 24 * 60 * 60_000 },
];

export function isEmptyDraft(d: RangeDraft): boolean {
  return !d.from && !d.to && !d.partition && !d.fromOffset && !d.toOffset;
}

/** Throws a user-readable message for a malformed range. */
export function toRange(d: RangeDraft): Range {
  const range: Range = {};
  const time = (s: string, what: string) => {
    const ms = Date.parse(s);
    if (Number.isNaN(ms)) throw new Error(`${what}: not a valid date/time`);
    return ms;
  };
  const offset = (s: string, what: string) => {
    const n = Number(s);
    if (!Number.isInteger(n) || n < 0) throw new Error(`${what}: offsets are whole numbers, 0 or more`);
    return n;
  };
  if (d.from) range.fromMs = time(d.from, "From");
  if (d.to) range.toMs = time(d.to, "To");
  if (range.fromMs !== undefined && range.toMs !== undefined && range.fromMs >= range.toMs) throw new Error("The range must end after it starts");
  if (d.partition) range.partition = Number(d.partition);
  if (d.fromOffset) range.fromOffset = offset(d.fromOffset, "From offset");
  if (d.toOffset) range.toOffset = offset(d.toOffset, "To offset");
  if (range.partition === undefined && (range.fromOffset !== undefined || range.toOffset !== undefined)) {
    throw new Error("Pick a partition to use an offset range");
  }
  return range;
}

export function describeRange(r: Range): string {
  const parts: string[] = [];
  const t = (ms: number) => new Date(ms).toLocaleString();
  if (r.fromMs !== undefined || r.toMs !== undefined) {
    parts.push(`${r.fromMs !== undefined ? t(r.fromMs) : "start"} → ${r.toMs !== undefined ? t(r.toMs) : "now"}`);
  }
  if (r.partition !== undefined) {
    const offsets = r.fromOffset !== undefined || r.toOffset !== undefined ? ` @ ${r.fromOffset ?? 0}…${r.toOffset ?? "end"}` : "";
    parts.push(`partition ${r.partition}${offsets}`);
  }
  return parts.join(" · ");
}

type Props = { profile: string; topic: string; value: RangeDraft; onChange: (d: RangeDraft) => void; onApply: () => void };

/** Limit a scan to a time window or one partition's offsets. Empty = the whole topic. */
export function RangePicker({ profile, topic, value, onChange, onApply }: Props) {
  const [partitions, setPartitions] = useState<PartitionInfo[]>([]);
  useEffect(() => {
    api.partitions(profile, topic).then(setPartitions).catch(() => setPartitions([]));
  }, [profile, topic]);
  const set = (patch: Partial<RangeDraft>) => onChange({ ...value, ...patch });
  const selected = partitions.find((p) => String(p.partition) === value.partition);

  return (
    <div className="range">
      <div className="range-row">
        <span className="dim">Time</span>
        <input type="datetime-local" value={value.from} onChange={(e) => set({ from: e.target.value })} aria-label="from" />
        <span className="dim">→</span>
        <input type="datetime-local" value={value.to} onChange={(e) => set({ to: e.target.value })} aria-label="to" />
        {PRESETS.map((p) => (
          <button key={p.label} type="button" className="link" onClick={() => set({ from: local(Date.now() - p.ms), to: "" })}>
            last {p.label}
          </button>
        ))}
      </div>
      <div className="range-row">
        <span className="dim">Partition</span>
        <select value={value.partition} onChange={(e) => set({ partition: e.target.value, fromOffset: "", toOffset: "" })} aria-label="partition">
          <option value="">all</option>
          {partitions.map((p) => (
            <option key={p.partition} value={p.partition}>
              {p.partition}
            </option>
          ))}
        </select>
        {value.partition !== "" && (
          <>
            <span className="dim">offsets</span>
            <input className="narrow-num" placeholder={selected ? String(selected.low) : "from"} inputMode="numeric" value={value.fromOffset} onChange={(e) => set({ fromOffset: e.target.value })} aria-label="from offset" />
            <span className="dim">…</span>
            <input className="narrow-num" placeholder={selected ? String(selected.high) : "to"} inputMode="numeric" value={value.toOffset} onChange={(e) => set({ toOffset: e.target.value })} aria-label="to offset" />
            <span className="dim small">end is exclusive{selected ? ` · retained ${selected.low}–${selected.high}` : ""}</span>
          </>
        )}
        <span className="spacer" />
        <button type="button" className="link" onClick={() => onChange(emptyDraft)}>
          clear
        </button>
        <button type="button" className="primary" onClick={onApply}>
          Apply range
        </button>
      </div>
    </div>
  );
}
