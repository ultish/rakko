import { useEffect, useMemo, useRef, useState } from "react";
import { useVirtualizer } from "@tanstack/react-virtual";
import type { ScanSort, Summary } from "./api";
import { formatBytes, formatTime } from "./format";
import { isPlainLeftClick, messageHref } from "./messageLink";

const ROW_HEIGHT = 30;

type Props = {
  profile: string;
  topic: string;
  rows: Summary[];
  selected: Summary | null;
  onSelect: (s: Summary) => void;
  /** Show a trailing "scanning…" row (more rows are still coming). */
  loading?: boolean;
  /** Called with the index of the last visible row as the user scrolls. */
  onVisibleEnd?: (index: number) => void;
  /** Scrolls back to the top whenever this changes (new filter / new topic). */
  resetKey?: unknown;
  empty?: string;
  /** Show the value as columns (one per JSON field) instead of a single preview. */
  table?: boolean;
  /** Server-side order (see `ScanSort`; `null` = newest first). Without `onSort`, headers sort the loaded rows locally. */
  sort?: ScanSort | null;
  onSort?: (s: ScanSort | null) => void;
};

type SortKey = "time" | "partition" | "offset" | "key" | "value" | "fmt";

const sortValue = (r: Summary, k: SortKey): string | number => {
  switch (k) {
    case "time":
      return r.timestamp_ms ?? -1;
    case "partition":
      return r.partition;
    case "offset":
      return r.offset;
    case "key":
      return r.key.preview;
    case "value":
      return r.value.preview;
    case "fmt":
      return r.value.format;
  }
};

const MAX_COLUMNS = 12;

/** Parses a row's value preview as a JSON object, when it is one (and not truncated). */
function valueObject(r: Summary): Record<string, unknown> | null {
  try {
    const v: unknown = JSON.parse(r.value.preview);
    return v && typeof v === "object" && !Array.isArray(v) ? (v as Record<string, unknown>) : null;
  } catch {
    return null;
  }
}

const cell = (v: unknown): string => (v === undefined ? "" : typeof v === "string" ? v : JSON.stringify(v));

/** Virtualized message rows, shared by the scan view and the live tail. */
export function MessageTable({ profile, topic, rows: unsorted, selected, onSelect, loading, onVisibleEnd, resetKey, empty, table, sort: serverSort, onSort }: Props) {
  const [localSort, setSort] = useState<{ key: SortKey; dir: 1 | -1 } | null>(null);
  const sort = onSort ? null : localSort;
  // Sorting orders what has been loaded so far; the scan itself always walks newest first.
  const rows = useMemo(() => {
    if (!sort) return unsorted;
    return [...unsorted].sort((a, b) => {
      const x = sortValue(a, sort.key);
      const y = sortValue(b, sort.key);
      return (x < y ? -1 : x > y ? 1 : 0) * sort.dir;
    });
  }, [unsorted, sort]);
  const parsed = useMemo(() => (table ? rows.map(valueObject) : []), [table, rows]);
  const columns = useMemo(() => {
    if (!table) return [];
    const seen = new Map<string, number>();
    for (const o of parsed.slice(0, 300)) for (const k of Object.keys(o ?? {})) seen.set(k, (seen.get(k) ?? 0) + 1);
    return [...seen.entries()].sort((a, b) => b[1] - a[1]).slice(0, MAX_COLUMNS).map(([k]) => k);
  }, [table, parsed]);
  const asColumns = table && columns.length > 0;
  const template = asColumns ? `minmax(90px, 200px) ${columns.map(() => "minmax(80px, 1fr)").join(" ")}` : undefined;
  const toggle = (key: SortKey) => {
    if (onSort) {
      if (key === "fmt") return;
      const active = serverSort ?? { key: "time" as const, desc: true };
      const next = active.key === key ? { key, desc: !active.desc } : { key, desc: false };
      onSort(next.key === "time" && next.desc ? null : next);
      return;
    }
    setSort((s) => (s?.key !== key ? { key, dir: 1 } : s.dir === 1 ? { key, dir: -1 } : null));
  };
  const head = (key: SortKey, label: string) => {
    const active = onSort ? (serverSort ?? { key: "time" as const, desc: true }) : null;
    const arrow = onSort ? (active?.key === key ? (active.desc ? " ▼" : " ▲") : "") : sort?.key === key ? (sort.dir === 1 ? " ▲" : " ▼") : "";
    return (
      <span
        className={onSort && key === "fmt" ? undefined : "sortable"}
        onClick={() => toggle(key)}
        title={onSort ? "Sort the whole topic (a non-default order scans everything first)" : "Sort the loaded messages (click again to reverse, once more to clear)"}
      >
        {label}
        {arrow}
      </span>
    );
  };
  const scroller = useRef<HTMLDivElement>(null);
  const virtualizer = useVirtualizer({
    count: rows.length + (loading ? 1 : 0),
    getScrollElement: () => scroller.current,
    estimateSize: () => ROW_HEIGHT,
    overscan: 14,
  });
  const items = virtualizer.getVirtualItems();
  const lastVisible = items.length > 0 ? items[items.length - 1].index : 0;

  useEffect(() => {
    onVisibleEnd?.(lastVisible);
  }, [lastVisible, rows.length, onVisibleEnd]);

  useEffect(() => {
    scroller.current?.scrollTo({ top: 0 });
  }, [resetKey]);

  return (
    <div className="table">
      <div className="row head" style={template ? { gridTemplateColumns: template } : undefined}>
        {asColumns ? (
          <>
            {head("key", "key")}
            {columns.map((c) => (
              <span key={c} className="clip">
                {c}
              </span>
            ))}
          </>
        ) : (
          <>
            {head("time", "time")}
            {head("partition", "p")}
            {head("offset", "offset")}
            {head("key", "key")}
            {head("value", "value")}
            {head("fmt", "fmt")}
          </>
        )}
      </div>
      <div className="scroller" ref={scroller}>
        <div style={{ height: virtualizer.getTotalSize(), position: "relative" }}>
          {items.map((item) => {
            const row = rows[item.index];
            const obj = asColumns ? parsed[item.index] : null;
            const style = { ...(template ? { gridTemplateColumns: template } : {}), position: "absolute" as const, top: 0, left: 0, width: "100%", height: item.size, transform: `translateY(${item.start}px)` };
            if (!row) {
              return (
                <div key="loading" className="row loading" style={style}>
                  scanning…
                </div>
              );
            }
            const isSelected = selected?.partition === row.partition && selected.offset === row.offset;
            return (
              <a
                key={`${row.partition}:${row.offset}`}
                className={`row${isSelected ? " selected" : ""}`}
                style={style}
                href={messageHref(profile, topic, row.partition, row.offset)}
                onClick={(e) => {
                  if (!isPlainLeftClick(e)) return;
                  e.preventDefault();
                  onSelect(row);
                }}
              >
                {asColumns ? (
                  <>
                    <span className="clip" title={row.key.preview}>
                      {row.key.preview}
                    </span>
                    {columns.map((c) => (
                      <span key={c} className="clip" title={cell(obj?.[c])}>
                        {cell(obj?.[c])}
                      </span>
                    ))}
                  </>
                ) : (
                  <>
                    <span className="dim">{formatTime(row.timestamp_ms)}</span>
                    <span>{row.partition}</span>
                    <span>{row.offset}</span>
                    <span className="clip" title={row.key.preview}>
                      {row.key.preview}
                    </span>
                    <span className="clip" title={`${formatBytes(row.value.size)}`}>
                      {row.value.preview}
                    </span>
                    <span className="fmt">{row.value.format}</span>
                  </>
                )}
              </a>
            );
          })}
        </div>
        {!loading && rows.length === 0 && empty && <div className="empty">{empty}</div>}
      </div>
    </div>
  );
}
