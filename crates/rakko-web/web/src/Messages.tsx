import { useCallback, useEffect, useRef, useState } from "react";
import { api, type Range, type ScanSort, type Summary } from "./api";
import { readParams, setParams } from "./urlState";
import { formatCount } from "./format";
import { MessageTable } from "./MessageTable";
import { QueryInput } from "./QueryInput";
import { describeRange, emptyDraft, isEmptyDraft, RangePicker, toRange, type RangeDraft } from "./RangePicker";

type Status = {
  scanned: number;
  total: number;
  done: boolean;
  error: string | null;
  skipped: number;
};

const PAGE = 100;
const PREFETCH = 150;

type SavedSearch = { name: string; text: string; query: string; draft: RangeDraft; sort: ScanSort | null };

const savedKey = (profile: string, topic: string) => `rakko-saved:${profile}:${topic}`;

function loadSaved(profile: string, topic: string): SavedSearch[] {
  try {
    const raw = JSON.parse(localStorage.getItem(savedKey(profile, topic)) ?? "[]");
    return Array.isArray(raw) ? (raw as SavedSearch[]) : [];
  } catch {
    return [];
  }
}

type Props = {
  profile: string;
  topic: string;
  selected: Summary | null;
  onSelect: (s: Summary) => void;
  /** Reports the filter the visible results were produced with (for export / live tail). */
  onApplied?: (text: string, query: string, range: Range) => void;
  /** Latest value per key (a compacted topic read as a table). */
  table?: boolean;
  /** Reports how many messages the topic really holds once an unfiltered scan completes. */
  onStored?: (count: number | null) => void;
  /** Reports the rows currently loaded, so the live view can start from them. */
  onRows?: (rows: Summary[]) => void;
};

/**
 * Message list for a topic: a server-side scan over the *whole* topic (newest first),
 * optionally filtered. The list is virtualized and pulls more matches from the scan as
 * you scroll, so a filter keeps finding results without ever loading the topic here.
 */
export function Messages({ profile, topic, selected, onSelect, onApplied, table = false, onStored, onRows }: Props) {
  const [text, setText] = useState("");
  const [query, setQuery] = useState("");
  const [rows, setRows] = useState<Summary[]>([]);
  const [status, setStatus] = useState<Status | null>(null);
  const [error, setError] = useState<string | null>(null);

  const generation = useRef(0);
  const scanId = useRef<string | null>(null);
  const rowsRef = useRef<Summary[]>([]);
  const statusRef = useRef<Status | null>(null);
  const demand = useRef(PAGE);
  const pumping = useRef(false);
  const [epoch, setEpoch] = useState(0);
  const [draft, setDraft] = useState<RangeDraft>(emptyDraft);
  const [rangeOpen, setRangeOpen] = useState(false);
  const [applied, setApplied] = useState<Range>({});
  const [sort, setSort] = useState<ScanSort | null>(null);
  const [saved, setSaved] = useState<SavedSearch[]>(() => loadSaved(profile, topic));
  useEffect(() => setSaved(loadSaved(profile, topic)), [profile, topic]);
  const persist = (next: SavedSearch[]) => {
    setSaved(next);
    try {
      localStorage.setItem(savedKey(profile, topic), JSON.stringify(next));
    } catch {
      /* storage unavailable: the list just won't survive a reload */
    }
  };

  const pump = useCallback(async (gen: number) => {
    if (pumping.current) return;
    pumping.current = true;
    try {
      while (gen === generation.current && scanId.current) {
        const have = rowsRef.current.length;
        const st = statusRef.current;
        if (st?.done || st?.error || have >= demand.current) break;
        const page = await api.matches(scanId.current, have, PAGE);
        if (gen !== generation.current) break;
        if (page.matches.length > 0) {
          rowsRef.current = rowsRef.current.concat(page.matches);
          setRows(rowsRef.current);
        }
        const next: Status = {
          scanned: page.scanned_offsets,
          total: page.total_offsets,
          done: page.done && rowsRef.current.length >= page.found,
          error: page.error,
          skipped: page.skipped_undecodable,
        };
        statusRef.current = next;
        setStatus(next);
      }
    } catch (e) {
      if (gen === generation.current) setError(e instanceof Error ? e.message : String(e));
    } finally {
      pumping.current = false;
    }
  }, []);

  const start = useCallback(
    async (filterText: string, filterQuery: string, range: Range, order: ScanSort | null) => {
      const gen = ++generation.current;
      setApplied(range);
      onApplied?.(filterText, filterQuery, range);
      const previous = scanId.current;
      scanId.current = null;
      if (previous) api.deleteScan(previous).catch(() => {});
      rowsRef.current = [];
      statusRef.current = null;
      demand.current = PAGE;
      setRows([]);
      setStatus(null);
      setError(null);
      setEpoch((e) => e + 1);
      try {
        const created = await api.createScan(profile, topic, filterText, filterQuery, range, table, order ?? undefined);
        if (gen !== generation.current) {
          api.deleteScan(created.scan_id).catch(() => {});
          return;
        }
        scanId.current = created.scan_id;
        const initial: Status = { scanned: 0, total: created.total_offsets, done: created.total_offsets === 0, error: null, skipped: 0 };
        statusRef.current = initial;
        setStatus(initial);
        // pumping may still be winding down from the previous scan; give it a tick.
        while (pumping.current && gen === generation.current) await new Promise((r) => setTimeout(r, 20));
        void pump(gen);
      } catch (e) {
        if (gen === generation.current) setError(e instanceof Error ? e.message : String(e));
      }
    },
    [profile, topic, pump, onApplied, table],
  );

  // Opening a topic: start from what the address bar says (a shared link / reload), else a plain browse.
  useEffect(() => {
    const params = readParams();
    const t = params.get("text") ?? "";
    const q = params.get("query") ?? "";
    let d = emptyDraft;
    let r: Range = {};
    try {
      const raw = params.get("range");
      if (raw) {
        d = { ...emptyDraft, ...(JSON.parse(raw) as Partial<RangeDraft>) };
        r = toRange(d);
      }
    } catch {
      d = emptyDraft;
      r = {};
    }
    const [sk, sd] = (params.get("sort") ?? "").split(":");
    const s: ScanSort | null = ["time", "offset", "partition", "key", "value"].includes(sk) ? { key: sk as ScanSort["key"], desc: sd === "desc" } : null;
    setText(t);
    setQuery(q);
    setDraft(d);
    setSort(s);
    setRangeOpen(!isEmptyDraft(d));
    void start(t, q, r, s);
    return () => {
      generation.current++;
      const id = scanId.current;
      scanId.current = null;
      if (id) api.deleteScan(id).catch(() => {});
    };
  }, [start]);

  const onVisibleEnd = useCallback(
    (lastVisible: number) => {
      demand.current = Math.max(demand.current, lastVisible + PREFETCH);
      void pump(generation.current);
    },
    [pump],
  );

  const remember = (order: ScanSort | null) =>
    setParams({
      text: text.trim() ? text : undefined,
      query: query.trim() ? query : undefined,
      range: isEmptyDraft(draft) ? undefined : JSON.stringify(draft),
      sort: order ? `${order.key}:${order.desc ? "desc" : "asc"}` : undefined,
    });

  const saveCurrent = () => {
    const name = window.prompt("Name for this search:", text || query || "my search")?.trim();
    if (!name) return;
    persist([...saved.filter((x) => x.name !== name), { name, text, query, draft, sort }]);
  };

  const applySaved = (x: SavedSearch) => {
    setText(x.text);
    setQuery(x.query);
    setDraft(x.draft);
    setSort(x.sort);
    setRangeOpen(!isEmptyDraft(x.draft));
    try {
      const range = toRange(x.draft);
      setParams({
        text: x.text.trim() ? x.text : undefined,
        query: x.query.trim() ? x.query : undefined,
        range: isEmptyDraft(x.draft) ? undefined : JSON.stringify(x.draft),
        sort: x.sort ? `${x.sort.key}:${x.sort.desc ? "desc" : "asc"}` : undefined,
      });
      void start(x.text, x.query, range, x.sort);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    }
  };

  const submit = (order: ScanSort | null = sort) => {
    try {
      const range = toRange(draft);
      remember(order);
      void start(text, query, range, order);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
      setRangeOpen(true);
    }
  };
  const pct = status && status.total > 0 ? Math.min(100, Math.round((status.scanned / status.total) * 100)) : status ? 100 : 0;
  const rangeLabel = describeRange(applied);
  const filtered = text.trim() !== "" || query.trim() !== "" || rangeLabel !== "";
  const stored = !table && !filtered && status?.done && !status.error && !error ? rows.length : null;
  useEffect(() => onStored?.(stored), [stored, onStored]);
  useEffect(() => onRows?.(rows), [rows, onRows]);

  return (
    <div className="messages">
      <form
        className="filters"
        onSubmit={(e) => {
          e.preventDefault();
          submit();
        }}
      >
        <input
          className="text-filter"
          value={text}
          onChange={(e) => setText(e.target.value)}
          placeholder="text contains…"
          spellCheck={false}
        />
        <QueryInput
          profile={profile}
          topic={topic}
          value={query}
          onChange={setQuery}
          onSubmit={() => submit()}
          placeholder={'query — value.customer.tier = "gold" AND value.qty > 2   (Tab to complete)'}
        />
        <button type="button" className={rangeOpen || !isEmptyDraft(draft) ? "toggle on" : "toggle"} onClick={() => setRangeOpen((o) => !o)} aria-expanded={rangeOpen}>
          Range{!isEmptyDraft(draft) ? " •" : ""}
        </button>
        <details className="menu">
          <summary title="Searches you saved for this topic (kept in this browser)">Saved{saved.length ? ` (${saved.length})` : ""} ▾</summary>
          <div>
            {saved.map((x) => (
              <span key={x.name} className="saved-item">
                <button type="button" onClick={() => applySaved(x)} title={[x.text, x.query].filter(Boolean).join("  ·  ") || "no filter"}>
                  {x.name}
                </button>
                <button type="button" className="link" aria-label={`delete ${x.name}`} onClick={() => persist(saved.filter((y) => y.name !== x.name))}>
                  ✕
                </button>
              </span>
            ))}
            {saved.length === 0 && <span className="dim pad small">Nothing saved yet.</span>}
            <button type="button" onClick={saveCurrent}>
              + Save current search…
            </button>
          </div>
        </details>
        <button type="submit" className="primary">
          {filtered ? "Search" : "Refresh"}
        </button>
      </form>

      {rangeOpen && <RangePicker profile={profile} topic={topic} value={draft} onChange={setDraft} onApply={() => submit()} />}

      <div className="status" role="status">
        <div className="progress" aria-hidden>
          <div style={{ width: `${pct}%` }} className={status?.done ? "done" : undefined} />
        </div>
        <span>
          {status && sort && rows.length === 0 && (table || sort.desc || !(sort.key === "time" || sort.key === "offset")) && !status.done && !status.error
            ? `sorting: reading the whole topic first · scanned ${pct}% of ${formatCount(status.total)} offsets`
            : status
            ? `${formatCount(rows.length)}${status.done ? "" : "+"} ${filtered ? "matches" : table ? "keys" : "messages"} · scanned ${pct}% of ${formatCount(status.total)} offsets${status.done ? " · complete" : ""}`
            : error
              ? ""
              : "starting scan…"}
        </span>
        {rangeLabel && <span className="range-tag">{rangeLabel}</span>}
        {status && status.skipped > 0 && (
          <span className="warn">
            ⚠ {formatCount(status.skipped)} message(s) couldn't be decoded (schema unavailable) and were not matched
          </span>
        )}
        {(error || status?.error) && <span className="err">{error ?? status?.error}</span>}
      </div>

      <MessageTable
        rows={rows}
        selected={selected}
        onSelect={onSelect}
        loading={status ? !status.done && !status.error : !error}
        onVisibleEnd={onVisibleEnd}
        resetKey={epoch}
        sort={sort}
        onSort={(next) => {
          setSort(next);
          submit(next);
        }}
        table={table}
        empty={filtered ? "No messages match this filter." : table ? "No live keys: the topic is empty or every key was deleted." : "This topic is empty."}
      />
    </div>
  );
}
