import { useCallback, useEffect, useRef, useState } from "react";
import { api, type Range, type Summary } from "./api";
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

type Props = {
  profile: string;
  topic: string;
  selected: Summary | null;
  onSelect: (s: Summary) => void;
  /** Reports the filter the visible results were produced with (for export / live tail). */
  onApplied?: (text: string, query: string, range: Range) => void;
};

/**
 * Message list for a topic: a server-side scan over the *whole* topic (newest first),
 * optionally filtered. The list is virtualized and pulls more matches from the scan as
 * you scroll, so a filter keeps finding results without ever loading the topic here.
 */
export function Messages({ profile, topic, selected, onSelect, onApplied }: Props) {
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
    async (filterText: string, filterQuery: string, range: Range) => {
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
        const created = await api.createScan(profile, topic, filterText, filterQuery, range);
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
    [profile, topic, pump, onApplied],
  );

  // New topic: forget filters and browse the whole topic.
  useEffect(() => {
    setText("");
    setQuery("");
    setDraft(emptyDraft);
    setRangeOpen(false);
    void start("", "", {});
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

  const submit = () => {
    try {
      void start(text, query, toRange(draft));
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
      setRangeOpen(true);
    }
  };
  const pct = status && status.total > 0 ? Math.min(100, Math.round((status.scanned / status.total) * 100)) : status ? 100 : 0;
  const rangeLabel = describeRange(applied);
  const filtered = text.trim() !== "" || query.trim() !== "" || rangeLabel !== "";

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
          onSubmit={submit}
          placeholder={'query — value.customer.tier = "gold" AND value.qty > 2   (Tab to complete)'}
        />
        <button type="button" className={rangeOpen || !isEmptyDraft(draft) ? "toggle on" : "toggle"} onClick={() => setRangeOpen((o) => !o)} aria-expanded={rangeOpen}>
          Range{!isEmptyDraft(draft) ? " •" : ""}
        </button>
        <button type="submit" className="primary">
          {filtered ? "Search" : "Refresh"}
        </button>
      </form>

      {rangeOpen && <RangePicker profile={profile} topic={topic} value={draft} onChange={setDraft} onApply={submit} />}

      <div className="status" role="status">
        <div className="progress" aria-hidden>
          <div style={{ width: `${pct}%` }} className={status?.done ? "done" : undefined} />
        </div>
        <span>
          {status
            ? `${formatCount(rows.length)}${status.done ? "" : "+"} ${filtered ? "matches" : "messages"} · scanned ${pct}% of ${formatCount(status.total)} offsets${status.done ? " · complete" : ""}`
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
        empty={filtered ? "No messages match this filter." : "This topic is empty."}
      />
    </div>
  );
}
