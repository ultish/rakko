import { useCallback, useEffect, useRef, useState } from "react";
import { urls, type Summary } from "./api";
import { formatCount } from "./format";
import { MessageTable } from "./MessageTable";
import { QueryInput } from "./QueryInput";
import { readParams, setParams } from "./urlState";

const MAX_ROWS = 5000;

type Props = {
  profile: string;
  topic: string;
  selected: Summary | null;
  onSelect: (s: Summary) => void;
  initialText: string;
  initialQuery: string;
};

/** Live tail: new messages from the log end, newest first, via server-sent events. */
export function Tail({ profile, topic, selected, onSelect, initialText, initialQuery }: Props) {
  const start = { text: readParams().get("text") ?? initialText, query: readParams().get("query") ?? initialQuery };
  const [text, setText] = useState(start.text);
  const [query, setQuery] = useState(start.query);
  const [applied, setApplied] = useState(start);
  const [rows, setRows] = useState<Summary[]>([]);
  const [paused, setPaused] = useState(false);
  const [state, setState] = useState<"connecting" | "live" | "error">("connecting");
  const [error, setError] = useState<string | null>(null);
  const [dropped, setDropped] = useState(0);
  const [total, setTotal] = useState(0);
  const pausedRef = useRef(false);
  const buffer = useRef<Summary[]>([]);
  // Counters live in refs and reach state with each batch: a state update per message
  // re-renders the page per message, which is what made a busy topic fall behind.
  const received = useRef(0);
  const skipped = useRef(0);

  pausedRef.current = paused;

  useEffect(() => {
    setRows([]);
    setTotal(0);
    setDropped(0);
    setError(null);
    setState("connecting");
    buffer.current = [];
    received.current = 0;
    skipped.current = 0;
    const source = new EventSource(urls.tail(profile, topic, applied.text, applied.query));
    source.onopen = () => setState("live");
    source.addEventListener("messages", (e) => {
      const batch = JSON.parse((e as MessageEvent<string>).data) as Summary[];
      received.current += batch.length;
      if (pausedRef.current) return;
      for (const row of batch) buffer.current.push(row);
    });
    source.addEventListener("dropped", (e) => {
      skipped.current += (JSON.parse((e as MessageEvent<string>).data) as { count: number }).count;
    });
    source.addEventListener("error", (e) => {
      const data = (e as MessageEvent<string | undefined>).data;
      if (data) {
        setError((JSON.parse(data) as { message: string }).message);
        setState("error");
        source.close();
      } else if (source.readyState === EventSource.CLOSED) {
        setState("error");
      } else {
        setState("connecting"); // the browser reconnects on its own
      }
    });
    // Batch renders: a busy topic must not re-render per message.
    const flush = window.setInterval(() => {
      setTotal(received.current);
      setDropped(skipped.current);
      if (buffer.current.length === 0) return;
      const incoming = buffer.current.reverse();
      buffer.current = [];
      setRows((prev) => incoming.concat(prev).slice(0, MAX_ROWS));
    }, 250);
    return () => {
      window.clearInterval(flush);
      source.close();
    };
  }, [profile, topic, applied]);

  const submit = () => {
    setApplied({ text, query });
    setParams({ text: text.trim() ? text : undefined, query: query.trim() ? query : undefined });
  };
  const noop = useCallback(() => {}, []);

  return (
    <div className="messages">
      <form
        className="filters"
        onSubmit={(e) => {
          e.preventDefault();
          submit();
        }}
      >
        <input className="text-filter" value={text} onChange={(e) => setText(e.target.value)} placeholder="text contains…" spellCheck={false} />
        <QueryInput profile={profile} topic={topic} value={query} onChange={setQuery} onSubmit={submit} placeholder="query — only matching new messages are shown" />
        <button type="submit" className="primary">
          Apply
        </button>
        <button type="button" onClick={() => setPaused((p) => !p)}>
          {paused ? "Resume" : "Pause"}
        </button>
        <button type="button" onClick={() => setRows([])}>
          Clear
        </button>
      </form>
      <div className="status" role="status">
        <span className={`dot ${state}`} aria-hidden />
        <span>
          {state === "live" ? (paused ? "paused" : "listening for new messages") : state === "connecting" ? "connecting…" : "disconnected"} ·{" "}
          {formatCount(total)} received
          {rows.length >= MAX_ROWS && ` · showing latest ${formatCount(MAX_ROWS)}`}
        </span>
        {dropped > 0 && <span className="warn">⚠ {formatCount(dropped)} message(s) skipped — the browser couldn't keep up</span>}
        {error && <span className="err">{error}</span>}
      </div>
      <MessageTable rows={rows} selected={selected} onSelect={onSelect} onVisibleEnd={noop} empty="Waiting for new messages…" />
    </div>
  );
}
