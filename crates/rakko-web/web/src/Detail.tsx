import { useEffect, useState } from "react";
import { api, urls, type Detail as DetailT, type Summary } from "./api";
import { Dialog } from "./Dialog";
import { formatBytes, formatTime } from "./format";

type Props = {
  profile: string;
  topic: string;
  message: Summary;
  onClose: () => void;
  /** Opens the produce dialog pre-filled with this message (edit & resend). */
  onProduceFrom: (v: { key?: string; value?: string; headers?: [string, string][] }) => void;
  onReplayed: () => void;
};

type PartState = { chunks: DetailT[]; loading: boolean; error: string | null };

const empty: PartState = { chunks: [], loading: true, error: null };

function Part({ title, state, onMore }: { title: string; state: PartState; onMore: () => void }) {
  const first = state.chunks[0];
  const last = state.chunks[state.chunks.length - 1];
  const text = state.chunks.map((c) => c.text).join("");
  const more = last ? last.end < last.total : false;
  return (
    <section className="part">
      <header>
        <h3>{title}</h3>
        {first && first.present && (
          <span className="meta">
            {first.format} · {formatBytes(first.size)}
          </span>
        )}
        {text && (
          <button
            className="link"
            onClick={() => {
              navigator.clipboard?.writeText(text).catch(() => {});
            }}
            title={more ? "copies the part loaded so far" : "copy"}
          >
            copy
          </button>
        )}
      </header>
      {state.error ? (
        <div className="err">{state.error}</div>
      ) : state.loading && !first ? (
        <div className="dim">loading…</div>
      ) : first && !first.present ? (
        <div className="dim">null</div>
      ) : (
        <>
          <pre>{text}</pre>
          {more && last && (
            <button className="more" disabled={state.loading} onClick={onMore}>
              {state.loading ? "loading…" : `Load more (${formatBytes(last.end)} of ${formatBytes(last.total)})`}
            </button>
          )}
        </>
      )}
    </section>
  );
}

/** Side panel: one message's key and value, fetched on demand and read in chunks. */
export function Detail({ profile, topic, message, onClose, onProduceFrom, onReplayed }: Props) {
  const [key, setKey] = useState<PartState>(empty);
  const [value, setValue] = useState<PartState>(empty);
  const [headers, setHeaders] = useState<[string, string][]>([]);
  const [notice, setNotice] = useState<{ kind: "ok" | "err"; text: string } | null>(null);
  const [deleting, setDeleting] = useState(false);

  const tombstone = async () => {
    if (!window.confirm(`Send a tombstone (null value) for this message's key to ${topic}? On a compacted topic this deletes the key; otherwise it is just another message.`)) return;
    try {
      const r = await api.tombstone(profile, topic, message);
      setNotice({ kind: "ok", text: `Tombstone sent to ${topic}/${r.partition}@${r.offset}` });
      onReplayed();
    } catch (e) {
      setNotice({ kind: "err", text: e instanceof Error ? e.message : String(e) });
    }
  };

  const replay = async () => {
    if (!window.confirm(`Re-send this message's exact bytes to ${topic}? It is appended as a new message.`)) return;
    try {
      const r = await api.replay(profile, topic, message);
      setNotice({ kind: "ok", text: `Replayed to ${topic}/${r.partition}@${r.offset}` });
      onReplayed();
    } catch (e) {
      setNotice({ kind: "err", text: e instanceof Error ? e.message : String(e) });
    }
  };

  // Edit & resend needs the whole decoded value, and only makes sense for text.
  const textOf = (s: PartState) => {
    const last = s.chunks[s.chunks.length - 1];
    return last && last.present && last.end >= last.total && !last.format.startsWith("avro") ? s.chunks.map((c) => c.text).join("") : undefined;
  };
  const editable = textOf(value) !== undefined || (value.chunks[0] && !value.chunks[0].present);

  useEffect(() => {
    let cancelled = false;
    setKey(empty);
    setValue(empty);
    setHeaders([]);
    setNotice(null);
    const load = (part: "key" | "value", set: (s: PartState) => void) =>
      api
        .detail(profile, topic, message, part)
        .then((d) => {
          if (cancelled) return;
          set({ chunks: [d], loading: false, error: null });
          if (part === "value") setHeaders(d.headers);
        })
        .catch((e: unknown) => !cancelled && set({ chunks: [], loading: false, error: e instanceof Error ? e.message : String(e) }));
    void load("key", setKey);
    void load("value", setValue);
    return () => {
      cancelled = true;
    };
  }, [profile, topic, message]);

  const more = (part: "key" | "value", state: PartState, set: (s: PartState) => void) => async () => {
    const last = state.chunks[state.chunks.length - 1];
    if (!last) return;
    set({ ...state, loading: true });
    try {
      const next = await api.detail(profile, topic, message, part, last.end);
      set({ chunks: [...state.chunks, next], loading: false, error: null });
    } catch (e) {
      set({ ...state, loading: false, error: e instanceof Error ? e.message : String(e) });
    }
  };

  return (
    <aside className="detail">
      <header>
        <div>
          <strong>
            {topic}/{message.partition}@{message.offset}
          </strong>
          <div className="dim">{formatTime(message.timestamp_ms)}</div>
        </div>
        <button className="link" onClick={onClose} aria-label="close">
          ✕
        </button>
      </header>
      <div className="actions">
        <button onClick={() => void replay()} title="Re-send the exact original bytes to the same topic">
          Replay
        </button>
        <button
          disabled={!editable}
          onClick={() => onProduceFrom({ key: textOf(key), value: textOf(value), headers })}
          title={editable ? "Open the producer pre-filled with this message" : "Only fully-loaded text messages can be edited (Avro/binary: use Replay)"}
        >
          Edit &amp; produce…
        </button>
        <details className="menu">
          <summary>Download ▾</summary>
          <div>
            <a href={urls.download(profile, topic, message, "value", "raw")}>value — raw bytes</a>
            <a href={urls.download(profile, topic, message, "value", "decoded")}>value — decoded (JSON/text)</a>
            <a href={urls.download(profile, topic, message, "key", "raw")}>key — raw bytes</a>
            <a href={urls.download(profile, topic, message, "key", "decoded")}>key — decoded</a>
            <a href={urls.download(profile, topic, message, "message")}>whole message (JSONL, re-importable)</a>
          </div>
        </details>
        <details className="menu">
          <summary>Delete ▾</summary>
          <div>
            <button onClick={() => void tombstone()}>Tombstone this key…</button>
            <button onClick={() => setDeleting(true)}>Delete this and all earlier in partition…</button>
          </div>
        </details>
        {notice && <span className={notice.kind === "ok" ? "ok small" : "err small"}>{notice.text}</span>}
      </div>
      {headers.length > 0 && (
        <section className="part">
          <header>
            <h3>Headers</h3>
          </header>
          <table className="headers">
            <tbody>
              {headers.map(([k, v], i) => (
                <tr key={i}>
                  <td>{k}</td>
                  <td>{v}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </section>
      )}
      {deleting && (
        <DeleteRecordsDialog
          profile={profile}
          topic={topic}
          message={message}
          onClose={() => setDeleting(false)}
          onDone={(text) => {
            setNotice({ kind: "ok", text });
            onReplayed();
          }}
        />
      )}
      <Part title="Key" state={key} onMore={more("key", key, setKey)} />
      <Part title="Value" state={value} onMore={more("value", value, setValue)} />
    </aside>
  );
}

function DeleteRecordsDialog({
  profile,
  topic,
  message,
  onClose,
  onDone,
}: {
  profile: string;
  topic: string;
  message: Summary;
  onClose: () => void;
  onDone: (text: string) => void;
}) {
  const [confirm, setConfirm] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const run = async () => {
    setBusy(true);
    setError(null);
    try {
      const r = await api.deleteRecords(profile, topic, message.partition, message.offset + 1);
      onDone(`Deleted partition ${message.partition} up to offset ${message.offset}; it now starts at ${r.low}`);
      onClose();
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };
  return (
    <Dialog title="Delete records" onClose={onClose}>
      <form
        className="dialog-body"
        onSubmit={(e) => {
          e.preventDefault();
          void run();
        }}
      >
        <p>
          Kafka cannot delete a single message in the middle of a log. This permanently deletes <strong>every message in partition{" "}
          {message.partition} from its start up to and including offset {message.offset}</strong>. It cannot be undone, and consumers positioned in that range will skip ahead.
        </p>
        <label className="confirm">
          Type <code>{topic}</code> to confirm
          <input value={confirm} onChange={(e) => setConfirm(e.target.value)} spellCheck={false} />
        </label>
        {error && <div className="err">{error}</div>}
        <footer>
          <button type="button" onClick={onClose}>
            Cancel
          </button>
          <button type="submit" className="danger" disabled={busy || confirm !== topic}>
            {busy ? "Deleting…" : "Delete records"}
          </button>
        </footer>
      </form>
    </Dialog>
  );
}
