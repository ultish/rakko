import { useState } from "react";
import { api } from "./api";
import { confirmMatches, runBulk, type BulkResult } from "./bulk";
import { Dialog } from "./Dialog";

export type BulkKind = "delete" | "clear";

const COPY = {
  delete: {
    title: "Delete topics",
    busy: "Deleting…",
    warning: "Permanently deletes these topics and every message in them. Producers and consumers using them will fail or recreate them.",
  },
  clear: {
    title: "Clear messages",
    busy: "Clearing…",
    warning: "Permanently deletes every message in every partition of these topics. The topics, their settings and consumer groups stay.",
  },
} as const;

export function BulkBar({ topics, onAction }: { topics: string[]; onAction: (kind: BulkKind) => void }) {
  if (topics.length === 0) return null;
  return (
    <div className="bulk-bar">
      <span>
        {topics.length} matching topic{topics.length === 1 ? "" : "s"}
      </span>
      <button className="link" onClick={() => onAction("delete")}>
        Delete topics
      </button>
      <button className="link" onClick={() => onAction("clear")}>
        Clear messages
      </button>
    </div>
  );
}

export function BulkTopicsDialog({ profile, kind, topics, onClose, onFinished }: { profile: string; kind: BulkKind; topics: string[]; onClose: () => void; onFinished: () => void }) {
  const [typed, setTyped] = useState("");
  const [busy, setBusy] = useState(false);
  const [results, setResults] = useState<BulkResult[] | null>(null);
  const copy = COPY[kind];
  const run = async () => {
    setBusy(true);
    const action = kind === "delete" ? (t: string) => api.deleteTopic(profile, t) : (t: string) => api.clearTopic(profile, t);
    setResults(await runBulk(topics, action));
    setBusy(false);
    onFinished();
  };
  const failed = results?.filter((r) => !r.ok).length ?? 0;
  return (
    <Dialog title={`${copy.title} (${topics.length})`} onClose={onClose}>
      <form
        className="dialog-body"
        onSubmit={(e) => {
          e.preventDefault();
          if (!results) void run();
        }}
      >
        {!results && <p>{copy.warning}</p>}
        <ul className="bulk-list">
          {topics.map((t) => {
            const r = results?.find((x) => x.topic === t);
            return (
              <li key={t}>
                <code>{t}</code>
                {r?.ok && <span className="ok"> done</span>}
                {r && !r.ok && <span className="err"> failed: {r.error}</span>}
              </li>
            );
          })}
        </ul>
        {results ? (
          <div className={failed ? "err" : "ok"}>
            {failed ? `${failed} of ${results.length} failed.` : `All ${results.length} succeeded.`}
          </div>
        ) : (
          <label className="confirm">
            Type <code>{topics.length}</code> to confirm
            <input value={typed} onChange={(e) => setTyped(e.target.value)} spellCheck={false} inputMode="numeric" autoFocus />
          </label>
        )}
        <footer>
          <button type="button" onClick={onClose} disabled={busy}>
            {results ? "Close" : "Cancel"}
          </button>
          {!results && (
            <button type="submit" className="danger" disabled={busy || !confirmMatches(typed, topics.length)}>
              {busy ? copy.busy : copy.title}
            </button>
          )}
        </footer>
      </form>
    </Dialog>
  );
}
