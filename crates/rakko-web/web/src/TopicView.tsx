import { useCallback, useEffect, useRef, useState } from "react";
import { api, urls, type Summary, type Topic } from "./api";
import { formatCount } from "./format";
import { Messages } from "./Messages";
import { ProduceDialog } from "./ProduceDialog";
import { Tail } from "./Tail";
import { DeleteTopicDialog, TopicConfigDialog } from "./TopicAdmin";

type Props = {
  profile: string;
  topic: string;
  info?: Topic;
  selected: Summary | null;
  onSelect: (s: Summary) => void;
  onChanged: () => void;
  onDeleted: () => void;
  produceFrom: ProduceInitial | null;
  onProduceFrom: (v: ProduceInitial | null) => void;
};

export type ProduceInitial = { key?: string; value?: string; headers?: [string, string][] };

/** One topic: browse/filter the whole topic or follow it live, plus produce/export/import. */
export function TopicView({ profile, topic, info, selected, onSelect, onChanged, onDeleted, produceFrom, onProduceFrom }: Props) {
  const [mode, setMode] = useState<"browse" | "live">("browse");
  const [applied, setApplied] = useState({ text: "", query: "" });
  const [producing, setProducing] = useState(false);
  const [admin, setAdmin] = useState<"config" | "delete" | null>(null);
  const [notice, setNotice] = useState<{ kind: "ok" | "err"; text: string } | null>(null);
  const fileInput = useRef<HTMLInputElement>(null);

  useEffect(() => {
    setMode("browse");
    setApplied({ text: "", query: "" });
    setNotice(null);
  }, [profile, topic]);

  const onApplied = useCallback((text: string, query: string) => setApplied({ text, query }), []);

  const importFile = async (file: File) => {
    if (!window.confirm(`Produce every message in "${file.name}" to ${topic}?`)) return;
    setNotice({ kind: "ok", text: `Importing ${file.name}…` });
    try {
      const r = await api.importFile(profile, topic, file);
      const detail = r.errors.length ? ` — ${r.errors.slice(0, 3).join("; ")}` : "";
      setNotice({ kind: r.failed ? "err" : "ok", text: `Imported ${formatCount(r.sent)} message(s), ${formatCount(r.failed)} failed${detail}` });
      onChanged();
    } catch (e) {
      setNotice({ kind: "err", text: e instanceof Error ? e.message : String(e) });
    }
  };

  const filtered = applied.text.trim() !== "" || applied.query.trim() !== "";

  return (
    <>
      <div className="topic-head">
        <h1>{topic}</h1>
        {info && (
          <span className="dim">
            {info.partitions} partitions · rf {info.replication_factor} · {info.compression} · ~{formatCount(info.messages)} messages
          </span>
        )}
        <span className="spacer" />
        <div className="seg" role="tablist">
          <button role="tab" aria-selected={mode === "browse"} className={mode === "browse" ? "on" : undefined} onClick={() => setMode("browse")}>
            Browse
          </button>
          <button role="tab" aria-selected={mode === "live"} className={mode === "live" ? "on" : undefined} onClick={() => setMode("live")}>
            Live
          </button>
        </div>
        <button onClick={() => setProducing(true)}>Produce…</button>
        <a
          className="button"
          href={urls.export(profile, topic, applied.text, applied.query)}
          download
          title={filtered ? "Download every message matching the current filter (whole topic)" : "Download the whole topic"}
        >
          Export{filtered ? " matches" : ""}
        </a>
        <button onClick={() => fileInput.current?.click()}>Import…</button>
        <details className="menu">
          <summary>⋯</summary>
          <div>
            <button onClick={() => setAdmin("config")}>Topic config…</button>
            <button onClick={() => setAdmin("delete")}>Delete topic…</button>
          </div>
        </details>
        <input
          ref={fileInput}
          type="file"
          accept=".jsonl,.ndjson,application/x-ndjson,text/plain"
          hidden
          onChange={(e) => {
            const f = e.target.files?.[0];
            e.target.value = "";
            if (f) void importFile(f);
          }}
        />
      </div>
      {notice && (
        <div className={notice.kind === "ok" ? "ok notice" : "err notice"} role="status">
          {notice.text}{" "}
          <button className="link" onClick={() => setNotice(null)}>
            dismiss
          </button>
        </div>
      )}
      {mode === "browse" ? (
        <Messages profile={profile} topic={topic} selected={selected} onSelect={onSelect} onApplied={onApplied} />
      ) : (
        <Tail profile={profile} topic={topic} selected={selected} onSelect={onSelect} initialText={applied.text} initialQuery={applied.query} />
      )}
      {admin === "config" && <TopicConfigDialog profile={profile} topic={topic} onClose={() => setAdmin(null)} />}
      {admin === "delete" && <DeleteTopicDialog profile={profile} topic={topic} onClose={() => setAdmin(null)} onDone={onDeleted} />}
      {(producing || produceFrom) && (
        <ProduceDialog
          profile={profile}
          topic={topic}
          initial={produceFrom ?? undefined}
          onClose={() => {
            setProducing(false);
            onProduceFrom(null);
            onChanged();
          }}
        />
      )}
    </>
  );
}
