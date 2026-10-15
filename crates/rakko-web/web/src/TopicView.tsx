import { useCallback, useEffect, useRef, useState } from "react";
import { api, urls, type Range, type Summary, type Topic } from "./api";
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

export type ProduceInitial = { key?: string; value?: string; headers?: [string, string][]; valueAvro?: boolean };

/** One topic: browse/filter the whole topic or follow it live, plus produce/export/import. */
export function TopicView({ profile, topic, info, selected, onSelect, onChanged, onDeleted, produceFrom, onProduceFrom }: Props) {
  const [mode, setMode] = useState<"browse" | "table" | "live">("browse");
  const [applied, setApplied] = useState<{ text: string; query: string; range: Range }>({ text: "", query: "", range: {} });
  const [producing, setProducing] = useState(false);
  const [admin, setAdmin] = useState<"config" | "delete" | null>(null);
  const [stored, setStored] = useState<number | null>(null);
  const [notice, setNotice] = useState<{ kind: "ok" | "err"; text: string } | null>(null);
  const fileInput = useRef<HTMLInputElement>(null);

  useEffect(() => {
    setMode("browse");
    setApplied({ text: "", query: "", range: {} });
    setNotice(null);
    setStored(null);
  }, [profile, topic]);

  const onApplied = useCallback((text: string, query: string, range: Range) => setApplied({ text, query, range }), []);

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

  const filtered =
    applied.text.trim() !== "" || applied.query.trim() !== "" || Object.values(applied.range).some((v) => v !== undefined);

  return (
    <>
      <div className="topic-head">
        <h1>{topic}</h1>
        {info && (
          <span className="dim">
            {info.partitions} partitions · rf {info.replication_factor} · {info.compression} ·{" "}
            <span title="Offsets between the oldest and newest message of each partition. Compaction and retention remove messages without lowering this, so it is an upper bound.">
              {stored !== null && stored !== info.messages
                ? `${formatCount(stored)} messages (${formatCount(info.messages)} offsets)`
                : `${stored === null ? "up to " : ""}${formatCount(info.messages)} messages`}
            </span>
          </span>
        )}
        <span className="spacer" />
        <div className="seg" role="tablist">
          <button role="tab" aria-selected={mode === "browse"} className={mode === "browse" ? "on" : undefined} onClick={() => setMode("browse")}>
            Browse
          </button>
          <button
            role="tab"
            aria-selected={mode === "table"}
            className={mode === "table" ? "on" : undefined}
            onClick={() => setMode("table")}
            title="The latest value of every key (a compacted topic read as a table)"
          >
            Table
          </button>
          <button role="tab" aria-selected={mode === "live"} className={mode === "live" ? "on" : undefined} onClick={() => setMode("live")}>
            Live
          </button>
        </div>
        <button onClick={() => setProducing(true)}>Produce…</button>
        <a
          className="button"
          href={urls.export(profile, topic, applied.text, applied.query, applied.range)}
          download
          title={filtered ? "Download every message matching the current filter and range" : "Download the whole topic"}
        >
          Export{filtered ? " matches" : ""}
        </a>
        <button onClick={() => fileInput.current?.click()}>Import…</button>
        <details className="menu">
          <summary>⋯</summary>
          <div>
            <button onClick={() => setAdmin("config")}>Topic settings…</button>
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
      {mode !== "live" ? (
        <Messages
          key={mode}
          profile={profile}
          topic={topic}
          table={mode === "table"}
          selected={selected}
          onSelect={onSelect}
          onApplied={onApplied}
          onStored={setStored}
        />
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
