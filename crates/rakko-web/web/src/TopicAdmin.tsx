import { useCallback, useEffect, useState } from "react";
import { api, type TopicConfig } from "./api";
import { Dialog } from "./Dialog";

const message = (e: unknown) => (e instanceof Error ? e.message : String(e));

const PRESETS: { label: string; name: string; value: string }[] = [
  { label: "compacted", name: "cleanup.policy", value: "compact" },
  { label: "keep 7 days", name: "retention.ms", value: "604800000" },
  { label: "keep forever", name: "retention.ms", value: "-1" },
  { label: "zstd", name: "compression.type", value: "zstd" },
];

export function CreateTopicDialog({ profile, onClose, onDone }: { profile: string; onClose: () => void; onDone: (name: string) => void }) {
  const [name, setName] = useState("");
  const [partitions, setPartitions] = useState("3");
  const [replication, setReplication] = useState("1");
  const [configs, setConfigs] = useState<{ name: string; value: string }[]>([]);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const create = async () => {
    setBusy(true);
    setError(null);
    try {
      const p = Number(partitions);
      const r = Number(replication);
      if (!Number.isInteger(p) || p < 1) throw new Error("partitions must be a whole number, at least 1");
      if (!Number.isInteger(r) || r < 1) throw new Error("replication factor must be a whole number, at least 1");
      const overrides: Record<string, string> = {};
      for (const c of configs) if (c.name.trim()) overrides[c.name.trim()] = c.value;
      await api.createTopic(profile, { name: name.trim(), partitions: p, replication_factor: r, configs: overrides });
      onDone(name.trim());
      onClose();
    } catch (e) {
      setError(message(e));
    } finally {
      setBusy(false);
    }
  };

  return (
    <Dialog title="Create topic" onClose={onClose}>
      <form
        className="dialog-body"
        onSubmit={(e) => {
          e.preventDefault();
          void create();
        }}
      >
        <label className="confirm">
          Name
          <input value={name} onChange={(e) => setName(e.target.value)} spellCheck={false} autoFocus />
        </label>
        <div className="header-row">
          <label className="confirm grow">
            Partitions
            <input value={partitions} onChange={(e) => setPartitions(e.target.value)} inputMode="numeric" />
          </label>
          <label className="confirm grow">
            Replication factor
            <input value={replication} onChange={(e) => setReplication(e.target.value)} inputMode="numeric" />
          </label>
        </div>
        <fieldset>
          <legend>Config overrides</legend>
          {configs.map((c, i) => (
            <div className="header-row" key={i}>
              <input placeholder="name" value={c.name} onChange={(e) => setConfigs(configs.map((x, j) => (j === i ? { ...x, name: e.target.value } : x)))} />
              <input placeholder="value" value={c.value} onChange={(e) => setConfigs(configs.map((x, j) => (j === i ? { ...x, value: e.target.value } : x)))} />
              <button type="button" className="link" onClick={() => setConfigs(configs.filter((_, j) => j !== i))} aria-label="remove">
                ✕
              </button>
            </div>
          ))}
          <div className="presets">
            {PRESETS.map((p) => (
              <button key={p.label} type="button" className="link" onClick={() => setConfigs([...configs.filter((c) => c.name !== p.name), { name: p.name, value: p.value }])}>
                + {p.label}
              </button>
            ))}
            <button type="button" className="link" onClick={() => setConfigs([...configs, { name: "", value: "" }])}>
              + other
            </button>
          </div>
        </fieldset>
        {error && <div className="err">{error}</div>}
        <footer>
          <button type="button" onClick={onClose}>
            Cancel
          </button>
          <button type="submit" className="primary" disabled={busy || !name.trim()}>
            {busy ? "Creating…" : "Create"}
          </button>
        </footer>
      </form>
    </Dialog>
  );
}

export function TopicConfigDialog({ profile, topic, onClose }: { profile: string; topic: string; onClose: () => void }) {
  const [entries, setEntries] = useState<TopicConfig[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [filter, setFilter] = useState("");
  const [onlyOverrides, setOnlyOverrides] = useState(false);
  const [edits, setEdits] = useState<Record<string, string>>({});
  const [resets, setResets] = useState<Set<string>>(new Set());
  const [busy, setBusy] = useState(false);
  const [saved, setSaved] = useState<string | null>(null);
  const internal = topic.startsWith("__");

  const load = useCallback(() => {
    setEntries(null);
    api.topicConfigsAll(profile, topic).then(setEntries).catch((e: unknown) => setError(message(e)));
  }, [profile, topic]);
  useEffect(load, [load]);

  const changes = Object.keys(edits).length + resets.size;
  const needle = filter.trim().toLowerCase();
  const shown = (entries ?? []).filter((e) => (!onlyOverrides || e.overridden) && e.name.toLowerCase().includes(needle));

  const save = async () => {
    setBusy(true);
    setError(null);
    setSaved(null);
    try {
      await api.alterTopicConfigs(profile, topic, edits, [...resets]);
      setEdits({});
      setResets(new Set());
      setSaved(`Saved ${changes} change${changes === 1 ? "" : "s"}.`);
      load();
    } catch (e) {
      setError(message(e));
    } finally {
      setBusy(false);
    }
  };

  return (
    <Dialog title={`${topic} — config`} onClose={onClose} wide>
      <div className="dialog-body">
        <div className="header-row">
          <input placeholder="filter settings…" value={filter} onChange={(e) => setFilter(e.target.value)} spellCheck={false} autoFocus />
          <label className="check">
            <input type="checkbox" checked={onlyOverrides} onChange={(e) => setOnlyOverrides(e.target.checked)} />
            set on this topic only
          </label>
        </div>
        {error && <div className="err">{error}</div>}
        {saved && <div className="ok">{saved}</div>}
        {!entries && !error && <div className="dim">loading…</div>}
        {entries && (
          <div className="config-scroll">
            <table className="grid">
              <thead>
                <tr>
                  <th>setting</th>
                  <th>value</th>
                  <th>from</th>
                  <th />
                </tr>
              </thead>
              <tbody>
                {shown.map((c) => {
                  const edited = edits[c.name] !== undefined;
                  const reset = resets.has(c.name);
                  const locked = internal || c.read_only || c.sensitive;
                  return (
                    <tr key={c.name} className={edited || reset ? "selected" : undefined}>
                      <td className="mono-wrap">{c.name}</td>
                      <td>
                        {locked ? (
                          <span className="mono-wrap">{c.sensitive ? "<sensitive>" : (c.value ?? "")}</span>
                        ) : (
                          <input
                            className="config-input"
                            value={reset ? "(default)" : (edits[c.name] ?? c.value ?? "")}
                            disabled={reset}
                            spellCheck={false}
                            onChange={(e) => setEdits({ ...edits, [c.name]: e.target.value })}
                          />
                        )}
                      </td>
                      <td className="dim">{c.source}</td>
                      <td>
                        {!locked && c.overridden && !reset && (
                          <button
                            className="link"
                            title="Remove the override so the broker default applies"
                            onClick={() => {
                              const { [c.name]: _drop, ...rest } = edits;
                              setEdits(rest);
                              setResets(new Set(resets).add(c.name));
                            }}
                          >
                            reset
                          </button>
                        )}
                        {(edited || reset) && (
                          <button
                            className="link"
                            onClick={() => {
                              const { [c.name]: _drop, ...rest } = edits;
                              setEdits(rest);
                              const next = new Set(resets);
                              next.delete(c.name);
                              setResets(next);
                            }}
                          >
                            undo
                          </button>
                        )}
                      </td>
                    </tr>
                  );
                })}
              </tbody>
            </table>
            {shown.length === 0 && <div className="dim pad">No matching settings.</div>}
          </div>
        )}
        <footer>
          <button onClick={onClose}>Close</button>
          <button className="primary" disabled={busy || changes === 0 || internal} onClick={() => void save()}>
            {busy ? "Saving…" : changes ? `Save ${changes} change${changes === 1 ? "" : "s"}` : "Save"}
          </button>
        </footer>
      </div>
    </Dialog>
  );
}

export function DeleteTopicDialog({ profile, topic, onClose, onDone }: { profile: string; topic: string; onClose: () => void; onDone: () => void }) {
  const [confirm, setConfirm] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const run = async () => {
    setBusy(true);
    setError(null);
    try {
      await api.deleteTopic(profile, topic);
      onDone();
      onClose();
    } catch (e) {
      setError(message(e));
    } finally {
      setBusy(false);
    }
  };
  return (
    <Dialog title={`Delete topic ${topic}`} onClose={onClose}>
      <form
        className="dialog-body"
        onSubmit={(e) => {
          e.preventDefault();
          void run();
        }}
      >
        <p>Permanently deletes the topic and every message in it. Producers and consumers using it will fail or recreate it.</p>
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
            {busy ? "Deleting…" : "Delete topic"}
          </button>
        </footer>
      </form>
    </Dialog>
  );
}
