import { useEffect, useState } from "react";
import { api } from "./api";
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
  const [entries, setEntries] = useState<{ name: string; value: string }[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  useEffect(() => {
    api.topicConfigs(profile, topic).then(setEntries).catch((e: unknown) => setError(message(e)));
  }, [profile, topic]);
  return (
    <Dialog title={`${topic} — non-default config`} onClose={onClose} wide>
      <div className="dialog-body">
        {error && <div className="err">{error}</div>}
        {!entries && !error && <div className="dim">loading…</div>}
        {entries && entries.length === 0 && <div className="dim">Everything uses the broker defaults.</div>}
        {entries && entries.length > 0 && (
          <table className="grid">
            <tbody>
              {entries.map((c) => (
                <tr key={c.name}>
                  <td className="dim">{c.name}</td>
                  <td className="mono-wrap">{c.value}</td>
                </tr>
              ))}
            </tbody>
          </table>
        )}
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
