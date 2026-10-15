import { useCallback, useEffect, useState } from "react";
import { api, type Group, type GroupDetail, type LagSample, type ResetTarget } from "./api";
import { LagChart } from "./LagChart";
import { Dialog } from "./Dialog";
import { formatCount } from "./format";

const message = (e: unknown) => (e instanceof Error ? e.message : String(e));

export function GroupList({ profile, selected, onSelect, refreshKey = 0 }: { profile: string; selected: string; onSelect: (g: string) => void; refreshKey?: number }) {
  const [groups, setGroups] = useState<Group[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [filter, setFilter] = useState("");

  const load = useCallback(() => {
    setError(null);
    api.groups(profile).then(setGroups).catch((e: unknown) => setError(message(e)));
  }, [profile]);
  useEffect(() => {
    setGroups(null);
    load();
  }, [load, refreshKey]);

  const needle = filter.trim().toLowerCase();
  return (
    <>
      <input placeholder="filter groups…" value={filter} onChange={(e) => setFilter(e.target.value)} spellCheck={false} />
      {error && <div className="err pad">{error}</div>}
      {!groups && !error && <div className="dim pad">loading groups…</div>}
      <ul>
        {(groups ?? [])
          .filter((g) => g.name.toLowerCase().includes(needle))
          .map((g) => (
            <li key={g.name}>
              <button className={g.name === selected ? "topic active" : "topic"} onClick={() => onSelect(g.name)}>
                <span className="name">{g.name}</span>
                <span className="count">{g.state}</span>
              </button>
            </li>
          ))}
      </ul>
      <button className="link refresh" onClick={load}>
        refresh
      </button>
    </>
  );
}

function ResetDialog({ profile, group, detail, onClose, onDone }: { profile: string; group: string; detail: GroupDetail; onClose: () => void; onDone: () => void }) {
  const [kind, setKind] = useState<ResetTarget["kind"]>("earliest");
  const [value, setValue] = useState("");
  const [confirm, setConfirm] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const target = (): ResetTarget => {
    if (kind === "earliest" || kind === "latest") return { kind };
    if (kind === "offset") {
      const n = Number(value);
      if (!Number.isInteger(n) || n < 0) throw new Error("offset must be a non-negative integer");
      return { kind, value: n };
    }
    const ms = Date.parse(value);
    if (Number.isNaN(ms)) throw new Error("enter a date/time, e.g. 2026-10-15T09:30");
    return { kind, value: ms };
  };

  const run = async () => {
    setBusy(true);
    setError(null);
    try {
      await api.resetGroup(profile, group, target());
      onDone();
      onClose();
    } catch (e) {
      setError(message(e));
    } finally {
      setBusy(false);
    }
  };

  return (
    <Dialog title={`Reset offsets — ${group}`} onClose={onClose}>
      <form
        className="dialog-body"
        onSubmit={(e) => {
          e.preventDefault();
          void run();
        }}
      >
        {detail.members.length > 0 && (
          <div className="warn">
            This group has {detail.members.length} active member(s). Resetting under a running consumer is unreliable — stop the consumers first.
          </div>
        )}
        <p>
          Moves the committed offset of all {detail.lags.length} partition(s) this group has consumed. Consumers will re-read or skip messages accordingly.
        </p>
        <div className="seg" role="radiogroup">
          {(["earliest", "latest", "offset", "timestamp"] as const).map((k) => (
            <button key={k} type="button" role="radio" aria-checked={kind === k} className={kind === k ? "on" : undefined} onClick={() => setKind(k)}>
              {k}
            </button>
          ))}
        </div>
        {kind === "offset" && <input value={value} onChange={(e) => setValue(e.target.value)} placeholder="offset (clamped per partition)" inputMode="numeric" />}
        {kind === "timestamp" && <input type="datetime-local" value={value} onChange={(e) => setValue(e.target.value)} />}
        <label className="confirm">
          Type <code>{group}</code> to confirm
          <input value={confirm} onChange={(e) => setConfirm(e.target.value)} spellCheck={false} />
        </label>
        {error && <div className="err">{error}</div>}
        <footer>
          <button type="button" onClick={onClose}>
            Cancel
          </button>
          <button type="submit" className="danger" disabled={busy || confirm !== group}>
            {busy ? "Resetting…" : "Reset offsets"}
          </button>
        </footer>
      </form>
    </Dialog>
  );
}

export function GroupView({ profile, group, onOpenTopic, onGone }: { profile: string; group: string; onOpenTopic: (t: string) => void; onGone: () => void }) {
  const [detail, setDetail] = useState<GroupDetail | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [resetting, setResetting] = useState(false);
  const [deleting, setDeleting] = useState(false);
  const [history, setHistory] = useState<{ interval_s: number; samples: LagSample[] } | null>(null);

  useEffect(() => {
    let cancelled = false;
    setHistory(null);
    const pull = () =>
      api
        .lagHistory(profile, group)
        .then((h) => !cancelled && setHistory(h))
        .catch(() => {});
    void pull();
    const t = window.setInterval(pull, 15_000);
    return () => {
      cancelled = true;
      window.clearInterval(t);
    };
  }, [profile, group]);

  const load = useCallback(() => {
    setError(null);
    api.group(profile, group).then(setDetail).catch((e: unknown) => setError(message(e)));
  }, [profile, group]);
  useEffect(() => {
    setDetail(null);
    load();
    const t = window.setInterval(load, 10_000);
    return () => window.clearInterval(t);
  }, [load]);

  return (
    <>
      <div className="topic-head">
        <h1>{group}</h1>
        {detail && (
          <span className="dim">
            {detail.state} · {detail.members.length} member(s) · total lag {formatCount(detail.total_lag)}
          </span>
        )}
        <span className="spacer" />
        <button onClick={load}>Refresh</button>
        <button className="danger" disabled={!detail || detail.lags.length === 0} onClick={() => setResetting(true)}>
          Reset offsets…
        </button>
        <button className="danger" disabled={!detail} onClick={() => setDeleting(true)} title="Delete this consumer group (it must have no active members)">
          Delete group…
        </button>
      </div>
      {error && <div className="err">{error}</div>}
      {!detail && !error && <div className="dim">loading…</div>}
      {detail && (
        <div className="panel-scroll">
          {history && <LagChart samples={history.samples} intervalS={history.interval_s} />}
          <table className="grid">
            <thead>
              <tr>
                <th>topic</th>
                <th>partition</th>
                <th className="num">committed</th>
                <th className="num">low</th>
                <th className="num">high</th>
                <th className="num">lag</th>
              </tr>
            </thead>
            <tbody>
              {detail.lags.map((l) => (
                <tr key={`${l.topic}:${l.partition}`}>
                  <td>
                    <button className="link" onClick={() => onOpenTopic(l.topic)}>
                      {l.topic}
                    </button>
                  </td>
                  <td>{l.partition}</td>
                  <td className="num">{l.committed ?? "—"}</td>
                  <td className="num">{formatCount(l.low)}</td>
                  <td className="num">{formatCount(l.high)}</td>
                  <td className={`num${l.lag ? " warn" : ""}`}>{l.lag === null ? "—" : formatCount(l.lag)}</td>
                </tr>
              ))}
            </tbody>
          </table>
          {detail.members.length > 0 && (
            <>
              <h3 className="section">Members</h3>
              <table className="grid">
                <thead>
                  <tr>
                    <th>client id</th>
                    <th>host</th>
                    <th>member id</th>
                  </tr>
                </thead>
                <tbody>
                  {detail.members.map((m) => (
                    <tr key={m.id}>
                      <td>{m.client_id}</td>
                      <td>{m.host}</td>
                      <td className="dim">{m.id}</td>
                    </tr>
                  ))}
                </tbody>
              </table>
            </>
          )}
        </div>
      )}
      {deleting && <DeleteGroupDialog profile={profile} group={group} members={detail?.members.length ?? 0} onClose={() => setDeleting(false)} onDone={onGone} />}
      {resetting && detail && <ResetDialog profile={profile} group={group} detail={detail} onClose={() => setResetting(false)} onDone={load} />}
    </>
  );
}

function DeleteGroupDialog({ profile, group, members, onClose, onDone }: { profile: string; group: string; members: number; onClose: () => void; onDone: () => void }) {
  const [confirm, setConfirm] = useState("");
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const run = async () => {
    setBusy(true);
    setError(null);
    try {
      await api.deleteGroup(profile, group);
      onDone();
      onClose();
    } catch (e) {
      setError(message(e));
    } finally {
      setBusy(false);
    }
  };
  return (
    <Dialog title={`Delete group ${group}`} onClose={onClose}>
      <form
        className="dialog-body"
        onSubmit={(e) => {
          e.preventDefault();
          void run();
        }}
      >
        <p>
          Permanently deletes the consumer group and its committed offsets. A consumer that later joins with this name starts from its configured reset policy (often the latest or earliest message).
        </p>
        {members > 0 && <p className="warn">This group has {members} active member(s); Kafka will refuse until they are stopped.</p>}
        <label className="confirm">
          Type <code>{group}</code> to confirm
          <input value={confirm} onChange={(e) => setConfirm(e.target.value)} spellCheck={false} />
        </label>
        {error && <div className="err">{error}</div>}
        <footer>
          <button type="button" onClick={onClose}>
            Cancel
          </button>
          <button type="submit" className="danger" disabled={busy || confirm !== group}>
            {busy ? "Deleting…" : "Delete group"}
          </button>
        </footer>
      </form>
    </Dialog>
  );
}
