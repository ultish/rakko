import { useEffect, useState } from "react";
import { api, type Brokers as BrokersT } from "./api";
import { usePolling } from "./usePolling";

const message = (e: unknown) => (e instanceof Error ? e.message : String(e));

export function Brokers({ profile }: { profile: string }) {
  const [data, setData] = useState<BrokersT | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [open, setOpen] = useState<number | null>(null);
  const [configs, setConfigs] = useState<{ name: string; value: string }[] | null>(null);
  const [configError, setConfigError] = useState<string | null>(null);

  useEffect(() => {
    setData(null);
    setOpen(null);
    api.brokers(profile).then(setData).catch((e: unknown) => setError(message(e)));
  }, [profile]);

  usePolling(() => {
    api.brokers(profile).then(setData).catch(() => {});
  }, 30_000);

  useEffect(() => {
    if (open === null) return;
    setConfigs(null);
    setConfigError(null);
    api.brokerConfigs(profile, open).then(setConfigs).catch((e: unknown) => setConfigError(message(e)));
  }, [profile, open]);

  return (
    <>
      <div className="topic-head">
        <h1>Brokers</h1>
        {data && (
          <span className={data.under_replicated + data.offline > 0 ? "warn" : "dim"}>
            {data.under_replicated} under-replicated · {data.offline} offline partitions
          </span>
        )}
      </div>
      {error && <div className="err">{error}</div>}
      {!data && !error && <div className="dim">loading…</div>}
      {data && (
        <div className="panel-scroll">
          <table className="grid">
            <thead>
              <tr>
                <th>id</th>
                <th>address</th>
                <th className="num">leads</th>
                <th className="num">replicas</th>
                <th />
              </tr>
            </thead>
            <tbody>
              {data.brokers.map((b) => (
                <tr key={b.id} className={open === b.id ? "selected" : undefined}>
                  <td>{b.id}</td>
                  <td>
                    {b.host}:{b.port}
                  </td>
                  <td className="num">{b.leader_partitions}</td>
                  <td className="num">{b.replica_partitions}</td>
                  <td>
                    <button className="link" onClick={() => setOpen(open === b.id ? null : b.id)}>
                      {open === b.id ? "hide config" : "config"}
                    </button>
                  </td>
                </tr>
              ))}
            </tbody>
          </table>
          {open !== null && (
            <>
              <h3 className="section">Broker {open} — non-default config</h3>
              {configError && <div className="err">{configError}</div>}
              {!configs && !configError && <div className="dim">loading…</div>}
              {configs && (
                <table className="grid">
                  <tbody>
                    {configs.map((c) => (
                      <tr key={c.name}>
                        <td className="dim">{c.name}</td>
                        <td className="mono-wrap">{c.value}</td>
                      </tr>
                    ))}
                  </tbody>
                </table>
              )}
            </>
          )}
        </div>
      )}
    </>
  );
}
