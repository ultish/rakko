import { useEffect, useState } from "react";
import { api, type PartitionDetail } from "./api";
import { formatBytes, formatCount } from "./format";
import { usePolling } from "./usePolling";

/** Where each partition lives: leader, replicas, in-sync replicas, and how much data it holds. */
export function PartitionsView({ profile, topic }: { profile: string; topic: string }) {
  const [parts, setParts] = useState<PartitionDetail[] | null>(null);
  const [error, setError] = useState<string | null>(null);

  const load = () => {
    setError(null);
    api.partitionDetails(profile, topic).then(setParts).catch((e: unknown) => setError(e instanceof Error ? e.message : String(e)));
  };
  useEffect(() => {
    setParts(null);
    load();
    // eslint-disable-next-line react-hooks/exhaustive-deps
  }, [profile, topic]);

  usePolling(() => {
    api.partitionDetails(profile, topic).then(setParts).catch(() => {});
  }, 15_000);

  const [size, setSize] = useState<{ bytes: number; partitions: { partition: number; bytes: number }[] } | null>(null);
  const [sizing, setSizing] = useState(false);
  useEffect(() => setSize(null), [profile, topic]);
  const estimate = () => {
    setSizing(true);
    api
      .topicSize(profile, topic)
      .then(setSize)
      .catch((e: unknown) => setError(e instanceof Error ? e.message : String(e)))
      .finally(() => setSizing(false));
  };

  const unhealthy = (p: PartitionDetail) => p.leader < 0 || p.isr.length < p.replicas.length;

  return (
    <div className="panel-scroll">
      {error && <div className="err pad">{error}</div>}
      {!parts && !error && <div className="dim pad">loading…</div>}
      {parts && (
        <>
          <div className="actions">
            <span className={parts.some(unhealthy) ? "warn" : "dim"}>
              {parts.length} partitions · {parts.filter(unhealthy).length} under-replicated or offline
            </span>
            <span className="spacer" />
            {size && (
              <span title="Average message payload (key, value, headers) from a few sampled messages per partition, times the stored offsets. One replica, uncompressed; the size on disk differs.">
                ≈ {formatBytes(size.bytes)} of data
              </span>
            )}
            <button onClick={estimate} disabled={sizing} title="Samples a few messages per partition; Kafka does not report a topic's size to clients">
              {sizing ? "Estimating…" : size ? "Re-estimate size" : "Estimate size"}
            </button>
            <button onClick={load}>Refresh</button>
          </div>
          <table className="grid">
            <thead>
              <tr>
                <th>partition</th>
                <th>leader</th>
                <th>replicas</th>
                <th>in-sync</th>
                <th className="num">oldest offset</th>
                <th className="num">next offset</th>
                <th className="num">messages</th>
                {size && <th className="num">≈ size</th>}
              </tr>
            </thead>
            <tbody>
              {parts.map((p) => (
                <tr key={p.partition}>
                  <td>{p.partition}</td>
                  <td className={p.leader < 0 ? "err" : undefined}>{p.leader < 0 ? "none (offline)" : `broker ${p.leader}`}</td>
                  <td>{p.replicas.join(", ")}</td>
                  <td className={unhealthy(p) ? "warn" : undefined}>
                    {p.isr.join(", ") || "—"}
                    {p.isr.length < p.replicas.length && ` (missing ${p.replicas.filter((r) => !p.isr.includes(r)).join(", ")})`}
                  </td>
                  <td className="num">{formatCount(p.low)}</td>
                  <td className="num">{formatCount(p.high)}</td>
                  <td className="num" title="offsets between oldest and next; compaction can make the real count lower">
                    {formatCount(p.messages)}
                  </td>
                  {size && <td className="num">{formatBytes(size.partitions.find((x) => x.partition === p.partition)?.bytes ?? null)}</td>}
                </tr>
              ))}
            </tbody>
          </table>
        </>
      )}
    </div>
  );
}
