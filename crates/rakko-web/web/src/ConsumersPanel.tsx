import { useEffect, useState } from "react";
import { api, type TopicConsumer } from "./api";
import { formatCount } from "./format";

/** Consumer groups with committed offsets on this topic, each linking to the group's page. */
export function ConsumersPanel({ profile, topic }: { profile: string; topic: string }) {
  const [groups, setGroups] = useState<TopicConsumer[] | null>(null);
  const [error, setError] = useState<string | null>(null);

  useEffect(() => {
    let live = true;
    setGroups(null);
    setError(null);
    api
      .topicConsumers(profile, topic)
      .then((g) => live && setGroups(g))
      .catch((e: unknown) => live && setError(e instanceof Error ? e.message : String(e)));
    return () => {
      live = false;
    };
  }, [profile, topic]);

  return (
    <details className="consumers" open>
      <summary>Consumers{groups ? ` (${groups.length})` : ""}</summary>
      {error && <div className="err pad">{error}</div>}
      {!groups && !error && <div className="dim pad">loading consumers…</div>}
      {groups && groups.length === 0 && <div className="dim pad">No consumer groups have committed offsets on this topic.</div>}
      {groups && groups.length > 0 && (
        <table>
          <thead>
            <tr>
              <th>Group</th>
              <th>State</th>
              <th>Lag</th>
            </tr>
          </thead>
          <tbody>
            {groups.map((g) => (
              <tr key={g.name}>
                <td>
                  <a href={`#/${encodeURIComponent(profile)}/groups/${encodeURIComponent(g.name)}`}>{g.name}</a>
                </td>
                <td>{g.state}</td>
                <td>{formatCount(g.total_lag)}</td>
              </tr>
            ))}
          </tbody>
        </table>
      )}
    </details>
  );
}
