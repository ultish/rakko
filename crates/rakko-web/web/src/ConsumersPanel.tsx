import { useEffect, useState } from "react";
import { api, type TopicConsumer } from "./api";
import { COLLAPSED_BY_DEFAULT, consumerRows, consumersHeading, toggle } from "./consumerList";

/** Consumer groups with committed offsets on this topic, each linking to the group's page. */
export function ConsumersPanel({ profile, topic }: { profile: string; topic: string }) {
  const [groups, setGroups] = useState<TopicConsumer[] | null>(null);
  const [expanded, setExpanded] = useState(!COLLAPSED_BY_DEFAULT);
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

  const rows = groups ? consumerRows(groups) : [];
  return (
    <section className="consumers">
      <button className="consumers-head" aria-expanded={expanded} aria-controls="consumers-body" onClick={() => setExpanded(toggle)}>
        <span className="chev" aria-hidden="true">{expanded ? "▾" : "▸"}</span>
        {consumersHeading(groups ? groups.length : null)}
      </button>
      {expanded && (
        <div id="consumers-body">
          {error && <div className="err pad">{error}</div>}
          {!groups && !error && <div className="dim pad">loading consumers…</div>}
          {groups && groups.length === 0 && <div className="dim pad">No consumer groups have committed offsets on this topic.</div>}
          {rows.length > 0 && (
            <table className="grid">
              <thead>
                <tr>
                  <th>group</th>
                  <th>state</th>
                  <th className="num">members</th>
                  <th className="num">lag</th>
                </tr>
              </thead>
              <tbody>
                {rows.map((r) => (
                  <tr key={r.name}>
                    <td>
                      <a href={`#/${encodeURIComponent(profile)}/groups/${encodeURIComponent(r.name)}`}>{r.name}</a>
                    </td>
                    <td>{r.state}</td>
                    <td className="num">{r.members}</td>
                    <td className={`num${r.lagging ? " warn" : ""}`}>{r.lag}</td>
                  </tr>
                ))}
              </tbody>
            </table>
          )}
        </div>
      )}
    </section>
  );
}
