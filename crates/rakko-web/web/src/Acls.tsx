import { useEffect, useMemo, useState } from "react";
import { api, type AclEntry } from "./api";
import { setParams, readParams } from "./urlState";
import { usePolling } from "./usePolling";

/** Read-only view of every ACL on the cluster. */
export function Acls({ profile }: { profile: string }) {
  const [acls, setAcls] = useState<AclEntry[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [filter, setFilter] = useState(() => readParams().get("q") ?? "");

  useEffect(() => {
    setAcls(null);
    setError(null);
    api.acls(profile).then(setAcls).catch((e: unknown) => setError(e instanceof Error ? e.message : String(e)));
  }, [profile]);

  usePolling(() => {
    api.acls(profile).then(setAcls).catch(() => {});
  }, 60_000);

  const shown = useMemo(() => {
    const needle = filter.trim().toLowerCase();
    return (acls ?? []).filter((a) => !needle || Object.values(a).some((v) => v.toLowerCase().includes(needle)));
  }, [acls, filter]);

  return (
    <>
      <div className="topic-head">
        <h1>ACLs</h1>
        {acls && <span className="dim">{acls.length} binding(s) · read-only</span>}
        <span className="spacer" />
        <input
          placeholder="filter by principal, resource, operation…"
          value={filter}
          spellCheck={false}
          onChange={(e) => {
            setFilter(e.target.value);
            setParams({ q: e.target.value });
          }}
        />
      </div>
      {error && <div className="err">{error}</div>}
      {!acls && !error && <div className="dim">loading…</div>}
      {acls && acls.length === 0 && <div className="dim pad">No ACLs are defined on this cluster (or it has no authorizer configured).</div>}
      {acls && acls.length > 0 && (
        <div className="panel-scroll">
          <table className="grid">
            <thead>
              <tr>
                <th>principal</th>
                <th>permission</th>
                <th>operation</th>
                <th>resource</th>
                <th>name</th>
                <th>match</th>
                <th>host</th>
              </tr>
            </thead>
            <tbody>
              {shown.map((a, i) => (
                <tr key={i}>
                  <td className="mono-wrap">{a.principal}</td>
                  <td className={a.permission === "DENY" ? "err" : "ok"}>{a.permission.toLowerCase()}</td>
                  <td>{a.operation}</td>
                  <td>{a.resource_type}</td>
                  <td className="mono-wrap">{a.resource_name}</td>
                  <td className="dim">{a.pattern.toLowerCase()}</td>
                  <td className="dim">{a.host}</td>
                </tr>
              ))}
            </tbody>
          </table>
        </div>
      )}
    </>
  );
}
