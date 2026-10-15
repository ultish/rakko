import { useCallback, useEffect, useState } from "react";
import { readParams, setParams } from "./urlState";
import { api, type SchemaDraft, type SchemaReference, type SubjectVersion } from "./api";
import { Dialog } from "./Dialog";

const message = (e: unknown) => (e instanceof Error ? e.message : String(e));
const LEVELS = ["BACKWARD", "BACKWARD_TRANSITIVE", "FORWARD", "FORWARD_TRANSITIVE", "FULL", "FULL_TRANSITIVE", "NONE"];

function pretty(schema: string): string {
  try {
    return JSON.stringify(JSON.parse(schema), null, 2);
  } catch {
    return schema; // Protobuf, or not JSON at all
  }
}

export function SubjectList({
  profile,
  selected,
  onSelect,
  refreshKey,
  onCreate,
}: {
  profile: string;
  selected: string;
  onSelect: (s: string) => void;
  refreshKey: number;
  onCreate: () => void;
}) {
  const [subjects, setSubjects] = useState<string[] | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [filter, setFilter] = useState("");

  useEffect(() => {
    setError(null);
    api.subjects(profile).then(setSubjects).catch((e: unknown) => setError(message(e)));
  }, [profile, refreshKey]);

  const needle = filter.trim().toLowerCase();
  return (
    <>
      <input placeholder="filter subjects…" value={filter} onChange={(e) => setFilter(e.target.value)} spellCheck={false} />
      <button className="link new" onClick={onCreate}>
        + new subject
      </button>
      {error && <div className="err pad">{error}</div>}
      {!subjects && !error && <div className="dim pad">loading subjects…</div>}
      <ul>
        {(subjects ?? [])
          .filter((s) => s.toLowerCase().includes(needle))
          .map((s) => (
            <li key={s}>
              <button className={s === selected ? "topic active" : "topic"} onClick={() => onSelect(s)} title={s}>
                <span className="name">{s}</span>
              </button>
            </li>
          ))}
      </ul>
    </>
  );
}

/** Register a version: a new subject, or the next version of an existing one. */
function RegisterDialog({
  profile,
  subject: fixedSubject,
  initial,
  onClose,
  onDone,
}: {
  profile: string;
  subject?: string;
  initial?: SubjectVersion;
  onClose: () => void;
  onDone: (subject: string) => void;
}) {
  const [subject, setSubject] = useState(fixedSubject ?? "");
  const [schemaType, setSchemaType] = useState(initial?.schemaType ?? "AVRO");
  const [schema, setSchema] = useState(initial ? pretty(initial.schema) : "");
  const [refs, setRefs] = useState<SchemaReference[]>(initial?.references ?? []);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [verdict, setVerdict] = useState<string | null>(null);

  const draft = (): SchemaDraft => {
    if (!subject.trim()) throw new Error("enter a subject name");
    if (!schema.trim()) throw new Error("enter a schema");
    return { schema, schema_type: schemaType, references: refs.filter((r) => r.name && r.subject) };
  };

  const check = async () => {
    setBusy(true);
    setError(null);
    setVerdict(null);
    try {
      const r = await api.checkSchema(profile, subject.trim(), draft());
      setVerdict(r.compatible ? "Compatible with the latest version." : "NOT compatible with the latest version.");
    } catch (e) {
      setError(message(e));
    } finally {
      setBusy(false);
    }
  };

  const register = async () => {
    setBusy(true);
    setError(null);
    try {
      await api.registerSchema(profile, subject.trim(), draft());
      onDone(subject.trim());
      onClose();
    } catch (e) {
      setError(message(e));
    } finally {
      setBusy(false);
    }
  };

  return (
    <Dialog title={fixedSubject ? `New version of ${fixedSubject}` : "New subject"} onClose={onClose} wide>
      <form
        className="dialog-body"
        onSubmit={(e) => {
          e.preventDefault();
          void register();
        }}
      >
        {!fixedSubject && (
          <label className="confirm">
            Subject
            <input value={subject} onChange={(e) => setSubject(e.target.value)} placeholder="e.g. orders-value or com.acme.OrderEvent" spellCheck={false} />
          </label>
        )}
        <div className="seg" role="radiogroup">
          {["AVRO", "JSON", "PROTOBUF"].map((t) => (
            <button key={t} type="button" role="radio" aria-checked={schemaType === t} className={schemaType === t ? "on" : undefined} onClick={() => setSchemaType(t)}>
              {t}
            </button>
          ))}
        </div>
        <textarea rows={16} value={schema} onChange={(e) => setSchema(e.target.value)} spellCheck={false} placeholder="schema definition" />
        <fieldset>
          <legend>References</legend>
          {refs.map((r, i) => (
            <div className="header-row" key={i}>
              <input placeholder="name (as used in schema)" value={r.name} onChange={(e) => setRefs(refs.map((x, j) => (j === i ? { ...x, name: e.target.value } : x)))} />
              <input placeholder="subject" value={r.subject} onChange={(e) => setRefs(refs.map((x, j) => (j === i ? { ...x, subject: e.target.value } : x)))} />
              <input
                className="narrow"
                placeholder="ver"
                inputMode="numeric"
                value={r.version || ""}
                onChange={(e) => setRefs(refs.map((x, j) => (j === i ? { ...x, version: Number(e.target.value) || 0 } : x)))}
              />
              <button type="button" className="link" onClick={() => setRefs(refs.filter((_, j) => j !== i))} aria-label="remove reference">
                ✕
              </button>
            </div>
          ))}
          <button type="button" className="link" onClick={() => setRefs([...refs, { name: "", subject: "", version: 1 }])}>
            + add reference
          </button>
        </fieldset>
        {verdict && <div className={verdict.startsWith("Compatible") ? "ok" : "err"}>{verdict}</div>}
        {error && <div className="err">{error}</div>}
        <footer>
          <button type="button" onClick={onClose}>
            Cancel
          </button>
          <button type="button" disabled={busy} onClick={() => void check()}>
            Check compatibility
          </button>
          <button type="submit" className="primary" disabled={busy}>
            {busy ? "Working…" : "Register"}
          </button>
        </footer>
      </form>
    </Dialog>
  );
}

function DeleteDialog({
  profile,
  subject,
  version,
  onClose,
  onDone,
}: {
  profile: string;
  subject: string;
  /** Absent = delete the whole subject. */
  version?: number;
  onClose: () => void;
  onDone: () => void;
}) {
  const [confirm, setConfirm] = useState("");
  const [permanent, setPermanent] = useState(false);
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);

  const run = async () => {
    setBusy(true);
    setError(null);
    try {
      if (version === undefined) await api.deleteSubject(profile, subject, permanent);
      else await api.deleteSchemaVersion(profile, subject, version, permanent);
      onDone();
      onClose();
    } catch (e) {
      setError(message(e));
    } finally {
      setBusy(false);
    }
  };

  return (
    <Dialog title={version === undefined ? `Delete subject ${subject}` : `Delete ${subject} v${version}`} onClose={onClose}>
      <form
        className="dialog-body"
        onSubmit={(e) => {
          e.preventDefault();
          void run();
        }}
      >
        <p>
          {version === undefined ? "Deletes the subject and every version of it." : `Deletes version ${version}.`} Messages already written with these schemas can no longer be decoded
          once a schema is permanently removed.
        </p>
        <label className="check">
          <input type="checkbox" checked={permanent} onChange={(e) => setPermanent(e.target.checked)} /> Permanent (otherwise the registry only hides it — a soft delete)
        </label>
        <label className="confirm">
          Type <code>{subject}</code> to confirm
          <input value={confirm} onChange={(e) => setConfirm(e.target.value)} spellCheck={false} />
        </label>
        {error && <div className="err">{error}</div>}
        <footer>
          <button type="button" onClick={onClose}>
            Cancel
          </button>
          <button type="submit" className="danger" disabled={busy || confirm !== subject}>
            {busy ? "Deleting…" : "Delete"}
          </button>
        </footer>
      </form>
    </Dialog>
  );
}

function Compatibility({ profile, subject }: { profile: string; subject?: string }) {
  const [level, setLevel] = useState<string | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [saved, setSaved] = useState(false);

  useEffect(() => {
    setLevel(null);
    setError(null);
    api.compatibility(profile, subject).then((r) => setLevel(r.level)).catch((e: unknown) => setError(message(e)));
  }, [profile, subject]);

  return (
    <label className="compat">
      <span className="dim">{subject ? "compatibility" : "global compatibility"}</span>
      <select
        value={level ?? ""}
        disabled={!level}
        onChange={(e) => {
          const next = e.target.value;
          setError(null);
          setSaved(false);
          api
            .setCompatibility(profile, next, subject)
            .then((r) => {
              setLevel(r.level);
              setSaved(true);
            })
            .catch((err: unknown) => setError(message(err)));
        }}
      >
        {level === null && <option value="">…</option>}
        {LEVELS.map((l) => (
          <option key={l}>{l}</option>
        ))}
      </select>
      {saved && <span className="ok small">saved</span>}
      {error && <span className="err small">{error}</span>}
    </label>
  );
}

export function SubjectView({ profile, subject, onChanged, onGone }: { profile: string; subject: string; onChanged: () => void; onGone: () => void }) {
  const [versions, setVersions] = useState<number[] | null>(null);
  const [version, setVersion] = useState<number | null>(null);
  const [detail, setDetail] = useState<SubjectVersion | null>(null);
  const [error, setError] = useState<string | null>(null);
  const [dialog, setDialog] = useState<"register" | "delete-version" | "delete-subject" | null>(null);

  const loadVersions = useCallback(
    (select?: number) => {
      setError(null);
      api
        .versions(profile, subject)
        .then((v) => {
          setVersions(v);
          const wanted = select ?? Number(readParams().get("v"));
          setVersion(wanted && v.includes(wanted) ? wanted : (v[v.length - 1] ?? null));
        })
        .catch((e: unknown) => setError(message(e)));
    },
    [profile, subject],
  );

  useEffect(() => {
    setVersions(null);
    setVersion(null);
    setDetail(null);
    setError(null);
    loadVersions();
  }, [loadVersions]);

  useEffect(() => {
    // `version` can briefly belong to the previous subject; only fetch once this subject's list says it exists.
    if (version === null || !versions?.includes(version)) return;
    let stale = false;
    setDetail(null);
    api
      .schemaVersion(profile, subject, version)
      .then((d) => !stale && setDetail(d))
      .catch((e: unknown) => !stale && setError(message(e)));
    return () => {
      stale = true;
    };
  }, [profile, subject, version, versions]);

  return (
    <>
      <div className="topic-head">
        <h1>{subject}</h1>
        {detail && (
          <span className="dim">
            {detail.schemaType} · schema id {detail.id}
          </span>
        )}
        <span className="spacer" />
        <Compatibility profile={profile} subject={subject} />
        <button onClick={() => setDialog("register")} disabled={!detail}>
          New version…
        </button>
        <button className="danger" onClick={() => setDialog("delete-version")} disabled={version === null}>
          Delete version…
        </button>
        <button className="danger" onClick={() => setDialog("delete-subject")}>
          Delete subject…
        </button>
      </div>
      {error && <div className="err">{error}</div>}
      {!versions && !error && <div className="dim">loading…</div>}
      {versions && (
        <div className="panel-scroll">
          <div className="seg versions" role="tablist" aria-label="versions">
            {versions.map((v) => (
              <button key={v} role="tab" aria-selected={v === version} className={v === version ? "on" : undefined} onClick={() => {
                  setVersion(v);
                  setParams({ v: String(v) });
                }}>
                v{v}
              </button>
            ))}
          </div>
          {detail && (
            <>
              {detail.references.length > 0 && (
                <>
                  <h3 className="section">References</h3>
                  <table className="grid">
                    <tbody>
                      {detail.references.map((r) => (
                        <tr key={r.name}>
                          <td>{r.name}</td>
                          <td>
                            {r.subject} v{r.version}
                          </td>
                        </tr>
                      ))}
                    </tbody>
                  </table>
                </>
              )}
              <h3 className="section">Schema</h3>
              <pre className="schema">{pretty(detail.schema)}</pre>
            </>
          )}
        </div>
      )}
      {dialog === "register" && detail && (
        <RegisterDialog
          profile={profile}
          subject={subject}
          initial={detail}
          onClose={() => setDialog(null)}
          onDone={() => {
            onChanged();
            loadVersions();
          }}
        />
      )}
      {dialog === "delete-version" && version !== null && (
        <DeleteDialog
          profile={profile}
          subject={subject}
          version={version}
          onClose={() => setDialog(null)}
          onDone={() => {
            onChanged();
            loadVersions();
          }}
        />
      )}
      {dialog === "delete-subject" && (
        <DeleteDialog
          profile={profile}
          subject={subject}
          onClose={() => setDialog(null)}
          onDone={() => {
            onChanged();
            onGone();
          }}
        />
      )}
    </>
  );
}

/** Shown when no subject is selected: global settings and a way to start one. */
export function SchemasHome({ profile, onCreated, onChanged }: { profile: string; onCreated: (s: string) => void; onChanged: () => void }) {
  const [creating, setCreating] = useState(false);
  return (
    <div className="placeholder">
      <img src="/otter.jpg" alt="" />
      <div>Pick a subject, or</div>
      <button onClick={() => setCreating(true)}>Register a new subject…</button>
      <Compatibility profile={profile} />
      {creating && (
        <RegisterDialog
          profile={profile}
          onClose={() => setCreating(false)}
          onDone={(s) => {
            onChanged();
            onCreated(s);
          }}
        />
      )}
    </div>
  );
}

export { RegisterDialog };
