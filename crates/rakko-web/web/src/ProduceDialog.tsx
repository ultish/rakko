import { useState } from "react";
import { api, type Field } from "./api";
import { Dialog } from "./Dialog";

type Props = {
  profile: string;
  topic: string;
  onClose: () => void;
  /** Pre-fills the form (replay-and-edit). */
  initial?: { key?: string; value?: string; headers?: [string, string][] };
};

type Mode = "text" | "base64" | "file" | "null";

type Side = { mode: Mode; text: string; file: File | null };

function toBase64(file: File): Promise<string> {
  return new Promise((resolve, reject) => {
    const reader = new FileReader();
    reader.onload = () => resolve(String(reader.result).split(",", 2)[1] ?? "");
    reader.onerror = () => reject(reader.error ?? new Error("could not read file"));
    reader.readAsDataURL(file);
  });
}

async function toField(side: Side): Promise<Field | undefined> {
  switch (side.mode) {
    case "null":
      return undefined;
    case "text":
      return { text: side.text };
    case "base64":
      return { base64: side.text };
    case "file":
      if (!side.file) throw new Error("choose a file");
      return { base64: await toBase64(side.file) };
  }
}

function SideEditor({ label, side, onChange, rows }: { label: string; side: Side; onChange: (s: Side) => void; rows: number }) {
  return (
    <fieldset>
      <legend>{label}</legend>
      <div className="seg" role="radiogroup">
        {(["text", "base64", "file", "null"] as Mode[]).map((m) => (
          <button
            key={m}
            type="button"
            role="radio"
            aria-checked={side.mode === m}
            className={side.mode === m ? "on" : undefined}
            onClick={() => onChange({ ...side, mode: m })}
          >
            {m === "null" ? "null" : m}
          </button>
        ))}
      </div>
      {(side.mode === "text" || side.mode === "base64") && (
        <textarea
          rows={rows}
          value={side.text}
          spellCheck={false}
          onChange={(e) => onChange({ ...side, text: e.target.value })}
          placeholder={side.mode === "base64" ? "base64-encoded bytes" : label.toLowerCase()}
        />
      )}
      {side.mode === "file" && (
        <input type="file" onChange={(e) => onChange({ ...side, file: e.target.files?.[0] ?? null })} />
      )}
      {side.mode === "null" && <div className="dim small">sent as null{label === "Value" ? " (a tombstone)" : ""}</div>}
    </fieldset>
  );
}

/** Produce one message: text, raw base64 bytes, or a file (up to ~20 MiB). */
export function ProduceDialog({ profile, topic, onClose, initial }: Props) {
  const [key, setKey] = useState<Side>({ mode: initial?.key !== undefined ? "text" : "null", text: initial?.key ?? "", file: null });
  const [value, setValue] = useState<Side>({ mode: "text", text: initial?.value ?? "", file: null });
  const [headers, setHeaders] = useState<{ key: string; value: string }[]>(
    (initial?.headers ?? []).map(([k, v]) => ({ key: k, value: v })),
  );
  const [busy, setBusy] = useState(false);
  const [error, setError] = useState<string | null>(null);
  const [result, setResult] = useState<string | null>(null);

  const send = async () => {
    setBusy(true);
    setError(null);
    setResult(null);
    try {
      const body = { key: await toField(key), value: await toField(value), headers: headers.filter((h) => h.key.trim()) };
      const r = await api.produce(profile, topic, body);
      setResult(`Produced to ${topic}/${r.partition}@${r.offset}`);
    } catch (e) {
      setError(e instanceof Error ? e.message : String(e));
    } finally {
      setBusy(false);
    }
  };

  return (
    <Dialog title={`Produce to ${topic}`} onClose={onClose} wide>
      <form
        className="dialog-body"
        onSubmit={(e) => {
          e.preventDefault();
          void send();
        }}
      >
        <SideEditor label="Key" side={key} onChange={setKey} rows={2} />
        <SideEditor label="Value" side={value} onChange={setValue} rows={10} />
        <fieldset>
          <legend>Headers</legend>
          {headers.map((h, i) => (
            <div className="header-row" key={i}>
              <input placeholder="name" value={h.key} onChange={(e) => setHeaders(headers.map((x, j) => (j === i ? { ...x, key: e.target.value } : x)))} />
              <input placeholder="value" value={h.value} onChange={(e) => setHeaders(headers.map((x, j) => (j === i ? { ...x, value: e.target.value } : x)))} />
              <button type="button" className="link" onClick={() => setHeaders(headers.filter((_, j) => j !== i))} aria-label="remove header">
                ✕
              </button>
            </div>
          ))}
          <button type="button" className="link" onClick={() => setHeaders([...headers, { key: "", value: "" }])}>
            + add header
          </button>
        </fieldset>
        {error && <div className="err">{error}</div>}
        {result && <div className="ok">{result}</div>}
        <footer>
          <button type="button" onClick={onClose}>
            Close
          </button>
          <button type="submit" className="primary" disabled={busy}>
            {busy ? "Sending…" : "Produce"}
          </button>
        </footer>
      </form>
    </Dialog>
  );
}
