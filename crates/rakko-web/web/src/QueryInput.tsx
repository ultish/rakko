import { useEffect, useRef, useState } from "react";
import { api } from "./api";

type Props = {
  profile: string;
  topic: string;
  value: string;
  onChange: (v: string) => void;
  onSubmit: () => void;
  placeholder?: string;
};

/**
 * Filter-query box with server-side autocomplete. Suggestions come from the server
 * (`/complete`), which derives them from decoded sample messages of the topic, so
 * field names are always ones the filter would actually match.
 */
export function QueryInput({ profile, topic, value, onChange, onSubmit, placeholder }: Props) {
  const [prefix, setPrefix] = useState("");
  const [candidates, setCandidates] = useState<string[]>([]);
  const [active, setActive] = useState(0);
  const [open, setOpen] = useState(false);
  const inputRef = useRef<HTMLInputElement>(null);
  const requestId = useRef(0);

  // Ask for suggestions for whatever sits before the caret, debounced.
  const refresh = (text: string, caret: number) => {
    const id = ++requestId.current;
    const controller = new AbortController();
    const timer = window.setTimeout(async () => {
      try {
        const result = await api.complete(profile, topic, text.slice(0, caret), controller.signal);
        if (id !== requestId.current) return;
        setPrefix(result.prefix);
        setCandidates(result.candidates);
        setActive(0);
        setOpen(result.candidates.length > 0);
      } catch {
        if (id === requestId.current) setOpen(false);
      }
    }, 120);
    return () => {
      window.clearTimeout(timer);
      controller.abort();
    };
  };

  const cancelRef = useRef<(() => void) | null>(null);
  useEffect(() => () => cancelRef.current?.(), []);

  const accept = (candidate: string) => {
    const input = inputRef.current;
    const caret = input?.selectionStart ?? value.length;
    const rest = value.slice(caret);
    // Root words continue into a path; field names are left for the user to extend.
    const inserted = candidate === "key" || candidate === "value" ? `${candidate}.` : candidate;
    const next = `${prefix}${inserted}${rest}`;
    const nextCaret = prefix.length + inserted.length;
    onChange(next);
    setOpen(false);
    requestAnimationFrame(() => {
      input?.setSelectionRange(nextCaret, nextCaret);
      input?.focus();
      cancelRef.current?.();
      cancelRef.current = refresh(next, nextCaret);
    });
  };

  return (
    <div className="query-input">
      <input
        ref={inputRef}
        value={value}
        placeholder={placeholder}
        spellCheck={false}
        autoComplete="off"
        onChange={(e) => {
          onChange(e.target.value);
          cancelRef.current?.();
          cancelRef.current = refresh(e.target.value, e.target.selectionStart ?? e.target.value.length);
        }}
        onKeyDown={(e) => {
          if (open && candidates.length > 0) {
            if (e.key === "ArrowDown") {
              e.preventDefault();
              setActive((a) => (a + 1) % candidates.length);
              return;
            }
            if (e.key === "ArrowUp") {
              e.preventDefault();
              setActive((a) => (a - 1 + candidates.length) % candidates.length);
              return;
            }
            if (e.key === "Tab" || e.key === "Enter") {
              e.preventDefault();
              accept(candidates[active]);
              return;
            }
            if (e.key === "Escape") {
              setOpen(false);
              return;
            }
          }
          if (e.key === "Enter") onSubmit();
        }}
        onFocus={(e) => {
          cancelRef.current?.();
          cancelRef.current = refresh(e.target.value, e.target.selectionStart ?? e.target.value.length);
        }}
        onBlur={() => window.setTimeout(() => setOpen(false), 120)}
      />
      {open && (
        <ul className="suggestions" role="listbox">
          {candidates.map((c, i) => (
            <li
              key={c}
              role="option"
              aria-selected={i === active}
              className={i === active ? "active" : undefined}
              onMouseDown={(e) => {
                e.preventDefault();
                accept(c);
              }}
            >
              <span className="dim">{prefix.split(/\s+/).pop()}</span>
              {c}
            </li>
          ))}
        </ul>
      )}
    </div>
  );
}
