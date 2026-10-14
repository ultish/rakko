import { useEffect, type ReactNode } from "react";

type Props = { title: string; onClose: () => void; children: ReactNode; wide?: boolean };

export function Dialog({ title, onClose, children, wide }: Props) {
  useEffect(() => {
    const onKey = (e: KeyboardEvent) => e.key === "Escape" && onClose();
    window.addEventListener("keydown", onKey);
    return () => window.removeEventListener("keydown", onKey);
  }, [onClose]);
  return (
    <div className="backdrop" onMouseDown={onClose}>
      <div className={`dialog${wide ? " wide" : ""}`} role="dialog" aria-label={title} onMouseDown={(e) => e.stopPropagation()}>
        <header>
          <h2>{title}</h2>
          <button className="link" onClick={onClose} aria-label="close">
            ✕
          </button>
        </header>
        {children}
      </div>
    </div>
  );
}
