import type { ReactNode } from "react";
import { isPlainLeftClick } from "./messageLink";

/** A real anchor: middle/Ctrl/Cmd-click and "open in new tab" use `href`; a plain click runs `onNavigate` without reloading. */
export function NavLink({ href, onNavigate, className, title, children }: { href: string; onNavigate: () => void; className?: string; title?: string; children: ReactNode }) {
  return (
    <a
      href={href}
      className={className}
      title={title}
      onClick={(e) => {
        if (!isPlainLeftClick(e)) return;
        e.preventDefault();
        onNavigate();
      }}
    >
      {children}
    </a>
  );
}
