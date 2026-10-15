import { useEffect, useRef } from "react";

/**
 * Calls `fn` every `ms` while the tab is visible, and straight away when the tab becomes
 * visible again, so a page left open in the background is current when you come back to it.
 * `fn` should refresh quietly (keep what's on screen, ignore transient errors).
 */
export function usePolling(fn: () => void, ms: number) {
  const latest = useRef(fn);
  latest.current = fn;
  useEffect(() => {
    const tick = () => {
      if (!document.hidden) latest.current();
    };
    const timer = window.setInterval(tick, ms);
    document.addEventListener("visibilitychange", tick);
    return () => {
      window.clearInterval(timer);
      document.removeEventListener("visibilitychange", tick);
    };
  }, [ms]);
}
