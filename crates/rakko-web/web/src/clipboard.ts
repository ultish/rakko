import { useEffect, useRef, useState } from "react";

export type CopyStatus = "idle" | "copied" | "failed";

export const COPY_FEEDBACK_MS = 1500;

type Deps<T> = {
  write: ((text: string) => Promise<void>) | undefined;
  onChange: (status: CopyStatus) => void;
  schedule: (fn: () => void, ms: number) => T;
  cancel: (timer: T) => void;
};

export function createCopyFeedback<T>({ write, onChange, schedule, cancel }: Deps<T>) {
  let timer: T | undefined;
  const show = (status: CopyStatus) => {
    if (timer !== undefined) cancel(timer);
    onChange(status);
    timer = schedule(() => {
      timer = undefined;
      onChange("idle");
    }, COPY_FEEDBACK_MS);
  };
  return {
    async copy(text: string) {
      try {
        if (!write) throw new Error("clipboard unavailable");
        await write(text);
        show("copied");
      } catch {
        show("failed");
      }
    },
    dispose() {
      if (timer !== undefined) cancel(timer);
      timer = undefined;
    },
  };
}

/** Copy-to-clipboard with a transient "copied" / "failed" status for button labels. */
export function useCopy() {
  const [status, setStatus] = useState<CopyStatus>("idle");
  const fb = useRef<ReturnType<typeof createCopyFeedback<number>>>(undefined);
  if (!fb.current) {
    fb.current = createCopyFeedback<number>({
      write: (t) => {
        if (!navigator.clipboard?.writeText) return Promise.reject(new Error("clipboard unavailable"));
        return navigator.clipboard.writeText(t);
      },
      onChange: setStatus,
      schedule: (fn, ms) => window.setTimeout(fn, ms),
      cancel: (t) => window.clearTimeout(t),
    });
  }
  useEffect(() => () => fb.current?.dispose(), []);
  return { status, copy: fb.current.copy };
}
