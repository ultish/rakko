/** Address of one message: the topic route plus `?msg=partition:offset`, which App loads on page load. */
export function messageHref(profile: string, topic: string, partition: number, offset: number): string {
  const path = [profile, "topics", topic].map(encodeURIComponent).join("/");
  return `#/${path}?msg=${encodeURIComponent(`${partition}:${offset}`)}`;
}

export function parseMsgParam(value: string | null): { partition: number; offset: number } | null {
  const m = /^(\d+):(\d+)$/.exec(value ?? "");
  return m ? { partition: Number(m[1]), offset: Number(m[2]) } : null;
}

/** A click the page handles itself; middle/Ctrl/Cmd/Shift clicks are left to the browser (new tab/window). */
export function isPlainLeftClick(e: { button: number; metaKey: boolean; ctrlKey: boolean; shiftKey: boolean; altKey: boolean }): boolean {
  return e.button === 0 && !e.metaKey && !e.ctrlKey && !e.shiftKey && !e.altKey;
}
