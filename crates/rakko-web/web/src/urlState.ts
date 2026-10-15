// View state that belongs in the address bar (#/profile/view/item?mode=table&text=…), so a
// reload or a shared link lands on the same screen. Written with replaceState: it doesn't
// add a history entry per keystroke, and doesn't fire `hashchange` (the path is unchanged).

export function readParams(): URLSearchParams {
  const hash = window.location.hash;
  const at = hash.indexOf("?");
  return new URLSearchParams(at < 0 ? "" : hash.slice(at + 1));
}

/** Sets (or, for `undefined`/empty, removes) query params on the current hash. */
export function setParams(patch: Record<string, string | undefined>) {
  const hash = window.location.hash;
  const at = hash.indexOf("?");
  const path = at < 0 ? hash : hash.slice(0, at);
  const params = readParams();
  for (const [k, v] of Object.entries(patch)) {
    if (v === undefined || v === "") params.delete(k);
    else params.set(k, v);
  }
  const query = params.toString();
  const next = `${path}${query ? `?${query}` : ""}`;
  if (next !== hash) window.history.replaceState(null, "", `${window.location.pathname}${window.location.search}${next}`);
}
