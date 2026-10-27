export type View = "topics" | "groups" | "schemas" | "brokers" | "acls";

/** Address of a view, or of one item in it: `#/<profile>/<view>[/<item>]`. App reads this back on page load. */
export function routeHref(profile: string, view: View, item = ""): string {
  const parts = [profile, view, ...(item ? [item] : [])].map(encodeURIComponent);
  return `#/${parts.join("/")}`;
}
