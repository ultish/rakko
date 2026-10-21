export type BulkResult = { topic: string; ok: true } | { topic: string; ok: false; error: string };

/** Topics the sidebar filter matches. A blank filter selects nothing: bulk actions never target the whole cluster. */
export function filterTopics<T extends { name: string }>(topics: T[], filter: string): T[] {
  const needle = filter.trim().toLowerCase();
  return needle ? topics.filter((t) => t.name.toLowerCase().includes(needle)) : [];
}

export function confirmMatches(typed: string, count: number): boolean {
  return count > 0 && typed.trim() === String(count);
}

/** Runs `action` on each topic in turn; a failure is recorded and the remaining topics still run. */
export async function runBulk(topics: string[], action: (topic: string) => Promise<unknown>): Promise<BulkResult[]> {
  const results: BulkResult[] = [];
  for (const topic of topics) {
    try {
      await action(topic);
      results.push({ topic, ok: true });
    } catch (e) {
      results.push({ topic, ok: false, error: e instanceof Error ? e.message : String(e) });
    }
  }
  return results;
}
