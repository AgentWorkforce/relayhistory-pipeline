import { eventFromConvergenceRow } from "./input.ts";
import { SESSION_SUMMARY_MAX_MESSAGES } from "./engine/summary-contract.ts";

type Row = Record<string, unknown>;
const object = (value: unknown): Row =>
  value !== null && typeof value === "object" && !Array.isArray(value) ? value as Row : {};
const text = (value: unknown): string | null =>
  typeof value === "string" && value.trim() ? value : null;

/** Preserve capture identity before the model's normalized subset drops it. */
function identities(row: Row): Array<Set<string>> {
  const payload = object(object(row.record).payload);
  return [
    [row.message_id, row.messageId, payload.message_id],
    [row.user_id, row.userId, payload.user_id],
    [row.tool_use_id, row.toolUseId, row.call_id, row.callId,
      row.tool_call_id, row.toolCallId, payload.tool_use_id, payload.call_id, payload.tool_call_id],
  ].map(values => new Set(values.map(text).filter((v): v is string => v !== null)));
}

function conflicts(a: Array<Set<string>>, b: Array<Set<string>>): boolean {
  return a.some((values, i) => values.size > 1 || b[i]!.size > 1 ||
    (values.size === 1 && b[i]!.size === 1 && [...values][0] !== [...b[i]!][0]));
}

/** Match the dashboard's one-to-one history/projected capture reconciliation.
 * Repeated prompts remain distinct: only a history representation can be removed,
 * and only against one unclaimed projected copy within two seconds. Text alone,
 * an undated import, or conflicting recorded identities never proves a mirror.
 */
export function canonicalHistoryWithoutMirrors(
  rows: readonly Row[], tenant?: { orgId: string; workspaceId: string },
): Row[] {
  const captures = rows.map(row => {
    const source = text(row.source), session = text(row.session_id);
    const event = source && session ? eventFromConvergenceRow(row, source, session) : null;
    if (!event || event.tsMs <= 0 || event.kind !== "text" || !["user", "assistant"].includes(event.role) || !event.text?.trim()) return null;
    return {
      key: JSON.stringify([row.org_id ?? tenant?.orgId ?? null, row.workspace_id ?? tenant?.workspaceId ?? null, source,
        event.sessionId, event.role, text(row.content)?.trim()]),
      history: row.kind === "history", ts: event.tsMs, ids: identities(row),
    };
  });
  // Preserve the existing pre-model eligibility ceiling; do not do quadratic
  // capture matching on a source the unchanged input contract already rejects.
  if (captures.filter(Boolean).length > SESSION_SUMMARY_MAX_MESSAGES) return [...rows];
  const projected = new Map<string, number[]>();
  captures.forEach((capture, i) => {
    if (!capture || capture.history) return;
    const group = projected.get(capture.key) ?? [];
    group.push(i); projected.set(capture.key, group);
  });
  const claimed = new Set<number>();
  return rows.filter((_, i) => {
    const capture = captures[i];
    if (!capture?.history) return true;
    let nearest: number | undefined, distance = Infinity;
    for (const candidate of projected.get(capture.key) ?? []) {
      const other = captures[candidate]!;
      const delta = Math.abs(other.ts - capture.ts);
      if (!claimed.has(candidate) && delta <= 2_000 && delta < distance && !conflicts(capture.ids, other.ids)) {
        nearest = candidate; distance = delta;
      }
    }
    if (nearest === undefined) return true;
    claimed.add(nearest); return false;
  });
}
