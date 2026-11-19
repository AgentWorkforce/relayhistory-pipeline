/**
 * An advisory's priority (docs/proposals/session-intelligence.md, "Priority and the human notice"): the one place
 * the ranking lives. GET /v1/coordination/advisories and GET /v1/session-intel/workspace both carry it, and Desktop
 * ranks its notices by it (M3-9).
 *
 * - P1: HIGH constraint_violation, HIGH intent_conflict, HIGH T1 or T2 collision (duplicate_work, superseded_work).
 * - P2: HIGH concurrent_edit (the 60-minute T3), and any other HIGH kind: an unknown kind is never P1 by default.
 * - P3: every LOW, and a fleet row (kind planned_fleet). An advisory row carries no orchestrator marker, so a fleet
 *   conflict is ranked by its urgency and kind; the advisor writes fleet conflicts LOW and to the orchestrator only,
 *   so they are P3 by urgency.
 *
 * Pure: no I/O, no clock. The table-driven test in __tests__/advisory-priority.test.ts pins every row.
 */
export type AdvisoryPriority = 1 | 2 | 3;

const P1_HIGH_KINDS: ReadonlySet<string> = new Set(["constraint_violation", "intent_conflict", "duplicate_work", "superseded_work"]);

export function advisoryPriority(a: { urgency: string; kind: string }): AdvisoryPriority {
  if (a.urgency !== "high") return 3;
  if (a.kind === "planned_fleet") return 3;
  return P1_HIGH_KINDS.has(a.kind) ? 1 : 2;
}

/**
 * READ_API_ADVISORY_NOTIFY_ROWS: the precision gate for Desktop's OS notifications (M3-9). A comma-separated list of
 * `<kind>:<urgency>` rows; an advisory is `notifyEligible` only when its exact (kind, urgency) is listed. Default
 * EMPTY: nothing is eligible until a row has passed the eval bars and Khaliq approves adding it. Desktop raises an OS
 * notification only for an eligible item and only when its own local setting allows; everything else stays in the
 * in-app list.
 *
 * Parsing is strict: unset, "" or whitespace only is the empty set; each entry is trimmed (spaces around the entry
 * and around the colon are ignored); kind and urgency are lowercase and must be known; an empty entry (a stray
 * comma), a missing or extra colon, an unknown kind or urgency, or an uppercase token is refused with
 * AdvisoryNotifyRowsError, whose message names the entry's position and at most the one bad token.
 */
export const READ_API_ADVISORY_NOTIFY_ROWS = "READ_API_ADVISORY_NOTIFY_ROWS";
/** Every kind the advisor writes: the detector's T1 to T3, the LOW pre-filter kinds, and the M2-4 conflict kinds. */
export const NOTIFY_KINDS: ReadonlySet<string> = new Set([
  "duplicate_work", "superseded_work", "concurrent_edit", "nearby_work", "similar_summary",
  "constraint_violation", "intent_conflict", "overlap",
]);
export const NOTIFY_URGENCIES: ReadonlySet<string> = new Set(["high", "low"]);
/** The longest token an error message quotes. */
const TOKEN_ECHO_MAX = 64;

export class AdvisoryNotifyRowsError extends Error {
  constructor(detail: string) {
    super(`${READ_API_ADVISORY_NOTIFY_ROWS}: ${detail}`);
    this.name = "AdvisoryNotifyRowsError";
  }
}

const quote = (token: string) => JSON.stringify(token.length > TOKEN_ECHO_MAX ? `${token.slice(0, TOKEN_ECHO_MAX)}...` : token);

/** The allowlist as a set of exact `<kind>:<urgency>` keys. Throws AdvisoryNotifyRowsError on any malformed entry. */
export function parseAdvisoryNotifyRows(raw: string | undefined): ReadonlySet<string> {
  const rows = new Set<string>();
  if (raw === undefined || raw.trim() === "") return rows;
  const entries = raw.split(",");
  entries.forEach((entry, i) => {
    const at = `entry ${i + 1}`;
    const parts = entry.split(":").map((p) => p.trim());
    if (parts.length !== 2 || parts[0] === "" || parts[1] === "") throw new AdvisoryNotifyRowsError(`${at} is not <kind>:<urgency>`);
    const [kind, urgency] = parts as [string, string];
    if (!NOTIFY_KINDS.has(kind)) throw new AdvisoryNotifyRowsError(`${at} has an unknown kind ${quote(kind)} (lowercase, one of the advisor's kinds)`);
    if (!NOTIFY_URGENCIES.has(urgency)) throw new AdvisoryNotifyRowsError(`${at} has an unknown urgency ${quote(urgency)} (high or low)`);
    rows.add(`${kind}:${urgency}`);
  });
  return rows;
}

/** Whether Desktop may raise an OS notification for this advisory: its exact (kind, urgency) is on the allowlist. */
export function advisoryNotifyEligible(a: { urgency: string; kind: string }, rows: ReadonlySet<string>): boolean {
  // Fail closed on anything but a Set (e.g. an index passed by `.map(advisoryItem)`): never eligible.
  return rows instanceof Set && rows.has(`${a.kind}:${a.urgency}`);
}
