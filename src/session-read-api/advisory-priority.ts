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
