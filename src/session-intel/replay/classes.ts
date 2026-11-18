/**
 * The alert taxonomy's classes (docs/proposals/session-intelligence.md, "Alert taxonomy"), and the mappings from
 * detector kinds, label incident kinds and adjudication verdicts onto them (docs/session-intel-replay.md,
 * "Classes").
 */
export const ALERT_CLASSES = ["constraint_violation", "intent_conflict", "collision", "overlap", "planned_fleet", "none"] as const;
export type AlertClass = typeof ALERT_CLASSES[number];

export const isAlertClass = (v: unknown): v is AlertClass => typeof v === "string" && (ALERT_CLASSES as readonly string[]).includes(v);

/**
 * A detector kind at an urgency, as a class. T1 and T2 are always `collision`; T3 (`concurrent_edit`) is a
 * collision only at HIGH (the 60-minute row), and `overlap` otherwise. The LOW pre-filter kinds are `overlap`.
 * A kind that names a class is that class; anything unknown is `none`.
 */
export function classOfKind(kind: string, urgency: "high" | "low"): AlertClass {
  switch (kind) {
    case "duplicate_work":
    case "superseded_work":
      return "collision";
    case "concurrent_edit":
      return urgency === "high" ? "collision" : "overlap";
    case "nearby_work":
    case "similar_summary":
      return "overlap";
    default:
      return isAlertClass(kind) ? kind : "none";
  }
}

/** An alert's class: the one the detector set, else its kind's. */
export const alertClassOf = (a: { kind: string; urgency: "high" | "low"; alertClass?: AlertClass }): AlertClass =>
  a.alertClass ?? classOfKind(a.kind, a.urgency);

/** A label incident kind as a class: the duplicate and superseded kinds are collisions. */
export function classOfIncidentKind(kind: string): AlertClass {
  switch (kind) {
    case "duplicate_work":
    case "superseded_work":
      return "collision";
    case "overlap":
      return "overlap";
    default:
      return isAlertClass(kind) ? kind : "none";
  }
}

/**
 * The classes an adjudication verdict confirms. `real_overlap` predates the taxonomy and does not say whether
 * the work collided or only overlapped, so it confirms either; `collision` and `overlap` are the precise forms.
 * `planned`, `unrelated` and `unclear` confirm no class (`unclear` is excluded from precision altogether).
 */
export function classesOfVerdict(verdict: string): readonly AlertClass[] {
  switch (verdict) {
    case "real_overlap": return ["collision", "overlap"];
    case "collision": return ["collision"];
    case "overlap": return ["overlap"];
    case "intent_conflict": return ["intent_conflict"];
    case "constraint_violation": return ["constraint_violation"];
    default: return [];
  }
}
