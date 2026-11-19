/**
 * The alert taxonomy's classes (docs/proposals/session-intelligence.md, "Alert taxonomy"), and the mappings from
 * detector kinds, label incident kinds, hard-negative reasons and adjudication verdicts onto them
 * (docs/session-intel-replay.md, "Classes").
 */
import { readFileSync } from "node:fs";
import { sha256Hex } from "./util.ts";

export const ALERT_CLASSES = ["constraint_violation", "intent_conflict", "collision", "overlap", "planned_fleet", "none"] as const;
export type AlertClass = typeof ALERT_CLASSES[number];

export const isAlertClass = (v: unknown): v is AlertClass => typeof v === "string" && (ALERT_CLASSES as readonly string[]).includes(v);

/** sha256 of this file: the class map a score was computed with (score reports and the held-out ledger carry it). */
export const CLASS_MAP_SHA256 = sha256Hex(readFileSync(new URL("./classes.ts", import.meta.url)));

/**
 * A detector kind at an urgency, as a class.
 * - T1 and T2 (`duplicate_work`, `superseded_work`) are always `collision`.
 * - T3 (`concurrent_edit`) is `collision` at HIGH (the rubric's 60-minute row). At LOW it is `overlap`: that is
 *   this harness's choice, not the rubric's, which never emits `concurrent_edit` at LOW.
 * - The LOW pre-filter kinds are `overlap`; a kind that names a class is that class.
 * An unmapped kind throws: a detector with a new kind must set `alertClass`, never fall silently into `none`.
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
      if (isAlertClass(kind)) return kind;
      throw new Error(`classes: kind ${JSON.stringify(kind.slice(0, 64))} has no class; the detector must set alertClass`);
  }
}

/** An alert's class: the one the detector set, else its kind's (which throws for an unmapped kind). */
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
 * A hard negative's reason as a class. A benign overlap is still an overlap by the taxonomy (same project and
 * shared files, no conflicting claims), so an `overlap` alert on it is class-correct; a planned fleet is
 * `planned_fleet`; an unrelated pair is `none`.
 */
export function classOfHardNegative(reason: string): AlertClass {
  switch (reason) {
    case "benign_overlap": return "overlap";
    case "planned_fleet": return "planned_fleet";
    default: return "none";
  }
}

/**
 * The classes an adjudication verdict confirms. `real_overlap` predates the taxonomy and does not say whether
 * the work collided or only overlapped, so it confirms either; `collision` and `overlap` are the precise forms.
 * `planned` confirms `planned_fleet`. `unrelated` and `unclear` confirm no class (`unclear` is excluded from
 * precision altogether).
 */
export function classesOfVerdict(verdict: string): readonly AlertClass[] {
  switch (verdict) {
    case "real_overlap": return ["collision", "overlap"];
    case "collision": return ["collision"];
    case "overlap": return ["overlap"];
    case "intent_conflict": return ["intent_conflict"];
    case "constraint_violation": return ["constraint_violation"];
    case "planned": return ["planned_fleet"];
    default: return [];
  }
}

/**
 * Two verdicts agree when the classes they confirm intersect (`real_overlap` and `collision` agree), or when
 * neither confirms a class and they are the same verdict (`unrelated` and `unrelated`).
 */
export function verdictsAgree(a: string, b: string): boolean {
  const x = classesOfVerdict(a), y = classesOfVerdict(b);
  if (!x.length && !y.length) return a === b;
  return x.some((c) => y.includes(c));
}
