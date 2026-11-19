/**
 * Labeled incidents and hard negatives (docs/session-intel-replay.md, "Labels"). Ground truth comes from
 * outcomes, never from a detector (docs/coordination-overlap-detection.md, "How it was tested").
 */
import { boundedString, isoTime, isRecord, onlyKeys, pairId, parseRef, sortedPair } from "./util.ts";

export type IncidentKind = "overlap" | "intent_conflict" | "constraint_violation" | "duplicate_work" | "superseded_work";
export type Split = "dev" | "heldout";
export type HardNegativeReason = "planned_fleet" | "unrelated" | "benign_overlap";

export interface Incident { id: string; pair: [string, string]; kind: IncidentKind; harmAt: string; split: Split; workstream: string }
/** `split` is optional: a hard negative without one counts in every split. */
export interface HardNegative { pair: [string, string]; reason: HardNegativeReason; split?: Split }
export interface Labels { incidents: Incident[]; hardNegatives: HardNegative[] }

const INCIDENT_KINDS: readonly IncidentKind[] = ["overlap", "intent_conflict", "constraint_violation", "duplicate_work", "superseded_work"];
const REASONS: readonly HardNegativeReason[] = ["planned_fleet", "unrelated", "benign_overlap"];
export const MAX_LABELS = 100_000;

function pair(v: unknown, where: string): [string, string] {
  if (!Array.isArray(v) || v.length !== 2) throw new Error(`${where}.pair must be two session refs`);
  const [a, b] = v.map((r, i) => {
    const s = boundedString(r, `${where}.pair[${i}]`, 600)!;
    parseRef(s);
    return s;
  }) as [string, string];
  if (a === b) throw new Error(`${where}.pair names the same session twice`);
  return sortedPair(a, b);
}
const split = (v: unknown, where: string): Split => {
  if (v !== "dev" && v !== "heldout") throw new Error(`${where}.split must be dev or heldout`);
  return v;
};

export function parseLabels(raw: unknown): Labels {
  if (!isRecord(raw)) throw new Error("labels must be an object");
  onlyKeys(raw, ["incidents", "hardNegatives"], "labels");
  if (!Array.isArray(raw.incidents) || !Array.isArray(raw.hardNegatives)) throw new Error("labels need incidents and hardNegatives arrays");
  if (raw.incidents.length + raw.hardNegatives.length > MAX_LABELS) throw new Error(`labels: more than ${MAX_LABELS} entries`);
  const ids = new Set<string>();
  const incidents = raw.incidents.map((v, i): Incident => {
    const where = `labels.incidents[${i}]`;
    if (!isRecord(v)) throw new Error(`${where} must be an object`);
    onlyKeys(v, ["id", "pair", "kind", "harmAt", "split", "workstream"], where);
    const id = boundedString(v.id, `${where}.id`, 200)!;
    if (ids.has(id)) throw new Error(`${where}.id repeats an incident id`);
    ids.add(id);
    if (!INCIDENT_KINDS.includes(v.kind as IncidentKind)) throw new Error(`${where}.kind must be one of ${INCIDENT_KINDS.join(", ")}`);
    return {
      id, pair: pair(v.pair, where), kind: v.kind as IncidentKind, harmAt: isoTime(v.harmAt, `${where}.harmAt`),
      split: split(v.split, where), workstream: boundedString(v.workstream, `${where}.workstream`, 200)!,
    };
  });
  const incidentPairs = new Set(incidents.map((x) => pairId(x.pair)));
  const hardNegatives = raw.hardNegatives.map((v, i): HardNegative => {
    const where = `labels.hardNegatives[${i}]`;
    if (!isRecord(v)) throw new Error(`${where} must be an object`);
    onlyKeys(v, ["pair", "reason", "split"], where);
    if (!REASONS.includes(v.reason as HardNegativeReason)) throw new Error(`${where}.reason must be one of ${REASONS.join(", ")}`);
    const p = pair(v.pair, where);
    if (incidentPairs.has(pairId(p))) throw new Error(`${where}.pair is also an incident pair`);
    return { pair: p, reason: v.reason as HardNegativeReason, ...(v.split === undefined ? {} : { split: split(v.split, where) }) };
  });
  return { incidents, hardNegatives };
}
