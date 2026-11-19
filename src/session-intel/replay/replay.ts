/**
 * The replay driver (docs/session-intel-replay.md, "Replay"): steps a simulated clock from the snapshot's
 * window.since to window.until every `intervalMs` (default 120000, COORDINATION_ADVISOR_INTERVAL_SECONDS'
 * default), runs each detector at each tick, keeps each alert's first sentAt, and returns the alerts in a
 * deterministic order. A replay in which a workspace scan failed or a row was refused is `incomplete`: alerts
 * may be missing, and held-out scoring refuses it.
 */
import { DEFAULT_INTERVAL_SECONDS } from "../../coordination-advisor/config.ts";
import { alertClassOf, isAlertClass } from "./classes.ts";
import { detectorHash, type ReplayAlert, type ReplayDetector } from "./detector.ts";
import type { Snapshot } from "./snapshot.ts";
import { ReplayView } from "./views.ts";
import { textCompare } from "./util.ts";

export const REPLAY_FORMAT = "session-intel-replay/2";
export const DEFAULT_INTERVAL_MS = DEFAULT_INTERVAL_SECONDS * 1_000;
export const MIN_INTERVAL_MS = 1_000;
export const MAX_TICKS = 200_000;

export interface ReplayDetectorInfo {
  name: string;
  version: string;
  params: Record<string, unknown>;
  hash: string;
  diagnostics: Record<string, number>;
}

export interface ReplayResult {
  format: typeof REPLAY_FORMAT;
  snapshot: { manifestSha256: string; createdAt: string; window: { since: string; until: string }; sessions: number };
  intervalMs: number;
  ticks: number;
  detectors: ReplayDetectorInfo[];
  /** Why alerts may be missing (e.g. "coordination-advisor failed=1"); empty when the replay is complete. */
  incomplete: string[];
  alerts: ReplayAlert[];
}

/** Diagnostics that mean a detector may have missed alerts: a failed workspace scan or a refused row. */
export const INCOMPLETE_DIAGNOSTICS = ["failed", "rejected"] as const;
export const incompleteReasons = (detectors: ReplayDetectorInfo[]) => detectors.flatMap((d) =>
  INCOMPLETE_DIAGNOSTICS.filter((k) => (d.diagnostics[k] ?? 0) > 0).map((k) => `${d.name} ${k}=${d.diagnostics[k]}`));

export const alertOrder = (a: ReplayAlert, b: ReplayAlert) => textCompare(a.sentAt, b.sentAt) || textCompare(a.detector, b.detector)
  || (a.urgency === b.urgency ? 0 : a.urgency === "high" ? -1 : 1) || textCompare(a.alertId, b.alertId);

export async function replaySnapshot(snapshot: Snapshot, opts: { detectors: ReplayDetector[]; intervalMs?: number }): Promise<ReplayResult> {
  const intervalMs = opts.intervalMs ?? DEFAULT_INTERVAL_MS;
  if (!Number.isSafeInteger(intervalMs) || intervalMs < MIN_INTERVAL_MS) throw new Error(`replay: intervalMs must be an integer >= ${MIN_INTERVAL_MS}`);
  const names = opts.detectors.map((d) => d.name);
  if (!names.length || new Set(names).size !== names.length) throw new Error("replay: at least one detector, with unique names");
  const since = Date.parse(snapshot.manifest.window.since);
  const until = Date.parse(snapshot.manifest.window.until);
  const ticks = Math.floor((until - since) / intervalMs) + 1;
  if (ticks > MAX_TICKS) throw new Error(`replay: ${ticks} ticks exceeds ${MAX_TICKS}; raise intervalMs`);
  const view = new ReplayView(snapshot);
  const seen = new Map<string, ReplayAlert>();
  for (let i = 0; i < ticks; i++) {
    const now = new Date(since + i * intervalMs);
    for (const d of opts.detectors) {
      for (const a of await d.run(view, now)) {
        if (a.detector !== d.name) throw new Error(`replay: detector ${d.name} returned an alert for ${a.detector}`);
        if (a.alertClass !== undefined && !isAlertClass(a.alertClass)) throw new Error(`replay: detector ${d.name} returned an unknown alert class`);
        // With no class set, the kind must map to one (classOfKind throws otherwise): every alert is scorable.
        if (a.alertClass === undefined) alertClassOf(a);
        const k = `${a.detector}\u0000${a.alertId}`;
        // The first sentAt wins: a later tick repeating an alert never moves it.
        if (!seen.has(k)) seen.set(k, a);
      }
    }
  }
  const detectors = opts.detectors.map((d): ReplayDetectorInfo => ({
    name: d.name, version: d.version, params: d.params, hash: detectorHash(d, { intervalMs }), diagnostics: d.diagnostics?.(view) ?? {},
  }));
  return {
    format: REPLAY_FORMAT,
    snapshot: {
      manifestSha256: snapshot.manifestSha256, createdAt: snapshot.manifest.createdAt,
      window: snapshot.manifest.window, sessions: snapshot.sessions.length,
    },
    intervalMs,
    ticks,
    detectors,
    incomplete: incompleteReasons(detectors),
    alerts: [...seen.values()].sort(alertOrder),
  };
}

/** Validates a replay result read back from alerts.json (shape and bounds only). */
export function parseReplayResult(raw: unknown): ReplayResult {
  const r = raw as ReplayResult;
  if (!r || typeof r !== "object" || r.format !== REPLAY_FORMAT) throw new Error(`alerts: format must be ${REPLAY_FORMAT}`);
  if (!r.snapshot || !Number.isSafeInteger(r.snapshot.sessions) || r.snapshot.sessions < 0) throw new Error("alerts: snapshot.sessions must be a count");
  if (!Array.isArray(r.detectors) || !r.detectors.every((d) => d && typeof d.name === "string" && /^[0-9a-f]{64}$/.test(d.hash)))
    throw new Error("alerts: detectors need a name and a sha256 hash");
  if (!Array.isArray(r.alerts) || r.alerts.length > 1_000_000) throw new Error("alerts: alerts must be an array of at most 1,000,000");
  if (!Array.isArray(r.incomplete) || !r.incomplete.every((x) => typeof x === "string")) throw new Error("alerts: incomplete must be a list of reasons");
  const known = new Set(r.detectors.map((d) => d.name));
  for (const a of r.alerts) {
    if (!a || typeof a.detector !== "string" || !known.has(a.detector) || typeof a.alertId !== "string"
      || (a.urgency !== "high" && a.urgency !== "low") || typeof a.kind !== "string" || typeof a.sentAt !== "string"
      || !Number.isFinite(Date.parse(a.sentAt)) || !Array.isArray(a.pair) || a.pair.length !== 2
      || !a.pair.every((p) => typeof p === "string") || typeof a.artifact !== "string" || !Array.isArray(a.evidence)
      || (a.alertClass !== undefined && !isAlertClass(a.alertClass)))
      throw new Error("alerts: an alert is malformed");
    try { alertClassOf(a); } catch { throw new Error("alerts: an alert's kind has no class and it sets no alertClass"); }
  }
  return r;
}
