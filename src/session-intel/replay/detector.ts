/**
 * The replay detector interface (docs/session-intel-replay.md, "Detectors") and the advisor adapter, which
 * drives the real scanOnce (src/coordination-advisor/scan.ts) with the as-of views and the real
 * AdvisorLimits at the config defaults. M2's intent and constraint conflict detectors plug in behind the
 * same interface.
 */
import { readFileSync } from "node:fs";
import { outcomeFacts } from "../../../packages/coordination-detect/src/index.ts";
import { DEFAULT_PAIR_COOLDOWN_SECONDS, DEFAULT_SESSION_HOURLY_CAP } from "../../coordination-advisor/config.ts";
import { ADVISOR_DETECTOR_PARAMS, AdvisorLimits, CANDIDATE_WINDOW_MS } from "../../coordination-advisor/rubric.ts";
import { scanOnce, SWEEP_EVERY_MS, type ScanStats } from "../../coordination-advisor/scan.ts";
import {
  RecordingRelaySender, ReplayAdvisorStore, snapshotCoordinationBackend, type RecordedWrite, type ReplayView, type SimClock,
} from "./views.ts";
import type { AlertClass } from "./classes.ts";
import { canonicalJson, refString, sha256Hex, sortedPair, textCompare } from "./util.ts";

export type AlertUrgency = "high" | "low";
export interface AlertEvidence { sessionId: string; eventId: string; ts: string; kind: string }

export interface ReplayAlert {
  detector: string;
  /** Unique within the detector. The advisor's is `<advisoryId>/<urgency>`, so a LOW-to-HIGH upgrade is its own HIGH alert. */
  alertId: string;
  kind: string;
  urgency: AlertUrgency;
  orgId: string;
  workspaceId: string;
  /** `<harness>:<sessionId>` refs, sorted. */
  pair: [string, string];
  artifact: string;
  /** Simulated time the alert reached the agents (HIGH: the DM; LOW: the row the pull path serves). */
  sentAt: string;
  /** The detector's own alert time: its latest evidence (sentAt when it has none). */
  alertAt: string;
  /** "dm" when a HIGH DM was recorded, else "pull". */
  channel: "dm" | "pull";
  evidence: AlertEvidence[];
  /**
   * The taxonomy class, when the detector decides it (M2's claim detectors). Absent, it is derived from kind
   * and urgency (classes.ts classOfKind); the advisor leaves it absent.
   */
  alertClass?: AlertClass;
}

export interface ReplayDetector {
  readonly name: string;
  readonly version: string;
  /** Everything that changes the detector's output; hashed with name and version. */
  readonly params: Record<string, unknown>;
  /** The alerts first raised at `now`. May keep per-view state across ticks. */
  run(view: ReplayView, now: Date): Promise<ReplayAlert[]>;
  /** Counters for the report (for the advisor: summed ScanStats). A non-zero `failed` means the replay is incomplete. */
  diagnostics?(view: ReplayView): Record<string, number>;
}

/** The replay settings that change a detector's alerts, hashed with it. */
export interface ReplayHashParams { intervalMs: number }

/**
 * sha256 of name, version, params and the replay settings (canonical JSON): the identity the held-out ledger
 * freezes. The same detector replayed at another interval is another hash.
 */
export const detectorHash = (d: { name: string; version: string; params: Record<string, unknown> }, replay: ReplayHashParams) =>
  sha256Hex(canonicalJson([d.name, d.version, d.params, { intervalMs: replay.intervalMs }]));

/**
 * The code that decides the advisor's replay alerts, hashed into its params: editing any of it changes the
 * hash. The scorer, labels and adjudication code are not here: they do not change which alerts exist.
 */
export const ADVISOR_SOURCES = [
  "../../coordination-advisor/scan.ts",
  "../../coordination-advisor/rubric.ts",
  "../../coordination-advisor/template.ts",
  "../../coordination-advisor/relay-sender.ts",
  "../../session-read-api/coordination-backend.ts",
  "../../../packages/coordination-detect/src/index.ts",
  "./views.ts",
  "./detector.ts",
  "./replay.ts",
  "./snapshot.ts",
  "./util.ts",
];
function sourceSha256(): string {
  return sha256Hex(ADVISOR_SOURCES.map((p) => sha256Hex(readFileSync(new URL(p, import.meta.url)))).join("\n"));
}

export interface AdvisorDetectorOptions {
  pairCooldownMs?: number;
  sessionHourlyCap?: number;
  /** The canary allowlist the replay assumes; default every workspace. */
  workspaces?: "*" | string[];
  /** Record HIGH DMs as if COORDINATION_ADVISOR_RELAY_DM_ENABLED were on (default true). */
  dm?: boolean;
}

interface AdvisorState {
  clock: SimClock; store: ReplayAdvisorStore; sender: RecordingRelaySender; limits: AdvisorLimits; stats: ScanStats | null;
  /** advisorTick's hourly retention sweep, in simulated time. */
  lastSweep: number; expired: number; deleted: number;
}

export const ADVISOR_DETECTOR_NAME = "coordination-advisor";

export function advisorDetector(opts: AdvisorDetectorOptions = {}): ReplayDetector {
  const pairCooldownMs = opts.pairCooldownMs ?? DEFAULT_PAIR_COOLDOWN_SECONDS * 1_000;
  const sessionHourlyCap = opts.sessionHourlyCap ?? DEFAULT_SESSION_HOURLY_CAP;
  const workspaces = opts.workspaces === undefined || opts.workspaces === "*" ? "*" as const : [...opts.workspaces].sort(textCompare);
  const dm = opts.dm ?? true;
  const states = new WeakMap<ReplayView, AdvisorState>();
  const stateFor = (view: ReplayView): AdvisorState => {
    let s = states.get(view);
    if (!s) {
      const clock: SimClock = { now: new Date(view.window.since) };
      s = { clock, store: new ReplayAdvisorStore(view, clock), sender: new RecordingRelaySender(clock), limits: new AdvisorLimits({ pairCooldownMs, sessionHourlyCap }), stats: null,
        lastSweep: Number.NEGATIVE_INFINITY, expired: 0, deleted: 0 };
      states.set(view, s);
    }
    return s;
  };
  return {
    name: ADVISOR_DETECTOR_NAME,
    version: `1+${outcomeFacts.name}/${outcomeFacts.version}`,
    params: {
      pairCooldownMs, sessionHourlyCap, workspaces, dm, candidateWindowMs: CANDIDATE_WINDOW_MS,
      detectorParams: ADVISOR_DETECTOR_PARAMS, sourceSha256: sourceSha256(),
    },
    async run(view, now) {
      const s = stateFor(view);
      if (s.clock.now.getTime() > now.getTime()) throw new Error("advisor replay: the clock went backwards");
      s.clock.now = now;
      const writesBefore = s.store.writes.length;
      const dmsBefore = s.sender.sent.length;
      const stats = await scanOnce({
        store: s.store, backend: snapshotCoordinationBackend(view, s.clock), limits: s.limits,
        workspaces: workspaces === "*" ? "*" : new Set(workspaces), ...(dm ? { sender: s.sender } : {}),
      }, now);
      // As advisorTick does: after the scan, a retention sweep once per hour (of simulated time).
      if (now.getTime() - s.lastSweep >= SWEEP_EVERY_MS) {
        s.lastSweep = now.getTime();
        const swept = await s.store.sweep();
        s.expired += swept.expired;
        s.deleted += swept.deleted;
      }
      s.stats = s.stats ? Object.fromEntries(Object.entries(s.stats).map(([k, v]) => [k, v + stats[k as keyof ScanStats]])) as unknown as ScanStats : stats;
      const dmed = new Set(s.sender.sent.slice(dmsBefore).map((d) => d.advisoryId));
      const fresh = s.store.writes.slice(writesBefore).filter((w) => w.result !== "skipped");
      const byId = new Map<string, RecordedWrite[]>();
      for (const w of fresh) {
        const k = `${w.row.advisory_id}/${w.row.urgency}`;
        byId.set(k, [...(byId.get(k) ?? []), w]);
      }
      const sentAt = now.toISOString();
      return [...byId].map(([alertId, ws]): ReplayAlert => {
        const w = ws[0]!;
        const evidence = w.row.evidence.map((e) => ({ sessionId: e.sessionId, eventId: e.eventId, ts: e.ts, kind: e.kind }));
        return {
          detector: ADVISOR_DETECTOR_NAME, alertId, kind: w.row.kind, urgency: w.row.urgency, orgId: w.orgId, workspaceId: w.workspaceId,
          pair: sortedPair(refString({ harness: w.row.harness, sessionId: w.row.session_id }), refString({ harness: w.row.peer_harness, sessionId: w.row.peer_session_id })),
          artifact: w.row.artifact, sentAt,
          alertAt: evidence.map((e) => e.ts).sort(textCompare).at(-1) ?? sentAt,
          channel: w.row.urgency === "high" && dmed.has(w.row.advisory_id) ? "dm" : "pull",
          evidence,
        };
      });
    },
    diagnostics(view) {
      const s = states.get(view);
      return { ...(s?.stats ?? {}), dmRecorded: s?.sender.sent.length ?? 0, sweptExpired: s?.expired ?? 0, sweptDeleted: s?.deleted ?? 0 };
    },
  };
}
