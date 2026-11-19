/**
 * The M2 conflict detectors as replay detectors (docs/session-intel-conflicts.md): `conflict-rule` (the lexical
 * baseline) and `conflict-model` (a model judge over an injected transport). Each tick: the candidates
 * (candidates.ts), the judge's input with the pre-judge vetoes, the judge, the citation check, and the urgency
 * and delivery (classify.ts). Offline: nothing is sent, and the model judge's budget is enforced here.
 */
import { readFileSync } from "node:fs";
import type { AlertEvidence, ReplayAlert, ReplayDetector } from "../replay/detector.ts";
import type { ReplayView } from "../replay/views.ts";
import { canonicalJson, sha256Hex, textCompare } from "../replay/util.ts";
import { conflictCandidates, CONFLICT_WINDOW_MS, MAX_PAIRS_PER_WORKSPACE, MAX_SESSIONS_PER_WORKSPACE, type CandidateOptions } from "./candidates.ts";
import {
  alertArtifact, buildJudgeInput, citable, decide, validateCitations, type ConflictDelivery, type JudgeAction, type JudgeClaim, type JudgeOutput, type PairJudge, type SideName,
} from "./classify.ts";
import { modelJudge, ruleJudge, type ModelJudgeOptions } from "./judges.ts";

export const CONFLICT_RULE_DETECTOR = "conflict-rule";
export const CONFLICT_MODEL_DETECTOR = "conflict-model";
export const CONFLICT_DETECTOR_VERSION = "1";

/** The per-workspace daily model budget (simulated UTC days). Dev defaults; part of the detector hash. */
export interface ModelBudget { maxCallsPerWorkspacePerDay: number; maxTokensPerWorkspacePerDay: number }
export const DEFAULT_MODEL_BUDGET: ModelBudget = { maxCallsPerWorkspacePerDay: 200, maxTokensPerWorkspacePerDay: 400_000 };
/** Judge results are reused while a pair's input is unchanged; at most this many are kept. */
export const JUDGE_CACHE_MAX = 50_000;

/** The code that decides these detectors' alerts, hashed into their params. */
export const CONFLICT_SOURCES = [
  "./candidates.ts", "./classify.ts", "./judges.ts", "./topics.ts", "./detector.ts",
  "../replay/views.ts", "../replay/snapshot.ts", "../replay/replay.ts", "../replay/util.ts", "../replay/detector.ts",
  "../../coordination-advisor/rubric.ts",
];
const sourceSha256 = () => sha256Hex(CONFLICT_SOURCES.map((p) => sha256Hex(readFileSync(new URL(p, import.meta.url)))).join("\n"));

/** The M2 fields on a conflict alert (the base ReplayAlert fields stay as M0 defines them). */
export interface ConflictAlert extends ReplayAlert {
  conflict: {
    /** The cited claim ids, `<harness>:<sessionId>#<claimId>`. */
    claimIds: string[];
    /** The action event that made it HIGH (or that a fleet conflict cites), `<harness>:<sessionId>@<eventId>`. */
    actionId: string | null;
    fleet: boolean;
    delivery: ConflictDelivery;
  };
}

export interface ConflictDetectorOptions extends CandidateOptions {
  /** Metered judges only; default DEFAULT_MODEL_BUDGET. */
  budget?: ModelBudget;
}

interface State {
  cache: Map<string, JudgeOutput>;
  /** `<org>\0<workspace>\0<YYYY-MM-DD>` -> calls and estimated tokens spent. */
  spend: Map<string, { calls: number; tokens: number }>;
  counters: Record<string, number>;
  /** Alert ids already raised: a repeat on a later tick is not returned again (the driver keeps the first anyway). */
  raised: Set<string>;
  lastNow: number;
}

const COUNTERS = [
  "ticks", "candidates", "truncated", "sessionsDropped", "uncitable", "judged", "cacheHits", "modelCalls", "modelTokens", "budgetSkipped",
  "invalidReplies", "vetoed", "none", "silenced", "failed", "alertsHigh", "alertsLow",
] as const;

/** Each cited claim's evidence ids (at the claim's time) and the action event, by session. */
function evidenceOf(claims: JudgeClaim[], sessionOf: (side: SideName) => string, action: JudgeAction | null): AlertEvidence[] {
  const out: AlertEvidence[] = [];
  for (const c of claims) for (const e of c.evidenceIds) out.push({ sessionId: sessionOf(c.side), eventId: e, ts: c.at, kind: `claim:${c.kind}` });
  if (action) out.push({ sessionId: sessionOf(action.side), eventId: action.eventId, ts: action.ts, kind: action.kind });
  return out;
}

export function conflictDetector(name: string, judge: PairJudge, opts: ConflictDetectorOptions = {}): ReplayDetector {
  const windowMs = opts.windowMs ?? CONFLICT_WINDOW_MS;
  const maxPairsPerWorkspace = opts.maxPairsPerWorkspace ?? MAX_PAIRS_PER_WORKSPACE;
  const maxSessionsPerWorkspace = opts.maxSessionsPerWorkspace ?? MAX_SESSIONS_PER_WORKSPACE;
  for (const [k, v] of Object.entries({ windowMs, maxPairsPerWorkspace, maxSessionsPerWorkspace }))
    if (!Number.isSafeInteger(v) || v < 1) throw new Error(`${name}: ${k} must be a positive integer`);
  const budget = judge.metered ? (opts.budget ?? DEFAULT_MODEL_BUDGET) : null;
  if (budget && !(Number.isSafeInteger(budget.maxCallsPerWorkspacePerDay) && budget.maxCallsPerWorkspacePerDay >= 0
    && Number.isSafeInteger(budget.maxTokensPerWorkspacePerDay) && budget.maxTokensPerWorkspacePerDay >= 0))
    throw new Error(`${name}: the model budget caps must be non-negative integers`);
  if (judge.metered && !judge.estimateTokens) throw new Error(`${name}: a metered judge must estimate its tokens`);
  const states = new WeakMap<ReplayView, State>();
  const stateFor = (view: ReplayView): State => {
    let s = states.get(view);
    if (!s) {
      s = { cache: new Map(), spend: new Map(), counters: Object.fromEntries(COUNTERS.map((k) => [k, 0])), raised: new Set(), lastNow: Number.NEGATIVE_INFINITY };
      states.set(view, s);
    }
    return s;
  };
  return {
    name,
    version: `${CONFLICT_DETECTOR_VERSION}+${judge.name}/${judge.version}`,
    params: {
      judge: judge.name, judgeVersion: judge.version, judgeParams: judge.params, windowMs, maxPairsPerWorkspace, maxSessionsPerWorkspace,
      ...(budget ? { budget } : {}), sourceSha256: sourceSha256(),
    },
    async run(view, now) {
      const s = stateFor(view);
      if (now.getTime() < s.lastNow) throw new Error(`${name}: the clock went backwards`);
      s.lastNow = now.getTime();
      const c = s.counters;
      c.ticks!++;
      const { candidates, truncated, sessionsDropped } = conflictCandidates(view.sessionsAsOf(now), now, { windowMs, maxPairsPerWorkspace, maxSessionsPerWorkspace });
      c.candidates! += candidates.length;
      c.truncated! += truncated;
      c.sessionsDropped! += sessionsDropped;
      const day = now.toISOString().slice(0, 10);
      const alerts: ConflictAlert[] = [];
      for (const cand of candidates) {
        const built = buildJudgeInput(cand, now);
        if (!citable(built.input)) { c.uncitable!++; continue; }
        const key = canonicalJson(built.input);
        let out = s.cache.get(key);
        if (out) c.cacheHits!++;
        else {
          if (budget) {
            const spendKey = `${cand.orgId}\u0000${cand.workspaceId}\u0000${day}`;
            const spent = s.spend.get(spendKey) ?? { calls: 0, tokens: 0 };
            const est = judge.estimateTokens!(built.input);
            if (spent.calls + 1 > budget.maxCallsPerWorkspacePerDay || spent.tokens + est > budget.maxTokensPerWorkspacePerDay) { c.budgetSkipped!++; continue; }
            // The budget is spent when the call is made, whatever its reply (a failed call still cost one).
            s.spend.set(spendKey, { calls: spent.calls + 1, tokens: spent.tokens + est });
            c.modelCalls!++;
            c.modelTokens! += est;
          }
          try {
            out = await judge.judge(built.input);
          } catch {
            // A failed judge call means an alert may be missing: the replay is incomplete (replay.ts).
            c.failed!++;
            continue;
          }
          c.judged!++;
          if (s.cache.size >= JUDGE_CACHE_MAX) s.cache.delete(s.cache.keys().next().value!);
          s.cache.set(key, out);
        }
        if (out.invalid) c.invalidReplies!++;
        const v = validateCitations(built, out);
        if (!v.ok) { c.vetoed!++; continue; }
        if (v.verdict === "none") { c.none!++; continue; }
        const d = decide(built.input, v.c, cand);
        if (!d) { c.silenced!++; continue; }
        const alertId = `${cand.pair[0]}|${cand.pair[1]}|${d.verdict}|${d.urgency}`;
        if (s.raised.has(alertId)) continue;
        s.raised.add(alertId);
        if (d.urgency === "high") c.alertsHigh!++; else c.alertsLow!++;
        const artifact = alertArtifact(cand.topics, v.c, d.action);
        const evidence = evidenceOf(v.c.claims, (side) => cand[side].session.sessionId, d.action);
        const sentAt = now.toISOString();
        const times = [...v.c.claims.map((x) => x.at), ...(d.action ? [d.action.ts] : [])].sort(textCompare);
        alerts.push({
          detector: name,
          alertId,
          kind: d.verdict, alertClass: d.verdict, urgency: d.urgency, orgId: cand.orgId, workspaceId: cand.workspaceId, pair: cand.pair, artifact,
          sentAt, alertAt: times.at(-1) ?? sentAt, channel: d.delivery.dm.length ? "dm" : "pull", evidence,
          conflict: { claimIds: v.c.claims.map((x) => x.id), actionId: d.action?.id ?? null, fleet: cand.fleet, delivery: d.delivery },
        });
      }
      return alerts;
    },
    diagnostics(view) {
      return { ...(states.get(view)?.counters ?? Object.fromEntries(COUNTERS.map((k) => [k, 0]))) };
    },
  };
}

export const ruleConflictDetector = (opts: ConflictDetectorOptions = {}) => conflictDetector(CONFLICT_RULE_DETECTOR, ruleJudge(), opts);
export const modelConflictDetector = (judge: ModelJudgeOptions, opts: ConflictDetectorOptions = {}) =>
  conflictDetector(CONFLICT_MODEL_DETECTOR, modelJudge(judge), opts);
