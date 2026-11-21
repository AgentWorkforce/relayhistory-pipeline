/** Offline T1/T2 experiment. This is not the production advisor, a priority feed or a delivery simulator. */
import { readFileSync } from "node:fs";
import { outcomeFacts } from "../../../packages/coordination-detect/src/index.ts";
import { DEFAULT_PAIR_COOLDOWN_SECONDS, DEFAULT_SESSION_HOURLY_CAP } from "../../coordination-advisor/config.ts";
import { AdvisorLimits, pairKey } from "../../coordination-advisor/rubric.ts";
import { actionKey, type ActionKind } from "../patterns/action-scope.ts";
import { ActionFrame, ACTION_FRAME_BOUNDS, ACTION_LOOKBACK_MS } from "./action-frame.ts";
import type { ReplayAlert, ReplayDetector } from "./detector.ts";
import type { ReplayView } from "./views.ts";
import { canonicalJson, refString, sha256Hex, sortedPair, textCompare } from "./util.ts";

export const ACTION_SCOPED_DETECTOR = "action-scoped-outcomes";
const SOURCES = ["./action-scoped.ts", "./action-frame.ts", "../patterns/action-scope.ts", "../patterns/corpus.ts", "../patterns/types.ts", "../import/normalize.ts", "../import/provenance.ts", "./snapshot.ts", "./util.ts", "./replay.ts", "../../coordination-advisor/rubric.ts", "../../coordination-advisor/config.ts", "../../../packages/coordination-detect/src/index.ts"];
export const ACTION_REPLAY_BOUNDS = { work: 20_000_000, findings: 10_000, perCallFindings: 65_536 } as const;
interface Candidate { repo: string; alert: ReplayAlert; recipients: [{ harness: string; sessionId: string }, { harness: string; sessionId: string }] }
interface State { frame: ActionFrame; limits: AdvisorLimits; pending: Map<string, Candidate>; known: Set<string>; emitted: Set<string>; work: number; delivered: number; skippedFleet: number }

export function actionScopedDetector(options: { now?: Date; workBudget?: number; pairCooldownMs?: number; sessionHourlyCap?: number } = {}): ReplayDetector {
  const workBudget = options.workBudget ?? ACTION_REPLAY_BOUNDS.work;
  const pairCooldownMs = options.pairCooldownMs ?? DEFAULT_PAIR_COOLDOWN_SECONDS * 1_000;
  const sessionHourlyCap = options.sessionHourlyCap ?? DEFAULT_SESSION_HOURLY_CAP;
  if (![workBudget, pairCooldownMs, sessionHourlyCap].every((n) => Number.isSafeInteger(n) && n > 0)) throw new Error("action replay: invalid bounded parameters");
  const states = new WeakMap<ReplayView, State>();
  const params = { scope: "explicit-v2-action-repositories", capabilities: ["T1", "T2"], branches: "unknown", usage: "unattributed", claims: "not_consumed", delivery: "hypothetical_pull", dedupe: "once_per_repository_finding", workBudget, pairCooldownMs, sessionHourlyCap,
    lookbackMs: ACTION_LOOKBACK_MS, frameBounds: ACTION_FRAME_BOUNDS, bounds: ACTION_REPLAY_BOUNDS,
    sourceSha256: sha256Hex(SOURCES.map((p) => sha256Hex(readFileSync(new URL(p, import.meta.url)))).join("\n")) };
  return {
    name: ACTION_SCOPED_DETECTOR, version: "1", params,
    async run(view, now) {
      let state = states.get(view);
      if (!state) {
        state = { frame: new ActionFrame(view.snapshot, options.now ?? new Date()), limits: new AdvisorLimits({ pairCooldownMs, sessionHourlyCap }), pending: new Map(), known: new Set(), emitted: new Set(), work: 0, delivered: 0, skippedFleet: 0 };
        states.set(view, state);
      }
      const s = state, iso = now.toISOString();
      const changed = s.frame.advance(now);
      if (s.frame.exhausted) return [];
      for (const [repo, lanes] of changed) {
        const facts = lanes.reduce((n, l) => n + l.writes.length + l.commits.length + l.outcomes.length, 0);
        const cost = 4 * facts * facts + lanes.length;
        if (cost > workBudget - s.work) { s.frame.reject(); return []; }
        s.work += cost;
        const output = outcomeFacts.detect({ workspaceId: view.snapshot.sessions[0]?.workspaceId ?? "", window: { since: view.window.since, until: iso }, lanes }, {}, ACTION_REPLAY_BOUNDS.perCallFindings);
        if (output.findings.length >= ACTION_REPLAY_BOUNDS.perCallFindings) { s.frame.reject(); return []; }
        const current = new Set<string>();
        for (const f of output.findings) {
          if ((f.kind !== "duplicate_work" && f.kind !== "superseded_work") || f.lanes.length !== 2) continue;
          const recipients = f.lanes.map((l) => ({ harness: l.source, sessionId: l.sessionId })) as Candidate["recipients"];
          const fleet = s.frame.planned(refString(recipients[0]), refString(recipients[1]));
          if (fleet === null || fleet) { s.skippedFleet++; continue; }
          const id = sha256Hex(canonicalJson([repo, f.id]));
          if (!s.known.has(id)) {
            if (s.known.size >= ACTION_REPLAY_BOUNDS.findings) { s.frame.reject(); return []; }
            s.known.add(id);
          }
          current.add(id);
          if (s.emitted.has(id)) continue;
          let knownAt = f.alertAt;
          for (const e of f.evidence) {
            const kind: ActionKind = f.kind === "duplicate_work" ? "write" : e.kind === "commit" ? "commit" : "outcome";
            const matches = recipients.filter((r) => r.sessionId === e.sessionId).flatMap((r) => {
              const record = s.frame.records.get(actionKey(r, kind, { eventId: e.eventId, ts: e.ts }));
              return record?.repository === repo && record.availableAt < iso ? [record.availableAt] : [];
            });
            if (!matches.length) throw new Error("action replay: finding evidence has no visible scope");
            for (const at of matches) if (at > knownAt) knownAt = at;
          }
          const first = s.frame.sessions.get(refString(recipients[0]))!;
          s.pending.set(id, { repo, recipients, alert: {
            detector: ACTION_SCOPED_DETECTOR, alertId: id, kind: f.kind, urgency: "high", orgId: first.orgId, workspaceId: first.workspaceId,
            pair: sortedPair(refString(recipients[0]), refString(recipients[1])), artifact: f.artifacts[0]!.value,
            sentAt: iso, alertAt: knownAt, channel: "pull", evidence: f.evidence.map((e) => ({ ...e })),
          } });
        }
        for (const [id, candidate] of s.pending) if (candidate.repo === repo && !current.has(id)) s.pending.delete(id);
      }
      const alerts: ReplayAlert[] = [];
      s.limits.prune(now.getTime());
      for (const [id, c] of [...s.pending].sort(([a, x], [b, y]) => textCompare(x.alert.alertAt, y.alert.alertAt) || textCompare(a, b))) {
        const pair = pairKey(c.recipients[0], c.recipients[1]);
        if (s.limits.inCooldown(pair, "high", now.getTime()) || !c.recipients.every((r) => s.limits.underCap(r, now.getTime()))) continue;
        s.limits.touchPair(pair, "high", now.getTime());
        for (const r of c.recipients) s.limits.recordSent(r, now.getTime());
        s.emitted.add(id); s.pending.delete(id); s.delivered++;
        alerts.push({ ...c.alert, sentAt: iso });
      }
      return alerts;
    },
    diagnostics(view): Record<string, number> {
      const s = states.get(view);
      return s ? { ...s.frame.stats, work: s.work, findings: s.known.size, hypotheticalPairs: s.delivered, pending: s.pending.size, skippedFleet: s.skippedFleet } : { rejected: 0 };
    },
  };
}
