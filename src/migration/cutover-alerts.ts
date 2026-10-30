/**
 * Cutover alerts, sent through the existing alert transport: nightcto's evidence
 * webhook (`POST /webhooks/cloud-runtime`, schema `cloud-runtime-evidence/2`,
 * nightcto docs/alerting-contract.md). nightcto dedupes by incidentFingerprint,
 * suppresses repeats, delivers and records the outcome. This module only decides
 * what is wrong; it has no delivery logic of its own beyond the POST.
 *
 * Evidence carries metric names, stages, codes and counts, under the same rule as
 * telemetry: no ids, content, paths or credentials.
 */
import type { MetricPoint } from "./cutover-metrics.ts";

export const SERVICE = "96bulls-cutover-metrics";
/**
 * Evidence with no workspace is deduped per occurrence by nightcto, so every
 * sample would page. These alerts are platform-wide, so one fixed scope lets
 * repeats collapse into one incident.
 */
export const EVIDENCE_SCOPE = "96bulls-platform";
const LAG_STAGES = new Set(["object-landed", "session-indexed"]);
export const LAG_ALERT_SECONDS = 60;
/** Samples a threshold must hold before it alerts. A blind sampler alerts at once. */
const SUSTAINED_SAMPLES = 2;
const BODY_LIMIT_BYTES = 16 * 1024;

export interface Alert {
  /** Stable for one problem: `<rule>:<subject>`. */
  key: string;
  rule: "metrics_blind" | "dlq_depth" | "stage_lag" | "stage_stalled";
  subject: string;
  value: number;
  /** The denominator: what was checked, so a reader can falsify the claim. */
  summary: string;
}

/** Everything wrong in one sample. Only asserts on sources that were read. */
export function findProblems(points: MetricPoint[]): Alert[] {
  const readOk = (kind: string) => points.some((p) => p.metric === "cutover.metrics.read_ok" && p.kind === kind && p.value === 1);
  const problems: Alert[] = [];
  const add = (rule: Alert["rule"], subject: string, value: number, summary: string) =>
    problems.push({ key: `${rule}:${subject}`, rule, subject, value, summary });

  for (const point of points.filter((p) => p.metric === "cutover.metrics.read_ok" && p.value === 0))
    add("metrics_blind", point.kind!, 1, `cutover metrics cannot read ${point.kind}; its panels are blind, which is not healthy`);

  const stages = new Set(points.filter((p) => p.metric === "pipeline.dlq.depth").map((p) => p.stage!));
  if (readOk("dead_letters") && readOk("jobs"))
    for (const stage of stages) {
      const depth = points
        .filter((p) => p.metric === "pipeline.dlq.depth" && p.stage === stage && p.state !== "replaying")
        .reduce((sum, p) => sum + p.value, 0);
      if (depth > 0) add("dlq_depth", stage, depth, `${depth} dead letters on ${stage} (dead plus pending, ${stages.size} stages checked)`);
    }

  for (const point of points.filter((p) => p.metric === "pipeline.stage.lag_seconds" && LAG_STAGES.has(p.stage!)))
    if (point.value >= LAG_ALERT_SECONDS)
      add("stage_lag", point.stage!, point.value, `${point.stage} oldest ready job waited ${Math.round(point.value)} s (limit ${LAG_ALERT_SECONDS} s)`);

  if (readOk("jobs") && readOk("events")) {
    const window = points.find((p) => p.metric === "cutover.metrics.window_seconds")?.value;
    for (const point of points.filter((p) => p.metric === "pipeline.stage.ready_depth" && p.value > 0 && LAG_STAGES.has(p.stage!))) {
      const ok = points.some((p) => p.metric === "pipeline.stage.events" && p.stage === point.stage && p.status === "ok" && p.value > 0);
      if (!ok) add("stage_stalled", point.stage!, point.value, `${point.stage} has ${point.value} ready jobs and no ok event in ${window} s`);
    }
  }
  return problems;
}

export interface Transitions {
  firing: Alert[];
  /** Still-open problems that stopped firing: each is sent as a recovery. */
  recovered: Alert[];
}

/** Holds what's open between samples, so a blip doesn't alert and a fix resolves. */
export class AlertTracker {
  private readonly streaks = new Map<string, number>();
  private readonly open = new Map<string, Alert>();

  update(problems: Alert[], points: MetricPoint[]): Transitions {
    const current = new Set(problems.map((p) => p.key));
    const firing: Alert[] = [];
    for (const problem of problems) {
      const streak = (this.streaks.get(problem.key) ?? 0) + 1;
      this.streaks.set(problem.key, streak);
      if (problem.rule === "metrics_blind" || streak >= SUSTAINED_SAMPLES) {
        firing.push(problem);
        this.open.set(problem.key, problem);
      }
    }
    for (const key of this.streaks.keys()) if (!current.has(key)) this.streaks.delete(key);
    // A source that couldn't be read proves nothing, so its problems stay open.
    const blind = points.some((p) => p.metric === "cutover.metrics.read_ok" && p.value === 0);
    const recovered: Alert[] = [];
    for (const [key, alert] of this.open)
      if (!current.has(key) && (alert.rule === "metrics_blind" || !blind)) {
        recovered.push(alert);
        this.open.delete(key);
      }
    return { firing, recovered };
  }
}

export interface EvidenceContext {
  environment: string;
  version: string;
  now: Date;
}

const SEVERITY: Record<Alert["rule"], number> = { metrics_blind: 7, dlq_depth: 8, stage_lag: 6, stage_stalled: 8 };

export function evidenceFor(alert: Alert, state: "firing" | "recovered", context: EvidenceContext) {
  const firing = state === "firing";
  const at = context.now.toISOString();
  const fingerprint = `96bulls:cutover:${alert.key}`;
  return {
    schemaVersion: "cloud-runtime-evidence/2",
    service: SERVICE,
    environment: context.environment,
    version: context.version,
    path: `cutover/${alert.rule}`,
    kind: !firing || alert.rule === "metrics_blind" ? "health" : alert.rule === "dlq_depth" ? "dlq_dead_letter" : "queue_processing_error",
    outcome: firing ? (alert.rule === "dlq_depth" ? "dlq" : "error") : "ok",
    severity: firing ? SEVERITY[alert.rule] : 1,
    occurredAt: at,
    summary: (firing ? alert.summary : `recovered: ${alert.key}`).slice(0, 300),
    counts: { errors: firing ? Math.round(alert.value) : 0 },
    category: "platform",
    stage: alert.subject,
    terminal: false,
    // Per minute: a retry of one send stays idempotent, a later sample is a new occurrence.
    occurrenceId: `${fingerprint}:${state}:${at.slice(0, 16)}`,
    incidentFingerprint: fingerprint,
    workspace: { id: "unknown", relayId: EVIDENCE_SCOPE },
    unknownReason: "platform-wide pipeline metric with no tenant",
  };
}

export interface EvidenceTarget {
  url: string;
  token: string;
}

/** None when unconfigured. Half a configuration, or a placeholder, is an error: code without a sink reads as health. */
export function evidenceTarget(env: Record<string, string | undefined>): EvidenceTarget | undefined {
  const url = env.NIGHTCTO_EVIDENCE_URL?.trim();
  const token = env.NIGHTCTO_EVIDENCE_TOKEN?.trim();
  if (!url && !token) return undefined;
  if (!url || !token || token.toLowerCase() === "unset" || token.startsWith("PLACEHOLDER_REPLACE_ME_"))
    throw new Error("NIGHTCTO_EVIDENCE_URL and NIGHTCTO_EVIDENCE_TOKEN must be set together");
  if (!url.startsWith("https://") || !url.endsWith("/webhooks/cloud-runtime"))
    throw new Error("NIGHTCTO_EVIDENCE_URL must be the https /webhooks/cloud-runtime endpoint");
  return { url, token };
}

/** Returns the HTTP status, or 0 if it never got one. Never throws, never reads the error text. */
export async function sendEvidence(target: EvidenceTarget, evidence: object, fetcher: typeof fetch = fetch): Promise<number> {
  const body = JSON.stringify(evidence);
  if (new TextEncoder().encode(body).byteLength > BODY_LIMIT_BYTES) return 0;
  try {
    const response = await fetcher(target.url, {
      method: "POST",
      headers: { authorization: `Bearer ${target.token}`, "content-type": "application/json" },
      body,
      signal: AbortSignal.timeout(5_000),
    });
    await response.body?.cancel();
    return response.status;
  } catch {
    return 0;
  }
}
