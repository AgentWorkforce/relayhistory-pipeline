import { createHash } from "node:crypto";
import { scrubText } from "../../pipeline/src/lib/scrub.ts";
import {
  INTERVENTIONS,
  KINDS,
  parseAssessment,
  type Assessment,
  type Finding,
} from "./contract.ts";
import {
  buildAnalysisPackets,
  type AnalysisPacketBundle,
  type PacketLimits,
} from "./packets.ts";

export type AnalysisPacket = AnalysisPacketBundle["packets"][number];
export interface MissingContextRequest {
  reason: string;
  sessionKeys: string[];
  evidenceIds: string[];
  requestedContext: string;
}
export interface CandidateFinding extends Omit<
  Finding,
  "id" | "incidentId" | "workstreamId" | "review" | "checkpoint"
> {
  status: "pending";
  checkpoint: string | null;
  strongestAlternativeExplanation: string;
  uncertainty: string;
}
export interface AnalysisResult {
  packetId: string;
  assessmentSha256: string;
  workstreamId: string;
  workstreamSummary: {
    state: "active" | "blocked" | "done" | "unknown";
    text: string;
    evidenceIds: string[];
  };
  observationReviews: Array<{
    observationId: string;
    disposition: string;
    rationale: string;
    evidenceIds: string[];
  }>;
  candidateFindings: CandidateFinding[];
  missingContextRequests: MissingContextRequest[];
}
export type AnalysisTransport = (request: {
  packet: AnalysisPacket;
  attempt: number;
  signal: AbortSignal;
}) => Promise<unknown>;
export interface AnalysisOptions {
  concurrency?: number;
  maxAttempts?: number;
  maxCalls?: number;
  timeoutMs?: number;
  maxResponseBytes?: number;
  packetLimits?: Partial<PacketLimits>;
  signal?: AbortSignal;
}
function invalid(): never {
  throw new Error("analysis response: invalid structured result");
}
function obj(value: unknown, keys: string[]): Record<string, unknown> {
  if (!value || typeof value !== "object" || Array.isArray(value)) invalid();
  const v = value as Record<string, unknown>;
  if (
    Object.keys(v).length !== keys.length ||
    Object.keys(v).some((k) => !keys.includes(k))
  )
    invalid();
  return v;
}
function text(value: unknown): string {
  if (
    typeof value !== "string" ||
    !value.trim() ||
    value.length > 16000 ||
    /[\u0000-\u0008\u000b\u000c\u000e-\u001f\u007f]/.test(value) ||
    scrubText(value) !== value
  )
    invalid();
  return value;
}
function choice<T extends string>(v: unknown, choices: readonly T[]): T {
  if (typeof v !== "string" || !choices.includes(v as T)) invalid();
  return v as T;
}
function array<T>(v: unknown, parse: (x: unknown) => T): T[] {
  if (!Array.isArray(v) || v.length > 200) invalid();
  return v.map(parse);
}
function refs(v: unknown, allowed: Set<string>, required = false): string[] {
  const out = array(v, text);
  if (
    new Set(out).size !== out.length ||
    out.some((id) => !allowed.has(id)) ||
    (required && !out.length)
  )
    invalid();
  return out;
}
/** Parses untrusted model output; citation membership is enforced independently of prose. */
export function parseAnalysisResult(
  raw: unknown,
  packet: AnalysisPacket,
): AnalysisResult {
  let value = raw;
  if (typeof raw === "string") {
    try {
      value = JSON.parse(raw);
    } catch {
      invalid();
    }
  }
  const r = obj(value, [
    "packetId",
    "assessmentSha256",
    "workstreamId",
    "workstreamSummary",
    "observationReviews",
    "candidateFindings",
    "missingContextRequests",
  ]);
  if (
    r.packetId !== packet.packetId ||
    r.assessmentSha256 !== packet.assessmentSha256 ||
    r.workstreamId !== packet.subject.workstreamId
  )
    invalid();
  const evidence = new Set(packet.evidence.map((e) => e.id));
  const sessions = new Set(packet.sessions.map((s) => s.key));
  const observations = new Set(packet.observations.map((o) => o.id));
  const summary = obj(r.workstreamSummary, ["state", "text", "evidenceIds"]);
  const result: AnalysisResult = {
    packetId: packet.packetId,
    assessmentSha256: packet.assessmentSha256,
    workstreamId: packet.subject.workstreamId,
    workstreamSummary: {
      state: choice(summary.state, ["active", "blocked", "done", "unknown"]),
      text: text(summary.text),
      evidenceIds: refs(summary.evidenceIds, evidence, true),
    },
    observationReviews: array(r.observationReviews, (item) => {
      const o = obj(item, [
        "observationId",
        "disposition",
        "rationale",
        "evidenceIds",
      ]);
      const observationId = text(o.observationId);
      if (!observations.has(observationId)) invalid();
      return {
        observationId,
        disposition: choice(o.disposition, [
          "intentional_delegation",
          "intentional_independent_review",
          "complementary_work",
          "unrelated",
          "possible_coordination_gap",
          "successful_coordination",
          "uncertain",
        ]),
        rationale: text(o.rationale),
        evidenceIds: refs(o.evidenceIds, evidence, true),
      };
    }),
    candidateFindings: array(r.candidateFindings, (item) => {
      const f = obj(item, [
        "status",
        "kind",
        "description",
        "factEvidenceIds",
        "actionEvidenceIds",
        "outcomeEvidenceIds",
        "checkpoint",
        "intervention",
        "communication",
        "proposedMessage",
        "strongestAlternativeExplanation",
        "uncertainty",
      ]);
      const checkpoint = f.checkpoint === null ? null : text(f.checkpoint);
      if (
        checkpoint !== null &&
        (!/^\d{4}-\d\d-\d\dT\d\d:\d\d:\d\d\.\d{3}Z$/.test(checkpoint) ||
          !Number.isFinite(Date.parse(checkpoint)) ||
          new Date(checkpoint).toISOString() !== checkpoint ||
          checkpoint < packet.window.since ||
          checkpoint >= packet.window.until)
      )
        invalid();
      const candidate: CandidateFinding = {
        status: choice(f.status, ["pending"]),
        kind: choice(f.kind, KINDS),
        description: text(f.description),
        factEvidenceIds: refs(f.factEvidenceIds, evidence),
        actionEvidenceIds: refs(f.actionEvidenceIds, evidence),
        outcomeEvidenceIds: refs(f.outcomeEvidenceIds, evidence),
        checkpoint,
        intervention: choice(f.intervention, INTERVENTIONS),
        communication: choice(f.communication, [
          "observed",
          "not_observed",
          "unknown",
        ]),
        proposedMessage:
          f.proposedMessage === null ? null : text(f.proposedMessage),
        strongestAlternativeExplanation: text(
          f.strongestAlternativeExplanation,
        ),
        uncertainty: text(f.uncertainty),
      };
      if (checkpoint !== null) {
        const events = new Map(packet.evidence.map((e) => [e.id, e]));
        if (
          candidate.factEvidenceIds.some(
            (id) => events.get(id)!.occurredAt > checkpoint,
          ) ||
          candidate.actionEvidenceIds.some(
            (id) => events.get(id)!.occurredAt < checkpoint,
          )
        )
          invalid();
        const lastAction = candidate.actionEvidenceIds.reduce(
          (latest, id) =>
            events.get(id)!.occurredAt > latest
              ? events.get(id)!.occurredAt
              : latest,
          checkpoint,
        );
        if (
          candidate.outcomeEvidenceIds.some(
            (id) => events.get(id)!.occurredAt < lastAction,
          )
        )
          invalid();
      }
      if (
        candidate.strongestAlternativeExplanation.length +
          candidate.uncertainty.length >
        15000
      )
        invalid();
      return candidate;
    }),
    missingContextRequests: array(r.missingContextRequests, (item) => {
      const m = obj(item, [
        "reason",
        "sessionKeys",
        "evidenceIds",
        "requestedContext",
      ]);
      return {
        reason: text(m.reason),
        sessionKeys: refs(m.sessionKeys, sessions),
        evidenceIds: refs(m.evidenceIds, evidence),
        requestedContext: text(m.requestedContext),
      };
    }),
  };
  if (
    new Set(result.observationReviews.map((o) => o.observationId)).size !==
    result.observationReviews.length
  )
    invalid();
  return result;
}

export async function executeAnalysis(
  input: unknown,
  transport: AnalysisTransport,
  options: AnalysisOptions = {},
) {
  const assessment = parseAssessment(input);
  const packets = buildAnalysisPackets(assessment, options.packetLimits);
  const limits = {
    concurrency: options.concurrency ?? 4,
    maxAttempts: options.maxAttempts ?? 2,
    maxCalls: options.maxCalls ?? 1000,
    timeoutMs: options.timeoutMs ?? 60000,
    maxResponseBytes: options.maxResponseBytes ?? 262144,
  };
  for (const [key, max] of Object.entries({
    concurrency: 16,
    maxAttempts: 5,
    maxCalls: 2500,
    timeoutMs: 300000,
    maxResponseBytes: 1048576,
  })) {
    const n = limits[key as keyof typeof limits];
    if (!Number.isInteger(n) || n < 1 || n > max)
      throw new Error("analysis: invalid execution limit");
  }
  const packetResults: Array<{
    packetId: string;
    status: "completed" | "failed" | "budget_exhausted" | "cancelled";
    attempts: number;
    result: AnalysisResult | null;
  }> = new Array(packets.packets.length);
  let cursor = 0;
  let calls = 0;
  async function worker() {
    while (cursor < packets.packets.length) {
      const index = cursor++;
      const packet = packets.packets[index]!;
      const row: (typeof packetResults)[number] = {
        packetId: packet.packetId,
        status: "failed",
        attempts: 0,
        result: null,
      };
      packetResults[index] = row;
      for (let attempt = 1; attempt <= limits.maxAttempts; attempt++) {
        if (options.signal?.aborted) {
          row.status = "cancelled";
          break;
        }
        if (calls >= limits.maxCalls) {
          row.status = "budget_exhausted";
          break;
        }
        calls++;
        row.attempts++;
        const controller = new AbortController();
        let timer: ReturnType<typeof setTimeout> | undefined;
        let abort: (() => void) | undefined;
        try {
          const response = await Promise.race([
            Promise.resolve().then(() =>
              transport({
                packet: structuredClone(packet),
                attempt,
                signal: controller.signal,
              }),
            ),
            new Promise<never>((_, reject) => {
              abort = () => {
                controller.abort();
                reject(new Error("analysis interrupted"));
              };
              options.signal?.addEventListener("abort", abort, { once: true });
              timer = setTimeout(abort, limits.timeoutMs);
              if (options.signal?.aborted) abort();
            }),
          ]);
          const serialized =
            typeof response === "string" ? response : JSON.stringify(response);
          if (
            typeof serialized !== "string" ||
            Buffer.byteLength(serialized) > limits.maxResponseBytes
          )
            invalid();
          row.result = parseAnalysisResult(serialized, packet);
          row.status = "completed";
          break;
        } catch {
          if (options.signal?.aborted) {
            row.status = "cancelled";
            break;
          }
        } finally {
          if (timer) clearTimeout(timer);
          if (abort) options.signal?.removeEventListener("abort", abort);
          controller.abort();
        }
      }
    }
  }
  await Promise.all(
    Array.from(
      { length: Math.min(limits.concurrency, packets.packets.length) },
      worker,
    ),
  );
  const merged: Assessment = structuredClone(assessment);
  const missingContextRequests: MissingContextRequest[] = [];
  for (const row of packetResults) {
    if (!row.result) continue;
    const result = row.result;
    const workstream = merged.workstreams.find(
      (w) => w.id === result.workstreamId,
    )!;
    // Model summaries remain proposals in packetResults; canonical summaries are not silently overwritten.
    missingContextRequests.push(...result.missingContextRequests);
    for (const [index, candidate] of result.candidateFindings.entries()) {
      if (
        !candidate.checkpoint ||
        !candidate.factEvidenceIds.length ||
        !candidate.actionEvidenceIds.length ||
        !candidate.outcomeEvidenceIds.length
      ) {
        missingContextRequests.push({
          reason:
            "Candidate lacks a complete fact/action/outcome chain or justified checkpoint.",
          sessionKeys: [...workstream.sessionKeys],
          evidenceIds: [
            ...new Set([
              ...candidate.factEvidenceIds,
              ...candidate.actionEvidenceIds,
              ...candidate.outcomeEvidenceIds,
            ]),
          ],
          requestedContext:
            "Provide the missing observed fact, dependent action, outcome, and intervention checkpoint before this draft can become a finding.",
        });
        continue;
      }
      const {
        status: _status,
        strongestAlternativeExplanation,
        uncertainty,
        ...finding
      } = candidate;
      const id = `analysis-${createHash("sha256")
        .update(JSON.stringify([row.packetId, index, candidate]))
        .digest("hex")}`;
      if (merged.findings.some((f) => f.id === id || f.incidentId === id))
        continue;
      const proposal: Finding = {
        ...finding,
        checkpoint: candidate.checkpoint,
        id,
        incidentId: id,
        workstreamId: workstream.id,
        review: {
          verdict: "pending",
          reviewer: null,
          reason: `Untrusted model proposal. Alternative: ${strongestAlternativeExplanation}\nUncertainty: ${uncertainty}`,
        },
      };
      // Cross-workstream packet context may inform analysis, but cannot become a subject finding.
      if (
        [
          ...proposal.factEvidenceIds,
          ...proposal.actionEvidenceIds,
          ...proposal.outcomeEvidenceIds,
        ].some(
          (id) =>
            !workstream.sessionKeys.includes(
              merged.evidence.find((e) => e.id === id)!.sessionKey,
            ),
        )
      ) {
        missingContextRequests.push({
          reason: "Candidate cites context outside the subject workstream.",
          sessionKeys: [...workstream.sessionKeys],
          evidenceIds: [],
          requestedContext:
            "Review workstream membership before connecting this draft to a report finding.",
        });
        continue;
      }
      merged.findings.push(proposal);
      workstream.assessment = "pending";
    }
  }
  return {
    assessment: parseAssessment(merged),
    packets,
    packetResults,
    missingContextRequests,
    execution: {
      calls,
      completedPackets: packetResults.filter((r) => r.status === "completed")
        .length,
      totalPackets: packets.packets.length,
    },
  };
}
