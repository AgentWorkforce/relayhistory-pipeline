/**
 * Bounded, one-tenant canonical-publication canary.
 *
 * This is deliberately not a second publisher. A trusted authority first
 * exposes every planned stage (including held cross-scope stages), then binds
 * the exact owner/plan/evidence tuple atomically while claiming an in-scope
 * stage. The existing publication job performs all image construction and
 * final source/head/pointer CAS work.
 */
import { createHash } from "node:crypto";
import {
  parseSessionKey,
  type ObjectStore,
} from "../../packages/object-store/src/index.ts";
import { publishBufferedStage } from "./publication-job.ts";
import { parsePublicationTicket } from "./publication-rpc.ts";
import {
  FRONTIER_PLAN_PROTOCOL,
  frontierSha256,
  type CanonicalFrontierRow,
  type FrontierReadiness,
  type FrontierReady,
} from "./frontier-plan.ts";
import type {
  PublicationRpc,
  PublicationTarget,
  PublicationTicket,
} from "./publication-protocol.ts";

const UUID = /^[a-f0-9]{8}(?:-[a-f0-9]{4}){3}-[a-f0-9]{12}$/u;
const HEX = /^[a-f0-9]{64}$/u;
const CODE = /^[a-z0-9_]{1,64}$/u;
const MAX_STAGES = 8;
const MAX_STAGE_BYTES = 16 * 1024 * 1024;
const MAX_IO_BYTES = 256 * 1024 * 1024;
const HOLD_REASONS = new Set<unknown>([
  "outside_canary_tenant",
  "frontier_not_ready",
  "owner_plan_conflict",
]);
const PLAN_VETO_REASONS = new Set([
  "publication_canary_cross_scope_eligible",
  "publication_canary_frontier_mismatch",
  "publication_canary_inspection_invalid",
  "publication_canary_inspection_mismatch",
  "publication_canary_plan_refused",
]);

export interface PublicationCanaryTenant {
  orgId: string;
  workspaceId: string;
}
export interface PublicationCanaryStage {
  stageToken: string;
  tenant: PublicationCanaryTenant;
  userId: string;
  originId: string;
  sha256: string;
  bytes: number;
}
export interface PublicationCanaryLimits {
  maxStages: number;
  maxRecordsPerStage: number;
  maxSessionsPerStage: number;
  maxStageMs: number;
  maxPlanMs: number;
  maxReads: number;
  maxReadBytes: number;
  maxPuts: number;
  maxPutBytes: number;
}
export interface PublicationCanaryPlan {
  version: 1;
  planId: string;
  ownerId: string;
  createdAt: string;
  expiresAt: string;
  tenant: PublicationCanaryTenant;
  /** Plan format is unchanged: no provider. The canary is an S3-only operator proof (its frontier is
   * S3-versioning evidence), so it publishes with provider s3 (canaryPublicationTarget). */
  target: Omit<PublicationTarget, "provider">;
  limits: PublicationCanaryLimits;
  /** Complete bounded inventory. Cross-scope entries remain here as holds. */
  stages: PublicationCanaryStage[];
}
export type PublicationCanaryHoldReason =
  | "outside_canary_tenant"
  | "frontier_not_ready"
  | "owner_plan_conflict";
export interface PublicationCanaryInspection {
  stage: PublicationCanaryStage;
  evidenceDigest: string;
  decision: "eligible" | "hold";
  holdReason?: PublicationCanaryHoldReason;
  frontier: FrontierReadiness;
}
export interface PublicationCanaryInspectionRequest {
  planId: string;
  ownerId: string;
  planDigest: string;
  stageToken: string;
}
export interface PublicationCanaryFrontierBinding {
  captureId: string;
  evidenceSha256: string;
  planSha256: string;
  row: CanonicalFrontierRow;
}
export interface PublicationCanaryBinding {
  planId: string;
  ownerId: string;
  planDigest: string;
  stageToken: string;
  evidenceDigest: string;
  frontier: PublicationCanaryFrontierBinding;
}
export interface PublicationCanaryAuthority {
  /** Read-only and complete: it must not lease/claim the stage. */
  inspect(
    input: PublicationCanaryInspectionRequest,
    signal: AbortSignal,
  ): Promise<PublicationCanaryInspection>;
  /** Atomically rechecks inspection evidence and binds owner+plan before claim. */
  claim(
    input: PublicationCanaryBinding,
    signal: AbortSignal,
  ): Promise<{ binding: PublicationCanaryBinding; ticket: PublicationTicket }>;
}
export interface PublicationCanaryIo {
  reads: number;
  readBytes: number;
  puts: number;
  putBytes: number;
}
export type PublicationCanaryOutcome =
  | {
      stageToken: string;
      status: "held";
      reason: PublicationCanaryHoldReason;
      tenant: PublicationCanaryTenant;
      evidenceDigest: string;
    }
  | {
      stageToken: string;
      status: "published";
      tenant: PublicationCanaryTenant;
      records: number;
      images: number;
      eligibleSessions: number;
      replay: boolean;
      io: PublicationCanaryIo;
      frontier: PublicationCanaryFrontierBinding;
    }
  | {
      stageToken: string;
      status: "refused";
      tenant: PublicationCanaryTenant;
      reason: string;
      io?: PublicationCanaryIo;
    };
export interface PublicationCanaryOutcomeStore {
  begin(planDigest: string, plan: PublicationCanaryPlan): Promise<void>;
  existing(planDigest: string): Promise<ReadonlyMap<string, PublicationCanaryOutcome>>;
  record(planDigest: string, outcome: PublicationCanaryOutcome): Promise<void>;
  finish(
    planDigest: string,
    summary: PublicationCanarySummary,
  ): Promise<void>;
}
export interface PublicationCanarySummary {
  planDigest: string;
  planned: number;
  published: number;
  held: number;
  refused: number;
  complete: boolean;
}
export interface PublicationCanaryDeps {
  authority: PublicationCanaryAuthority;
  rpc: PublicationRpc;
  store(signal: AbortSignal): ObjectStore;
  outcomes: PublicationCanaryOutcomeStore;
  signal?: AbortSignal;
  now?: () => number;
}

const refuse = (reason: string): Error & { code: string } =>
  Object.assign(new Error(reason), { code: reason });
const safeId = (value: unknown): value is string =>
  typeof value === "string" &&
  value.length > 0 &&
  value.length <= 255 &&
  !/[\x00-\x1f/\\]/u.test(value);
const sameTenant = (a: PublicationCanaryTenant, b: PublicationCanaryTenant) =>
  a.orgId === b.orgId && a.workspaceId === b.workspaceId;
const sameStage = (value: unknown, b: PublicationCanaryStage) => {
  if (!value || typeof value !== "object") return false;
  const a = value as Partial<PublicationCanaryStage>;
  return Boolean(
    a.tenant &&
      a.stageToken === b.stageToken &&
      sameTenant(a.tenant, b.tenant) &&
      a.userId === b.userId &&
      a.originId === b.originId &&
      a.sha256 === b.sha256 &&
      a.bytes === b.bytes,
  );
};
const validInspectionDecision = (inspection: PublicationCanaryInspection) =>
  validFrontierEnvelope(inspection.frontier) &&
  (inspection.decision === "eligible"
    ? inspection.holdReason === undefined
    : inspection.decision === "hold" && HOLD_REASONS.has(inspection.holdReason));
const isPlanVeto = (outcome: PublicationCanaryOutcome) =>
  outcome.status === "refused" && PLAN_VETO_REASONS.has(outcome.reason);
const sameBinding = (value: unknown, b: PublicationCanaryBinding) => {
  if (!value || typeof value !== "object") return false;
  const a = value as Partial<PublicationCanaryBinding>;
  return Boolean(
    a.frontier &&
      a.planId === b.planId &&
      a.ownerId === b.ownerId &&
      a.planDigest === b.planDigest &&
      a.stageToken === b.stageToken &&
      a.evidenceDigest === b.evidenceDigest &&
      a.frontier.captureId === b.frontier.captureId &&
      a.frontier.evidenceSha256 === b.frontier.evidenceSha256 &&
      a.frontier.planSha256 === b.frontier.planSha256 &&
      canonicalFrontierRowEqual(a.frontier.row, b.frontier.row),
  );
};
const canaryPublicationTarget = (target: PublicationCanaryPlan["target"]): PublicationTarget => ({
  bucket: target.bucket,
  region: target.region,
  keyRoot: target.keyRoot,
  generation: target.generation,
  provider: "s3",
});
const targetEqual = (a: PublicationCanaryPlan["target"], b: PublicationCanaryPlan["target"]) =>
  a.bucket === b.bucket &&
  a.region === b.region &&
  a.keyRoot === b.keyRoot &&
  a.generation === b.generation;
const boundedInt = (n: unknown, min: number, max: number) =>
  Number.isSafeInteger(n) && Number(n) >= min && Number(n) <= max;

function validFrontierEnvelope(value: unknown): value is FrontierReadiness {
  if (!value || typeof value !== "object") return false;
  const frontier = value as Partial<FrontierReadiness>;
  if (
    frontier.protocol !== FRONTIER_PLAN_PROTOCOL ||
    !["hold", "ready"].includes(String(frontier.status))
  )
    return false;
  if (frontier.status === "hold")
    return (
      (frontier.captureId === null ||
        (typeof frontier.captureId === "string" && HEX.test(frontier.captureId))) &&
      Array.isArray(frontier.reasons) &&
      frontier.reasons.length > 0 &&
      Array.isArray(frontier.missingEvidence) &&
      frontier.missingEvidence.length > 0
    );
  const ready = frontier as Partial<FrontierReady>;
  return (
    typeof ready.captureId === "string" &&
    HEX.test(ready.captureId) &&
    typeof ready.evidenceSha256 === "string" &&
    HEX.test(ready.evidenceSha256) &&
    typeof ready.planSha256 === "string" &&
    HEX.test(ready.planSha256) &&
    Array.isArray(ready.frontiers)
  );
}

const canonicalFrontierRowEqual = (
  a: unknown,
  b: CanonicalFrontierRow,
) => {
  if (!a || typeof a !== "object") return false;
  const row = a as Partial<CanonicalFrontierRow>;
  return (
    row.org_id === b.org_id &&
    row.workspace_id === b.workspace_id &&
    row.user_id === b.user_id &&
    row.origin_id === b.origin_id &&
    row.source_frontier_sha256 === b.source_frontier_sha256 &&
    row.record_maxima_sha256 === b.record_maxima_sha256 &&
    row.valid_until === b.valid_until &&
    row.target_bucket === b.target_bucket &&
    row.target_region === b.target_region &&
    row.key_root === b.key_root &&
    row.schema_generation === b.schema_generation
  );
};

function frontierBinding(
  value: unknown,
  stage: PublicationCanaryStage,
  plan: PublicationCanaryPlan,
  now: number,
): PublicationCanaryFrontierBinding | undefined {
  if (!validFrontierEnvelope(value) || value.status !== "ready") return undefined;
  const { planSha256, ...unsigned } = value;
  try {
    if (frontierSha256(unsigned) !== planSha256) return undefined;
  } catch {
    return undefined;
  }
  if (
    !value.scope ||
    !value.target ||
    !sameTenant(value.scope, plan.tenant) ||
    value.target.bucket !== plan.target.bucket ||
    value.target.region !== plan.target.region ||
    value.target.keyRoot !== plan.target.keyRoot ||
    value.target.schemaGeneration !== plan.target.generation ||
    !Number.isFinite(Date.parse(value.validUntil)) ||
    Date.parse(value.validUntil) <= now
  )
    return undefined;
  const identities = new Set<string>();
  let matched: CanonicalFrontierRow | undefined;
  for (const candidate of value.frontiers) {
    const row = candidate as Partial<CanonicalFrontierRow>;
    if (
      !row ||
      typeof row !== "object" ||
      row.org_id !== value.scope.orgId ||
      row.workspace_id !== value.scope.workspaceId ||
      !safeId(row.user_id) ||
      !safeId(row.origin_id) ||
      typeof row.source_frontier_sha256 !== "string" ||
      !HEX.test(row.source_frontier_sha256) ||
      typeof row.record_maxima_sha256 !== "string" ||
      !HEX.test(row.record_maxima_sha256) ||
      row.valid_until !== value.validUntil ||
      row.target_bucket !== value.target.bucket ||
      row.target_region !== value.target.region ||
      row.key_root !== value.target.keyRoot ||
      row.schema_generation !== value.target.schemaGeneration
    )
      return undefined;
    const identity = `${row.user_id}\0${row.origin_id}`;
    if (identities.has(identity)) return undefined;
    identities.add(identity);
    if (row.user_id === stage.userId && row.origin_id === stage.originId)
      matched = row as CanonicalFrontierRow;
  }
  if (!matched) return undefined;
  return {
    captureId: value.captureId,
    evidenceSha256: value.evidenceSha256,
    planSha256: value.planSha256,
    row: matched,
  };
}

function persistedFrontierMatches(
  value: unknown,
  stage: PublicationCanaryStage,
  plan: PublicationCanaryPlan,
): boolean {
  if (!value || typeof value !== "object") return false;
  const frontier = value as Partial<PublicationCanaryFrontierBinding>;
  if (
    typeof frontier.captureId !== "string" ||
    !HEX.test(frontier.captureId) ||
    typeof frontier.evidenceSha256 !== "string" ||
    !HEX.test(frontier.evidenceSha256) ||
    typeof frontier.planSha256 !== "string" ||
    !HEX.test(frontier.planSha256) ||
    !frontier.row
  )
    return false;
  const row = frontier.row;
  return (
    row.org_id === stage.tenant.orgId &&
    row.workspace_id === stage.tenant.workspaceId &&
    row.user_id === stage.userId &&
    row.origin_id === stage.originId &&
    typeof row.source_frontier_sha256 === "string" &&
    HEX.test(row.source_frontier_sha256) &&
    typeof row.record_maxima_sha256 === "string" &&
    HEX.test(row.record_maxima_sha256) &&
    typeof row.valid_until === "string" &&
    Number.isFinite(Date.parse(row.valid_until)) &&
    row.target_bucket === plan.target.bucket &&
    row.target_region === plan.target.region &&
    row.key_root === plan.target.keyRoot &&
    row.schema_generation === plan.target.generation
  );
}

function sessionKeyMatches(
  key: unknown,
  head: PublicationTicket["heads"][number],
  prefix: string,
): key is string {
  if (typeof key !== "string") return false;
  const parsed = parseSessionKey(key);
  return Boolean(
    parsed &&
      parsed.prefix === prefix &&
      parsed.workspaceId === head.physicalWorkspace &&
      parsed.machineId === head.machineId &&
      parsed.harness === head.physicalSource &&
      parsed.sessionId === head.physicalSession,
  );
}

export function publicationCanaryPlanDigest(plan: PublicationCanaryPlan): string {
  return createHash("sha256").update(JSON.stringify(plan)).digest("hex");
}

export function assertPublicationCanaryPlan(
  plan: PublicationCanaryPlan,
  target?: PublicationCanaryPlan["target"],
  now = Date.now(),
): void {
  const l = plan?.limits;
  if (
    plan?.version !== 1 ||
    !UUID.test(plan.planId) ||
    !safeId(plan.ownerId) ||
    !safeId(plan.tenant?.orgId) ||
    !safeId(plan.tenant?.workspaceId) ||
    !plan.target ||
    !safeId(plan.target.bucket) ||
    !safeId(plan.target.region) ||
    !safeId(plan.target.keyRoot) ||
    !HEX.test(plan.target.generation) ||
    !Number.isFinite(Date.parse(plan.createdAt)) ||
    !Number.isFinite(Date.parse(plan.expiresAt)) ||
    Date.parse(plan.createdAt) > now ||
    Date.parse(plan.expiresAt) <= now ||
    Date.parse(plan.expiresAt) - Date.parse(plan.createdAt) > 24 * 60 * 60 * 1000 ||
    !l ||
    !boundedInt(l.maxStages, 1, MAX_STAGES) ||
    !boundedInt(l.maxRecordsPerStage, 1, 500) ||
    !boundedInt(l.maxSessionsPerStage, 1, 500) ||
    !boundedInt(l.maxStageMs, 1000, 60 * 60 * 1000) ||
    !boundedInt(l.maxPlanMs, l.maxStageMs, 60 * 60 * 1000) ||
    !boundedInt(l.maxReads, 1, 4096) ||
    !boundedInt(l.maxReadBytes, 1, MAX_IO_BYTES) ||
    !boundedInt(l.maxPuts, 1, 4096) ||
    !boundedInt(l.maxPutBytes, 1, MAX_IO_BYTES) ||
    !Array.isArray(plan.stages) ||
    plan.stages.length < 1 ||
    plan.stages.length > l.maxStages ||
    plan.stages.length > MAX_STAGES ||
    (target && !targetEqual(plan.target, target))
  )
    throw refuse("publication_canary_plan_invalid");
  const seen = new Set<string>();
  let total = 0;
  for (const stage of plan.stages) {
    if (
      !UUID.test(stage?.stageToken) ||
      seen.has(stage.stageToken) ||
      !safeId(stage.tenant?.orgId) ||
      !safeId(stage.tenant?.workspaceId) ||
      !safeId(stage.userId) ||
      !safeId(stage.originId) ||
      !HEX.test(stage.sha256) ||
      !boundedInt(stage.bytes, 1, MAX_STAGE_BYTES)
    )
      throw refuse("publication_canary_plan_invalid");
    seen.add(stage.stageToken);
    total += stage.bytes;
  }
  if (total > l.maxReadBytes)
    throw refuse("publication_canary_plan_invalid");
}

function oneShotRpc(
  base: PublicationRpc,
  expectedStage: string,
  ticket: PublicationTicket,
): PublicationRpc {
  let used = false;
  return {
    ...base,
    async claim(stageToken, signal) {
      signal.throwIfAborted();
      if (used || stageToken !== expectedStage)
        throw refuse("publication_canary_claim_reuse");
      used = true;
      return ticket;
    },
  };
}

function ticketMatches(
  ticket: PublicationTicket,
  stage: PublicationCanaryStage,
  plan: PublicationCanaryPlan,
): boolean {
  return (
    ticket.schemaGeneration === plan.target.generation &&
    ticket.stageToken === stage.stageToken &&
    ticket.stage.sha256 === stage.sha256 &&
    ticket.stage.bytes === stage.bytes &&
    sameTenant(ticket.tenant, plan.tenant) &&
    sameTenant(ticket.tenant, stage.tenant) &&
    ticket.tenant.userId === stage.userId &&
    ticket.originId === stage.originId &&
    ticket.records.length <= plan.limits.maxRecordsPerStage &&
    ticket.heads.length <= plan.limits.maxSessionsPerStage &&
    ticket.stage.key.startsWith(
      `${plan.target.keyRoot}/${plan.tenant.orgId}/buffered-batches/`,
    ) &&
    ticket.heads.every((head) => {
      const baseKey = head.basePointer?.manifest_key;
      return (
        head.physicalWorkspace === plan.tenant.workspaceId &&
        head.prefix.startsWith(
          `${plan.target.keyRoot}/${plan.tenant.orgId}/upload-generations/g=`,
        ) &&
        (head.previousKey === null ||
          sessionKeyMatches(head.previousKey, head, head.prefix)) &&
        (baseKey === undefined ||
          sessionKeyMatches(
            baseKey,
            head,
            `${plan.target.keyRoot}/${plan.tenant.orgId}`,
          ))
      );
    })
  );
}

function meteredStore(
  raw: ObjectStore,
  plan: PublicationCanaryPlan,
  ticket: PublicationTicket,
  signal: AbortSignal,
): { store: ObjectStore; io: PublicationCanaryIo } {
  const io: PublicationCanaryIo = { reads: 0, readBytes: 0, puts: 0, putBytes: 0 };
  const writePrefixes = ticket.heads.map(
    (head) => `${head.prefix}/v1/ws=${head.physicalWorkspace}/`,
  );
  const sourcePrefixes = ticket.heads.flatMap((head) => {
    const baseKey = head.basePointer?.manifest_key;
    return typeof baseKey === "string"
      ? [baseKey.slice(0, baseKey.lastIndexOf("/") + 1)]
      : [];
  });
  const checkedKey = (value: unknown, prefixes: readonly string[]) => {
    const v = String(value);
    if (v !== ticket.stage.key && !prefixes.some((prefix) => v.startsWith(prefix)))
      throw refuse("publication_canary_store_scope");
    return v;
  };
  const readStream = (stream: ReadableStream<Uint8Array>) => {
    const reader = stream.getReader();
    return new ReadableStream<Uint8Array>({
      async pull(controller) {
        signal.throwIfAborted();
        const next = await reader.read();
        if (next.done) return controller.close();
        io.readBytes += next.value.byteLength;
        if (io.readBytes > plan.limits.maxReadBytes) {
          void reader.cancel().catch(() => {});
          return controller.error(refuse("publication_canary_read_bytes"));
        }
        controller.enqueue(next.value);
      },
      cancel(reason) {
        return reader.cancel(reason);
      },
    });
  };
  const store = new Proxy(raw, {
    get(target, property) {
      if (["delete", "list", "presignGet"].includes(String(property)))
        return async () => {
          throw refuse("publication_canary_store_operation");
        };
      if (property === "put")
        return async (rawKey: unknown, body: Uint8Array, ...rest: unknown[]) => {
          checkedKey(rawKey, writePrefixes);
          io.puts += 1;
          io.putBytes += body.byteLength;
          if (
            io.puts > plan.limits.maxPuts ||
            io.putBytes > plan.limits.maxPutBytes
          )
            throw refuse("publication_canary_put_bound");
          return Reflect.apply(target.put, target, [rawKey, body, ...rest]);
        };
      if (property === "get")
        return async (rawKey: unknown, ...rest: unknown[]) => {
          checkedKey(rawKey, [...writePrefixes, ...sourcePrefixes]);
          io.reads += 1;
          if (io.reads > plan.limits.maxReads)
            throw refuse("publication_canary_read_bound");
          return readStream(
            await Reflect.apply(target.get, target, [rawKey, ...rest]),
          );
        };
      if (property === "getObject")
        return async (rawKey: unknown, ...rest: unknown[]) => {
          checkedKey(rawKey, [...writePrefixes, ...sourcePrefixes]);
          io.reads += 1;
          if (io.reads > plan.limits.maxReads)
            throw refuse("publication_canary_read_bound");
          const value = await Reflect.apply(target.getObject, target, [
            rawKey,
            ...rest,
          ]);
          return { ...value, body: readStream(value.body) };
        };
      if (property === "head")
        return async (rawKey: unknown, ...rest: unknown[]) => {
          checkedKey(rawKey, writePrefixes);
          io.reads += 1;
          if (io.reads > plan.limits.maxReads)
            throw refuse("publication_canary_read_bound");
          return Reflect.apply(target.head, target, [rawKey, ...rest]);
        };
      const value = Reflect.get(target, property);
      return typeof value === "function" ? value.bind(target) : value;
    },
  }) as ObjectStore;
  return { store, io };
}

const reason = (error: unknown) => {
  const raw = (error as { code?: unknown })?.code;
  return typeof raw === "string" && CODE.test(raw)
    ? raw
    : "publication_canary_failed";
};

export async function runPublicationCanary(
  plan: PublicationCanaryPlan,
  deps: PublicationCanaryDeps,
): Promise<PublicationCanarySummary> {
  const now = deps.now ?? Date.now;
  assertPublicationCanaryPlan(plan, undefined, now());
  const planDigest = publicationCanaryPlanDigest(plan);
  await deps.outcomes.begin(planDigest, plan);
  const existing = await deps.outcomes.existing(planDigest);
  const planned = new Map(plan.stages.map((stage) => [stage.stageToken, stage]));
  for (const [stageToken, outcome] of existing) {
    const stage = planned.get(stageToken);
    if (
      !stage ||
      outcome.stageToken !== stageToken ||
      !sameTenant(outcome.tenant, stage.tenant) ||
      !["held", "published", "refused"].includes(outcome.status) ||
      (outcome.status === "published" &&
        !persistedFrontierMatches(outcome.frontier, stage, plan))
    )
      throw refuse("publication_canary_outcome_invalid");
  }
  const stop = new AbortController();
  const remainingMs = Date.parse(plan.expiresAt) - now();
  const signal = AbortSignal.any([
    stop.signal,
    AbortSignal.timeout(Math.max(1, Math.min(plan.limits.maxPlanMs, remainingMs))),
    ...(deps.signal ? [deps.signal] : []),
  ]);
  const inspections = new Map<string, PublicationCanaryInspection>();
  const frontierBindings = new Map<string, PublicationCanaryFrontierBinding>();
  const outcomes = new Map(existing);
  // A triggering refusal is durable before the remaining plan veto outcomes
  // are written. Restore the veto after a crash in that exact interval.
  const persistedPlanRefused = [...existing.values()].some(isPlanVeto);
  if (
    persistedPlanRefused &&
    [...existing.values()].some((outcome) => outcome.status === "published")
  )
    throw refuse("publication_canary_outcome_invalid");
  let planRefused = persistedPlanRefused;
  try {
    // Inspect every stage before the first claim: cross-scope work can never be
    // hidden behind an earlier in-scope failure.
    for (const stage of plan.stages) {
      signal.throwIfAborted();
      if (outcomes.has(stage.stageToken)) continue;
      const partial: PublicationCanaryInspectionRequest = {
        planId: plan.planId,
        ownerId: plan.ownerId,
        planDigest,
        stageToken: stage.stageToken,
      };
      const value: unknown = await deps.authority.inspect(partial, signal);
      if (
        !value ||
        typeof value !== "object" ||
        typeof (value as PublicationCanaryInspection).evidenceDigest !== "string" ||
        !HEX.test((value as PublicationCanaryInspection).evidenceDigest) ||
        !sameStage((value as PublicationCanaryInspection).stage, stage)
      ) {
        const outcome: PublicationCanaryOutcome = {
          stageToken: stage.stageToken,
          status: "refused",
          tenant: stage.tenant,
          reason: "publication_canary_inspection_mismatch",
        };
        await deps.outcomes.record(planDigest, outcome);
        outcomes.set(stage.stageToken, outcome);
        planRefused = true;
        continue;
      }
      const inspected = value as PublicationCanaryInspection;
      if (!validInspectionDecision(inspected)) {
        const outcome: PublicationCanaryOutcome = {
          stageToken: stage.stageToken,
          status: "refused",
          tenant: stage.tenant,
          reason: "publication_canary_inspection_invalid",
        };
        await deps.outcomes.record(planDigest, outcome);
        outcomes.set(stage.stageToken, outcome);
        planRefused = true;
        continue;
      }
      const inside = sameTenant(inspected.stage.tenant, plan.tenant);
      if (!inside) {
        const validHold =
          inspected.decision === "hold" &&
          inspected.holdReason === "outside_canary_tenant";
        const outcome: PublicationCanaryOutcome = validHold
          ? {
              stageToken: stage.stageToken,
              status: "held",
              reason: "outside_canary_tenant",
              tenant: stage.tenant,
              evidenceDigest: inspected.evidenceDigest,
            }
          : {
              stageToken: stage.stageToken,
              status: "refused",
              tenant: stage.tenant,
              reason: "publication_canary_cross_scope_eligible",
            };
        await deps.outcomes.record(planDigest, outcome);
        outcomes.set(stage.stageToken, outcome);
        if (!validHold) planRefused = true;
        continue;
      }
      if (inspected.decision === "hold") {
        const holdReason = inspected.holdReason;
        if (!holdReason || holdReason === "outside_canary_tenant") {
          const outcome: PublicationCanaryOutcome = {
            stageToken: stage.stageToken,
            status: "refused",
            tenant: stage.tenant,
            reason: "publication_canary_inspection_invalid",
          };
          await deps.outcomes.record(planDigest, outcome);
          outcomes.set(stage.stageToken, outcome);
          planRefused = true;
          continue;
        }
        const outcome: PublicationCanaryOutcome = {
          stageToken: stage.stageToken,
          status: "held",
          reason: holdReason,
          tenant: stage.tenant,
          evidenceDigest: inspected.evidenceDigest,
        };
        await deps.outcomes.record(planDigest, outcome);
        outcomes.set(stage.stageToken, outcome);
        continue;
      }
      inspections.set(stage.stageToken, inspected);
    }

    if (!planRefused) {
      const checkedAt = now();
      for (const stage of plan.stages) {
        if (outcomes.has(stage.stageToken)) continue;
        const inspected = inspections.get(stage.stageToken);
        if (!inspected) continue;
        const frontier = frontierBinding(inspected.frontier, stage, plan, checkedAt);
        if (frontier) {
          frontierBindings.set(stage.stageToken, frontier);
          continue;
        }
        const outcome: PublicationCanaryOutcome = {
          stageToken: stage.stageToken,
          status: "refused",
          tenant: stage.tenant,
          reason: "publication_canary_frontier_mismatch",
        };
        await deps.outcomes.record(planDigest, outcome);
        outcomes.set(stage.stageToken, outcome);
        planRefused = true;
      }
    }

    if (planRefused) {
      for (const stage of plan.stages) {
        if (outcomes.has(stage.stageToken)) continue;
        const outcome: PublicationCanaryOutcome = {
          stageToken: stage.stageToken,
          status: "refused",
          tenant: stage.tenant,
          reason: "publication_canary_plan_refused",
        };
        await deps.outcomes.record(planDigest, outcome);
        outcomes.set(stage.stageToken, outcome);
      }
    }

    for (const stage of plan.stages) {
      signal.throwIfAborted();
      if (outcomes.has(stage.stageToken)) continue;
      const inspected = inspections.get(stage.stageToken);
      if (!inspected) throw refuse("publication_canary_inspection_missing");
      const frontier = frontierBindings.get(stage.stageToken);
      if (!frontier) throw refuse("publication_canary_frontier_mismatch");
      const binding: PublicationCanaryBinding = {
        planId: plan.planId,
        ownerId: plan.ownerId,
        planDigest,
        stageToken: stage.stageToken,
        evidenceDigest: inspected.evidenceDigest,
        frontier,
      };
      let claimed: Awaited<ReturnType<PublicationCanaryAuthority["claim"]>> | undefined;
      let io: PublicationCanaryIo | undefined;
      try {
        claimed = await deps.authority.claim(binding, signal);
        const ticket = parsePublicationTicket(claimed.ticket, canaryPublicationTarget(plan.target));
        if (!sameBinding(claimed.binding, binding) || !ticketMatches(ticket, stage, plan)) {
          const cleanup = AbortSignal.timeout(5000);
          await deps.rpc.abandon(ticket, cleanup).catch(() => {});
          throw refuse("publication_canary_ticket_mismatch");
        }
        const metered = meteredStore(
          deps.store(signal),
          plan,
          ticket,
          signal,
        );
        io = metered.io;
        const result = await publishBufferedStage(
          {
            target: canaryPublicationTarget(plan.target),
            rpc: oneShotRpc(deps.rpc, stage.stageToken, ticket),
            store: () => metered.store,
          },
          stage.stageToken,
          {
            enabled: true,
            signal,
            maxJobMs: plan.limits.maxStageMs,
            candidateLimit: 1,
          },
        );
        if (!result.enabled) throw refuse("publication_canary_disabled");
        const outcome: PublicationCanaryOutcome = {
          stageToken: stage.stageToken,
          status: "published",
          tenant: stage.tenant,
          records: result.records,
          images: result.images,
          eligibleSessions: result.eligibleSessions,
          replay: result.replay,
          io,
          frontier,
        };
        await deps.outcomes.record(planDigest, outcome);
        outcomes.set(stage.stageToken, outcome);
      } catch (error) {
        const outcome: PublicationCanaryOutcome = {
          stageToken: stage.stageToken,
          status: "refused",
          tenant: stage.tenant,
          reason: reason(error),
          ...(io ? { io } : {}),
        };
        await deps.outcomes.record(planDigest, outcome);
        outcomes.set(stage.stageToken, outcome);
      }
    }
  } finally {
    stop.abort(refuse("publication_canary_complete"));
  }
  const values = plan.stages.flatMap((stage) => {
    const outcome = outcomes.get(stage.stageToken);
    return outcome ? [outcome] : [];
  });
  const summary: PublicationCanarySummary = {
    planDigest,
    planned: plan.stages.length,
    published: values.filter((v) => v.status === "published").length,
    held: values.filter((v) => v.status === "held").length,
    refused: values.filter((v) => v.status === "refused").length,
    complete: values.length === plan.stages.length,
  };
  if (!summary.complete) throw refuse("publication_canary_incomplete");
  await deps.outcomes.finish(planDigest, summary);
  return summary;
}
