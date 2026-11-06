/**
 * Read-only publication-frontier attestation.
 *
 * This module deliberately has no SQL or object-store client.  A reviewed
 * capture adapter supplies bounded evidence; this evaluator either produces
 * the exact rows consumed by migration 20261104060000 or a durable HOLD.  It
 * never turns a sample, an absent row, or an unknown legacy fact into truth.
 */
import { createHash } from "node:crypto";
import type { SeedPointer } from "./seed-proof.ts";

export const FRONTIER_CAPTURE_PROTOCOL = "canonical-publication-frontier-capture-v1" as const;
export const FRONTIER_PLAN_PROTOCOL = "canonical-publication-frontier-plan-v1" as const;
export const FRONTIER_MAX_AGE_SECONDS = 300;

const HEX64 = /^[a-f0-9]{64}$/;
const HEX40 = /^[a-f0-9]{40}$/;
const DECIMAL = /^(0|[1-9][0-9]{0,18})$/;
const MAX_INT64 = 9223372036854775807n;
const UTC = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3,6}Z$/;
const CONTROL = /[\u0000-\u001f\u007f]/u;

export type FrontierScanKind = "origin-inventory" | "record-maxima" | "export-roots" | "privacy-controls" | "receipt-decisions";

export interface FrontierScope {
  orgId: string;
  workspaceId: string;
}

export interface FrontierTenant extends FrontierScope {
  userId: string;
  originId: string;
}

export interface FrontierTarget {
  bucket: string;
  region: string;
  keyRoot: string;
  schemaGeneration: string;
}

/** Values sampled both before and after every origin capture. */
export interface FrontierAuthority {
  sourceIdentitySha256: string | null;
  sourceGenerationSha256: string | null;
  sourceHead: string | null;
  controlEpoch: string | null;
  privacyBaselineSha256: string | null;
  privacyVersion: string | null;
}

/**
 * The cursor chain is produced by the capture adapter. `terminalCursor=null`
 * means the adapter read a bounded page past the last row, not merely that it
 * stopped at its configured limit.
 */
export interface FrontierScanEvidence {
  kind: FrontierScanKind;
  adapter: string;
  querySha256: string | null;
  snapshotSha256: string | null;
  startedAt: string;
  finishedAt: string;
  pageLimit: number;
  maxPages: number;
  pagesRead: number;
  itemCount: number;
  byteCount: number;
  checkpointCount: number;
  pageChainSha256: string | null;
  itemsSha256: string | null;
  openingStateSha256: string | null;
  closingStateSha256: string | null;
  terminalCursor: string | null;
  complete: boolean;
}

export interface FrontierExportRoot {
  tenant: FrontierTenant;
  kind: "legacy-export" | "buffered-stage";
  provider: "s3";
  bucket: string;
  keyRoot: string;
  manifestKey: string;
  source: string;
  sessionId: string;
  machineId: string;
  versionId: string | null;
  manifestRevision: string;
  contentSha256: string;
  recordCount: number;
  inventoryComplete: boolean;
  versioning: "Enabled" | "Disabled" | "Unknown";
}

export interface FrontierRecordMaximum {
  tenant: FrontierTenant;
  recordId: string;
  revision: string | null;
  revisionId: string | null;
  digest: string | null;
  source: string | null;
  sessionId: string | null;
  operation: "upsert" | "delete" | null;
  authority: "seed-proof" | "receipt-seat" | null;
  exportRootSha256: string | null;
}

export interface FrontierReceiptDecision {
  tenant: FrontierTenant;
  recordId: string;
  revision: string;
  revisionId: string;
  source: string;
  sessionId: string | null;
  seat: string;
  bodyAccepted: boolean;
  candidateDigest: string;
  firstDigest: string;
}

export interface FrontierOriginCapture {
  tenant: FrontierTenant;
  opening: FrontierAuthority;
  closing: FrontierAuthority;
  sourceRecordCount: number | null;
  scans: FrontierScanEvidence[];
  exportRoots: FrontierExportRoot[];
  recordMaxima: FrontierRecordMaximum[];
  receiptDecisions: FrontierReceiptDecision[];
}

export interface CanonicalFrontierCapture {
  protocol: typeof FRONTIER_CAPTURE_PROTOCOL;
  captureId: string;
  startedAt: string;
  capturedAt: string;
  scope: FrontierScope;
  target: FrontierTarget;
  expectedOrigins: Array<{ userId: string; originId: string }>;
  inventoryScan: FrontierScanEvidence;
  origins: FrontierOriginCapture[];
}

/** Exact column names and values expected by 20261104060000. */
export interface CanonicalFrontierRow {
  org_id: string;
  workspace_id: string;
  user_id: string;
  origin_id: string;
  source_frontier_sha256: string;
  record_maxima_sha256: string;
  valid_until: string;
  target_bucket: string;
  target_region: string;
  key_root: string;
  schema_generation: string;
}

export type FrontierHoldReason =
  | "capture_invalid"
  | "capture_stale"
  | "inventory_incomplete"
  | "origin_coverage_incomplete"
  | "scan_incomplete"
  | "scan_stale"
  | "scan_hash_mismatch"
  | "source_race"
  | "privacy_changed"
  | "tenant_mismatch"
  | "unknown_legacy_fact"
  | "export_not_immutable"
  | "record_coverage_incomplete"
  | "record_conflict"
  | "receipt_decision_conflict";

export interface FrontierHold {
  protocol: typeof FRONTIER_PLAN_PROTOCOL;
  status: "hold";
  captureId: string | null;
  reasons: FrontierHoldReason[];
  missingEvidence: string[];
}

export interface FrontierReady {
  protocol: typeof FRONTIER_PLAN_PROTOCOL;
  status: "ready";
  captureId: string;
  evidenceSha256: string;
  planSha256: string;
  scope: FrontierScope;
  target: FrontierTarget;
  validUntil: string;
  frontiers: CanonicalFrontierRow[];
}

export type FrontierReadiness = FrontierHold | FrontierReady;

const hash = (value: string) => createHash("sha256").update(value).digest("hex");

/** Canonical JSON used by both capture adapters and this evaluator. */
export function frontierCanonicalJson(value: unknown): string {
  const visit = (input: unknown): unknown => {
    if (input === null || typeof input === "string" || typeof input === "boolean") return input;
    if (typeof input === "number") {
      if (!Number.isSafeInteger(input)) throw new Error("frontier_non_integer_number");
      return input;
    }
    if (Array.isArray(input)) return input.map(visit);
    if (typeof input === "object") {
      const prototype = Object.getPrototypeOf(input);
      if (prototype !== Object.prototype && prototype !== null) throw new Error("frontier_non_json_object");
      const object = input as Record<string, unknown>;
      const out: Record<string, unknown> = Object.create(null) as Record<string, unknown>;
      for (const key of Object.keys(object).sort()) {
        if (object[key] === undefined) throw new Error("frontier_undefined_value");
        out[key] = visit(object[key]);
      }
      return out;
    }
    throw new Error("frontier_unsupported_value");
  };
  return JSON.stringify(visit(value));
}

export const frontierSha256 = (value: unknown): string => hash(frontierCanonicalJson(value));
export const frontierCaptureId = (capture: CanonicalFrontierCapture): string => frontierSha256({ ...capture, captureId: null });

export function frontierExportRootSha256(root: FrontierExportRoot): string {
  return frontierSha256(root);
}

/** Maps the existing strict seed-pointer identity; it does not invent origin or object-version facts. */
export function frontierExportRootFromSeedPointer(pointer: SeedPointer, evidence: {
  originId: string;
  keyRoot: string;
  versionId: string;
  recordCount: number;
  inventoryComplete: true;
  versioning: "Enabled";
  kind?: FrontierExportRoot["kind"];
}): FrontierExportRoot {
  return {
    tenant: { orgId: pointer.org_id, workspaceId: pointer.workspace_id, userId: pointer.user_id, originId: evidence.originId },
    kind: evidence.kind ?? "legacy-export",
    provider: "s3",
    bucket: pointer.bucket,
    keyRoot: evidence.keyRoot,
    manifestKey: pointer.manifest_key,
    source: pointer.harness,
    sessionId: pointer.session_id,
    machineId: pointer.machine_id,
    versionId: evidence.versionId,
    manifestRevision: pointer.revision,
    contentSha256: pointer.content_sha256,
    recordCount: evidence.recordCount,
    inventoryComplete: evidence.inventoryComplete,
    versioning: evidence.versioning,
  };
}

export function frontierScanItems(kind: FrontierScanKind, value: CanonicalFrontierCapture | FrontierOriginCapture): unknown {
  if (kind === "origin-inventory") {
    if (!("expectedOrigins" in value)) throw new Error("frontier_inventory_scope");
    return [...value.expectedOrigins].sort(originIdentityOrder);
  }
  if (!("recordMaxima" in value)) throw new Error("frontier_origin_scope");
  if (kind === "record-maxima") return sorted(value.recordMaxima, recordOrder);
  if (kind === "export-roots") return sorted(value.exportRoots, exportOrder);
  if (kind === "receipt-decisions") return sorted(value.receiptDecisions, decisionOrder);
  return { opening: value.opening, closing: value.closing };
}

export function frontierScanItemsSha256(kind: FrontierScanKind, value: CanonicalFrontierCapture | FrontierOriginCapture): string {
  return frontierSha256(frontierScanItems(kind, value));
}
function frontierScanItemCount(kind: FrontierScanKind, value: CanonicalFrontierCapture | FrontierOriginCapture): number {
  const items = frontierScanItems(kind, value);
  return Array.isArray(items) ? items.length : 1;
}

function sorted<T>(items: T[], compare: (a: T, b: T) => number): T[] { return [...items].sort(compare); }
function textOrder(a: string, b: string): number { return a < b ? -1 : a > b ? 1 : 0; }
function identityKey(value: FrontierTenant): string { return frontierCanonicalJson([value.orgId, value.workspaceId, value.userId, value.originId]); }
function originIdentityOrder(a: { userId: string; originId: string }, b: { userId: string; originId: string }): number {
  return textOrder(frontierCanonicalJson([a.userId, a.originId]), frontierCanonicalJson([b.userId, b.originId]));
}
function recordOrder(a: FrontierRecordMaximum, b: FrontierRecordMaximum): number { return textOrder(frontierCanonicalJson([identityKey(a.tenant), a.recordId]), frontierCanonicalJson([identityKey(b.tenant), b.recordId])); }
function exportOrder(a: FrontierExportRoot, b: FrontierExportRoot): number { return textOrder(frontierCanonicalJson([identityKey(a.tenant), a.manifestKey, a.versionId]), frontierCanonicalJson([identityKey(b.tenant), b.manifestKey, b.versionId])); }
function decisionOrder(a: FrontierReceiptDecision, b: FrontierReceiptDecision): number { return textOrder(frontierCanonicalJson([identityKey(a.tenant), a.recordId, a.revision, a.revisionId, a.candidateDigest]), frontierCanonicalJson([identityKey(b.tenant), b.recordId, b.revision, b.revisionId, b.candidateDigest])); }

function exactTenant(actual: FrontierTenant, expected: FrontierTenant): boolean {
  return actual.orgId === expected.orgId && actual.workspaceId === expected.workspaceId && actual.userId === expected.userId && actual.originId === expected.originId;
}

function validTime(value: unknown): value is string {
  if (typeof value !== "string" || !UTC.test(value) || !Number.isFinite(Date.parse(value))) return false;
  return new Date(value).toISOString() === value.replace(/\.(\d{3})\d{0,3}Z$/, ".$1Z");
}
function validText(value: unknown, maxBytes: number, emptyOk = false): value is string {
  return typeof value === "string" && (emptyOk || value.length > 0) && Buffer.byteLength(value) <= maxBytes && !CONTROL.test(value);
}
function validId(value: unknown, emptyOk = false): value is string { return validText(value, 512, emptyOk); }
function nonNegative(value: unknown): value is number { return Number.isSafeInteger(value) && (value as number) >= 0; }
function validDecimal(value: unknown, zeroOk = true): value is string {
  return typeof value === "string" && DECIMAL.test(value) && BigInt(value) <= MAX_INT64 && (zeroOk || value !== "0");
}

const REQUIRED_ADAPTERS: Record<FrontierScanKind, string> = {
  "origin-inventory": "legacy-origin-inventory-v1",
  "record-maxima": "legacy-record-maxima-v1",
  "export-roots": "ingest-seed-s3-export-roots-v1",
  "privacy-controls": "ordered-privacy-controls-v1",
  "receipt-decisions": "first-ack-seat-decisions-v1",
};

interface Findings {
  reasons: Set<FrontierHoldReason>;
  missing: Set<string>;
}
function finding(findings: Findings, reason: FrontierHoldReason, path: string): void { findings.reasons.add(reason); findings.missing.add(path); }

function validateScan(scan: FrontierScanEvidence, expectedKind: FrontierScanKind, expectedItemsSha256: string, expectedItemCount: number, captureStartedAt: number, capturedAt: number, expectedOpeningStateSha256: string, expectedClosingStateSha256: string, findings: Findings, path: string): void {
  if (!scan || scan.kind !== expectedKind || scan.adapter !== REQUIRED_ADAPTERS[expectedKind]) finding(findings, "capture_invalid", `${path}.adapter`);
  if (!HEX64.test(scan?.querySha256 ?? "") || !HEX64.test(scan?.snapshotSha256 ?? "") || !HEX64.test(scan?.pageChainSha256 ?? "")) finding(findings, "unknown_legacy_fact", `${path}.provenance`);
  if (!validTime(scan?.startedAt) || !validTime(scan?.finishedAt) || Date.parse(scan.startedAt) > Date.parse(scan.finishedAt) || Date.parse(scan.startedAt) < captureStartedAt || Date.parse(scan.finishedAt) > capturedAt) finding(findings, "capture_invalid", `${path}.time`);
  if (!nonNegative(scan?.itemCount) || !nonNegative(scan?.byteCount) || !nonNegative(scan?.checkpointCount) || !Number.isSafeInteger(scan?.pageLimit) || scan.pageLimit < 1 || scan.pageLimit > 10_000 || !Number.isSafeInteger(scan?.maxPages) || scan.maxPages < 1 || scan.maxPages > 1_000_000 || !Number.isSafeInteger(scan?.pagesRead) || scan.pagesRead < 1 || scan.pagesRead > scan.maxPages) finding(findings, "capture_invalid", `${path}.bounds`);
  else if (scan.pagesRead !== Math.ceil(scan.itemCount / scan.pageLimit) + 1 || scan.checkpointCount !== Math.max(0, scan.pagesRead - 1)) finding(findings, "scan_incomplete", `${path}.pageClosure`);
  if (scan?.complete !== true || scan?.terminalCursor !== null) finding(findings, "scan_incomplete", `${path}.terminalCursor`);
  if (validTime(scan?.finishedAt) && (capturedAt - Date.parse(scan.finishedAt) > FRONTIER_MAX_AGE_SECONDS * 1000 || Date.parse(scan.finishedAt) > capturedAt + 30_000)) finding(findings, "scan_stale", `${path}.finishedAt`);
  if (scan?.itemsSha256 !== expectedItemsSha256) finding(findings, "scan_hash_mismatch", `${path}.itemsSha256`);
  if (scan?.itemCount !== expectedItemCount) finding(findings, "scan_hash_mismatch", `${path}.itemCount`);
  if (!HEX64.test(scan?.openingStateSha256 ?? "") || !HEX64.test(scan?.closingStateSha256 ?? "")) finding(findings, "unknown_legacy_fact", `${path}.stateSha256`);
  else {
    if (scan.openingStateSha256 !== expectedOpeningStateSha256 || scan.closingStateSha256 !== expectedClosingStateSha256) finding(findings, "scan_hash_mismatch", `${path}.stateSha256`);
    if (scan.openingStateSha256 !== scan.closingStateSha256) finding(findings, "source_race", `${path}.closingStateSha256`);
  }
}

function authorityComplete(value: FrontierAuthority): boolean {
  return HEX64.test(value.sourceIdentitySha256 ?? "") && HEX64.test(value.sourceGenerationSha256 ?? "") && HEX40.test(value.sourceHead ?? "") && HEX64.test(value.controlEpoch ?? "") && HEX64.test(value.privacyBaselineSha256 ?? "") && validDecimal(value.privacyVersion);
}

function captureShape(capture: CanonicalFrontierCapture): boolean {
  return capture?.protocol === FRONTIER_CAPTURE_PROTOCOL && HEX64.test(capture.captureId) && validTime(capture.startedAt) && validTime(capture.capturedAt) && Date.parse(capture.startedAt) <= Date.parse(capture.capturedAt) && validId(capture.scope?.orgId) && validId(capture.scope?.workspaceId, true) && /^[a-z0-9][a-z0-9.-]{1,253}[a-z0-9]$/.test(capture.target?.bucket ?? "") && /^[a-z0-9-]{1,63}$/.test(capture.target?.region ?? "") && /^[a-z0-9][a-z0-9-]{0,62}$/.test(capture.target?.keyRoot ?? "") && HEX64.test(capture.target?.schemaGeneration ?? "") && Array.isArray(capture.expectedOrigins) && Array.isArray(capture.origins);
}

function hold(captureId: string | null, findings: Findings): FrontierHold {
  return {
    protocol: FRONTIER_PLAN_PROTOCOL,
    status: "hold",
    captureId,
    reasons: [...findings.reasons].sort(),
    missingEvidence: [...findings.missing].sort(),
  };
}

function evaluateCanonicalFrontierUnsafe(capture: CanonicalFrontierCapture, options: { now?: Date } = {}): FrontierReadiness {
  const findings: Findings = { reasons: new Set(), missing: new Set() };
  if (!captureShape(capture)) {
    finding(findings, "capture_invalid", "capture.shape");
    return hold(HEX64.test(capture?.captureId ?? "") ? capture.captureId : null, findings);
  }
  const now = (options.now ?? new Date()).getTime();
  const captureStartedAt = Date.parse(capture.startedAt);
  const capturedAt = Date.parse(capture.capturedAt);
  const validUntilMs = capturedAt + FRONTIER_MAX_AGE_SECONDS * 1000;
  if (!Number.isFinite(now) || now < capturedAt - 30_000 || now >= validUntilMs || capturedAt - captureStartedAt > FRONTIER_MAX_AGE_SECONDS * 1000) finding(findings, "capture_stale", "capture.capturedAt");
  if (capture.captureId !== frontierCaptureId(capture)) finding(findings, "scan_hash_mismatch", "capture.captureId");

  const expected = sorted(capture.expectedOrigins, originIdentityOrder);
  if (new Set(expected.map(frontierCanonicalJson)).size !== expected.length || expected.some(o => !validId(o.userId) || !validId(o.originId))) finding(findings, "capture_invalid", "capture.expectedOrigins");
  const inventoryOpeningState = frontierSha256(originsAuthority(capture.origins, "opening"));
  const inventoryClosingState = frontierSha256(originsAuthority(capture.origins, "closing"));
  validateScan(capture.inventoryScan, "origin-inventory", frontierScanItemsSha256("origin-inventory", capture), frontierScanItemCount("origin-inventory", capture), captureStartedAt, capturedAt, inventoryOpeningState, inventoryClosingState, findings, "capture.inventoryScan");
  if (!capture.inventoryScan.complete || capture.inventoryScan.itemCount !== expected.length) finding(findings, "inventory_incomplete", "capture.inventoryScan.itemCount");

  const origins = sorted(capture.origins, (a, b) => originIdentityOrder(a.tenant, b.tenant));
  const actualIdentity = origins.map(o => ({ userId: o.tenant.userId, originId: o.tenant.originId }));
  if (new Set(actualIdentity.map(frontierCanonicalJson)).size !== actualIdentity.length || frontierCanonicalJson(actualIdentity) !== frontierCanonicalJson(expected)) finding(findings, "origin_coverage_incomplete", "capture.origins");

  const frontiers: CanonicalFrontierRow[] = [];
  for (const [index, origin] of origins.entries()) {
    const path = `capture.origins[${index}]`;
    const tenant: FrontierTenant = { ...capture.scope, userId: origin.tenant.userId, originId: origin.tenant.originId };
    if (!exactTenant(origin.tenant, tenant)) finding(findings, "tenant_mismatch", `${path}.tenant`);
    if (!authorityComplete(origin.opening) || !authorityComplete(origin.closing)) finding(findings, "unknown_legacy_fact", `${path}.authority`);
    if (frontierCanonicalJson(origin.opening) !== frontierCanonicalJson(origin.closing)) {
      finding(findings, "source_race", `${path}.closing`);
      if (origin.opening.controlEpoch !== origin.closing.controlEpoch || origin.opening.privacyBaselineSha256 !== origin.closing.privacyBaselineSha256 || origin.opening.privacyVersion !== origin.closing.privacyVersion) finding(findings, "privacy_changed", `${path}.closing.privacy`);
    }
    if (!nonNegative(origin.sourceRecordCount)) finding(findings, "unknown_legacy_fact", `${path}.sourceRecordCount`);

    const byKind = new Map<FrontierScanKind, FrontierScanEvidence>();
    for (const scan of origin.scans ?? []) {
      if (!Object.hasOwn(REQUIRED_ADAPTERS, scan.kind)) { finding(findings, "capture_invalid", `${path}.scans.unknown`); continue; }
      if (byKind.has(scan.kind)) finding(findings, "capture_invalid", `${path}.scans.${scan.kind}.duplicate`);
      byKind.set(scan.kind, scan);
    }
    for (const kind of ["record-maxima", "export-roots", "privacy-controls", "receipt-decisions"] as const) {
      const scan = byKind.get(kind);
      if (!scan) finding(findings, "scan_incomplete", `${path}.scans.${kind}`);
      else validateScan(scan, kind, frontierScanItemsSha256(kind, origin), frontierScanItemCount(kind, origin), captureStartedAt, capturedAt, frontierSha256(origin.opening), frontierSha256(origin.closing), findings, `${path}.scans.${kind}`);
    }

    const rootHashes = new Map<string, FrontierExportRoot>();
    for (const [rootIndex, root] of sorted(origin.exportRoots ?? [], exportOrder).entries()) {
      const rootPath = `${path}.exportRoots[${rootIndex}]`;
      if (!exactTenant(root.tenant, tenant)) finding(findings, "tenant_mismatch", `${rootPath}.tenant`);
      if (root.provider !== "s3" || !["legacy-export", "buffered-stage"].includes(root.kind) || root.versioning !== "Enabled" || root.inventoryComplete !== true || !validText(root.versionId, 1024) || !HEX64.test(root.contentSha256) || !validDecimal(root.manifestRevision, false) || !nonNegative(root.recordCount) || !validText(root.bucket, 255) || !validText(root.keyRoot, 1024) || !validText(root.manifestKey, 1024) || !validId(root.source) || !validId(root.sessionId) || !validId(root.machineId) || !(root.manifestKey === root.keyRoot || root.manifestKey.startsWith(`${root.keyRoot}/`))) finding(findings, "export_not_immutable", rootPath);
      const digest = frontierExportRootSha256(root);
      if (rootHashes.has(digest)) finding(findings, "record_conflict", `${rootPath}.duplicate`);
      rootHashes.set(digest, root);
    }

    const maxima = sorted(origin.recordMaxima ?? [], recordOrder);
    const maximaById = new Map<string, FrontierRecordMaximum>();
    const coveredByRoot = new Map<string, number>();
    for (const [recordIndex, record] of maxima.entries()) {
      const recordPath = `${path}.recordMaxima[${recordIndex}]`;
      if (!exactTenant(record.tenant, tenant)) finding(findings, "tenant_mismatch", `${recordPath}.tenant`);
      if (!validId(record.recordId) || !validDecimal(record.revision, false) || !validId(record.revisionId) || !HEX64.test(record.digest ?? "") || !validId(record.source) || (record.sessionId !== null && !validId(record.sessionId)) || !["upsert", "delete"].includes(record.operation ?? "") || !["seed-proof", "receipt-seat"].includes(record.authority ?? "") || !HEX64.test(record.exportRootSha256 ?? "")) finding(findings, "unknown_legacy_fact", recordPath);
      if (maximaById.has(record.recordId)) finding(findings, "record_conflict", `${recordPath}.recordId`);
      maximaById.set(record.recordId, record);
      if (!rootHashes.has(record.exportRootSha256 ?? "")) finding(findings, "record_coverage_incomplete", `${recordPath}.exportRootSha256`);
      else coveredByRoot.set(record.exportRootSha256!, (coveredByRoot.get(record.exportRootSha256!) ?? 0) + 1);
    }
    if (origin.sourceRecordCount !== maxima.length) finding(findings, "record_coverage_incomplete", `${path}.sourceRecordCount`);
    for (const [rootDigest, root] of rootHashes) if ((coveredByRoot.get(rootDigest) ?? 0) !== root.recordCount) finding(findings, "record_coverage_incomplete", `${path}.exportRoots.${root.manifestKey}.recordCount`);

    const decisions = sorted(origin.receiptDecisions ?? [], decisionOrder);
    const accepted = new Map<string, FrontierReceiptDecision>();
    const decisionKeys = new Set<string>();
    for (const [decisionIndex, decision] of decisions.entries()) {
      const decisionPath = `${path}.receiptDecisions[${decisionIndex}]`;
      if (!exactTenant(decision.tenant, tenant)) finding(findings, "tenant_mismatch", `${decisionPath}.tenant`);
      if (!validId(decision.recordId) || !validDecimal(decision.revision, false) || !validId(decision.revisionId) || !validId(decision.source) || (decision.sessionId !== null && !validId(decision.sessionId)) || !validDecimal(decision.seat, false) || typeof decision.bodyAccepted !== "boolean" || !HEX64.test(decision.candidateDigest) || !HEX64.test(decision.firstDigest)) finding(findings, "unknown_legacy_fact", decisionPath);
      const decisionKey = frontierCanonicalJson([decision.recordId, decision.revision, decision.revisionId, decision.candidateDigest]);
      if (decisionKeys.has(decisionKey)) finding(findings, "receipt_decision_conflict", `${decisionPath}.duplicate`);
      decisionKeys.add(decisionKey);
      const maximum = maximaById.get(decision.recordId);
      if (!maximum || maximum.authority !== "receipt-seat" || decision.revision !== maximum.revision || decision.firstDigest !== maximum.digest) finding(findings, "receipt_decision_conflict", `${decisionPath}.maximum`);
      if (decision.bodyAccepted) {
        if (decision.candidateDigest !== decision.firstDigest || accepted.has(decision.recordId)) finding(findings, "receipt_decision_conflict", `${decisionPath}.bodyAccepted`);
        accepted.set(decision.recordId, decision);
      }
    }
    for (const [recordId, record] of maximaById) {
      if (record.authority !== "receipt-seat") continue;
      const decision = accepted.get(recordId);
      if (!decision || decision.revision !== record.revision || decision.revisionId !== record.revisionId || decision.firstDigest !== record.digest || decision.source !== record.source || decision.sessionId !== record.sessionId) finding(findings, "receipt_decision_conflict", `${path}.recordMaxima.${recordId}.receiptDecision`);
    }
    for (const recordId of accepted.keys()) if (maximaById.get(recordId)?.authority !== "receipt-seat") finding(findings, "receipt_decision_conflict", `${path}.receiptDecisions.${recordId}.maximum`);

    const sourceFrontier = {
      protocol: FRONTIER_CAPTURE_PROTOCOL,
      scope: tenant,
      authority: origin.closing,
      scans: sorted(origin.scans ?? [], (a, b) => textOrder(a.kind, b.kind)),
      exportRoots: sorted(origin.exportRoots ?? [], exportOrder),
    };
    const recordFrontier = {
      protocol: FRONTIER_CAPTURE_PROTOCOL,
      scope: tenant,
      sourceRecordCount: origin.sourceRecordCount,
      recordMaxima: maxima,
      receiptDecisions: decisions,
    };
    frontiers.push({
      org_id: tenant.orgId,
      workspace_id: tenant.workspaceId,
      user_id: tenant.userId,
      origin_id: tenant.originId,
      source_frontier_sha256: frontierSha256(sourceFrontier),
      record_maxima_sha256: frontierSha256(recordFrontier),
      valid_until: new Date(validUntilMs).toISOString(),
      target_bucket: capture.target.bucket,
      target_region: capture.target.region,
      key_root: capture.target.keyRoot,
      schema_generation: capture.target.schemaGeneration,
    });
  }

  if (findings.reasons.size) return hold(capture.captureId, findings);
  const evidenceSha256 = frontierSha256(capture);
  const base = {
    protocol: FRONTIER_PLAN_PROTOCOL,
    status: "ready" as const,
    captureId: capture.captureId,
    evidenceSha256,
    scope: capture.scope,
    target: capture.target,
    validUntil: new Date(validUntilMs).toISOString(),
    frontiers,
  };
  return { ...base, planSha256: frontierSha256(base) };
}

function originsAuthority(origins: FrontierOriginCapture[], edge: "opening" | "closing"): unknown {
  return sorted(origins, (a, b) => originIdentityOrder(a.tenant, b.tenant)).map(origin => ({ tenant: origin.tenant, authority: origin[edge] }));
}

/** Malformed untrusted JSON is a deterministic hold, never an evaluator crash. */
export function evaluateCanonicalFrontier(capture: CanonicalFrontierCapture, options: { now?: Date } = {}): FrontierReadiness {
  try { return evaluateCanonicalFrontierUnsafe(capture, options); }
  catch {
    const findings: Findings = { reasons: new Set(["capture_invalid"]), missing: new Set(["capture.shape"]) };
    const candidate = capture as unknown as { captureId?: unknown } | null;
    return hold(typeof candidate?.captureId === "string" && HEX64.test(candidate.captureId) ? candidate.captureId : null, findings);
  }
}
