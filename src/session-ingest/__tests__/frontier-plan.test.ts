import { describe, expect, test } from "bun:test";
import { parseFrontierJson, readFrontierBundle, MAX_FRONTIER_BUNDLE_BYTES } from "../frontier-plan-input.ts";
import { mkdtempSync, rmSync, writeFileSync, symlinkSync, truncateSync, readFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { spawnSync } from "node:child_process";
import { createHash } from "node:crypto";
import {
  FRONTIER_CAPTURE_PROTOCOL,
  evaluateCanonicalFrontier,
  frontierCaptureId,
  frontierExportRecordsSha256,
  frontierExportRootSha256,
  frontierRecordExportEvidenceSha256,
  frontierScanItemsSha256,
  frontierSha256,
  type CanonicalFrontierCapture,
  type FrontierAuthority,
  type FrontierExportBinding,
  type FrontierOriginCapture,
  type FrontierRecordMaximum,
  type FrontierScanEvidence,
  type FrontierScanKind,
} from "../frontier-plan.ts";

const H = (value: string) => frontierSha256(value);
const AT = "2026-11-06T04:00:04.000Z";
const NOW = new Date("2026-11-06T04:00:05.000Z");
const tenant = { orgId: "org-canary", workspaceId: "workspace-canary", userId: "user-canary", originId: "origin-canary" };
const authority: FrontierAuthority = {
  sourceIdentitySha256: H("source-identity"),
  sourceGenerationSha256: H("source-generation"),
  sourceHead: "a".repeat(40),
  controlEpoch: H("control-epoch"),
  privacyBaselineSha256: H("privacy-baseline"),
  privacyVersion: "19",
};

const adapters: Record<FrontierScanKind, string> = {
  "origin-inventory": "legacy-origin-inventory-v1",
  "record-maxima": "legacy-record-maxima-v1",
  "export-roots": "ingest-seed-s3-export-roots-v1",
  "privacy-controls": "ordered-privacy-controls-v1",
  "receipt-decisions": "first-ack-seat-decisions-v1",
};

function scan(kind: FrontierScanKind, itemCount: number, itemsSha256: string, state = frontierSha256(authority)): FrontierScanEvidence {
  const pageItemCounts = itemCount === 0 ? [0] : [itemCount, 0];
  return {
    kind,
    adapter: adapters[kind],
    querySha256: H(`query:${kind}`),
    snapshotSha256: H(`snapshot:${kind}`),
    startedAt: "2026-11-06T04:00:00.000Z",
    finishedAt: "2026-11-06T04:00:03.000Z",
    pageLimit: 100,
    maxPages: 1000,
    pagesRead: pageItemCounts.length,
    pageItemCounts,
    itemCount,
    byteCount: 4096,
    checkpointCount: pageItemCounts.filter(count => count > 0).length,
    pageChainSha256: H(`page-chain:${kind}`),
    itemsSha256,
    openingStateSha256: state,
    closingStateSha256: state,
    terminalCursor: null,
    complete: true,
  };
}

function reseal(capture: CanonicalFrontierCapture): CanonicalFrontierCapture {
  capture.captureId = frontierCaptureId(capture);
  return capture;
}

function readyCapture(): CanonicalFrontierCapture {
  const historicalRoot = {
    tenant: { ...tenant }, kind: "legacy-export" as const, provider: "s3" as const,
    bucket: "source-versioned", keyRoot: "96bulls/org-canary", manifestKey: "96bulls/org-canary/legacy/manifest.json",
    source: "claude", sessionId: "legacy-session", machineId: "legacy-machine",
    versionId: "v-legacy-1", manifestRevision: "11", contentSha256: H("legacy-object"), recordCount: 1,
    inventoryComplete: true, versioning: "Enabled" as const,
  };
  const bufferedRoot = {
    tenant: { ...tenant }, kind: "buffered-stage" as const, provider: "s3" as const,
    bucket: "source-versioned", keyRoot: "96bulls/org-canary/buffered-batches", manifestKey: "96bulls/org-canary/buffered-batches/stage-1.json.zst",
    source: "claude", sessionId: "session-A", machineId: "buffer-machine",
    versionId: "v-buffered-1", manifestRevision: "1", contentSha256: H("buffered-object"), recordCount: 1,
    inventoryComplete: true, versioning: "Enabled" as const,
  };
  const historicalDigest = frontierExportRootSha256(historicalRoot);
  const bufferedDigest = frontierExportRootSha256(bufferedRoot);
  const legacyMaximum: FrontierRecordMaximum = { tenant: { ...tenant }, recordId: "legacy-record", revision: "9", revisionId: "legacy-revision-9", digest: H("legacy-body"), kind: "session_event", source: "claude", sessionId: "legacy-session", operation: "upsert", authority: "seed-proof", exportRootSha256: historicalDigest, exportContentSha256: H("legacy-export-content"), exportEvidenceSha256: null };
  legacyMaximum.exportEvidenceSha256 = frontierRecordExportEvidenceSha256(legacyMaximum);
  const bufferedMaximum: FrontierRecordMaximum = { tenant: { ...tenant }, recordId: "shared-record", revision: "3", revisionId: "winner-revision", digest: H("winner-body"), kind: "session_event", source: "claude", sessionId: "session-A", operation: "upsert", authority: "receipt-seat", exportRootSha256: bufferedDigest, exportContentSha256: H("winner-body"), exportEvidenceSha256: null };
  bufferedMaximum.exportEvidenceSha256 = frontierRecordExportEvidenceSha256(bufferedMaximum);
  const origin: FrontierOriginCapture = {
    tenant: { ...tenant }, opening: { ...authority }, closing: { ...authority }, sourceRecordCount: 2, privacyRowCount: 1,
    scans: [], exportRoots: [bufferedRoot, historicalRoot],
    recordMaxima: [legacyMaximum, bufferedMaximum],
    receiptDecisions: [
      { tenant: { ...tenant }, recordId: "shared-record", revision: "3", revisionId: "loser-revision", source: "codex", sessionId: "session-B", seat: "201", bodyAccepted: false, candidateDigest: H("loser-body"), firstDigest: H("winner-body") },
      { tenant: { ...tenant }, recordId: "shared-record", revision: "3", revisionId: "winner-revision", source: "claude", sessionId: "session-A", seat: "201", bodyAccepted: true, candidateDigest: H("winner-body"), firstDigest: H("winner-body") },
    ],
  };
  origin.scans = [
    scan("record-maxima", origin.recordMaxima.length, frontierScanItemsSha256("record-maxima", origin)),
    scan("export-roots", origin.exportRoots.length, frontierScanItemsSha256("export-roots", origin)),
    scan("privacy-controls", 1, frontierScanItemsSha256("privacy-controls", origin)),
    scan("receipt-decisions", origin.receiptDecisions.length, frontierScanItemsSha256("receipt-decisions", origin)),
  ];
  const capture = {
    protocol: FRONTIER_CAPTURE_PROTOCOL,
    captureId: "0".repeat(64),
    startedAt: "2026-11-06T04:00:00.000Z", capturedAt: AT,
    scope: { orgId: tenant.orgId, workspaceId: tenant.workspaceId },
    target: { bucket: "canonical-target", region: "us-east-1", keyRoot: "96bulls", schemaGeneration: H("schema-generation") },
    expectedOrigins: [{ userId: tenant.userId, originId: tenant.originId }],
    inventoryScan: {} as FrontierScanEvidence,
    origins: [origin],
  } satisfies CanonicalFrontierCapture;
  const inventoryState = frontierSha256(capture.origins.map(value => ({ tenant: value.tenant, authority: value.opening })));
  capture.inventoryScan = scan("origin-inventory", 1, frontierScanItemsSha256("origin-inventory", capture), inventoryState);
  return reseal(capture);
}

function refreshOriginHashes(capture: CanonicalFrontierCapture): void {
  const origin = capture.origins[0]!;
  for (const scanEvidence of origin.scans) {
    scanEvidence.itemsSha256 = frontierScanItemsSha256(scanEvidence.kind, origin);
    scanEvidence.itemCount = scanEvidence.kind === "privacy-controls" ? 1 :
      scanEvidence.kind === "record-maxima" ? origin.recordMaxima.length :
      scanEvidence.kind === "export-roots" ? origin.exportRoots.length : origin.receiptDecisions.length;
    scanEvidence.pageItemCounts = scanEvidence.itemCount === 0 ? [0] : [scanEvidence.itemCount, 0];
    scanEvidence.pagesRead = scanEvidence.pageItemCounts.length;
    scanEvidence.checkpointCount = scanEvidence.pageItemCounts.filter(count => count > 0).length;
  }
}

function bindingsFor(capture: CanonicalFrontierCapture): FrontierExportBinding[] {
  return capture.origins.flatMap(origin => origin.exportRoots.map(root => {
    const exportRootSha256 = frontierExportRootSha256(root);
    const records = origin.recordMaxima.filter(record => record.exportRootSha256 === exportRootSha256);
    return { exportRootSha256, recordsSha256: frontierExportRecordsSha256(records) };
  }));
}
const evaluate = (capture: CanonicalFrontierCapture, now = NOW, exportBindings = bindingsFor(capture)) =>
  evaluateCanonicalFrontier(capture, { now, exportBindings });

describe("canonical frontier read-only planner", () => {
  test("full immutable coverage produces deterministic 040600 rows and accepts a losing equal-revision body", () => {
    const capture = readyCapture();
    const one = evaluate(capture);
    const two = evaluate(structuredClone(capture));
    expect(one).toEqual(two);
    expect(one.status).toBe("ready");
    if (one.status !== "ready") throw new Error("expected ready");
    expect(one.frontiers).toHaveLength(1);
    expect(one.frontiers[0]).toMatchObject({
      org_id: tenant.orgId, workspace_id: tenant.workspaceId, user_id: tenant.userId, origin_id: tenant.originId,
      target_bucket: "canonical-target", target_region: "us-east-1", key_root: "96bulls",
    });
    expect(one.planSha256).toMatch(/^[a-f0-9]{64}$/);
  });

  test("unsupported non-ASCII or surrogate identifiers HOLD without changing planner order", () => {
    const bytes = readFileSync(new URL("../../../docs/contracts/frontier-attester-20261107/publication-frontier-contract-vectors-20261107.json", import.meta.url));
    expect(createHash("sha256").update(bytes).digest("hex")).toBe("fd3f8908524b708b24e1511dc88795eb365304f7e3d74cb6267f4491a3231abe");
    const vector = JSON.parse(bytes.toString("utf8")).plannerNonAsciiOrderVector as { identifiers: string[]; expected: string };
    expect(vector.expected).toBe("HOLD_UNTIL_REVIEWED_049_AMENDMENT");
    for (const id of [...vector.identifiers, "\ud800", "bad id"]) {
      const capture = readyCapture();
      const maximum = capture.origins[0]!.recordMaxima[0]!;
      maximum.recordId = id;
      maximum.exportEvidenceSha256 = frontierRecordExportEvidenceSha256(maximum);
      refreshOriginHashes(capture); reseal(capture);
      expect(evaluate(capture)).toMatchObject({ status: "hold", reasons: expect.arrayContaining(["unknown_legacy_fact"]) });
    }
  });

  test("sampled, stale, or unknown legacy evidence holds instead of inventing completeness", () => {
    const incomplete = readyCapture();
    incomplete.origins[0]!.scans[0]!.complete = false;
    incomplete.origins[0]!.scans[0]!.terminalCursor = "more-rows";
    reseal(incomplete);
    expect(evaluate(incomplete)).toMatchObject({ status: "hold", reasons: expect.arrayContaining(["scan_incomplete"]) });

    const unknown = readyCapture();
    unknown.origins[0]!.recordMaxima[0]!.revision = null;
    refreshOriginHashes(unknown); reseal(unknown);
    expect(evaluate(unknown)).toMatchObject({ status: "hold", reasons: expect.arrayContaining(["unknown_legacy_fact"]) });

    const stale = readyCapture();
    expect(evaluate(stale, new Date("2026-11-06T04:20:00.000Z"))).toMatchObject({ status: "hold", reasons: expect.arrayContaining(["capture_stale"]) });
  });

  test("source races and privacy-control changes invalidate the entire origin", () => {
    const capture = readyCapture();
    capture.origins[0]!.closing.privacyVersion = "20";
    refreshOriginHashes(capture); reseal(capture);
    expect(evaluate(capture)).toMatchObject({ status: "hold", reasons: expect.arrayContaining(["privacy_changed", "source_race"]) });
  });

  test("wrong-tenant roots and records cannot be borrowed", () => {
    const capture = readyCapture();
    capture.origins[0]!.exportRoots[0]!.tenant.workspaceId = "foreign-workspace";
    refreshOriginHashes(capture); reseal(capture);
    expect(evaluate(capture)).toMatchObject({ status: "hold", reasons: expect.arrayContaining(["tenant_mismatch"]) });
  });

  test("two accepted bodies for one native record hold while a single loser remains auditable", () => {
    const capture = readyCapture();
    capture.origins[0]!.receiptDecisions[0]!.bodyAccepted = true;
    capture.origins[0]!.receiptDecisions[0]!.firstDigest = capture.origins[0]!.receiptDecisions[0]!.candidateDigest;
    refreshOriginHashes(capture); reseal(capture);
    expect(evaluate(capture)).toMatchObject({ status: "hold", reasons: expect.arrayContaining(["receipt_decision_conflict"]) });
  });

  test("unversioned exports and incomplete record/root coverage hold", () => {
    const unversioned = readyCapture();
    unversioned.origins[0]!.exportRoots[0]!.versioning = "Disabled";
    const oldDigest = unversioned.origins[0]!.recordMaxima[1]!.exportRootSha256;
    unversioned.origins[0]!.recordMaxima[1]!.exportRootSha256 = frontierExportRootSha256(unversioned.origins[0]!.exportRoots[0]!);
    expect(oldDigest).not.toBe(unversioned.origins[0]!.recordMaxima[1]!.exportRootSha256);
    refreshOriginHashes(unversioned); reseal(unversioned);
    expect(evaluate(unversioned)).toMatchObject({ status: "hold", reasons: expect.arrayContaining(["export_not_immutable"]) });

    const missing = readyCapture();
    missing.origins[0]!.sourceRecordCount = 3;
    refreshOriginHashes(missing); reseal(missing);
    expect(evaluate(missing)).toMatchObject({ status: "hold", reasons: expect.arrayContaining(["record_coverage_incomplete"]) });
  });

  test("resealed immutable-root and receipt fabrications remain HOLD", () => {
    const literalNull = readyCapture();
    literalNull.origins[0]!.exportRoots[0]!.versionId = "null";
    const oldNullRoot = literalNull.origins[0]!.recordMaxima[1]!.exportRootSha256;
    literalNull.origins[0]!.recordMaxima[1]!.exportRootSha256 = frontierExportRootSha256(literalNull.origins[0]!.exportRoots[0]!);
    expect(oldNullRoot).not.toBe(literalNull.origins[0]!.recordMaxima[1]!.exportRootSha256);
    refreshOriginHashes(literalNull); reseal(literalNull);
    expect(evaluate(literalNull)).toMatchObject({ status: "hold", reasons: expect.arrayContaining(["export_not_immutable"]) });

    const wrongSession = readyCapture();
    wrongSession.origins[0]!.exportRoots[1]!.source = "codex";
    wrongSession.origins[0]!.exportRoots[1]!.sessionId = "unrelated-session";
    wrongSession.origins[0]!.recordMaxima[0]!.exportRootSha256 = frontierExportRootSha256(wrongSession.origins[0]!.exportRoots[1]!);
    refreshOriginHashes(wrongSession); reseal(wrongSession);
    expect(evaluate(wrongSession)).toMatchObject({ status: "hold", reasons: expect.arrayContaining(["record_coverage_incomplete"]) });

    const foreignNamespace = readyCapture();
    const trustedBindings = bindingsFor(foreignNamespace);
    foreignNamespace.origins[0]!.exportRoots[1]!.keyRoot = "exports/foreign-org";
    foreignNamespace.origins[0]!.exportRoots[1]!.manifestKey = "exports/foreign-org/legacy/manifest.json";
    foreignNamespace.origins[0]!.recordMaxima[0]!.exportRootSha256 = frontierExportRootSha256(foreignNamespace.origins[0]!.exportRoots[1]!);
    refreshOriginHashes(foreignNamespace); reseal(foreignNamespace);
    expect(evaluate(foreignNamespace, NOW, trustedBindings)).toMatchObject({ status: "hold", reasons: expect.arrayContaining(["export_binding_missing"]) });

    const seatConflict = readyCapture();
    seatConflict.origins[0]!.receiptDecisions[0]!.seat = "999";
    refreshOriginHashes(seatConflict); reseal(seatConflict);
    expect(evaluate(seatConflict)).toMatchObject({ status: "hold", reasons: expect.arrayContaining(["receipt_decision_conflict"]) });
  });

  test("capture JSON without trusted export bindings is never READY", () => {
    const capture = readyCapture();
    expect(evaluateCanonicalFrontier(capture, { now: NOW })).toMatchObject({
      status: "hold",
      reasons: expect.arrayContaining(["export_binding_missing"]),
    });
  });

  test("capture content hash prevents post-capture mutation", () => {
    const capture = readyCapture();
    capture.target.bucket = "mutated-after-capture";
    expect(evaluate(capture)).toMatchObject({ status: "hold", reasons: expect.arrayContaining(["scan_hash_mismatch"]) });
  });

  test("omitted terminal page, origin, root, maximum, or receipt seat always refuses", () => {
    const cases: Array<[string, (capture: CanonicalFrontierCapture) => void, string]> = [
      ["page", capture => { capture.inventoryScan.pageItemCounts.pop(); capture.inventoryScan.pagesRead--; }, "scan_incomplete"],
      ["origin", capture => { capture.origins = []; }, "origin_coverage_incomplete"],
      ["root", capture => { capture.origins[0]!.exportRoots = []; refreshOriginHashes(capture); }, "record_coverage_incomplete"],
      ["maximum", capture => { capture.origins[0]!.recordMaxima.pop(); refreshOriginHashes(capture); }, "record_coverage_incomplete"],
      ["seat", capture => { capture.origins[0]!.receiptDecisions = []; refreshOriginHashes(capture); }, "receipt_decision_conflict"],
    ];
    for (const [, mutate, reason] of cases) {
      const capture = readyCapture(); mutate(capture); reseal(capture);
      expect(evaluateCanonicalFrontier(capture, { now: NOW })).toMatchObject({ status: "hold", reasons: expect.arrayContaining([reason]) });
    }
  });

  test("resealed self-consistent root and seat fabrications never become ready", () => {
    const cases: Array<[(capture: CanonicalFrontierCapture) => void, string]> = [
      [capture => { capture.origins[0]!.exportRoots[0]!.versionId = "null"; }, "export_not_immutable"],
      [capture => {
        const origin = capture.origins[0]!, root = origin.exportRoots.find(item => item.kind === "legacy-export")!;
        root.source = "unrelated"; root.sessionId = "unrelated-session";
        const maximum = origin.recordMaxima.find(item => item.authority === "seed-proof")!;
        maximum.exportRootSha256 = frontierExportRootSha256(root); maximum.exportEvidenceSha256 = frontierRecordExportEvidenceSha256(maximum);
      }, "record_coverage_incomplete"],
      [capture => {
        const origin = capture.origins[0]!, root = origin.exportRoots.find(item => item.kind === "legacy-export")!;
        root.keyRoot = "exports/foreign-org"; root.manifestKey = "exports/foreign-org/legacy/manifest.json";
        const maximum = origin.recordMaxima.find(item => item.authority === "seed-proof")!;
        maximum.exportRootSha256 = frontierExportRootSha256(root); maximum.exportEvidenceSha256 = frontierRecordExportEvidenceSha256(maximum);
      }, "export_not_immutable"],
      [capture => {
        const origin = capture.origins[0]!, root = origin.exportRoots.find(item => item.kind === "buffered-stage")!;
        root.keyRoot = "96bulls/org-canary/buffered-batches-foreign"; root.manifestKey = `${root.keyRoot}/stage.json.zst`;
        const maximum = origin.recordMaxima.find(item => item.authority === "receipt-seat")!;
        maximum.exportRootSha256 = frontierExportRootSha256(root); maximum.exportEvidenceSha256 = frontierRecordExportEvidenceSha256(maximum);
      }, "export_not_immutable"],
      [capture => { capture.origins[0]!.receiptDecisions.find(item => !item.bodyAccepted)!.seat = "999"; }, "receipt_decision_conflict"],
    ];
    for (const [mutate, reason] of cases) {
      const capture = readyCapture(); mutate(capture); refreshOriginHashes(capture); reseal(capture);
      expect(evaluateCanonicalFrontier(capture, { now: NOW })).toMatchObject({ status: "hold", reasons: expect.arrayContaining([reason]) });
    }
  });

  test("malformed nested JSON and prototype-named keys hold without crashing or changing canonical hashes", () => {
    const malformed = readyCapture() as unknown as { captureId: string; origins: Array<{ tenant: null }> };
    malformed.origins[0]!.tenant = null;
    expect(evaluateCanonicalFrontier(malformed as never, { now: NOW })).toMatchObject({ status: "hold", reasons: ["capture_invalid"] });
    expect(frontierSha256(JSON.parse('{"__proto__":{"polluted":true},"constructor":"value"}'))).toMatch(/^[a-f0-9]{64}$/);
    expect(({} as { polluted?: boolean }).polluted).toBeUndefined();
  });

  test("offline CLI evaluates only a local capture and exposes no apply mode", () => {
    const directory = mkdtempSync(join(tmpdir(), "frontier-cli-"));
    try {
      const evidence = join(directory, "capture.json");
      const capture = readyCapture();
      writeFileSync(evidence, JSON.stringify({ capture, exportBindings: bindingsFor(capture) }));
      const script = join(import.meta.dir, "../../../scripts/canonical-frontier-plan.ts");
      const ready = spawnSync(process.execPath, ["--no-env-file", script, "--evidence", evidence, "--now", NOW.toISOString()], { encoding: "utf8" });
      expect(ready.status).toBe(0);
      expect(JSON.parse(ready.stdout)).toMatchObject({ status: "ready", qualification: "OFFLINE_ONLY", protocol: "canonical-publication-frontier-plan-v1" });
      const refused = spawnSync(process.execPath, ["--no-env-file", script, "--evidence", evidence, "--now", NOW.toISOString(), "--apply"], { encoding: "utf8" });
      expect(refused.status).toBe(1);
      expect(JSON.parse(refused.stderr)).toMatchObject({ status: "error", code: "frontier_plan_usage" });
    } finally { rmSync(directory, { recursive: true, force: true }); }
  });


  test("offline CLI rejects duplicate keys and resealed unexpected scope fields without echoing them", () => {
    const directory = mkdtempSync(join(tmpdir(), "frontier-untrusted-cli-"));
    try {
      const capture = readyCapture();
      const bundle = { capture, exportBindings: bindingsFor(capture) };
      const script = join(import.meta.dir, "../../../scripts/canonical-frontier-plan.ts");
      const evidence = join(directory, "capture.json");
      const run = () => spawnSync(process.execPath, ["--no-env-file", script, "--evidence", evidence, "--now", NOW.toISOString()], { encoding: "utf8" });
      const raw = JSON.stringify(bundle);
      writeFileSync(evidence, '{"capture":null,' + raw.slice(1));
      const duplicate = run();
      expect(duplicate.status).toBe(1);
      expect(JSON.parse(duplicate.stderr)).toEqual({ status: "error", code: "frontier_plan_evidence_invalid" });
      Object.assign(capture.scope, { unreviewed: "PRIVATE-SCOPE-SENTINEL" }); reseal(capture);
      writeFileSync(evidence, JSON.stringify(bundle));
      const extra = run();
      expect(extra.status).toBe(1);
      expect(extra.stdout + extra.stderr).not.toContain("PRIVATE-SCOPE-SENTINEL");
      writeFileSync(evidence, Buffer.from([0xff]));
      expect(run().status).toBe(1);
      rmSync(evidence);
      const absent = run();
      expect(absent.status).toBe(1);
      expect(absent.stderr).not.toContain(evidence);
    } finally { rmSync(directory, { recursive: true, force: true }); }
  });

  test("offline file reader refuses symlinks and oversize files before materializing input", async () => {
    const directory = mkdtempSync(join(tmpdir(), "frontier-file-bounds-"));
    try {
      const file = join(directory, "capture.json"), link = join(directory, "linked.json");
      writeFileSync(file, "{}"); symlinkSync(file, link);
      await expect(readFrontierBundle(link)).rejects.toBeDefined();
      const fifo = join(directory, "input.fifo");
      expect(spawnSync("mkfifo", [fifo]).status).toBe(0);
      await expect(readFrontierBundle(fifo)).rejects.toMatchObject({ code: "frontier_plan_evidence_invalid" });
      truncateSync(file, MAX_FRONTIER_BUNDLE_BYTES + 1);
      await expect(readFrontierBundle(file)).rejects.toMatchObject({ code: "frontier_plan_evidence_invalid" });
    } finally { rmSync(directory, { recursive: true, force: true }); }
  });

  test("strict JSON handles escaped duplicate keys, prototype keys and structural limits", () => {
    expect(() => parseFrontierJson('{"a":1,"\\u0061":2}')).toThrow();
    expect(() => parseFrontierJson('{"x":{"a":1,"a":2}}')).toThrow();
    expect(() => parseFrontierJson('['.repeat(66) + '0' + ']'.repeat(66))).toThrow();
    expect(() => parseFrontierJson('{"n":9007199254740992}')).toThrow();
    expect(() => parseFrontierJson('{"n":NaN}')).toThrow();
    expect(() => parseFrontierJson('[1,]')).toThrow();
    expect(() => parseFrontierJson('{"a":1,}')).toThrow();
    expect(() => parseFrontierJson('true false')).toThrow();
    const value = parseFrontierJson('{"__proto__":{"polluted":true},"escaped":"a\\\"b"}') as Record<string, unknown>;
    expect(Object.getPrototypeOf(value)).toBeNull();
    expect(value.escaped).toBe('a"b');
    expect(({} as { polluted?: boolean }).polluted).toBeUndefined();
  });

  test("live-capable capture runner accepts credentials only through bounded stdin", () => {
    const directory = mkdtempSync(join(tmpdir(), "frontier-capture-cli-"));
    try {
      const script = join(import.meta.dir, "../../../scripts/capture-canonical-frontier.ts");
      const argvSecret = spawnSync(process.execPath, ["--no-env-file", script, "--source-url", "postgresql://user:secret@example.invalid/db"], { encoding: "utf8", input: "{}" });
      expect(argvSecret.status).toBe(1);
      expect(JSON.parse(argvSecret.stderr)).toMatchObject({ code: "frontier_capture_usage" });
      expect(argvSecret.stderr).not.toContain("secret");
      const malformed = spawnSync(process.execPath, ["--no-env-file", script, "--output", join(directory, "capture.json"), "--cache-dir", join(directory, "cache")], { encoding: "utf8", input: JSON.stringify({ protocol: "fixture" }) });
      expect(malformed.status).toBe(1);
      expect(JSON.parse(malformed.stderr)).toMatchObject({ code: "frontier_capture_config_shape" });
    } finally { rmSync(directory, { recursive: true, force: true }); }
  });
});
