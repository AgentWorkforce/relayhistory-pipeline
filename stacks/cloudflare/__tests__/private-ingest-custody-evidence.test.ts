import { test, expect } from "bun:test";
import { createHash } from "node:crypto";
import { mkdtempSync, mkdirSync, writeFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { spawnSync } from "node:child_process";
import { fileURLToPath } from "node:url";
import { privateIngestProfileForPhase, PRIVATE_INGEST_RELEASE as P } from "../../../src/session-ingest/private-ingest-release.ts";
import { sessionIngestVars, TRANSCRIPT_BUCKET } from "../session-ingest.ts";
import {
  REVIEWED_CUSTODY_SHADOW_MATCH,
  custodyEvidenceText,
  custodyEvidenceConfirmationSuffix,
  requireCustodyEvidenceText,
  requireCustodyShadowMatch,
  requireRelayhistoryWitness,
  type ShadowMatchReceipt,
  type ReviewedShadowMatch,
} from "../../../src/session-ingest/private-ingest-custody-evidence.ts";

const now = Date.parse("2026-11-11T01:00:00.000Z");
const live = {
  phase: "step-0-shadow",
  deploymentId: "11111111-1111-1111-1111-111111111111",
  versionId: "22222222-2222-2222-2222-222222222222",
  createdOn: "2026-11-11T00:00:00.000Z",
};
const neonRuntime = { name: "NEON_APP_DATABASE_URL", type: "secret_text" };
function fixture(change: Partial<ShadowMatchReceipt> = {}): ReviewedShadowMatch {
  const receipt: ShadowMatchReceipt = {
    version: 1, kind: "custody-shadow-match", generation: "a".repeat(64),
    deploymentId: live.deploymentId, versionId: live.versionId,
    observedFrom: "2026-11-11T00:01:00.000Z", observedThrough: "2026-11-11T00:11:00.000Z",
    batchMatches: 3, controlMatches: 2, mismatches: 0, errors: 0,
    copySealReceiptSha256: "b".repeat(64), mirrorReceiptSha256: "c".repeat(64), ...change,
  };
  return { receipt, receiptSha256: createHash("sha256").update(JSON.stringify(receipt)).digest("hex") };
}

test("no actual receipt is qualified; the source constant is dormant and the first frozen promotion refuses", () => {
  expect(REVIEWED_CUSTODY_SHADOW_MATCH).toBeNull();
  expect(custodyEvidenceConfirmationSuffix()).toBe("");
  expect(() => requireCustodyShadowMatch(live, "a".repeat(64), undefined, now))
    .toThrow("private_ingest_shadow_receipt_missing");
  const text = custodyEvidenceText();
  expect(JSON.parse(text)).toEqual({ version: 1, shadowMatch: null });
  expect(() => requireCustodyEvidenceText(text)).not.toThrow();
  expect(() => requireCustodyEvidenceText(text + " ")).toThrow("private_ingest_custody_evidence_drift");
  expect(() => requireCustodyEvidenceText(custodyEvidenceText(fixture()))).toThrow("private_ingest_custody_evidence_drift");
});

test("fresh match receipt binds generation, exact live shadow deployment/version and typed confirmation digest", () => {
  const f = fixture();
  expect(requireCustodyShadowMatch(live, "a".repeat(64), f, now)).toBe(f.receiptSha256);
  expect(custodyEvidenceConfirmationSuffix(f)).toBe(` SHADOW ${f.receiptSha256}`);
  expect(JSON.parse(custodyEvidenceText(f)).shadowMatch).toEqual(f);
  for (const changed of [
    { ...live, phase: "baseline-drain" }, { ...live, phase: "step-1-custody" },
    { ...live, deploymentId: "33333333-3333-3333-3333-333333333333" },
    { ...live, versionId: "44444444-4444-4444-4444-444444444444" },
  ]) expect(() => requireCustodyShadowMatch(changed, "a".repeat(64), f, now))
    .toThrow("private_ingest_shadow_receipt_predecessor");
  expect(() => requireCustodyShadowMatch(live, "d".repeat(64), f, now))
    .toThrow("private_ingest_shadow_receipt_predecessor");
});

test("stale, future, short and pre-deployment evidence refuses even with a valid digest", () => {
  for (const [l, f, at] of [
    [live, fixture(), now + 24 * 60 * 60_000],
    [live, fixture(), Date.parse("2026-11-11T00:10:00.000Z")],
    [live, fixture({ observedThrough: "2026-11-11T00:02:00.000Z" }), now],
    [{ ...live, createdOn: "2026-11-11T00:02:00.000Z" }, fixture(), now],
    [{ ...live, createdOn: null }, fixture(), now],
  ] as const) expect(() => requireCustodyShadowMatch(l, "a".repeat(64), f, at))
    .toThrow("private_ingest_shadow_receipt_stale");
});

test("receipt validation excludes failed comparisons, raw tenant fields and forged digests", () => {
  for (const change of [
    { batchMatches: 0 }, { controlMatches: 0 }, { batchMatches: 1.5 },
    { mismatches: 1 }, { errors: 1 }, { kind: "mode-readback" },
    { generation: "" }, { copySealReceiptSha256: "0".repeat(64) },
    { org_id: "tenant-value" },
  ]) expect(() => custodyEvidenceText(fixture(change as any))).toThrow("private_ingest_shadow_receipt_shape");
  expect(() => custodyEvidenceText({ ...fixture(), receiptSha256: "d".repeat(64) }))
    .toThrow("private_ingest_shadow_receipt_digest");
  expect(() => custodyEvidenceText(fixture({ observedFrom: "2026-11-11T00:01:00Z" })))
    .toThrow("private_ingest_shadow_receipt_time");
});

test("rollback requires an explicit complete RHC version readback with a Neon witness mode", () => {
  for (const bindings of [[], [{ name: "DELIVERY_CUSTODY_SOURCE_MODE", type: "plain_text", text: "" }],
    [{ name: "DELIVERY_CUSTODY_SOURCE_MODE", type: "plain_text", text: "relayhistory" }]]) {
    expect(requireRelayhistoryWitness(live, { id: live.versionId, resources: { bindings: [neonRuntime, ...bindings] } }))
      .toEqual({ deploymentId: live.deploymentId, versionId: live.versionId, mode: "relayhistory" });
  }
  for (const binding of [
    { name: "DELIVERY_CUSTODY_SOURCE_MODE", type: "plain_text", text: "frozen-supabase" },
    { name: "DELIVERY_CUSTODY_SOURCE_MODE", type: "plain_text", text: "supabase-authority" },
    { name: "DELIVERY_CUSTODY_SOURCE_MODE", type: "plain_text", text: "unknown" },
    { name: "DELIVERY_CUSTODY_SOURCE_MODE", type: "secret_text" },
  ]) expect(() => requireRelayhistoryWitness(live, { id: live.versionId, resources: { bindings: [neonRuntime, binding] } }))
    .toThrow("private_ingest_rollback_witness_mode");
  expect(() => requireRelayhistoryWitness(live, { id: live.versionId }))
    .toThrow("private_ingest_rollback_witness_readback");
  expect(() => requireRelayhistoryWitness(live, { id: "33333333-3333-3333-3333-333333333333", resources: { bindings: [] } }))
    .toThrow("private_ingest_rollback_witness_readback");
  expect(() => requireRelayhistoryWitness(live, { id: live.versionId, resources: { bindings: [] } }))
    .toThrow("private_ingest_rollback_witness_runtime_binding");
  const b = { name: "DELIVERY_CUSTODY_SOURCE_MODE", type: "plain_text", text: "" };
  expect(() => requireRelayhistoryWitness(live, { id: live.versionId, resources: { bindings: [neonRuntime, b, b] } }))
    .toThrow("private_ingest_rollback_witness_mode");
});

const kms = `arn:aws:kms:us-east-1:${TRANSCRIPT_BUCKET.split("-").at(-1)}:key/00000000-0000-0000-0000-000000000000`;
function ingestVersion(phase: "step-0-shadow" | "step-1-custody") {
  const input = privateIngestProfileForPhase(phase, kms);
  const bindings: any[] = Object.entries(sessionIngestVars(input.sessionIngest))
    .map(([name, text]) => ({ name, type: "plain_text", text }));
  bindings.push(
    { name: "HYPERDRIVE", type: "hyperdrive", id: input.sessionIngest.hyperdriveId },
    { name: "INGEST_FANOUT", type: "queue", queue_name: "96bulls-session-ingest-fanout" },
    { name: "INGEST_ADMISSION", type: "queue", queue_name: "96bulls-session-ingest-admission" },
    ...[["S3_ACCESS_KEY_ID", "s3-access-key-id"], ["S3_SECRET_ACCESS_KEY", "s3-secret-access-key"],
      ["INGEST_SIGNING_KEY", "signing-key"]].map(([name, suffix]) => ({ name, type: "secrets_store_secret",
      store_id: input.sessionIngest.secretsStoreId, secret_name: `96bulls-session-ingest-${suffix}` })),
  );
  return { id: live.versionId, resources: { bindings, script_runtime: {
    compatibility_date: "2026-10-01", compatibility_flags: ["nodejs_compat", "new_module_registry"],
  } } };
}

function ciPreflight(from: "step-0-shadow" | "step-1-custody", rhcMode: string | null, tamper = false, missingPlanEvidence = false) {
  const dir = mkdtempSync(join(tmpdir(), "custody-ci-gate-"));
  try {
    mkdirSync(join(dir, "config/cloudflare"), { recursive: true });
    const desired = privateIngestProfileForPhase(from === "step-0-shadow" ? "step-1-custody" : "step-0-shadow", kms);
    const { kmsKeyArn: _kms, ...tracked } = desired.sessionIngest;
    writeFileSync(join(dir, "config/cloudflare/private-ingest-production.json"), JSON.stringify({ sessionIngest: tracked }));
    const module = fileURLToPath(new URL("../../../scripts/private-ingest-ci.ts", import.meta.url));
    const active = { deployments: [{ id: live.deploymentId, created_on: live.createdOn,
      versions: [{ version_id: live.versionId, percentage: 100 }] }] };
    const script = `
      import { stagePrivateIngestInput, preflightPrivateIngest, approvePrivateIngestCapture } from ${JSON.stringify(module)};
      const calls = [];
      globalThis.fetch = async (url, init) => {
        if ((init?.method ?? "GET") !== "GET") throw Error("test_mutation_forbidden");
        const path = new URL(url).pathname; calls.push(path);
        let result;
        if (path.endsWith("/deployments")) result = ${JSON.stringify(active)};
        else if (path.includes("/scripts/${P.worker}/versions/")) result = ${JSON.stringify(ingestVersion(from))};
        else if (path.includes("/scripts/${P.relayhistoryWorker}/versions/")) result = ${JSON.stringify({
          id: live.versionId, resources: { bindings: rhcMode === null ? [neonRuntime] : [neonRuntime,
            { name: "DELIVERY_CUSTODY_SOURCE_MODE", type: "plain_text", text: rhcMode },
          ] },
        })};
        else throw Error("test_unexpected_request");
        return Response.json({ success: true, result });
      };
      try {
        stagePrivateIngestInput();
        if (${missingPlanEvidence}) {
          (await import("node:fs")).unlinkSync("docs/plans/cf-sessions-prod.custody-evidence.json");
          approvePrivateIngestCapture();
        }
        if (${tamper}) (await import("node:fs")).appendFileSync("docs/plans/cf-sessions-prod.custody-evidence.json", " ");
        const receipt = await preflightPrivateIngest();
        console.log(JSON.stringify({ receipt, calls }));
      } catch (e) { console.log(JSON.stringify({ error: e.code ?? e.message, calls })); }
    `;
    const result = spawnSync(process.execPath, ["--eval", script], { cwd: dir, encoding: "utf8", timeout: 40_000,
      env: { PATH: process.env.PATH, PRIVATE_INGEST_KMS_KEY_ARN: kms,
        PRIVATE_INGEST_RELEASE_MODE: "plan", CLOUDFLARE_API_TOKEN: "synthetic-test-token-only" } });
    if (result.status !== 0) throw Error(`test_child_failed:${result.status}:${result.stderr.slice(-1000)}`);
    return JSON.parse(result.stdout.trim().split("\n").at(-1)!);
  } finally { rmSync(dir, { recursive: true, force: true }); }
}

test("actual CI preflight refuses mode-only frozen promotion with no reviewed match receipt", () => {
  const result = ciPreflight("step-0-shadow", null);
  expect(result.error).toBe("private_ingest_shadow_receipt_missing");
  expect(result.calls).toHaveLength(2);
});

test("actual CI preflight refuses a capsule changed after stage or prior-plan download", () => {
  const result = ciPreflight("step-1-custody", null, true);
  expect(result.error).toBe("private_ingest_custody_evidence_drift");
  expect(result.calls).toHaveLength(2);
});

test("actual CI capture approval refuses a prior plan missing its evidence capsule", () => {
  const result = ciPreflight("step-1-custody", null, false, true);
  expect(result.error).toBe("private_ingest_custody_evidence_missing");
  expect(result.calls).toHaveLength(0);
});

test("actual CI rollback preflight checks RHC before allowing the Worker to return to shadow", () => {
  for (const mode of ["frozen-supabase", "supabase-authority"]) {
    const result = ciPreflight("step-1-custody", mode);
    expect(result.error).toBe("private_ingest_rollback_witness_mode");
    expect(result.calls).toHaveLength(4);
  }
  const result = ciPreflight("step-1-custody", null);
  expect(result.receipt).toMatchObject({ from: "step-1-custody", to: "step-0-shadow",
    rollbackWitness: { mode: "relayhistory", versionId: live.versionId } });
  expect(result.calls).toHaveLength(4);
}, 30_000);
