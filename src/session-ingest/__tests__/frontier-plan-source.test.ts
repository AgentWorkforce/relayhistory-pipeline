import { afterEach, describe, expect, spyOn, test } from "bun:test";
import { createHash } from "node:crypto";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { zstdDecompressSync } from "node:zlib";
import { objectStoreManifestReader } from "../../../packages/pipeline/src/lib/pipeline/adapter.ts";
import { canonicalManifestIdentity } from "../../shared/manifest-identity.ts";
import { objectKey, readAll, type ManifestV1 } from "../../shared/vendor.ts";
import {
  captureSeedScopeExportEvidence,
  createS3FrontierVersioningReader,
  FrontierSourceEvidenceError,
  type FrontierBucketVersioningReader,
} from "../frontier-plan-source.ts";
import { SeedProofError, SeedProofIndex, type ProofDeps, type SeedPointer } from "../seed-proof.ts";
import type { SeedSql } from "../seed.ts";
import { prepareDelivery } from "../vendor/delivery/prepare.ts";
import { writeBatchObjects } from "../writer.ts";
import { TENANT, batch, record, scratchStore } from "./support.ts";

const cleanup: Array<() => void> = [];
afterEach(() => cleanup.splice(0).forEach(close => close()));
const sha = (value: string) => createHash("sha256").update(value).digest("hex");

/**
 * Both layers read Date.now(): captureSeedScopeExportEvidence's checkDeadline
 * (frontier_seed_deadline) runs before/after every await, and
 * SeedProofIndex.addPointer refuses at entry (seed_proof_bound) and after the
 * manifest read (seed_proof_deadline). Real sleeps race these checks, so the
 * deadline cases freeze the wall clock and advance it only inside the
 * dependency whose layer the case targets. Each case restores its clock;
 * afterEach restores it again if an assertion threw first.
 */
function frozenClock() {
  let now = Date.now();
  const spy = spyOn(Date, "now").mockImplementation(() => now);
  cleanup.push(() => spy.mockRestore());
  return { advance(ms: number) { now += ms; }, restore() { spy.mockRestore(); } };
}
async function rejection(promise: Promise<unknown>): Promise<unknown> {
  try { await promise; } catch (error) { return error; }
  throw new Error("expected rejection");
}

async function fixture() {
  const fs = scratchStore(); cleanup.push(fs.done);
  const directory = mkdtempSync(join(tmpdir(), "frontier-source-"));
  cleanup.push(() => rmSync(directory, { recursive: true, force: true }));
  const prepared = await prepareDelivery(TENANT, await batch([record({
    record_id: "record-frontier", revision: 7, revision_id: "revision-frontier",
    payload: { text: "private fixture never enters frontier metadata", ts_ms: 1_761_955_200_000 },
  })]));
  const written = await writeBatchObjects({ store: fs.store, keyRoot: "96bulls", sleep: async () => {} }, {
    orgId: TENANT.orgId, workspaceId: TENANT.workspaceId, userId: TENANT.userId,
    originId: prepared.originId, batchDigest: prepared.batchDigest, records: prepared.records,
  });
  const stored = written.manifests[0]!;
  const raw = await readAll((await fs.store.getObject(objectKey(stored.key))).body);
  const manifest = JSON.parse(new TextDecoder().decode(raw)) as ManifestV1;
  const identity = canonicalManifestIdentity(manifest);
  const pointer: SeedPointer = {
    org_id: TENANT.orgId, workspace_id: TENANT.workspaceId, user_id: TENANT.userId,
    harness: manifest.session.harness, session_id: identity.sessionId, machine_id: identity.machineId,
    bucket: "fixture-versioned", storage_provider: "s3", manifest_key: stored.key,
    revision: String(manifest.revision), content_sha256: manifest.raw.content_sha256, raw_format: manifest.raw.format,
    capture: { pointer_id: sha(`pointer:${stored.key}`), scope_sha256: sha("scope"), reset_epoch: "0", fence_versions: {} },
  };
  const baseReader = objectStoreManifestReader(fs.store, { provider: "s3", bucket: "fixture-versioned" });
  const deps: ProofDeps = {
    store: fs.store, bucket: "fixture-versioned", provider: "s3",
    reader: { ...baseReader, async read(key) { const value = await baseReader.read(key); return value ? { ...value, version: "manifest-version-7" } : null; } },
    prefixFor: org => `96bulls/${org}`,
    decompress: (bytes, max) => new Uint8Array(zstdDecompressSync(bytes, { maxOutputLength: max })),
  };
  const capture = (fingerprint = sha("scope-capture")) => ({
    protocol: 1 as const, capture_id: null, expires_at: null, fingerprint, source_epoch: "11", pointers: [pointer],
  });
  const calls: unknown[] = [];
  let rpcCount = 0;
  const sql: SeedSql = { async query(text, params) {
    calls.push({ text, params }); rpcCount++;
    return { rows: [{ r: capture() }] } as never;
  } };
  const versioningCalls: string[] = [];
  const versioning: FrontierBucketVersioningReader = { async read(bucket) { versioningCalls.push(bucket); return "Enabled"; } };
  const input = {
    tenant: { orgId: TENANT.orgId, workspaceId: TENANT.workspaceId, userId: TENANT.userId, originId: prepared.originId },
    generation: "a".repeat(64), sourceHead: "b".repeat(40), source: pointer.harness, sessionId: pointer.session_id,
    keyRoot: `96bulls/${TENANT.orgId}`, cachePath: join(directory, "frontier.sqlite"),
    maxRecords: 10, maxEvidenceBytes: 1024 * 1024, deadlineMs: 10_000,
  };
  return { ...fs, directory, deps, pointer, capture, calls, get rpcCount() { return rpcCount; }, sql, versioning, versioningCalls, input };
}

describe("frontier seed source evidence", () => {
  test("derives closure, record counts, and the same-response manifest version", async () => {
    const f = await fixture();
    const evidence = await captureSeedScopeExportEvidence(f.sql, f.deps, f.versioning, f.input);
    expect(evidence).toMatchObject({ pointerCount: 1, recordCount: 1, sourceEpoch: "11", roots: [{
      tenant: f.input.tenant, manifestKey: f.pointer.manifest_key, versionId: "manifest-version-7",
      recordCount: 1, inventoryComplete: true, versioning: "Enabled",
    }] });
    expect(evidence.records[0]).toMatchObject({ evidence: { record_id: "record-frontier", pointer_id: f.pointer.capture.pointer_id } });
    expect(evidence.records[0]!.exportRootSha256).toMatch(/^[a-f0-9]{64}$/);
    expect(evidence.bindings).toEqual([{
      exportRootSha256: evidence.records[0]!.exportRootSha256,
      recordsSha256: expect.stringMatching(/^[a-f0-9]{64}$/),
    }]);
    expect(evidence.snapshotSha256).toMatch(/^[a-f0-9]{64}$/);
    expect(f.calls).toHaveLength(2);
    expect(f.calls.every(call => JSON.parse(((call as { params: string[] }).params[0]!)).mode === "preview")).toBe(true);
    expect(f.versioningCalls).toEqual(["fixture-versioned", "fixture-versioned"]);
  });

  test("bounds records, evidence bytes and deadline and ignores import ACK progress", async () => {
    const bounded = await fixture();
    await expect(captureSeedScopeExportEvidence(bounded.sql, bounded.deps, bounded.versioning, { ...bounded.input, maxRecords: 0 }))
      .rejects.toMatchObject({ code: "seed_proof_bound" });

    const bytes = await fixture();
    await expect(captureSeedScopeExportEvidence(bytes.sql, bytes.deps, bytes.versioning, { ...bytes.input, maxEvidenceBytes: 1 }))
      .rejects.toMatchObject({ code: "seed_proof_bound" });

    // Deadline expires during the manifest read: the outer checks before it
    // pass, so the seed-proof reader's post-read check is the layer that fires.
    const expired = await fixture();
    const reader = expired.deps.reader;
    const expiredClock = frozenClock();
    expired.deps.reader = { ...reader, async read(key) { expiredClock.advance(5); return reader.read(key); } };
    const expiredError = await rejection(captureSeedScopeExportEvidence(expired.sql, expired.deps, expired.versioning, { ...expired.input, deadlineMs: 1 }));
    expect(expiredError).toBeInstanceOf(SeedProofError);
    expect(expiredError).toMatchObject({ code: "seed_proof_deadline" });
    expiredClock.restore();

    // Deadline expires during the opening versioning read: the frontier
    // source's own checkDeadline fires before any seed-proof work starts.
    const versioningExpired = await fixture();
    const versioningClock = frozenClock();
    versioningExpired.versioning.read = async () => { versioningClock.advance(5); return "Enabled"; };
    const versioningError = await rejection(captureSeedScopeExportEvidence(versioningExpired.sql, versioningExpired.deps, versioningExpired.versioning, { ...versioningExpired.input, deadlineMs: 1 }));
    expect(versioningError).toBeInstanceOf(FrontierSourceEvidenceError);
    expect(versioningError).toMatchObject({ code: "frontier_seed_deadline" });
    versioningClock.restore();

    const resumed = await fixture();
    const first = await captureSeedScopeExportEvidence(resumed.sql, resumed.deps, resumed.versioning, resumed.input);
    const index = new SeedProofIndex(resumed.input.cachePath, resumed.input.tenant.orgId, resumed.input.sourceHead);
    const batch = [...index.evidenceBatches()][0]!;
    index.acknowledged(batch.batch_sha256, batch.records.length);
    index.close();
    const second = await captureSeedScopeExportEvidence(resumed.sql, resumed.deps, resumed.versioning, resumed.input);
    expect(second).toMatchObject({ pointerCount: first.pointerCount, recordCount: first.recordCount });
    expect(second.records).toHaveLength(1);
    expect(second.roots).toHaveLength(1);
  });

  test("source mutation, absent versions, and bucket changes fail closed", async () => {
    const raced = await fixture();
    let reads = 0;
    raced.sql.query = async () => ({ rows: [{ r: raced.capture(++reads === 1 ? sha("opening") : sha("closing")) }] }) as never;
    await expect(captureSeedScopeExportEvidence(raced.sql, raced.deps, raced.versioning, raced.input))
      .rejects.toMatchObject({ code: "frontier_seed_source_changed" });

    const missing = await fixture();
    const base = missing.deps.reader;
    missing.deps.reader = { ...base, async read(key) { const value = await base.read(key); return value ? { ...value, version: undefined } : null; } };
    await expect(captureSeedScopeExportEvidence(missing.sql, missing.deps, missing.versioning, missing.input))
      .rejects.toMatchObject({ code: "frontier_seed_manifest_version" });

    const changed = await fixture();
    let versionReads = 0;
    const status: FrontierBucketVersioningReader = { async read() { return ++versionReads === 1 ? "Enabled" : "Disabled"; } };
    await expect(captureSeedScopeExportEvidence(changed.sql, changed.deps, status, changed.input))
      .rejects.toMatchObject({ code: "frontier_seed_bucket_changed" });
  });

  test("bounded evidence enumeration ignores import ACK progress and refuses byte/deadline overflow", async () => {
    const cached = await fixture();
    const first = await captureSeedScopeExportEvidence(cached.sql, cached.deps, cached.versioning, cached.input);
    const index = new SeedProofIndex(cached.input.cachePath, cached.input.tenant.orgId, cached.input.sourceHead);
    try {
      const batch = [...index.batches()][0]!;
      index.acknowledged(batch.batch_sha256, batch.records.length);
    } finally { index.close(); }
    const second = await captureSeedScopeExportEvidence(cached.sql, cached.deps, cached.versioning, cached.input);
    expect(second.records).toEqual(first.records);
    expect(second.recordCount).toBe(1);

    const bytes = await fixture();
    await expect(captureSeedScopeExportEvidence(bytes.sql, bytes.deps, bytes.versioning, { ...bytes.input, maxEvidenceBytes: 1 }))
      .rejects.toMatchObject({ code: "seed_proof_bound" });

    // Same layer as above via the CLI's maxDurationMs: the clock only moves
    // inside the manifest read, so addPointer's entry check cannot pre-empt it.
    const deadline = await fixture();
    const reader = deadline.deps.reader;
    const clock = frozenClock();
    deadline.deps.reader = { ...reader, async read(key) { clock.advance(5); return reader.read(key); } };
    const deadlineError = await rejection(captureSeedScopeExportEvidence(deadline.sql, deadline.deps, deadline.versioning, { ...deadline.input, maxDurationMs: 1 }));
    expect(deadlineError).toBeInstanceOf(SeedProofError);
    expect(deadlineError).toMatchObject({ code: "seed_proof_deadline" });
    clock.restore();
  });
});

describe("signed S3 versioning source", () => {
  const credentials = { accessKeyId: "synthetic-access", secretAccessKey: "synthetic-secret" };
  test("parses a complete authoritative response and maps suspension", async () => {
    const bodies = [
      '<?xml version="1.0"?><VersioningConfiguration xmlns="http://s3.amazonaws.com/doc/2006-03-01/"><Status>Enabled</Status></VersioningConfiguration>',
      '<VersioningConfiguration><Status>Suspended</Status></VersioningConfiguration>',
    ];
    const seen: Array<{ url: string; authorization: string | null }> = [];
    const reader = createS3FrontierVersioningReader({ region: "us-east-1", endpoint: "http://127.0.0.1:9000", allowFixtureEndpoint: true, credentials, fetch: (async (input, init) => {
      seen.push({ url: String(input), authorization: new Headers(init?.headers).get("authorization") });
      expect(init?.redirect).toBe("error");
      return new Response(bodies.shift(), { status: 200 });
    }) as typeof fetch });
    expect(await reader.read("fixture-bucket")).toBe("Enabled");
    expect(await reader.read("fixture-bucket")).toBe("Disabled");
    expect(seen.every(item => item.url.includes("versioning=") && item.authorization?.startsWith("AWS4-HMAC-SHA256"))).toBe(true);
  });

  test("refuses truncated, ambiguous, and oversized documents", async () => {
    for (const body of [
      "<VersioningConfiguration><Status>Enabled</Status>",
      "<VersioningConfiguration><Status>Enabled</Status><Status>Suspended</Status></VersioningConfiguration>",
      `<VersioningConfiguration><Status>Enabled</Status>${" ".repeat(200)}</VersioningConfiguration>`,
    ]) {
      const reader = createS3FrontierVersioningReader({ region: "us-east-1", endpoint: "http://127.0.0.1:9000", allowFixtureEndpoint: true, credentials, maxBytes: 128,
        fetch: (async () => new Response(body, { status: 200 })) as typeof fetch });
      await expect(reader.read("fixture-bucket")).rejects.toMatchObject({ code: expect.stringContaining("frontier_versioning_") });
    }
  });

  test("pins AWS HTTPS identity and rejects non-loopback fixture endpoints and redirects", async () => {
    expect(() => createS3FrontierVersioningReader({ region: "us-east-1", endpoint: "http://minio.invalid", allowFixtureEndpoint: true, credentials })).toThrow("frontier_versioning_endpoint");
    expect(() => createS3FrontierVersioningReader({ region: "us-east-1", endpoint: "https://example.com", allowFixtureEndpoint: true, credentials })).toThrow("frontier_versioning_endpoint");
    const seen: string[] = [];
    const aws = createS3FrontierVersioningReader({ region: "us-east-1", credentials, fetch: (async input => {
      seen.push(String(input));
      return new Response("<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>", { status: 200 });
    }) as typeof fetch });
    expect(await aws.read("fixture-bucket")).toBe("Enabled");
    expect(seen).toEqual(["https://fixture-bucket.s3.us-east-1.amazonaws.com/?versioning="]);

    for (const changed of ["redirected", "identity"] as const) {
      const reader = createS3FrontierVersioningReader({ region: "us-east-1", endpoint: "http://127.0.0.1:9000", allowFixtureEndpoint: true, credentials, fetch: (async () => {
        const response = new Response("<VersioningConfiguration><Status>Enabled</Status></VersioningConfiguration>", { status: 200 });
        Object.defineProperty(response, changed === "redirected" ? "redirected" : "url", { value: changed === "redirected" ? true : "http://127.0.0.1:9001/fixture-bucket?versioning=" });
        return response;
      }) as typeof fetch });
      await expect(reader.read("fixture-bucket")).rejects.toMatchObject({ code: "frontier_versioning_identity" });
    }
  });
});
