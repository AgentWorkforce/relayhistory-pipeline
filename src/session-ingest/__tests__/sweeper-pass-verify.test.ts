/**
 * sweeper-pass-verify (src/session-ingest/sweeper-pass-verify.ts, scripts/backlog-sweeper-verify.ts): synthetic
 * receipts written by the real writer into a scratch store and served to the production CLI through a fake S3
 * endpoint (s3GetOnly signs a real GET). Covers valid, wrong digest, stale / future, missing and unexpected fields,
 * writer provenance (absent in production, mismatch, swapped versions), pointer / receipt digest binding, the closed
 * summary on success and every refusal, and that nothing but a GET is ever made.
 */
import { afterEach, describe, expect, test } from "bun:test";
import { createHash } from "node:crypto";
import { readFileSync } from "node:fs";
import { dirname, join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { ObjectStoreError, objectKey, readAll, type ObjectStore } from "../../shared/vendor.ts";
import type { RearmSweepCounts } from "../rearm-sweeper.ts";
import { rearmScopeHash, rearmScopes } from "../rearm-sweeper.ts";
import {
  freshSweeperPass, REARM_ALL_ORGS, REARM_RECEIPT_FUTURE_SKEW_MS, rearmBucketHash, rearmPassLatestKey, rearmPassReceiptKey, writeRearmPassReceipt,
  type RearmPassReceipt, type RearmReceiptReadStore,
} from "../rearm-sweeper-receipt.ts";
import { BACKLOG_OPS_SWEEPER_PASS_MAX_AGE_MS } from "../backlog-sweeper-limits.ts";
import { BACKLOG_OPS_SWEEPER_PASS_MAX_AGE_MS as AUTHORITY_MAX_AGE } from "../backlog-authority.ts";
import { S3_GET_ONLY_MAX_BYTES, s3GetOnly } from "../s3-get-only.ts";
import {
  SWEEPER_PASS_REASONS, sweeperVerifyHash, validSweeperPassSummary, verifySweeperPass, type SweeperPassReadVersions, type SweeperPassSummary,
  type SweeperPassVerifyRequest, type WriterProvenance,
} from "../sweeper-pass-verify.ts";
import { exitCodeFor, main as cli } from "../../../scripts/backlog-sweeper-verify.ts";
import { scratchStore } from "./support.ts";

const ROOT = "96bulls";
const ORG_A = "org-a", ORG_B = "org-b";
const DIGEST = `sha256:${"b".repeat(64)}`, OTHER_DIGEST = `sha256:${"c".repeat(64)}`, SHA = "a".repeat(40);
const JOB_UID = "0f0e0d0c-0b0a-4908-8706-050403020100", POD_UID = "1a2b3c4d-5e6f-4a0b-9c1d-2e3f4a5b6c7d";
const JOB = "rearm-sweeper-29000000", POD = "rearm-sweeper-29000000-x7k2q";
const BUCKET = "transcripts", REGION = "us-east-1";
const T0 = Date.now() + 10 * 60_000;
const FIN = T0 + 5000;
const NOW = FIN + 60_000;
const MAX = BACKLOG_OPS_SWEEPER_PASS_MAX_AGE_MS;
const ENV = { AWS_ACCESS_KEY_ID: "AKIATESTONLY0000", AWS_SECRET_ACCESS_KEY: "test-only-secret" } as NodeJS.ProcessEnv;
const ARGS = ["--bucket", BUCKET, "--region", REGION];

const cleanups: (() => void)[] = [];
afterEach(() => cleanups.splice(0).forEach((f) => f()));
function fresh() { const s = scratchStore(); cleanups.push(s.done); return s.store; }

const zeroCounts = (): RearmSweepCounts => ({ listed: 0, erased: 0, superseded: 0, alreadyDone: 0, pending: 0, resetRunning: 0, reenabled: 0, fenceAbsent: 0,
  fenceUnreadable: 0, corrupt: 0, ownerMissing: 0, machineMismatch: 0, machineUnbound: 0, layoutCollision: 0, layoutScanTooLarge: 0, machineDerived: 0,
  emptyUnbound: 0, deferred: 0, gone: 0, failed: 0, wouldErase: 0, wouldSupersede: 0, versions: 0 });
const WORKLOAD = { namespace: "ingest", cronJobName: "rearm-sweeper", jobName: JOB, jobUid: JOB_UID, podName: POD, podUid: POD_UID };
const receiptFor = (org: string, over: Partial<RearmPassReceipt> = {}): RearmPassReceipt => {
  const orgs = org === REARM_ALL_ORGS ? null : [org];
  return { v: 1, kind: "rearm-sweeper-pass", org, enabled: true, dryRun: false, complete: true, fromStart: true, failures: 0, startedAtMs: T0, finishedAtMs: FIN,
    scopeHash: rearmScopeHash(rearmScopes(ROOT, orgs)), scopes: 1, keyRoot: ROOT, bucketHash: rearmBucketHash(BUCKET),
    revision: { gitSha: SHA, imageDigest: DIGEST }, workload: { ...WORKLOAD }, counts: zeroCounts(), ...over };
};
const request = (over: Partial<SweeperPassVerifyRequest> = {}): SweeperPassVerifyRequest => ({
  org: ORG_A, orgs: [ORG_A], bucket: BUCKET, ledgerBucket: null, namespace: "ingest", cronJobName: "rearm-sweeper", imageDigest: DIGEST, ...over });

async function seeded(receipt: RearmPassReceipt = receiptFor(ORG_A)) {
  const store = fresh();
  expect(await writeRearmPassReceipt(store, receipt)).toBe("written");
  return store;
}
const sha = (b: Uint8Array) => createHash("sha256").update(b).digest("hex");
const bytesOf = async (store: ObjectStore, key: string) => readAll((await store.getObject(objectKey(key))).body);
const putRaw = (store: ObjectStore, key: string, body: unknown) =>
  store.put(objectKey(key), new TextEncoder().encode(typeof body === "string" ? body : JSON.stringify(body)), { contentType: "application/json" });

/**
 * A fake S3 endpoint over a scratch store: answers signed GETs only (anything else fails the test), with an S3
 * version id per object (default: derived from its bytes; `versions` overrides), an ETag and Last-Modified.
 */
function fakeS3(store: ObjectStore, opts: { versions?: Record<string, string>; status?: number } = {}) {
  const calls: { method: string; key: string; signed: boolean; identity: boolean }[] = [];
  const fetcher = (async (input: URL | RequestInfo, init?: RequestInit) => {
    const url = new URL(String(input));
    const method = init?.method ?? "GET";
    const headers = new Headers(init?.headers);
    const key = decodeURIComponent(url.pathname.slice(1));
    calls.push({ method, key, signed: (headers.get("authorization") ?? "").startsWith("AWS4-HMAC-SHA256 "),
      identity: url.searchParams.get("response-content-encoding") === "identity" });
    expect(url.host).toBe(`${BUCKET}.s3.${REGION}.amazonaws.com`);
    if (method !== "GET") throw new Error(`fake s3: ${method} refused`);
    if (opts.status) return new Response("<Error/>", { status: opts.status });
    let got;
    try { got = await store.getObject(objectKey(key)); } catch { return new Response("<Error><Code>NoSuchKey</Code></Error>", { status: 404 }); }
    const bytes = await readAll(got.body);
    const leaf = key.split("/").at(-1)!;
    const versionId = opts.versions?.[leaf] ?? `v-${sha(bytes).slice(0, 16)}`;
    return new Response(bytes, { status: 200, headers: { etag: `"${sha(bytes).slice(0, 32)}"`, "x-amz-version-id": versionId,
      "last-modified": new Date(FIN).toUTCString(), "content-length": String(bytes.length) } });
  }) as typeof fetch;
  return { fetcher, calls };
}
const getOnly = (store: ObjectStore, opts?: Parameters<typeof fakeS3>[1]) => {
  const f = fakeS3(store, opts);
  return { store: s3GetOnly({ bucket: BUCKET, region: REGION, credentials: { accessKeyId: "AKIATESTONLY0000", secretAccessKey: "test-only-secret" }, fetch: f.fetcher }), calls: f.calls };
};

/** A synthetic trusted writer record: (versionId, sha256) pairs the sweeper wrote; another writer's pairs. */
function provenance(sweeper: { versionId: string; sha256: string }[], other: { versionId: string; sha256: string }[] = []): WriterProvenance & { seen: SweeperPassReadVersions[] } {
  const seen: SweeperPassReadVersions[] = [];
  const f = (async (read: SweeperPassReadVersions) => {
    seen.push(read);
    const verdict = (o: SweeperPassReadVersions["pointer"]) => {
      if (o.versionId === undefined) return "unverified";
      if (other.some((x) => x.versionId === o.versionId)) return "mismatch";
      const rec = sweeper.find((x) => x.versionId === o.versionId);
      if (!rec) return "unverified";
      return rec.sha256 === o.sha256 ? "verified" : "mismatch";
    };
    const a = verdict(read.pointer), b = verdict(read.receipt);
    return a === "mismatch" || b === "mismatch" ? "mismatch" : a === "verified" && b === "verified" ? "verified" : "unverified";
  }) as WriterProvenance & { seen: SweeperPassReadVersions[] };
  f.seen = seen;
  return f;
}
/** Records of what the sweeper wrote for the scope as seeded (default version ids of fakeS3). */
async function sweeperRecords(store: ObjectStore, scope = ORG_A) {
  const p = await bytesOf(store, rearmPassLatestKey(ROOT, scope)), r = await bytesOf(store, rearmPassReceiptKey(ROOT, scope, T0));
  return [{ versionId: `v-${sha(p).slice(0, 16)}`, sha256: sha(p) }, { versionId: `v-${sha(r).slice(0, 16)}`, sha256: sha(r) }];
}

const verify = async (store: ObjectStore, q: unknown = request(), now = NOW, prov?: WriterProvenance) => {
  const s = await verifySweeperPass(getOnly(store).store, q, now, prov, ROOT);
  expect(validSweeperPassSummary(s)).toBe(true);
  return s;
};
const reason = async (...a: Parameters<typeof verify>) => (await verify(...a)).reason;

/** Every raw identifier the synthetic inputs carry: none may appear in a summary. */
const RAW = [ORG_A, ORG_B, BUCKET, "ingest", "rearm-sweeper", JOB, POD, JOB_UID, POD_UID, "b".repeat(64), SHA, ROOT, ".ledger", "AKIATESTONLY0000", "v-"];
const noRaw = (s: SweeperPassSummary) => {
  const text = JSON.stringify({ ...s, mode: undefined });
  for (const raw of RAW) expect({ raw, leaked: text.includes(raw) }).toEqual({ raw, leaked: false });
};

describe("sweeper-pass-verify: valid", () => {
  test("production: a real pass meeting every check is refused writer_unverified (no trusted writer evidence)", async () => {
    const store = await seeded();
    const s = await verify(store);
    expect(s).toMatchObject({ v: 1, mode: "sweeper-pass-verify", verdict: "refused", reason: "writer_unverified", age_ms: 60_000, max_age_ms: MAX });
    expect(s.checks).toEqual({ scope: "ok", homes: "ok", namespace: "ok", cron_job: "ok", image_digest: "ok", git_sha: "ok", job: "ok", pod: "ok",
      workload: "ok", match: "ok", fresh: "ok", writer: "unchecked" });
    expect(exitCodeFor(s)).toBe(1);
    noRaw(s);
  });

  test("with trusted writer evidence for the exact versions read it verifies; the summary binds both objects", async () => {
    const store = await seeded();
    const prov = provenance(await sweeperRecords(store));
    const s = await verify(store, request(), NOW, prov);
    expect(s.verdict).toBe("verified");
    expect(s.reason).toBe("ok");
    expect(exitCodeFor(s)).toBe(0);
    const p = await bytesOf(store, rearmPassLatestKey(ROOT, ORG_A)), r = await bytesOf(store, rearmPassReceiptKey(ROOT, ORG_A, T0));
    expect(s.latest_sha256).toBe(sha(p));
    expect(s.receipt_sha256).toBe(sha(r));
    // The provider saw exactly the versions and bytes read, LastModified from the same responses.
    expect(prov.seen).toHaveLength(1);
    expect(prov.seen[0]!.pointer).toMatchObject({ versionId: `v-${sha(p).slice(0, 16)}`, sha256: sha(p), lastModifiedMs: Math.floor(FIN / 1000) * 1000 });
    expect(prov.seen[0]!.receipt).toMatchObject({ versionId: `v-${sha(r).slice(0, 16)}`, sha256: sha(r) });
    expect(Object.isFrozen(prov.seen[0]!.pointer)).toBe(true);
    expect(s.latest_version_hash).toMatch(/^[0-9a-f]{32}$/);
    expect(s.latest_version_hash).not.toBe(s.receipt_version_hash);
    expect(s.scope_hash).toBe(sweeperVerifyHash("scope", ORG_A));
    noRaw(s);
    // freshSweeperPass (authority-execute's reader) agrees on the same store.
    expect(await freshSweeperPass(store, ROOT, ORG_A, NOW, MAX, { orgs: [ORG_A], bucket: BUCKET, ledgerBucket: null,
      writer: { namespace: "ingest", cronJobName: "rearm-sweeper" }, revisions: { imageDigests: [DIGEST] } })).toMatchObject({ ok: true });
  });

  test("an every-org sweeper reads .all; pins on Job / Pod / git sha must be equal", async () => {
    const all = await seeded(receiptFor(REARM_ALL_ORGS));
    expect(await reason(all, request({ orgs: null }), NOW, provenance(await sweeperRecords(all, REARM_ALL_ORGS)))).toBe("ok");
    const store = await seeded();
    const prov = provenance(await sweeperRecords(store));
    expect(await reason(store, request({ jobName: JOB, jobUid: JOB_UID, podName: POD, podUid: POD_UID, gitSha: SHA }), NOW, prov)).toBe("ok");
    expect(await reason(store, request({ jobUid: POD_UID }), NOW, prov)).toBe("job_identity_mismatch");
    expect(await reason(store, request({ jobName: "rearm-sweeper-1" }), NOW, prov)).toBe("job_identity_mismatch");
    expect(await reason(store, request({ podUid: "2a2b3c4d-5e6f-4a0b-9c1d-2e3f4a5b6c7d" }), NOW, prov)).toBe("pod_identity_mismatch");
    expect(await reason(store, request({ podName: `${JOB}-other` }), NOW, prov)).toBe("pod_identity_mismatch");
    expect(await reason(store, request({ gitSha: "d".repeat(40) }), NOW, prov)).toBe("git_sha_mismatch");
  });

  test("the window is authority-execute's own constant; the request cannot widen it", async () => {
    expect(MAX).toBe(AUTHORITY_MAX_AGE);
    expect(MAX).toBe(6 * 60 * 60 * 1000);
    expect(await reason(await seeded(), { ...request(), maxAgeMs: 7 * 24 * 3600_000 }, FIN + MAX + 1)).toBe("invalid_request");
  });
});

describe("sweeper-pass-verify: wrong digest and serving identity", () => {
  test("the image digest must be present and equal the ONE trusted digest; git sha alone never suffices", async () => {
    expect(await reason(await seeded(), request({ imageDigest: OTHER_DIGEST, gitSha: SHA }))).toBe("image_digest_mismatch");
    expect(await reason(await seeded(receiptFor(ORG_A, { revision: { gitSha: SHA } })), request({ gitSha: SHA }))).toBe("image_digest_mismatch");
    expect(await reason(await seeded(receiptFor(ORG_A, { revision: { gitSha: SHA, imageDigest: OTHER_DIGEST } })))).toBe("image_digest_mismatch");
    expect(await reason(await seeded(), { ...request(), imageDigest: undefined, gitShas: [SHA] })).toBe("invalid_request");
  });

  test("namespace, CronJob, homes and scope must be the serving sweeper's", async () => {
    const store = await seeded();
    expect(await reason(store, request({ namespace: "other" }))).toBe("namespace_mismatch");
    expect(await reason(store, request({ cronJobName: "other-sweeper" }))).toBe("cronjob_mismatch");
    expect(await reason(store, request({ bucket: "other-bucket" }))).toBe("home_mismatch");
    expect(await reason(store, request({ ledgerBucket: "ledger-home" }))).toBe("home_mismatch");
    expect(await reason(store, request({ orgs: [ORG_A, ORG_B] }))).toBe("scope_mismatch");
    expect(await reason(store, request({ orgs: [ORG_B] }))).toBe("out_of_scope");
    expect(await reason(store, request({ orgs: null }))).toBe("missing");
  });

  test("a Job of another CronJob, a Pod of another Job, or a Pod sharing the Job's UID is refused", async () => {
    expect(await reason(await seeded(receiptFor(ORG_A, { workload: { ...WORKLOAD, jobName: "other-29000000" } })))).toBe("job_identity_invalid");
    expect(await reason(await seeded(receiptFor(ORG_A, { workload: { ...WORKLOAD, podName: "rearm-sweeper-1-x7k2q" } })))).toBe("pod_identity_invalid");
    expect(await reason(await seeded(receiptFor(ORG_A, { workload: { ...WORKLOAD, podUid: JOB_UID } })))).toBe("pod_identity_invalid");
  });
});

describe("sweeper-pass-verify: freshness", () => {
  test("stale after BACKLOG_OPS_SWEEPER_PASS_MAX_AGE_MS; future beyond REARM_RECEIPT_FUTURE_SKEW_MS (the verifier's clock)", async () => {
    const store = await seeded();
    const prov = provenance(await sweeperRecords(store));
    expect(await reason(store, request(), FIN + MAX, prov)).toBe("ok");
    const stale = await verify(store, request(), FIN + MAX + 1, prov);
    expect(stale).toMatchObject({ reason: "stale", age_ms: MAX + 1 });
    expect(stale.checks.fresh).toBe("fail");
    expect(await reason(store, request(), FIN - REARM_RECEIPT_FUTURE_SKEW_MS, prov)).toBe("ok");
    expect(await reason(store, request(), FIN - REARM_RECEIPT_FUTURE_SKEW_MS - 1, prov)).toBe("future");
  });
});

describe("sweeper-pass-verify: missing fields", () => {
  test("a missing Job or Pod name or UID, namespace or CronJob refuses", async () => {
    const drop = (k: keyof typeof WORKLOAD) => { const w: Record<string, string> = { ...WORKLOAD }; delete w[k]; return receiptFor(ORG_A, { workload: w }); };
    const cases: [keyof typeof WORKLOAD, string][] = [["jobName", "job_identity_invalid"], ["jobUid", "job_identity_invalid"], ["podName", "pod_identity_invalid"],
      ["podUid", "pod_identity_invalid"], ["namespace", "namespace_mismatch"], ["cronJobName", "cronjob_mismatch"]];
    for (const [k, code] of cases) expect({ k, got: await reason(await seeded(drop(k))) }).toEqual({ k, got: code });
  });

  test("no pointer is missing; a pointer naming no receipt is malformed", async () => {
    expect(await reason(fresh())).toBe("missing");
    const store = await seeded();
    await store.delete(objectKey(rearmPassReceiptKey(ROOT, ORG_A, T0)));
    expect(await reason(store)).toBe("malformed");
  });

  test("a request missing any required key is invalid, and nothing is read", async () => {
    const store = await seeded();
    for (const k of ["org", "orgs", "bucket", "ledgerBucket", "namespace", "cronJobName", "imageDigest"] as const) {
      const q: Record<string, unknown> = { ...request() };
      delete q[k];
      const g = getOnly(store);
      const s = await verifySweeperPass(g.store, q, NOW, undefined, ROOT);
      expect({ k, got: s.reason, reads: g.calls.length }).toEqual({ k, got: "invalid_request", reads: 0 });
    }
  });
});

describe("sweeper-pass-verify: unexpected fields", () => {
  test("an unexpected key in the receipt, its workload or revision, or the pointer is malformed", async () => {
    const extra = [receiptFor(ORG_A, { live: true } as never), receiptFor(ORG_A, { workload: { ...WORKLOAD, nodeName: "n1" } as never }),
      receiptFor(ORG_A, { revision: { gitSha: SHA, imageDigest: DIGEST, tag: "latest" } as never })];
    for (const r of extra) {
      const store = fresh();
      // The writer refuses these itself: put the bytes directly, as a foreign writer would.
      await putRaw(store, rearmPassReceiptKey(ROOT, ORG_A, T0), r);
      const rb = await bytesOf(store, rearmPassReceiptKey(ROOT, ORG_A, T0));
      await putRaw(store, rearmPassLatestKey(ROOT, ORG_A), { v: 1, kind: "rearm-sweeper-latest", org: ORG_A, startedAtMs: T0, finishedAtMs: FIN, receiptSha256: sha(rb) });
      expect(await reason(store)).toBe("malformed");
    }
    const store = await seeded();
    const pointer = JSON.parse(new TextDecoder().decode(await bytesOf(store, rearmPassLatestKey(ROOT, ORG_A))));
    await putRaw(store, rearmPassLatestKey(ROOT, ORG_A), { ...pointer, live: true });
    expect(await reason(store)).toBe("malformed");
  });

  test("a Lambda function name on a CronJob pass is unexpected", async () => {
    expect(await reason(await seeded(receiptFor(ORG_A, { workload: { ...WORKLOAD, lambdaFunction: "rearm" } })))).toBe("workload_unexpected");
  });

  test("an unexpected request key (no provider, window or writer option can be supplied), or a malformed value, is invalid", async () => {
    const store = await seeded();
    const bad: unknown[] = [{ ...request(), writerProvenance: "trust-me" }, { ...request(), writer: { verified: true } }, { ...request(), maxAgeMs: 1 },
      { ...request(), imageDigests: [DIGEST] }, request({ imageDigest: "b".repeat(64) }), request({ podUid: "not-a-uuid" }), request({ orgs: [] }),
      request({ ledgerBucket: BUCKET }), request({ org: ".all" }), null, [], "x"];
    for (const q of bad) expect({ q, got: await reason(store, q) }).toEqual({ q, got: "invalid_request" });
  });
});

describe("sweeper-pass-verify: writer provenance and digest binding", () => {
  test("another writer's version, or a record for the version with other bytes, is writer_mismatch", async () => {
    const store = await seeded();
    const [p, r] = await sweeperRecords(store);
    expect(await reason(store, request(), NOW, provenance([p!], [r!]))).toBe("writer_mismatch");
    expect(await reason(store, request(), NOW, provenance([p!, { ...r!, sha256: "0".repeat(64) }]))).toBe("writer_mismatch");
    const s = await verify(store, request(), NOW, provenance([], [p!]));
    expect(s).toMatchObject({ reason: "writer_mismatch", checks: { writer: "fail" } });
  });

  test("evidence for only one object, a provider that throws or answers junk, is writer_unverified", async () => {
    const store = await seeded();
    const [p, r] = await sweeperRecords(store);
    expect(await reason(store, request(), NOW, provenance([p!]))).toBe("writer_unverified");
    expect(await reason(store, request(), NOW, provenance([r!]))).toBe("writer_unverified");
    expect(await reason(store, request(), NOW, async () => { throw new Error("boom"); })).toBe("writer_unverified");
    expect(await reason(store, request(), NOW, (async () => "yes") as unknown as WriterProvenance)).toBe("writer_unverified");
  });

  test("swapped versions: evidence for the versions the sweeper wrote does not cover other versions read", async () => {
    const store = await seeded();
    const prov = provenance(await sweeperRecords(store));
    // The same bytes served as other versions (a re-put, a restore, a rollback): not the attested versions.
    for (const versions of [{ "latest.json": "v-swapped" }, { [`${T0}.json`]: "v-swapped" }]) {
      const g = getOnly(store, { versions });
      const s = await verifySweeperPass(g.store, request(), NOW, prov, ROOT);
      expect({ versions, got: s.reason }).toEqual({ versions, got: "writer_unverified" });
    }
    // The pointer and the receipt version ids swapped with each other: the bytes no longer match the records.
    const [p, r] = await sweeperRecords(store);
    const g = getOnly(store, { versions: { "latest.json": r!.versionId, [`${T0}.json`]: p!.versionId } });
    expect((await verifySweeperPass(g.store, request(), NOW, prov, ROOT)).reason).toBe("writer_mismatch");
    // No version id on either object: nothing to bind evidence to, so even a provider that vouches for anything is
    // not asked, and the result is writer_unverified.
    let asked = 0;
    const vouches: WriterProvenance = async () => { asked++; return "verified"; };
    for (const strip of ["latest.json", `${T0}.json`, "both"]) {
      const unversioned: RearmReceiptReadStore = { async getObject(k) {
        const got = await store.getObject(k);
        return strip === "both" || String(k).endsWith(strip) ? { ...got, meta: { ...got.meta, version: { eTag: "e" } } } : got;
      } };
      const s = await verifySweeperPass(unversioned, request(), NOW, vouches, ROOT);
      expect({ strip, reason: s.reason }).toEqual({ strip, reason: "writer_unverified" });
    }
    expect(asked).toBe(0);
    // The same provider over versioned reads does pass (the refusal above is the version binding, not the provider).
    expect((await verifySweeperPass(getOnly(store).store, request(), NOW, vouches, ROOT)).reason).toBe("ok");
    expect(asked).toBe(1);
  });

  test("pointer / receipt digest mismatch is malformed and no evidence is consulted", async () => {
    const edited = await seeded();
    await putRaw(edited, rearmPassReceiptKey(ROOT, ORG_A, T0), receiptFor(ORG_A, { finishedAtMs: FIN + 1 }));
    const pointerBad = await seeded();
    const pointer = JSON.parse(new TextDecoder().decode(await bytesOf(pointerBad, rearmPassLatestKey(ROOT, ORG_A))));
    await putRaw(pointerBad, rearmPassLatestKey(ROOT, ORG_A), { ...pointer, receiptSha256: "0".repeat(64) });
    for (const store of [edited, pointerBad]) {
      const prov = provenance([]);
      const s = await verify(store, request(), NOW, prov);
      expect(s).toMatchObject({ reason: "malformed", latest_sha256: null, receipt_sha256: null });
      expect(prov.seen).toHaveLength(0);
    }
  });
});

describe("sweeper-pass-verify: the production CLI", () => {
  const stdin = (q: unknown) => () => JSON.stringify(q) + "\n";

  test("end to end through s3GetOnly: signed GETs only, writer_unverified, exit 1, closed summary", async () => {
    const store = await seeded();
    const f = fakeS3(store);
    const s = await cli(ARGS, ENV, stdin(request()), { fetch: f.fetcher, now: () => NOW });
    expect(s).toMatchObject({ verdict: "refused", reason: "writer_unverified" });
    expect(validSweeperPassSummary(s)).toBe(true);
    expect(exitCodeFor(s)).toBe(1);
    noRaw(s);
    expect(f.calls.map((c) => [c.method, c.key.split("/").at(-1), c.signed, c.identity])).toEqual([["GET", "latest.json", true, true], ["GET", `${T0}.json`, true, true]]);
  });

  test("every refusal is the same closed summary; usage, input and credential errors make no request", async () => {
    const store = await seeded();
    const cases: [string[], NodeJS.ProcessEnv, () => string, string][] = [
      [["--bucket", BUCKET], ENV, stdin(request()), "usage"],
      [[...ARGS, "--org", ORG_A], ENV, stdin(request()), "usage"],
      [["--bucket", "Bad_Bucket", "--region", REGION], ENV, stdin(request()), "usage"],
      [ARGS, ENV, () => "{nope", "stdin_invalid"],
      [ARGS, ENV, () => `${JSON.stringify(request())}\n${JSON.stringify(request())}\n`, "stdin_invalid"],
      [ARGS, ENV, () => "x".repeat(70_000), "stdin_too_large"],
      [ARGS, ENV, stdin({ ...request(), extra: 1 }), "invalid_request"],
      [ARGS, {}, stdin(request()), "aws_credentials_required"],
    ];
    for (const [argv, env, input, code] of cases) {
      const f = fakeS3(store);
      const s = await cli(argv, env, input, { fetch: f.fetcher, now: () => NOW });
      expect({ code, got: s.reason, valid: validSweeperPassSummary(s), exit: exitCodeFor(s), requests: f.calls.length })
        .toEqual({ code, got: code, valid: true, exit: 1, requests: 0 });
      expect(s).toMatchObject({ scope_hash: null, expectation_hash: null, latest_sha256: null, receipt_sha256: null, age_ms: null });
    }
  });

  test("403 is access_denied, 500 and network failure are store_error, with no error text in the summary", async () => {
    const store = await seeded();
    expect((await cli(ARGS, ENV, stdin(request()), { fetch: fakeS3(store, { status: 403 }).fetcher, now: () => NOW })).reason).toBe("access_denied");
    expect((await cli(ARGS, ENV, stdin(request()), { fetch: fakeS3(store, { status: 500 }).fetcher, now: () => NOW })).reason).toBe("store_error");
    const s = await cli(ARGS, ENV, stdin(request()), { fetch: (async () => { throw new Error(`connect ${BUCKET} ${ROOT}/x`); }) as unknown as typeof fetch, now: () => NOW });
    expect(s.reason).toBe("store_error");
    noRaw(s);
    const missing = await cli(ARGS, ENV, stdin(request({ org: ORG_B, orgs: [ORG_B] })), { fetch: fakeS3(store).fetcher, now: () => NOW });
    expect(missing.reason).toBe("missing");
  });

  test("the summary schema is closed: an extra key, a raw value or a verified summary with a failed check is invalid", async () => {
    const store = await seeded();
    const ok = await verify(store, request(), NOW, provenance(await sweeperRecords(store)));
    expect(validSweeperPassSummary(ok)).toBe(true);
    expect(validSweeperPassSummary({ ...ok, org: ORG_A })).toBe(false);
    expect(validSweeperPassSummary({ ...ok, scope_hash: ORG_A })).toBe(false);
    expect(validSweeperPassSummary({ ...ok, reason: "writer_unverified" })).toBe(false);
    expect(validSweeperPassSummary({ ...ok, checks: { ...ok.checks, writer: "unchecked" } })).toBe(false);
    expect(validSweeperPassSummary({ ...ok, max_age_ms: 1 })).toBe(false);
    expect(validSweeperPassSummary({ ...ok, receipt_version_hash: null })).toBe(false);
    expect(validSweeperPassSummary({ ...ok, verdict: "refused", reason: "error: bucket transcripts" })).toBe(false);
    expect(SWEEPER_PASS_REASONS.every((r) => /^[a-z_]{2,32}$/.test(r))).toBe(true);
  });
});

describe("sweeper-pass-verify: no writes, ever", () => {
  test("s3GetOnly is a frozen { getObject }: no put / delete / list / head / presignGet; only the pass-key alphabet", async () => {
    const ro = s3GetOnly({ bucket: BUCKET, region: REGION, credentials: { accessKeyId: "a", secretAccessKey: "b" }, fetch: (async () => { throw new Error("no"); }) as never }) as unknown as Record<string, unknown>;
    expect(Object.keys(ro)).toEqual(["getObject"]);
    expect(Object.isFrozen(ro)).toBe(true);
    expect(Object.getPrototypeOf(ro)).toBe(Object.prototype);
    for (const m of ["put", "delete", "list", "head", "get", "presignGet", "backend"]) expect({ m, v: ro[m] }).toEqual({ m, v: undefined });
    for (const key of ["96bulls/.ledger/rearm-sweeper/cursor-x.json", "96bulls/.ledger/rearm-sweeper/passes/../latest.json", "other/thing.json",
      "96bulls/.ledger/rearm-sweeper/passes/org-a/latest.json?x=1"]) {
      await expect((ro.getObject as RearmReceiptReadStore["getObject"])(key as never)).rejects.toMatchObject({ code: "invalid_argument" });
    }
    expect(() => s3GetOnly({ bucket: "evil.com/x", region: REGION, credentials: { accessKeyId: "a", secretAccessKey: "b" } })).toThrow();
    // A body over the cap is refused, declared or streamed, before it is read into memory.
    const key = objectKey(rearmPassLatestKey(ROOT, ORG_A));
    const big = (headers: Record<string, string>) => s3GetOnly({ bucket: BUCKET, region: REGION, credentials: { accessKeyId: "a", secretAccessKey: "b" },
      fetch: (async () => new Response(new Uint8Array(S3_GET_ONLY_MAX_BYTES + 1), { headers })) as never });
    await expect(big({ "content-length": String(S3_GET_ONLY_MAX_BYTES + 1) }).getObject(key)).rejects.toMatchObject({ code: "backend" });
    await expect(big({}).getObject(key).then((g) => readAll(g.body))).rejects.toMatchObject({ code: "backend" });
  });

  test("the verifier touches only getObject on the store it is handed", async () => {
    const store = await seeded();
    const touched: string[] = [];
    const spy = new Proxy({}, { get(_t, p) { touched.push(String(p)); const v = (store as any)[p]; return typeof v === "function" ? v.bind(store) : v; } }) as RearmReceiptReadStore;
    for (const q of [request(), request({ imageDigest: OTHER_DIGEST }), request({ orgs: null }), { bogus: 1 }]) await verifySweeperPass(spy, q, NOW, undefined, ROOT);
    expect([...new Set(touched)]).toEqual(["getObject"]);
  });

  test("import isolation: the CLI never loads the authority path and calls no store factory; no provider is wired", () => {
    const here = dirname(fileURLToPath(import.meta.url));
    const repo = resolve(here, "../../..");
    const seen = new Set<string>();
    const resolveSpec = (from: string, spec: string): string | null => {
      if (spec === "@rhc-object-store") return join(repo, "packages/object-store/src/index.ts");
      if (spec.startsWith("@rhc/")) return join(repo, "packages/pipeline/src", spec.slice(5));
      if (spec.startsWith(".")) return resolve(dirname(from), spec).replace(/\.js$/, ".ts");
      return null;
    };
    const walk = (file: string) => {
      if (seen.has(file)) return;
      seen.add(file);
      let src: string;
      try { src = readFileSync(file, "utf8"); } catch { return; }
      for (const m of src.matchAll(/^(?:import|export)\s+(?!type\b)[^;]*?from\s+"([^"]+)";/gms)) {
        const next = resolveSpec(file, m[1]!);
        if (next) walk(next);
      }
    };
    const cliFile = join(repo, "scripts/backlog-sweeper-verify.ts");
    walk(cliFile);
    const reached = [...seen].map((f) => f.slice(repo.length + 1));
    for (const forbidden of ["src/session-ingest/backlog-authority.ts", "src/session-ingest/authority-evidence.ts", "src/session-ingest/backlog-authority-schema.ts",
      "src/session-ingest/ledger-tool-store.ts", "src/session-ingest/backlog-ops.ts", "src/session-ingest/revision-reshard.ts", "scripts/backlog-authority.ts",
      "scripts/backlog-ops.ts"]) {
      expect({ forbidden, reached: reached.includes(forbidden) }).toEqual({ forbidden, reached: false });
    }
    // The CLI, the verifier and the GET-only reader themselves construct nothing that writes, and wire no provider.
    for (const f of ["scripts/backlog-sweeper-verify.ts", "src/session-ingest/sweeper-pass-verify.ts", "src/session-ingest/s3-get-only.ts"]) {
      const code = readFileSync(join(repo, f), "utf8").replace(/\/\*[\s\S]*?\*\//g, "").replace(/\/\/.*$/gm, "");
      for (const bad of [/createS3Store/, /createFsStore/, /BaseStore/, /\.put\(/, /\.delete\(/, /\.list\(/, /presign/, /ifNoneMatch/, /ifMatch/, /authorityExecute/,
        /writeRearmPassReceipt/, /method:\s*"(?!GET")/]) {
        expect({ f, bad: String(bad), hit: bad.test(code) }).toEqual({ f, bad: String(bad), hit: false });
      }
    }
    const cliCode = readFileSync(cliFile, "utf8").replace(/\/\*[\s\S]*?\*\//g, "").replace(/\/\/.*$/gm, "");
    expect(/verifySweeperPass\(store, request, \(deps\.now \?\? Date\.now\)\(\)\)/.test(cliCode)).toBe(true); // three arguments: no provider
    expect(/[Pp]rovenance/.test(cliCode)).toBe(false);
  });

  test("write-factory trap: the production CLI, run with every store factory and the authority modules trapped, fires none", () => {
    const harness = join(dirname(fileURLToPath(import.meta.url)), "fixtures/sweeper-verify-trap-harness.ts");
    const p = Bun.spawnSync([process.execPath, harness], { env: { PATH: process.env.PATH ?? "", HOME: process.env.HOME ?? "", TMPDIR: process.env.TMPDIR ?? "" },
      timeout: 60_000 });
    expect(p.exitCode).toBe(0);
    const out = JSON.parse(new TextDecoder().decode(p.stdout).trim().split("\n").at(-1)!);
    expect(out).toMatchObject({ seededOk: "written", hitsBeforeCli: [], hitsByCli: [], methods: ["GET", "GET"], armed: [true, true, true, true], authorityArmed: true });
    expect(out.summary).toMatchObject({ mode: "sweeper-pass-verify", verdict: "refused", reason: "writer_unverified" });
    expect(validSweeperPassSummary(out.summary)).toBe(true);
  });

  test("access denied from a store throws nothing out of the verifier", async () => {
    const denied: RearmReceiptReadStore = { getObject: async () => { throw new ObjectStoreError("unauthorized", "s3", "denied"); } };
    expect((await verifySweeperPass(denied, request(), NOW, undefined, ROOT)).reason).toBe("access_denied");
  });
});
