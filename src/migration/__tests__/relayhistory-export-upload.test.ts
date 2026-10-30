/**
 * The export upload courier against a local, in-process fake ingest (node:http
 * on 127.0.0.1) that speaks #43's contract: migration-assertion admission
 * (409 migration_assertion_replayed on a spent jti or nonce), the chunk hop,
 * the manifest commit (create-only, identical retry re-enqueues the same work
 * key), the plan-digest commitment, and a fake RelayAuth mint. No real ingest,
 * RelayAuth, S3, Neon or Supabase is contacted, and no credential exists.
 */
import { afterEach, describe, expect, test } from "bun:test";
import { createHash, randomUUID } from "node:crypto";
import { createServer, type IncomingMessage, type ServerResponse } from "node:http";
import { readFileSync } from "node:fs";
import { mkdtemp, readFile, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { AddressInfo } from "node:net";
import { MigrationAssertionClient } from "../relayhistory-export-auth.ts";
import type { DeliveryRecord } from "../relayhistory-export.ts";
import { computeContentSha256, manifestKey } from "@rhc-object-store";
import {
  buildExportManifest,
  canonical,
  contentSha256,
  derivePlanContract,
  planContract,
  plannedManifestFromRecords,
  runExportUpload,
  uploadFromManifest,
  uploadFromPlanned,
  type ExportPlan,
  type ExportUploadConfig,
  type PlanContract,
  type PlanContractFn,
  type UploadIdentity,
  type UploadInput,
} from "../relayhistory-export-upload.ts";

const GRANT = "mgr_test";
const planDigest = (plan: ExportPlan) => planContract(plan, GRANT).planDigest;
const sha = (b: Uint8Array | string) => createHash("sha256").update(b).digest("hex");
const b64 = (v: unknown) => Buffer.from(JSON.stringify(v)).toString("base64url");

type Route = "chunk" | "manifest";
type Injected = { status: number; code: string } | "hang" | undefined;

interface FakeIngest {
  url: string;
  mints: number;
  /** jti and nonce of every consumed assertion (the ledger). */
  consumed: Set<string>;
  chunkWrites: number;
  manifestWrites: number;
  queue: { workKey: string; jti: string }[];
  requests: { route: Route; key: string; jti: string }[];
  /** Override the jti of the n-th mint (1-based), e.g. to hand out a spent one. */
  mintJti?: (n: number) => string | undefined;
  /** Inject a response before the route's own handling (n is per route, 1-based). */
  before?: (route: Route, n: number) => Injected;
  /** Chunks the ingest accepts per assertion (contract v1: 256). */
  chunkLimit: number;
  /** The issuer's canonical stored plan: it derives each signed identity from it. */
  storedPlan?: PlanContract;
  /** Override the upload identity the n-th mint signs (default: the stored plan's). */
  signUpload?: (n: number, derived: UploadIdentity) => UploadIdentity;
  /** The upload identity each jti signs, and every key it was presented for. */
  signed: Map<string, UploadIdentity>;
  usedFor: Map<string, Set<string>>;
  /** After the n-th manifest commit landed, "hang" drops the response. */
  afterCommit?: (n: number) => "hang" | undefined;
  signedDigest: string;
  close(): Promise<void>;
}

const servers: FakeIngest[] = [];
const dirs: string[] = [];
afterEach(async () => {
  await Promise.all(servers.splice(0).map((s) => s.close()));
  await Promise.all(dirs.splice(0).map((d) => rm(d, { recursive: true, force: true })));
});

async function body(req: IncomingMessage): Promise<Buffer> {
  const parts: Buffer[] = [];
  for await (const part of req) parts.push(part as Buffer);
  return Buffer.concat(parts);
}

async function fakeIngest(signedDigest: string): Promise<FakeIngest> {
  const chunks = new Map<string, string>();
  const manifests = new Map<string, string>();
  const counts: Record<Route, number> = { chunk: 0, manifest: 0 };
  const commits = { n: 0 };
  const jtiChunks = new Map<string, Map<string, string>>();
  const send = (res: ServerResponse, status: number, value: unknown) => {
    res.writeHead(status, { "content-type": "application/json" });
    res.end(JSON.stringify(value));
  };
  const refuse = (res: ServerResponse, status: number, code: string) => send(res, status, { error: { code, message: code } });
  const claimsOf = (req: IncomingMessage) => {
    const token = (req.headers.authorization ?? "").replace(/^Bearer /, "");
    return JSON.parse(Buffer.from(token.split(".")[1] ?? "", "base64url").toString()) as {
      jti: string;
      meta: { nonce: string; planDigest: string; grantId: string; sourceUserId: string; upload: UploadIdentity };
    };
  };
  const fake: FakeIngest = {
    url: "",
    mints: 0,
    consumed: new Set(),
    chunkWrites: 0,
    manifestWrites: 0,
    queue: [],
    requests: [],
    chunkLimit: 256,
    signed: new Map(),
    usedFor: new Map(),
    signedDigest,
    close: async () => {
      server.closeAllConnections();
      await new Promise<void>((r) => server.close(() => r()));
    },
  };
  const server = createServer((req, res) => {
    void (async () => {
      const url = new URL(req.url ?? "/", "http://fake");
      const raw = await body(req);
      if (req.method === "POST" && url.pathname === "/v1/tokens/migration-assertion") {
        fake.mints += 1;
        const input = JSON.parse(raw.toString()) as { nonce: string; grantId: string; sourceUserId: string; workspaceId: string; upload: { key: string } };
        const jti = fake.mintJti?.(fake.mints) ?? `jti_${randomUUID()}`;
        const exp = Math.floor(Date.now() / 1000) + 60;
        // The issuer derives the identity from its stored plan, never from the request.
        const derived = fake.storedPlan?.uploads.find((u) => u.key === input.upload?.key);
        if (!derived || derived.source_user_id !== input.sourceUserId || derived.workspace_id !== input.workspaceId)
          return refuse(res, 403, "migration_grant_forbidden");
        const upload = fake.signUpload?.(fake.mints, derived) ?? derived;
        fake.signed.set(jti, upload);
        const meta = { tokenClass: "migration_assertion", nonce: input.nonce, grantId: input.grantId, planDigest: fake.signedDigest, sourceUserId: input.sourceUserId, workspaceId: input.workspaceId, upload };
        return send(res, 201, {
          accessToken: `${b64({ alg: "none" })}.${b64({ sub: "agent_relayhistory_migration", jti, exp, meta })}.fake`,
          accessTokenExpiresAt: new Date(exp * 1000).toISOString(),
          tokenType: "Bearer",
        });
      }
      const route: Route | undefined =
        req.method === "POST" && url.pathname === "/v1/pipeline/chunks" ? "chunk"
          : req.method === "POST" && url.pathname === "/v1/pipeline/manifests" ? "manifest" : undefined;
      if (!route) return refuse(res, 403, "migration_assertion_route");
      const claims = claimsOf(req);
      counts[route] += 1;
      const injected = fake.before?.(route, counts[route]);
      const upload = claims.meta.upload;
      const key = route === "manifest"
        ? String((JSON.parse(raw.toString()) as { key: string }).key)
        : manifestKey({
          workspaceId: upload.workspace_id,
          machineId: url.searchParams.get("machine") ?? "",
          harness: url.searchParams.get("harness") ?? "",
          sessionId: url.searchParams.get("session") ?? "",
          startDate: url.searchParams.get("date") ?? "",
        });
      fake.requests.push({ route, key, jti: claims.jti });
      fake.usedFor.set(claims.jti, (fake.usedFor.get(claims.jti) ?? new Set()).add(key));
      // Contract v1: the request's key and session must be the signed upload's.
      if (key !== upload.key) return refuse(res, 403, "migration_upload_forbidden");
      if (route === "chunk" && (url.searchParams.get("machine") !== upload.session.machine_id ||
        url.searchParams.get("harness") !== upload.session.harness ||
        url.searchParams.get("session") !== upload.session.session_id ||
        url.searchParams.get("date") !== upload.session.started_at.slice(0, 10)))
        return refuse(res, 403, "migration_upload_forbidden");
      if (injected === "hang") return; // never answers
      if (injected) return refuse(res, injected.status, injected.code);
      // Admission: a consumed jti or nonce is a replay, before any storage call.
      if (fake.consumed.has(claims.jti) || fake.consumed.has(claims.meta.nonce)) return refuse(res, 409, "migration_assertion_replayed");
      if (route === "chunk") {
        const digest = sha(raw);
        // Ledger: one chunk per (jti, seq); at most chunkLimit per assertion.
        const perJti = (jtiChunks.get(claims.jti) ?? new Map<string, string>());
        jtiChunks.set(claims.jti, perJti);
        const seq = url.searchParams.get("seq") ?? "";
        const prior = perJti.get(seq);
        if (prior !== undefined && prior !== digest) return refuse(res, 409, "migration_chunk_conflict");
        if (prior === undefined && perJti.size >= fake.chunkLimit) return refuse(res, 403, "migration_chunk_limit");
        perJti.set(seq, digest);
        const chunkKey = `${url.searchParams.get("machine")}/${url.searchParams.get("session")}/${url.searchParams.get("seq")}-${url.searchParams.get("offset")}-${digest.slice(0, 8)}`;
        const created = !chunks.has(chunkKey);
        if (created) {
          chunks.set(chunkKey, digest);
          fake.chunkWrites += 1;
        }
        return send(res, created ? 201 : 200, {
          key: chunkKey,
          entry: {
            seq: Number(url.searchParams.get("seq")),
            epoch: Number(url.searchParams.get("epoch")),
            start_offset: Number(url.searchParams.get("offset")),
            bytes: raw.byteLength,
            sha256: digest,
            sha256_8: digest.slice(0, 8),
            stored_bytes: raw.byteLength,
            stored_sha256: digest,
            compression: "zstd",
            stored_raw_bytes: raw.byteLength,
            raw_sha256: digest,
          },
          scrub: { ruleset: "relayhistory-scrub", version: 1 },
          created,
        });
      }
      const posted = JSON.parse(raw.toString()) as { key: string; manifest: { extensions: Record<string, Record<string, unknown>> } };
      const marker = posted.manifest.extensions["client.relayhistory_export"] ?? {};
      if (marker.source_user_id !== claims.meta.sourceUserId) return refuse(res, 403, "migration_workspace_forbidden");
      if (marker.grant_id !== claims.meta.grantId) return refuse(res, 403, "migration_grant_forbidden");
      if (marker.plan_digest !== claims.meta.planDigest) return refuse(res, 403, "migration_plan_mismatch");
      // Recompute the entry from the request (never an echoed field).
      const recomputed = derivePlanContract({
        grantId: claims.meta.grantId,
        exportedAt: "unused",
        uploads: [uploadFromManifest(posted.key, posted.manifest as never)],
      }).uploads[0]!;
      if (recomputed.entry_sha256 !== upload.entry_sha256 || recomputed.content_sha256 !== upload.content_sha256 ||
        canonical(recomputed.session) !== canonical(upload.session))
        return refuse(res, 403, "migration_upload_forbidden");
      const text = JSON.stringify(posted.manifest);
      const existing = manifests.get(posted.key);
      if (existing === undefined) {
        manifests.set(posted.key, text);
        fake.manifestWrites += 1;
      } else if (existing !== text) return refuse(res, 409, "revision_conflict");
      // The ledger row and the queue row in one unit.
      fake.consumed.add(claims.jti);
      fake.consumed.add(claims.meta.nonce);
      const workKey = `${posted.key}@"${sha(text).slice(0, 8)}"`; // manifestWorkKey: key@version
      fake.queue.push({ workKey, jti: claims.jti });
      commits.n += 1;
      if (fake.afterCommit?.(commits.n) === "hang") return; // committed, response lost
      return send(res, 202, { workKey, traceId: "t", revision: 1, scrubbed: true, version: { eTag: `"${sha(text).slice(0, 8)}"` } });
    })();
  });
  await new Promise<void>((r) => server.listen(0, "127.0.0.1", r));
  fake.url = `http://127.0.0.1:${(server.address() as AddressInfo).port}`;
  servers.push(fake);
  return fake;
}

const record = (i: number, session: string): DeliveryRecord => ({
  kind: "session_event",
  origin_id: "o1",
  record_id: `${session}-r${i}`,
  revision: "1",
  operation: "upsert",
  received_at: "2026-10-01T00:00:00Z",
  payload: { ts_ms: Date.UTC(2026, 9, 1) + i * 1000, text: `line ${i} `.repeat(20) },
});

/** Two sessions, each split into 2-3 chunks. */
function testPlan(): ExportPlan {
  const manifests = ["sess-a", "sess-b"].map((session) =>
    plannedManifestFromRecords({
      orgId: "org_1",
      workspaceId: "ws_1",
      source: "claude",
      sessionId: session,
      machine: "m1",
      sourceUserId: "user_1",
      records: [0, 1, 2, 3, 4].map((i) => record(i, session)),
      maxChunkBytes: 600,
    })!,
  );
  return { exportedAt: "2026-10-30T12:00:00.000Z", manifests };
}

async function setup(overrides: Partial<ExportUploadConfig> = {}) {
  const plan = testPlan();
  const digest = await planDigest(plan);
  const fake = await fakeIngest(digest);
  fake.storedPlan = planContract(plan, GRANT);
  const dir = await mkdtemp(join(tmpdir(), "export-upload-"));
  dirs.push(dir);
  const sleeps: number[] = [];
  const cfg: ExportUploadConfig = {
    execute: true,
    ingestUrl: fake.url,
    grantId: GRANT,
    expectedPlanDigest: digest,
    assertions: new MigrationAssertionClient({ baseUrl: fake.url, grantId: GRANT, apiKey: async () => "fake-test-value" }),
    checkpointPath: join(dir, "checkpoint.json"),
    backoffMs: [5, 10, 20],
    sleep: async (ms) => void sleeps.push(ms),
    requestTimeoutMs: 2_000,
    ...overrides,
  };
  const chunkCount = plan.manifests.reduce((n, m) => n + m.chunks.length, 0);
  return { plan, digest, fake, cfg, sleeps, chunkCount };
}

describe("export plan digest", () => {
  test("commits to the exact chunk bytes, and the manifest sent derives the same digest and identity", async () => {
    const plan = testPlan();
    expect(plan.manifests[0]!.chunks.length).toBeGreaterThan(1);
    const contract = planContract(plan, GRANT);
    const digest = contract.planDigest;
    expect(digest).toMatch(/^[0-9a-f]{64}$/);
    // Pure: the same input gives the same output; the grant is bound in.
    expect(canonical(planContract(testPlan(), GRANT))).toBe(canonical(contract));
    expect(planContract(plan, "mgr_other").planDigest).not.toBe(digest);
    expect(contract.uploads.map((u) => u.key)).toEqual(contract.entries.map((e) => e.key));
    expect(contract.uploads.every((u) => u.plan_digest === digest && u.grant_id === GRANT)).toBe(true);
    expect(new Set(contract.uploads.map((u) => u.entry_sha256)).size).toBe(2);
    // One byte changed in one chunk changes the digest and that upload's identity only.
    const tampered = testPlan();
    const data = new Uint8Array(tampered.manifests[1]!.chunks[0]!.data);
    data[0] = data[0]! ^ 1;
    tampered.manifests[1]!.chunks[0]!.data = data;
    const t = planContract(tampered, GRANT);
    expect(t.planDigest).not.toBe(digest);
    // The other upload's identity changes only in the plan digest it binds.
    expect(t.uploads[0]).toEqual({ ...contract.uploads[0]!, plan_digest: t.planDigest });
    expect(t.uploads[1]!.content_sha256).not.toBe(contract.uploads[1]!.content_sha256);
    // The sync content digest is object-store's.
    const chunks = contract.entries[0]!.chunks;
    expect(contentSha256(chunks)).toBe(await computeContentSha256(chunks));
    // A manifest built from the server-sealed entries maps back to the same input.
    const m = plan.manifests[0]!;
    const sealed = chunks.map((c) => ({ ...c, sha256_8: c.sha256.slice(0, 8), stored_bytes: c.bytes, stored_sha256: c.sha256, compression: "zstd" as const, stored_raw_bytes: c.bytes, raw_sha256: c.sha256 }));
    const manifest = await buildExportManifest(plan, m, sealed, { ruleset: "relayhistory-scrub", version: 1 }, { grantId: GRANT, planDigest: digest });
    const key = contract.entries[0]!.key;
    expect(uploadFromManifest(key, manifest)).toEqual(uploadFromPlanned(m));
    expect(manifest.raw.content_sha256).toBe(contract.uploads[0]!.content_sha256);
    const marker = (manifest.extensions as Record<string, Record<string, unknown>>)["client.relayhistory_export"]!;
    expect(marker).toMatchObject({ grant_id: GRANT, plan_digest: digest, source_user_id: "user_1", source: "neon.delivery_records" });
  });

  test("the contract is one swappable function: the courier derives everything through it", async () => {
    let calls = 0;
    const swapped: PlanContractFn = (input) => {
      calls += 1;
      return derivePlanContract(input);
    };
    const { fake, cfg, plan } = await setup({ contract: swapped });
    const result = await runExportUpload(plan, cfg);
    expect(result.committed).toHaveLength(2);
    expect(calls).toBe(3); // the plan once, then once per manifest about to be sent
    expect(fake.queue).toHaveLength(2);
  });

  test("refuses a plan that does not hash to the grant's digest, before any request", async () => {
    const { fake, cfg, plan } = await setup({ expectedPlanDigest: "0".repeat(64) });
    await expect(runExportUpload(plan, cfg)).rejects.toMatchObject({ code: "plan_digest_mismatch" });
    expect(fake.mints).toBe(0);
    expect(fake.requests).toHaveLength(0);
  });

  test("refuses an assertion that signs another plan, before sending any bytes", async () => {
    const { fake, cfg, plan } = await setup();
    fake.signedDigest = "f".repeat(64);
    const result = await runExportUpload(plan, cfg);
    expect(result.stopped).toMatchObject({ code: "assertion_plan_mismatch" });
    expect(fake.requests).toHaveLength(0);
    expect(fake.mints).toBe(1);
  });
});

describe("export upload guard", () => {
  test("refuses without execute: true and makes no request", async () => {
    const { fake, cfg, plan } = await setup({ execute: false });
    await expect(runExportUpload(plan, cfg)).rejects.toMatchObject({ code: "execute_required" });
    expect(fake.mints).toBe(0);
    expect(fake.requests).toHaveLength(0);
  });

  test("refuses a non-https, non-loopback ingest URL", async () => {
    const { cfg, plan } = await setup();
    await expect(runExportUpload(plan, { ...cfg, ingestUrl: "http://ingest.example" })).rejects.toThrow("https");
    await expect(runExportUpload(plan, { ...cfg, ingestUrl: "https://u:p@ingest.example" })).rejects.toThrow("credentials");
  });

  test("the CLI still refuses --execute", async () => {
    const { main } = await import("../relayhistory-export.ts");
    const argv = process.argv;
    process.argv = [...argv.slice(0, 2), "--execute"];
    try {
      await expect(main()).rejects.toThrow("--execute is not available yet");
    } finally {
      process.argv = argv;
    }
  });
});

describe("export upload courier", () => {
  test("seals every chunk then commits each manifest under its own fresh assertion", async () => {
    const { fake, cfg, plan, chunkCount } = await setup();
    const result = await runExportUpload(plan, cfg);
    expect(result).toMatchObject({ skipped: [], failed: [] });
    expect(result.stopped).toBeUndefined();
    expect(result.committed).toHaveLength(2);
    expect(fake.mints).toBe(2); // one per manifest
    expect(fake.chunkWrites).toBe(chunkCount);
    expect(fake.manifestWrites).toBe(2);
    expect(fake.queue).toHaveLength(2);
    // Chunks precede their manifest, all under the manifest's one assertion.
    const first = fake.requests.filter((r) => r.jti === fake.requests[0]!.jti);
    expect(first.at(-1)!.route).toBe("manifest");
    expect(first.slice(0, -1).every((r) => r.route === "chunk")).toBe(true);
  });

  describe("per-upload identity", () => {
    test("each assertion signs one upload's identity and is used only for that upload", async () => {
      const { fake, cfg, plan } = await setup();
      fake.before = (route, n) => (route === "manifest" && n === 1 ? { status: 409, code: "migration_assertion_replayed" } : undefined);
      const result = await runExportUpload(plan, cfg);
      expect(result.committed).toHaveLength(2);
      const contract = planContract(plan, GRANT);
      expect(fake.signed.size).toBe(3); // 2 uploads + 1 re-mint
      for (const [jti, used] of fake.usedFor) {
        const signed = fake.signed.get(jti)!;
        expect([...used]).toEqual([signed.key]); // never presented for another key
        expect(contract.uploads.map((u) => canonical(u))).toContain(canonical(signed));
      }
      // Every committed queue row was committed under an assertion signing that upload.
      for (const q of fake.queue) expect(fake.signed.get(q.jti)!.key).toBe(result.committed.find((c) => c.workKey === q.workKey)!.key);
      expect(fake.requests.every((r) => r.key === fake.signed.get(r.jti)!.key)).toBe(true);
    });

    test("refuses an assertion that signs another upload's identity, before sending any bytes", async () => {
      const { fake, cfg, plan } = await setup();
      const other = planContract(plan, GRANT).uploads[1]!;
      fake.signUpload = (n, requested) => (n === 1 ? other : requested);
      const result = await runExportUpload(plan, cfg);
      expect(result.stopped).toMatchObject({ code: "assertion_upload_mismatch" });
      expect(fake.requests).toHaveLength(0);
      expect(fake.queue).toHaveLength(0);
    });

    test.each([
      ["entry digest", { entry_sha256: "0".repeat(64) }],
      ["key", { key: "v1/ws=ws_1/m=m2/h=claude/d=2026/10/01/s=sess-a/manifest.json" }],
      ["workspace", { workspace_id: "ws_2" }],
      ["content digest", { content_sha256: "0".repeat(64) }],
      ["plan digest", { plan_digest: "0".repeat(64) }],
      ["grant", { grant_id: "mgr_other" }],
      ["org", { org_id: "org_2" }],
      ["session key", { session: { machine_id: "m2", harness: "claude", session_id: "sess-a", started_at: "2026-10-01T00:00:00.000Z" } }],
    ])("refuses an assertion whose signed identity differs in the %s", async (_field, change) => {
      const { fake, cfg, plan } = await setup();
      fake.signUpload = (_n, derived) => ({ ...derived, ...change });
      const result = await runExportUpload(plan, cfg);
      expect(result.stopped).toMatchObject({ code: "assertion_upload_mismatch" });
      expect(fake.requests).toHaveLength(0);
    });
  });

  describe("409 migration_assertion_replayed", () => {
    test("on the manifest: re-mints once, retries the same content and work key, never double-writes", async () => {
      const { fake, cfg, plan, chunkCount } = await setup();
      // The ledger refuses the first manifest commit as a replay (e.g. a
      // concurrent consumer spent the jti between admission and commit).
      fake.before = (route, n) => (route === "manifest" && n === 1 ? { status: 409, code: "migration_assertion_replayed" } : undefined);
      const result = await runExportUpload(plan, cfg);
      expect(result.failed).toEqual([]);
      expect(result.committed).toHaveLength(2);
      expect(fake.mints).toBe(3); // 2 manifests + 1 re-mint
      const manifestPosts = fake.requests.filter((r) => r.route === "manifest");
      expect(manifestPosts).toHaveLength(3);
      expect(manifestPosts[0]!.key).toBe(manifestPosts[1]!.key); // same manifest retried
      expect(manifestPosts[0]!.jti).not.toBe(manifestPosts[1]!.jti); // with a fresh assertion
      expect(fake.chunkWrites).toBe(chunkCount);
      expect(fake.requests.filter((r) => r.route === "chunk")).toHaveLength(chunkCount); // sealed chunks not resent
      expect(fake.manifestWrites).toBe(2);
      expect(fake.queue).toHaveLength(2);
      expect(result.committed[0]!.workKey).toBe(fake.queue[0]!.workKey);
    });

    test("on a chunk (spent assertion from the mint): re-mints once and the upload completes", async () => {
      const { fake, cfg, plan, chunkCount } = await setup();
      fake.consumed.add("jti_spent");
      fake.mintJti = (n) => (n === 1 ? "jti_spent" : undefined);
      const result = await runExportUpload(plan, cfg);
      expect(result.failed).toEqual([]);
      expect(result.committed).toHaveLength(2);
      expect(fake.mints).toBe(3);
      expect(fake.requests[0]).toMatchObject({ route: "chunk", jti: "jti_spent" });
      expect(fake.chunkWrites).toBe(chunkCount);
      expect(fake.manifestWrites).toBe(2);
      expect(fake.queue.map((q) => q.jti)).not.toContain("jti_spent");
    });

    test("a second 409 with a fresh assertion fails that manifest loudly; the run moves on", async () => {
      const { fake, cfg, plan } = await setup();
      fake.before = (route, n) => (route === "manifest" && n <= 2 ? { status: 409, code: "migration_assertion_replayed" } : undefined);
      const result = await runExportUpload(plan, cfg);
      const [a, b] = plan.manifests.map((m) => m.location);
      expect(result.failed).toEqual([{ key: expect.stringContaining(`s=${a!.sessionId}`), code: "migration_assertion_replayed" }]);
      expect(result.committed.map((c) => c.key)).toEqual([expect.stringContaining(`s=${b!.sessionId}`)]);
      expect(result.stopped).toBeUndefined();
      expect(fake.mints).toBe(3); // first manifest: original + exactly one re-mint
      expect(fake.manifestWrites).toBe(1);
      expect(fake.queue).toHaveLength(1);
      const cp = JSON.parse(await readFile(cfg.checkpointPath, "utf8"));
      expect(cp.manifests[result.failed[0]!.key].status).toBe("sealing"); // not recorded as committed
    });
  });

  describe("503", () => {
    test("migration_ledger_unavailable: bounded backoff with fresh assertions, then success", async () => {
      const { fake, cfg, plan, sleeps } = await setup();
      fake.before = (route, n) => (route === "manifest" && n <= 2 ? { status: 503, code: "migration_ledger_unavailable" } : undefined);
      const result = await runExportUpload(plan, cfg);
      expect(result.committed).toHaveLength(2);
      expect(result.failed).toEqual([]);
      expect(sleeps).toEqual([5, 10]);
      expect(fake.mints).toBe(4);
      expect(fake.manifestWrites).toBe(2);
      expect(fake.queue).toHaveLength(2);
    });

    test("a 503 that persists stops the run fail-closed after the backoff budget", async () => {
      const { fake, cfg, plan, sleeps } = await setup();
      fake.before = (route) => (route === "manifest" ? { status: 503, code: "migration_ledger_unavailable" } : undefined);
      const result = await runExportUpload(plan, cfg);
      expect(result.stopped).toMatchObject({ code: "migration_ledger_unavailable", status: 503 });
      expect(result.committed).toEqual([]);
      expect(sleeps).toEqual([5, 10, 20]);
      expect(fake.requests.filter((r) => r.route === "manifest")).toHaveLength(4); // 1 + 3 retries
      expect(new Set(fake.requests.map((r) => r.key).filter((k) => k.includes("sess-b")))).toEqual(new Set()); // second manifest never attempted
      expect(fake.queue).toHaveLength(0);
    });
  });

  describe("401 migration_assertion_expired", () => {
    test("re-mints once and the retry succeeds, no double write", async () => {
      const { fake, cfg, plan, chunkCount, sleeps } = await setup();
      fake.before = (route, n) => (route === "manifest" && n === 1 ? { status: 401, code: "migration_assertion_expired" } : undefined);
      const result = await runExportUpload(plan, cfg);
      expect(result.stopped).toBeUndefined();
      expect(result.failed).toEqual([]);
      expect(result.committed).toHaveLength(2);
      expect(fake.mints).toBe(3); // 2 manifests + 1 re-mint
      const posts = fake.requests.filter((r) => r.route === "manifest");
      expect(posts[0]!.key).toBe(posts[1]!.key);
      expect(posts[0]!.jti).not.toBe(posts[1]!.jti);
      expect(fake.chunkWrites).toBe(chunkCount);
      expect(fake.manifestWrites).toBe(2);
      expect(fake.queue).toHaveLength(2);
      expect(sleeps).toEqual([]);
    });

    test("a second expiry with the fresh assertion stops the run", async () => {
      const { fake, cfg, plan } = await setup();
      fake.before = (route) => (route === "manifest" ? { status: 401, code: "migration_assertion_expired" } : undefined);
      const result = await runExportUpload(plan, cfg);
      expect(result.stopped).toMatchObject({ code: "migration_assertion_expired", status: 401 });
      expect(fake.mints).toBe(2);
      expect(fake.requests.filter((r) => r.route === "manifest")).toHaveLength(2);
      expect(fake.queue).toHaveLength(0);
    });

    test.each([
      [401, "invalid_token"], // e.g. invalid signature
      [401, "invalid_claims"], // generic "jwt is expired" from the verifier
      [401, "missing_authorization"],
      [401, "http_401"], // no error code at all
    ])("a generic %i %s is terminal: no re-mint", async (status, code) => {
      const { fake, cfg, plan } = await setup();
      fake.before = (route) => (route === "manifest" ? { status, code } : undefined);
      const result = await runExportUpload(plan, cfg);
      expect(result.stopped).toMatchObject({ code, status });
      expect(fake.mints).toBe(1);
      expect(fake.requests.filter((r) => r.route === "manifest")).toHaveLength(1);
    });
  });

  describe("canonical user id", () => {
    const withUser = (plan: ExportPlan, ids: string[]) => {
      plan.manifests.forEach((m, i) => (m.sourceUserId = ids[i]!));
      return plan;
    };

    test("relay_ aliases normalize to one form in the plan, mint and manifest", async () => {
      const bare = planContract(withUser(testPlan(), ["user_1", "user_1"]), GRANT);
      const aliased = planContract(withUser(testPlan(), ["relay_user_1", "user_1"]), GRANT);
      expect(canonical(aliased)).toBe(canonical(bare));
      expect(aliased.uploads.every((u) => u.source_user_id === "user_1")).toBe(true);
      const { fake, cfg } = await setup();
      const minted: string[] = [];
      const inner = cfg.assertions;
      const result = await runExportUpload(withUser(testPlan(), ["relay_user_1", "relay_user_1"]), {
        ...cfg,
        assertions: { mint: (u, w, s) => (minted.push(u), inner.mint(u, w, s)) },
      });
      expect(result.committed).toHaveLength(2);
      expect(minted).toEqual(["user_1", "user_1"]);
      expect(fake.manifestWrites).toBe(2);
    });

    test("refuses a signed identity that carries the relay_ alias instead of the canonical form", async () => {
      const { fake, cfg, plan } = await setup();
      fake.signUpload = (_n, derived) => ({ ...derived, source_user_id: `relay_${derived.source_user_id}` });
      const result = await runExportUpload(plan, cfg);
      expect(result.stopped).toMatchObject({ code: "assertion_upload_mismatch" });
      expect(fake.requests).toHaveLength(0);
    });

    test("refuses a signed identity for another user", async () => {
      const { fake, cfg, plan } = await setup();
      fake.signUpload = (_n, derived) => ({ ...derived, source_user_id: "user_2" });
      const result = await runExportUpload(plan, cfg);
      expect(result.stopped).toMatchObject({ code: "assertion_upload_mismatch" });
      expect(fake.requests).toHaveLength(0);
    });

    test.each(["relay_", "relay_relay_user_1", "bad user", ""])("refuses a malformed user id %p", (id) => {
      expect(() => planContract(withUser(testPlan(), [id, "user_1"]), GRANT)).toThrow("malformed");
    });
  });

  describe("401 and 403 stop the run", () => {
    test.each([
      [403, "migration_assertion_route"],
      [403, "migration_workspace_forbidden"],
      [403, "migration_grant_forbidden"],
      [403, "migration_plan_mismatch"],
      [401, "invalid_claims"],
    ])("%i %s: no retry, no further manifests", async (status, code) => {
      const { fake, cfg, plan, sleeps } = await setup();
      fake.before = (route) => (route === "manifest" ? { status, code } : undefined);
      const result = await runExportUpload(plan, cfg);
      expect(result.stopped).toMatchObject({ code, status });
      expect(result.committed).toEqual([]);
      expect(fake.mints).toBe(1);
      expect(fake.requests.filter((r) => r.route === "manifest")).toHaveLength(1);
      expect(sleeps).toEqual([]);
      expect(fake.queue).toHaveLength(0);
    });

    test("403 on a chunk stops before the manifest", async () => {
      const { fake, cfg, plan } = await setup();
      fake.before = (route) => (route === "chunk" ? { status: 403, code: "migration_workspace_forbidden" } : undefined);
      const result = await runExportUpload(plan, cfg);
      expect(result.stopped).toMatchObject({ code: "migration_workspace_forbidden", status: 403 });
      expect(fake.requests).toHaveLength(1);
    });
  });

  describe("lost response after commit", () => {
    test("the retry with a fresh assertion is idempotent: same work key re-queued, success recorded once", async () => {
      const { fake, cfg, plan, chunkCount } = await setup({ requestTimeoutMs: 300 });
      fake.afterCommit = (n) => (n === 1 ? "hang" : undefined);
      const result = await runExportUpload(plan, cfg);
      expect(result.failed).toEqual([]);
      expect(result.committed).toHaveLength(2);
      const firstKey = result.committed[0]!.key;
      expect(result.committed.filter((c) => c.key === firstKey)).toHaveLength(1); // recorded once
      expect(fake.manifestWrites).toBe(2); // the retry took the identical branch
      expect(fake.chunkWrites).toBe(chunkCount);
      // The server re-queued the same work key under a second (fresh) assertion.
      expect(fake.queue).toHaveLength(3);
      expect(fake.queue[0]!.workKey).toBe(fake.queue[1]!.workKey);
      expect(fake.queue[0]!.jti).not.toBe(fake.queue[1]!.jti);
      expect(result.committed[0]!.workKey).toBe(fake.queue[0]!.workKey);
      const cp = JSON.parse(await readFile(cfg.checkpointPath, "utf8"));
      expect(cp.manifests[firstKey]).toMatchObject({ status: "committed", work_key: fake.queue[0]!.workKey });
    });
  });

  describe("checkpoint and resume", () => {
    test("a run that stopped resumes without re-uploading committed manifests", async () => {
      const { fake, cfg, plan } = await setup();
      fake.before = (route, n) => (route === "manifest" && n === 2 ? { status: 401, code: "invalid_claims" } : undefined);
      const first = await runExportUpload(plan, cfg);
      expect(first.committed).toHaveLength(1);
      expect(first.stopped).toMatchObject({ code: "invalid_claims" });
      const done = first.committed[0]!.key;
      fake.before = undefined;
      fake.requests.length = 0;
      const second = await runExportUpload(plan, cfg);
      expect(second.skipped).toEqual([done]);
      expect(second.committed.map((c) => c.key)).not.toContain(done);
      expect(second.committed).toHaveLength(1);
      expect(fake.requests.filter((r) => r.route === "manifest" && r.key === done)).toHaveLength(0);
      expect(fake.manifestWrites).toBe(2);
      expect(fake.queue).toHaveLength(2);
    });

    test("chunks sealed before a stop are not resent on resume", async () => {
      const { fake, cfg, plan, chunkCount } = await setup();
      fake.before = (route, n) => (route === "manifest" && n === 1 ? { status: 403, code: "migration_workspace_forbidden" } : undefined);
      await runExportUpload(plan, cfg);
      const sealedFirst = plan.manifests[0]!.chunks.length;
      expect(fake.chunkWrites).toBe(sealedFirst);
      fake.before = undefined;
      fake.requests.length = 0;
      const result = await runExportUpload(plan, cfg);
      expect(result.committed).toHaveLength(2);
      expect(fake.requests.filter((r) => r.route === "chunk")).toHaveLength(chunkCount - sealedFirst);
      expect(fake.chunkWrites).toBe(chunkCount);
    });

    test("refuses a checkpoint of another plan, and stores no token", async () => {
      const { cfg, plan } = await setup();
      await runExportUpload(plan, cfg);
      const text = await readFile(cfg.checkpointPath, "utf8");
      expect(text).not.toContain("Bearer");
      expect(text).not.toMatch(/eyJ[A-Za-z0-9_-]+\./); // no JWT
      expect(text).not.toContain("fake-test-value");
      const cp = JSON.parse(text);
      await writeFile(cfg.checkpointPath, JSON.stringify({ ...cp, plan_digest: "1".repeat(64) }));
      await expect(runExportUpload(plan, cfg)).rejects.toMatchObject({ code: "checkpoint_mismatch" });
    });
  });
});

describe("contract v1 vectors (#43 docs/contracts/relayhistory-export-plan-entry-v1, at 15127919)", () => {
  // Copied byte for byte from the fixture commit; see fixtures/.
  const vectors = JSON.parse(
    readFileSync(join(import.meta.dir, "fixtures", "relayhistory-export-plan-entry-v1.vectors.json"), "utf8"),
  ) as {
    grant_id: string;
    exported_at: string;
    inputs: UploadInput[];
    entries: { entry: Record<string, unknown>; canonical_json: string; entry_sha256: string }[];
    plan: { canonical_json: string; plan_digest: string };
    upload_identities: UploadIdentity[];
    negative: { name: string; upload: UploadIdentity; request_key?: string; sponsor_id?: string; expect: string }[];
  };
  const contract = derivePlanContract({ grantId: vectors.grant_id, exportedAt: vectors.exported_at, uploads: vectors.inputs });

  test("derivePlanContract reproduces every entry byte for byte", () => {
    expect(contract.entries).toHaveLength(vectors.entries.length);
    contract.entries.forEach((entry, i) => {
      expect(canonical(entry)).toBe(vectors.entries[i]!.canonical_json);
      expect(entry).toEqual(vectors.entries[i]!.entry as never);
      expect(sha(canonical(entry))).toBe(vectors.entries[i]!.entry_sha256);
    });
  });

  test("the plan digest is the vector's, over the vector's canonical plan", () => {
    expect(sha(vectors.plan.canonical_json)).toBe(vectors.plan.plan_digest);
    expect(contract.planDigest).toBe(vectors.plan.plan_digest);
  });

  test("the upload identities are the vector's, byte for byte", () => {
    expect(contract.uploads).toEqual(vectors.upload_identities);
    contract.uploads.forEach((u, i) => expect(canonical(u)).toBe(canonical(vectors.upload_identities[i]!)));
  });

  test("relay_ aliases in the input derive the canonical vectors", () => {
    const aliased = vectors.inputs.map((u) => ({ ...u, org_id: `relay_${u.org_id}`, source_user_id: `relay_${u.source_user_id}` }));
    const again = derivePlanContract({ grantId: vectors.grant_id, exportedAt: vectors.exported_at, uploads: aliased });
    expect(again.planDigest).toBe(vectors.plan.plan_digest);
    expect(again.uploads).toEqual(vectors.upload_identities);
  });

  test.each(["relay_ alias in source_user_id (not canonical)", "relay_ alias in workspace_id (not canonical)"])(
    "negative vector %p: the courier's identity check refuses it",
    (name) => {
      const neg = vectors.negative.find((n) => n.name === name)!;
      expect(neg.expect).toBe("invalid_claims");
      expect(canonical(neg.upload)).not.toBe(canonical(contract.uploads[0]!));
    },
  );

  test("negative vectors for another key or user never match what the courier presents", () => {
    for (const neg of vectors.negative.filter((n) => n.expect === "migration_upload_forbidden")) {
      if (neg.request_key) expect(neg.request_key).not.toBe(neg.upload.key); // the courier sends only to upload.key
      if (neg.sponsor_id) expect(neg.sponsor_id).not.toBe(neg.upload.source_user_id); // it mints as upload.source_user_id
    }
  });
});

describe("contract v1 codes", () => {
  test("a signed identity with a relay_ alias workspace is refused before any byte", async () => {
    const { fake, cfg, plan } = await setup();
    fake.signUpload = (_n, derived) => ({ ...derived, workspace_id: `relay_${derived.workspace_id}` });
    const result = await runExportUpload(plan, cfg);
    expect(result.stopped).toMatchObject({ code: "assertion_upload_mismatch" });
    expect(fake.requests).toHaveLength(0);
  });

  test("403 migration_upload_forbidden stops the run, no retry", async () => {
    const { fake, cfg, plan } = await setup();
    fake.before = (route) => (route === "manifest" ? { status: 403, code: "migration_upload_forbidden" } : undefined);
    const result = await runExportUpload(plan, cfg);
    expect(result.stopped).toMatchObject({ code: "migration_upload_forbidden", status: 403 });
    expect(fake.mints).toBe(1);
  });

  test("409 migration_chunk_conflict fails that manifest and the run moves on", async () => {
    const { fake, cfg, plan } = await setup();
    fake.before = (route, n) => (route === "chunk" && n === 1 ? { status: 409, code: "migration_chunk_conflict" } : undefined);
    const result = await runExportUpload(plan, cfg);
    expect(result.failed).toEqual([{ key: expect.stringContaining("s=sess-a"), code: "migration_chunk_conflict" }]);
    expect(result.committed).toHaveLength(1);
    expect(fake.mints).toBe(2); // no re-mint for a conflict
  });

  test("at most chunksPerAssertion chunks go under one assertion, then a fresh mint", async () => {
    const { fake, cfg, plan, chunkCount } = await setup({ chunksPerAssertion: 1 });
    fake.chunkLimit = 1;
    const result = await runExportUpload(plan, cfg);
    expect(result.committed).toHaveLength(2);
    expect(result.failed).toEqual([]);
    expect(fake.mints).toBe(chunkCount); // one assertion per chunk; the manifest reuses the last
    // Proactive: the ingest never had to refuse a chunk for the limit.
    expect(fake.requests.filter((r) => r.route === "chunk")).toHaveLength(chunkCount);
    expect(fake.requests.some((r) => r.route === "chunk" && fake.signed.get(r.jti) === undefined)).toBe(false);
    for (const [, used] of fake.usedFor) expect(used.size).toBe(1);
    expect(fake.chunkWrites).toBe(chunkCount);
    expect(fake.manifestWrites).toBe(2);
  });

  test("403 migration_chunk_limit re-mints and continues; sealed chunks are not resent", async () => {
    const { fake, cfg, plan, chunkCount } = await setup();
    fake.chunkLimit = 1; // the ingest is stricter than the courier's budget
    const result = await runExportUpload(plan, cfg);
    expect(result.committed).toHaveLength(2);
    expect(fake.chunkWrites).toBe(chunkCount);
    expect(fake.manifestWrites).toBe(2);
    const sealedPosts = fake.requests.filter((r) => r.route === "chunk").length;
    expect(sealedPosts).toBe(chunkCount + (chunkCount - plan.manifests.length)); // one refused post per extra chunk
  });

  test("a second 403 migration_chunk_limit with no progress stops the run", async () => {
    const { fake, cfg, plan } = await setup();
    fake.before = (route) => (route === "chunk" ? { status: 403, code: "migration_chunk_limit" } : undefined);
    const result = await runExportUpload(plan, cfg);
    expect(result.stopped).toMatchObject({ code: "migration_chunk_limit", status: 403 });
    expect(fake.mints).toBe(2); // exactly one re-mint
    expect(fake.requests).toHaveLength(2);
    expect(fake.chunkWrites).toBe(0);
  });
});
