/**
 * The REAL production composition, buildIngestApp, end to end: auth defaults,
 * the dedicated migration verifier, and the full migration-assertion contract
 * (signed pair, recomputed upload identity, chunk bounds, single-use ledger).
 * Only the database (local PGlite with every migration) and the raw store
 * (a temp fs store) are substituted. Synthetic ids and a throwaway key only;
 * nothing credential-like is written to disk.
 */
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, test } from "bun:test";
import { createHash, createSign, generateKeyPairSync, randomBytes } from "node:crypto";
import { mkdtemp, readdir, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { PGlite } from "../../../packages/pipeline/node_modules/@electric-sql/pglite/dist/index.js";
import { createFsStore } from "../../../packages/object-store/src/fs.ts";
import {
  backfillSha256,
  canonicalJson,
  planEntryV2Sha256,
  uploadIdentityOf,
  uploadIdentityV2Of,
  type BackfillBase,
  type PlanEntry,
  type PlanEntryV2,
  type UploadIdentity,
  type UploadIdentityV2,
} from "@rhc/lib/pipeline/migration-assertions.ts";
import {
  computeContentSha256,
  manifestKey,
  pipelineQueueDeps,
  pollPostgresStage,
  postgresDeadLetterStore,
  postgresPipelineQueue,
  sealRawChunk,
  type Bindings,
  type SessionLocation,
  type SqlExecutor,
} from "../../shared/vendor.ts";
import { sqlPipelineEventSink } from "../../hoopdb/sink.ts";
import { buildIngestApp } from "../ingest.ts";

const ORIGIN = "https://history.example.test";
const ISSUER = "https://relayauth.example.test";
const JWKS_URL = "https://relayauth.example.test/.well-known/jwks.json";
const KID = "ingest-auth-test";
const ORG = "org_ingest";
const WORKSPACE = "ws_ingest";
const USER = "user_ingest_a";
const PLAN = "a".repeat(64);
const GRANT = `mgr_${"1".repeat(32)}`;
const TRACE = "019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c18";
const AT = "2026-10-29T10:00:00.000Z";
const SOURCE = new TextEncoder().encode('{"type":"user","text":"hello"}\n');
const LOCATION: SessionLocation = { workspaceId: WORKSPACE, machineId: "machine-a", harness: "claude", sessionId: "session-a", startDate: "2026-10-29" };
const DEFAULT_LOCATION = LOCATION;
const ROOT_DIR = join(import.meta.dir, "../../..");

const { privateKey, publicKey } = generateKeyPairSync("rsa", { modulusLength: 2048 });
const JWKS = { keys: [{ ...publicKey.export({ format: "jwk" }), kid: KID, alg: "RS256" }] };

function sign(claims: Record<string, unknown>) {
  for (const [k, v] of Object.entries(claims)) if (v === undefined) delete claims[k];
  const b64 = (v: unknown) => Buffer.from(JSON.stringify(v)).toString("base64url");
  const head = b64({ alg: "RS256", kid: KID, typ: "JWT" });
  const body = b64(claims);
  return `${head}.${body}.${createSign("RSA-SHA256").update(`${head}.${body}`).end().sign(privateKey, "base64url")}`;
}

const now = () => Math.floor(Date.now() / 1000);

function migrationToken(upload: UploadIdentity | UploadIdentityV2, overrides: Record<string, unknown> = {}, meta: Record<string, unknown> = {}) {
  return sign({
    sub: "agent_relayhistory_migration", org: ORG, wks: WORKSPACE, scopes: ["rth:sync"], sponsorId: USER,
    sponsorChain: [USER, "agent_relayhistory_migration"], token_type: "access", iss: ISSUER, aud: [ORIGIN],
    iat: now(), exp: now() + 60, jti: `tok_${randomBytes(16).toString("hex")}`,
    ...overrides,
    meta: {
      tokenClass: "migration_assertion", grantId: GRANT, planDigest: PLAN, nonce: randomBytes(24).toString("base64url"),
      sourceUserId: USER, actorId: "actor", actorOrgId: ORG, workspaceId: WORKSPACE, upload, ...meta,
    },
  });
}

/** An origin-bound courier assertion (live delivery), as relayauth mints for this History. */
const liveToken = () =>
  sign({ sub: "agent_relayhistory_assertion", org: ORG, wks: WORKSPACE, sponsorId: USER, scopes: ["rth:sync"], token_type: "access", iss: ISSUER, aud: [ORIGIN], iat: now(), exp: now() + 600, meta: { tokenClass: "relayhistory_assertion" } });

/** A generic RelayAuth bearer: a valid JWT of no assertion class (like a relay_pa_ token's JWT). */
const genericToken = (aud: string[] = ["relayhistory"]) =>
  sign({ sub: USER, org: ORG, wks: WORKSPACE, sponsorId: USER, scopes: ["rth:sync", "rth:read"], token_type: "access", iss: ISSUER, aud, iat: now(), exp: now() + 600 });

let db: PGlite;
let root: string;
const savedEnv = { ...process.env };
const realFetch = globalThis.fetch;

beforeAll(async () => {
  db = new PGlite();
  await db.exec(await readFile(join(ROOT_DIR, "supabase/tests/supabase_shim.sql"), "utf8"));
  for (const dir of ["supabase/migrations", "src/hoopdb/migrations"])
    for (const file of (await readdir(join(ROOT_DIR, dir))).filter((f) => f.endsWith(".sql")).sort())
      await db.exec(await readFile(join(ROOT_DIR, dir, file), "utf8"));
  globalThis.fetch = (async (input: RequestInfo | URL, init?: RequestInit) =>
    String(input) === JWKS_URL ? Response.json(JWKS) : realFetch(input, init)) as typeof fetch;
});
afterAll(async () => {
  globalThis.fetch = realFetch;
  await db?.close();
});
beforeEach(async () => {
  root = await mkdtemp(join(tmpdir(), "96bulls-ingest-auth-"));
  await db.exec("TRUNCATE sessions.migration_assertion_uses; TRUNCATE sessions.migration_assertion_chunks; TRUNCATE sessions.pipeline_jobs");
});
afterEach(async () => {
  for (const k of Object.keys(process.env)) if (!(k in savedEnv)) delete process.env[k];
  Object.assign(process.env, savedEnv);
  await rm(root, { recursive: true, force: true });
});

const pgExecutor = () => {
  const wrap = (q: { query: PGlite["query"] }): SqlExecutor => ({
    query: async (text, params) => ({ rows: (await q.query(text, params)).rows as never[] }),
  });
  return { ...wrap(db), transaction: <T>(work: (tx: SqlExecutor) => Promise<T>) => db.transaction((tx) => work(wrap(tx))) };
};

const count = async (table: string) => (await db.query<{ n: number }>(`SELECT count(*)::int AS n FROM ${table}`)).rows[0]!.n;

/** buildIngestApp under an environment (unset by default: the production defaults). */
async function ingest(extra: Record<string, string> = {}) {
  Object.assign(process.env, {
    LOCKER_BUCKET: "raw",
    TENANT_ORG_ID: ORG,
    AUTH_ISSUER: ISSUER,
    RELAYAUTH_JWKS_URL: JWKS_URL,
    RELAYHISTORY_ORIGIN: ORIGIN,
    ...extra,
  });
  delete process.env.ALLOW_INTERIM_RELAYAUTH_BEARER;
  if (extra.ALLOW_INTERIM_RELAYAUTH_BEARER) process.env.ALLOW_INTERIM_RELAYAUTH_BEARER = extra.ALLOW_INTERIM_RELAYAUTH_BEARER;
  const store = createFsStore({ root });
  const { app, env } = await buildIngestApp({ db: pgExecutor(), raw: { store, provider: "fs", bucket: "raw" } as never });
  const req = (path: string, token: string, init: RequestInit = {}) =>
    app.request(path, { ...init, headers: { authorization: `Bearer ${token}`, ...(init.headers as Record<string, string>) } }, env);
  /** A chunk request; a migration one carries its plan entry text in front of the bytes. */
  const chunk = (token: string, location: SessionLocation = LOCATION, opts: { entry?: string; seq?: number; bytes?: Uint8Array; epoch?: number; offset?: number; format?: string } = {}) => {
    const bytes = opts.bytes ?? SOURCE;
    const entry = opts.entry === undefined ? undefined : new TextEncoder().encode(opts.entry);
    const body = entry ? Buffer.concat([entry, bytes]) : bytes;
    return req(`/v1/pipeline/chunks?machine=${location.machineId}&harness=${location.harness}&session=${location.sessionId}&date=${location.startDate}&seq=${opts.seq ?? 0}&epoch=${opts.epoch ?? 0}&offset=${opts.offset ?? 0}&format=${opts.format ?? "jsonl"}`, token, {
      method: "POST",
      headers: { "x-relay-trace-id": TRACE, ...(entry ? { "x-relay-migration-plan-entry-length": String(entry.byteLength) } : {}) },
      body,
    });
  };
  const commit = (token: string, manifest: Record<string, unknown>, key = manifestKey(LOCATION)) =>
    req("/v1/pipeline/manifests", token, {
      method: "POST",
      headers: {
        "content-type": "application/json",
        "x-machine-id": (manifest.session as { machine_id: string }).machine_id,
        "x-session-id": (manifest.session as { session_id: string }).session_id,
      },
      body: JSON.stringify({ key, manifest }),
    });
  return { req, chunk, commit };
}

/** The planned upload: its chunk entry (as the server seals it), manifest and signed identity. */
async function planned(LOCATION: SessionLocation = DEFAULT_LOCATION, owner = `migration:${GRANT}`) {
  const scratch = await mkdtemp(join(tmpdir(), "96bulls-plan-"));
  try {
    const sealed = await sealRawChunk(createFsStore({ root: scratch }), {
      location: LOCATION, format: "jsonl", seq: 0, epoch: 0, startOffset: 0, source: SOURCE, traceId: TRACE,
    });
    const manifest = {
      schema_v: 1, layout: "v1", trace_id: TRACE, revision: 1, created_at: AT, updated_at: AT,
      session: { workspace_id: WORKSPACE, machine_id: LOCATION.machineId, harness: LOCATION.harness, session_id: LOCATION.sessionId, started_at: AT },
      // A cold export is owned by its migration grant (#42's owner fence); a live session by its machine.
      writer: { delivery_instance_id: owner, plugin_version: "test" },
      raw: {
        owner, state: "closed", updated_at: AT, format: "jsonl", compression: "zstd", chunk_count: 1,
        raw_bytes: sealed.entry.bytes, stored_bytes: sealed.entry.stored_bytes,
        content_sha256: await computeContentSha256([sealed.entry]), epoch: 0,
        epochs: [{ epoch: 0, opened_at: AT, reason: "initial", first_seq: 0, opened_by: owner }],
        next_offset: sealed.entry.bytes, chunks: [sealed.entry], scrub: sealed.scrub,
      },
      // The export's source fields, as the courier writes them; the ingest
      // requires each to equal the signed values.
      extensions: {
        "96bulls": { session_uuid: "019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c19", session_uuid_source: "minted", tenant_id: ORG, user_id: USER },
        "client.relayhistory_export": { version: 1, source: "neon.delivery_records", exported_at: AT, grant_id: GRANT, plan_digest: PLAN, source_user_id: USER },
      },
    };
    const chunks = [{
      seq: 0, epoch: 0, start_offset: 0, bytes: SOURCE.byteLength,
      sha256: createHash("sha256").update(SOURCE).digest("hex"), records: sealed.entry.records ?? 0,
    }];
    const entry: PlanEntry = {
      grant_id: GRANT, key: manifestKey(LOCATION), org_id: ORG, workspace_id: WORKSPACE, source_user_id: USER,
      session: { machine_id: LOCATION.machineId, harness: LOCATION.harness, session_id: LOCATION.sessionId, started_at: AT },
      state: "closed", chunks, content_sha256: await computeContentSha256(chunks), raw_bytes: SOURCE.byteLength,
    };
    return { manifest, entryText: canonicalJson(entry), upload: await uploadIdentityOf(entry, PLAN) };
  } finally {
    await rm(scratch, { recursive: true, force: true });
  }
}

async function code(response: Response) {
  return ((await response.json()) as { error?: { code?: string } }).error?.code;
}

const MIGRATION_ON = { RELAYHISTORY_ACCEPT_MIGRATION_ASSERTIONS: "true", RELAYHISTORY_MIGRATION_GRANTS: `${GRANT}=${PLAN}` };

describe("buildIngestApp auth (production composition)", () => {
  test("default config: a generic RelayAuth bearer is refused (401) on every upload route, and so is a migration token", async () => {
    const { upload, manifest } = await planned();
    const i = await ingest();
    for (const token of [genericToken(), genericToken([ORIGIN]), migrationToken(upload)]) {
      for (const response of [await i.chunk(token), await i.commit(token, manifest)]) expect(response.status).toBe(401);
    }
  });

  test("with the migration flag on (interim off): a generic bearer is still 401, a migration token is accepted under its full contract", async () => {
    const { upload, manifest, entryText } = await planned();
    const i = await ingest(MIGRATION_ON);
    for (const response of [await i.chunk(genericToken()), await i.chunk(genericToken([ORIGIN])), await i.commit(genericToken(), manifest)])
      expect(response.status).toBe(401);
    const token = migrationToken(upload);
    const chunk = await i.chunk(token, LOCATION, { entry: entryText });
    expect(chunk.status).toBe(201);
    const commit = await i.commit(token, manifest);
    expect(commit.status).toBe(202);
    const body = (await commit.json()) as { workKey: string };
    // The work key is <signed manifest key>@<signed entry_sha256>.
    expect(body.workKey).toBe(`${upload.key}@${upload.entry_sha256}`);
    expect(await count("sessions.migration_assertion_uses")).toBe(1);
    expect(await count("sessions.pipeline_jobs")).toBe(1);
    expect(await count("sessions.migration_assertion_chunks")).toBe(1);
  });

  test("with the interim bearer explicitly on and an origin set: a generic bearer is still refused on the upload routes", async () => {
    const { manifest } = await planned();
    for (const extra of [
      { ALLOW_INTERIM_RELAYAUTH_BEARER: "true" },
      { ALLOW_INTERIM_RELAYAUTH_BEARER: "true", RELAYHISTORY_ACCEPT_LEGACY_ASSERTIONS: "true" },
    ] as Record<string, string>[]) {
      const i = await ingest({ ...MIGRATION_ON, ...extra });
      for (const response of [
        await i.chunk(genericToken()),
        await i.commit(genericToken(), manifest),
        await i.req(`/v1/pipeline/manifests/status?key=${encodeURIComponent(manifestKey(LOCATION))}`, genericToken()),
      ]) {
        expect([401, 403]).toContain(response.status);
      }
      // With legacy assertions accepted the generic JWT verifies, and the
      // upload-route guard still refuses it.
      if (extra.RELAYHISTORY_ACCEPT_LEGACY_ASSERTIONS) {
        const legacy = await i.chunk(genericToken());
        expect(legacy.status).toBe(403);
        expect(await code(legacy)).toBe("origin_bound_assertion_required");
      }
    }
  });

  test("refuses each contract violation end to end", async () => {
    const { upload, manifest, entryText } = await planned();
    const i = await ingest(MIGRATION_ON);
    const fresh = { ...LOCATION, machineId: "machine-fresh" };
    const otherContent = structuredClone(manifest);
    (otherContent.raw.chunks[0] as { records?: number }).records = 42;
    const cases: [string, number, () => Promise<Response>][] = [
      // An arbitrary fresh machine key, on chunks, status and the commit.
      ["migration_upload_forbidden", 403, () => i.chunk(migrationToken(upload), fresh, { entry: entryText })],
      // Per-chunk: an in-plan key, but bytes or seq outside the signed entry, or no entry.
      ["migration_upload_forbidden", 403, () => i.chunk(migrationToken(upload), LOCATION, { entry: entryText, bytes: new TextEncoder().encode('{"type":"user","text":"evil"}\n') })],
      ["migration_upload_forbidden", 403, () => i.chunk(migrationToken(upload), LOCATION, { entry: entryText, seq: 1 })],
      ["migration_upload_forbidden", 403, () => i.chunk(migrationToken(upload), LOCATION)],
      ["migration_upload_forbidden", 403, () => i.chunk(migrationToken(upload), LOCATION, { entry: entryText.replace('"closed"', '"open"') })],
      // Provenance: 96bulls.user_id must be the signed source user.
      ["migration_upload_forbidden", 403, () => i.commit(migrationToken(upload), { ...manifest, extensions: { ...manifest.extensions, "96bulls": { user_id: "user_ingest_b" } } })],
      ["migration_upload_forbidden", 403, () => i.commit(migrationToken(upload), { ...manifest, extensions: undefined })],
      ["migration_upload_forbidden", 403, () => i.commit(migrationToken(upload), { ...manifest, extensions: { "96bulls": manifest.extensions["96bulls"] } })],
      ["migration_upload_forbidden", 403, () => i.req(`/v1/pipeline/manifests/status?key=${encodeURIComponent(manifestKey(fresh))}`, migrationToken(upload))],
      ["migration_upload_forbidden", 403, () => i.commit(migrationToken(upload), { ...manifest, session: { ...manifest.session, machine_id: "machine-fresh" } }, manifestKey(fresh))],
      // Other content than the signed identity.
      ["migration_upload_forbidden", 403, () => i.commit(migrationToken(upload), otherContent)],
      // Another user's identity under this user's token.
      ["migration_upload_forbidden", 403, () => i.chunk(migrationToken({ ...upload, source_user_id: "user_ingest_b" }), LOCATION, { entry: entryText })],
      // Signed pair is not the grant pair.
      ["migration_workspace_forbidden", 403, () => i.chunk(migrationToken(upload, {}, { workspaceId: "ws_other" }))],
      // A grant this ingest does not accept.
      ["migration_grant_forbidden", 403, () => i.chunk(migrationToken(upload, {}, { grantId: "mgr_other", upload: { ...upload, grant_id: "mgr_other" } }))],
      // A relay_ alias in the identity: not canonical.
      ["invalid_claims", 401, () => i.chunk(migrationToken({ ...upload, source_user_id: `relay_${USER}` }))],
      // Expired: a distinct, re-mintable code.
      ["migration_assertion_expired", 401, () => i.chunk(migrationToken(upload, { iat: now() - 56, exp: now() - 6 }))],
      // A bad signature stays terminal.
      ["invalid_token", 401, () => { const [h, b] = migrationToken(upload).split("."); return i.chunk(`${h}.${b}.${"A".repeat(342)}`); }],
      // Outside the three upload routes.
      ["migration_assertion_route", 403, () => i.req("/v1/pipeline/events", migrationToken(upload), { method: "POST", body: "{}" })],
    ];
    for (const [expected, status, run] of cases) {
      const response = await run();
      expect(`${response.status} ${await code(response)}`).toBe(`${status} ${expected}`);
    }
    expect(await count("sessions.migration_assertion_uses")).toBe(0);
    expect(await count("sessions.pipeline_jobs")).toBe(0);
    expect(await count("sessions.migration_assertion_chunks")).toBe(0);

    // Replay: the same assertion after its commit is 409 on every route.
    const token = migrationToken(upload);
    expect((await i.chunk(token, LOCATION, { entry: entryText })).status).toBe(201);
    expect((await i.commit(token, manifest)).status).toBe(202);
    for (const response of [await i.commit(token, manifest), await i.chunk(token, LOCATION, { entry: entryText })]) {
      expect(response.status).toBe(409);
      expect(await code(response)).toBe("migration_assertion_replayed");
    }
    expect(await count("sessions.migration_assertion_uses")).toBe(1);
    expect(await count("sessions.pipeline_jobs")).toBe(1);
  });

  test("E2E: route, enqueue, consume object-landed, then session-indexed, for a migration job and a live job", async () => {
    const i = await ingest({ ...MIGRATION_ON, ALLOW_INTERIM_RELAYAUTH_BEARER: "true" });
    // Migration upload.
    const { upload, manifest, entryText } = await planned();
    const token = migrationToken(upload);
    expect((await i.chunk(token, LOCATION, { entry: entryText })).status).toBe(201);
    const migrated = await i.commit(token, manifest);
    expect(migrated.status).toBe(202);
    const migratedKey = ((await migrated.json()) as { workKey: string }).workKey;
    expect(migratedKey).toBe(`${upload.key}@${upload.entry_sha256}`);
    // Live upload of another session (origin-bound courier assertion).
    const liveLocation = { ...LOCATION, sessionId: "session-live" };
    const live = await planned(liveLocation, liveLocation.machineId);
    const liveManifest = { ...live.manifest, extensions: undefined };
    expect((await i.chunk(liveToken(), liveLocation)).status).toBe(201);
    const liveCommit = await i.req("/v1/pipeline/manifests", liveToken(), {
      method: "POST",
      headers: { "content-type": "application/json", "x-machine-id": liveLocation.machineId, "x-session-id": liveLocation.sessionId },
      body: JSON.stringify({ key: manifestKey(liveLocation), manifest: liveManifest }),
    });
    expect(liveCommit.status).toBe(202);
    const liveKey = ((await liveCommit.json()) as { workKey: string }).workKey;
    expect(liveKey.startsWith(`${manifestKey(liveLocation)}@`)).toBe(true);
    expect(liveKey).not.toBe(`${manifestKey(liveLocation)}@${upload.entry_sha256}`);

    // Consume both through the real stage handlers on the same Postgres queue.
    const sql = pgExecutor();
    const indexed: string[] = [];
    const deadLetters = postgresDeadLetterStore(sql);
    const deps = pipelineQueueDeps({} as Bindings, {
      queue: postgresPipelineQueue(sql),
      sink: sqlPipelineEventSink(sql),
      deadLetters,
      index: { upsertRawPointer: async (pointer: { manifestKey: string }) => void indexed.push(pointer.manifestKey) } as never,
    });
    expect(await pollPostgresStage({ sql, stage: "object-landed", deps })).toMatchObject({ claimed: 2, acked: 2, retried: 0, exhausted: 0 });
    const { rows } = await db.query<{ stage: string; work_key: string }>("SELECT stage, work_key FROM sessions.pipeline_jobs ORDER BY id");
    expect(rows.map((r) => r.stage)).toEqual(["session-indexed", "session-indexed"]);
    expect(rows.map((r) => r.work_key).sort()).toEqual([migratedKey, liveKey].sort());
    expect(await pollPostgresStage({ sql, stage: "session-indexed", deps })).toMatchObject({ claimed: 2, acked: 2, exhausted: 0 });
    expect(indexed.sort()).toEqual([manifestKey(LOCATION), manifestKey(liveLocation)].sort());
    expect(await count("sessions.pipeline_jobs")).toBe(0);
    expect(await deadLetters.list({ stage: "object-landed" })).toEqual([]);
    const { rows: errors } = await db.query("SELECT 1 FROM sessions.pipeline_events WHERE error->>'code' = 'invalid_work_key' OR status IN ('error', 'dead_lettered')");
    expect(errors).toHaveLength(0);
  });

  test("v2 backfill (O4): a signed backfill epoch appended to a live ndjson session commits and is consumed; every base or chunk deviation is refused", async () => {
    const i = await ingest({ ...MIGRATION_ON, ALLOW_INTERIM_RELAYAUTH_BEARER: "true" });
    const live = { ...LOCATION, sessionId: "session-straddle" };
    const key = manifestKey(live);
    const record = (n: number) => new TextEncoder().encode(`${JSON.stringify({ kind: "event", record_id: `r${n}`, origin_id: "o1", revision: 1, received_at: AT, payload: { n } })}\n`);
    // The live session: one ndjson chunk, owned by its machine, with the user in its 96bulls extension.
    const liveChunk = await i.chunk(liveToken(), live, { bytes: record(1), format: "ndjson" });
    expect(liveChunk.status).toBe(201);
    const liveSealed = (await liveChunk.json()) as { entry: Record<string, unknown> & { bytes: number; stored_bytes: number }; scrub: unknown };
    const liveManifest = {
      schema_v: 1, layout: "v1", trace_id: TRACE, revision: 1, created_at: AT, updated_at: AT,
      session: { workspace_id: WORKSPACE, machine_id: live.machineId, harness: live.harness, session_id: live.sessionId, started_at: AT },
      writer: { delivery_instance_id: live.machineId, plugin_version: "test" },
      raw: {
        owner: live.machineId, state: "open", updated_at: AT, format: "ndjson", compression: "zstd", chunk_count: 1,
        raw_bytes: liveSealed.entry.bytes, stored_bytes: liveSealed.entry.stored_bytes,
        content_sha256: await computeContentSha256([liveSealed.entry as never]), epoch: 0,
        epochs: [{ epoch: 0, opened_at: AT, reason: "initial", first_seq: 0, opened_by: live.machineId }],
        next_offset: liveSealed.entry.bytes, chunks: [liveSealed.entry], scrub: liveSealed.scrub,
      },
      extensions: { "96bulls": { session_uuid: "019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c19", session_uuid_source: "minted", tenant_id: ORG, user_id: USER } },
    };
    const liveCommit = await i.req("/v1/pipeline/manifests", liveToken(), {
      method: "POST",
      headers: { "content-type": "application/json", "x-machine-id": live.machineId, "x-session-id": live.sessionId },
      body: JSON.stringify({ key, manifest: liveManifest }),
    });
    expect(liveCommit.status).toBe(202);
    const liveVersion = ((await liveCommit.json()) as { version: { eTag?: string; version?: string } }).version;

    // The plan's v2 entry: two backfill source chunks, offsets from 0.
    const sources = [record(100), record(101)];
    const chunks = sources.map((b, n) => ({
      start_offset: n === 0 ? 0 : sources[0]!.byteLength,
      bytes: b.byteLength,
      sha256: createHash("sha256").update(b).digest("hex"),
      records: 1,
    }));
    const entry: PlanEntryV2 = {
      v: 2, kind: "backfill", grant_id: GRANT, key, org_id: ORG, workspace_id: WORKSPACE, source_user_id: USER,
      session: { machine_id: live.machineId, harness: live.harness, session_id: live.sessionId, started_at: AT },
      format: "ndjson", chunks, backfill_sha256: await backfillSha256(chunks),
      raw_bytes: sources.reduce((n, b) => n + b.byteLength, 0),
    };
    const entryText = canonicalJson(entry);
    const base: BackfillBase = {
      revision: 1, version: String(liveVersion.version ?? liveVersion.eTag), owner: live.machineId, writer: live.machineId,
      chunk_count: 1, epoch: 0, content_sha256: liveManifest.raw.content_sha256,
    };
    const upload = await uploadIdentityV2Of(entry, PLAN, base);
    expect(upload.entry_sha256).toBe(await planEntryV2Sha256(entry));

    // Chunk negatives: wrong seq (not base.chunk_count + i), wrong epoch, wrong bytes, a v1 entry text.
    for (const [name, opts] of [
      ["seq 0 (inside the live range)", { seq: 0, epoch: 1, bytes: sources[0] }],
      ["epoch 0 (not base.epoch + 1)", { seq: 1, epoch: 0, bytes: sources[0] }],
      ["other bytes", { seq: 1, epoch: 1, bytes: record(999) }],
      ["chunk 1's bytes at chunk 0's seq", { seq: 1, epoch: 1, bytes: sources[1], offset: 0 }],
    ] as const) {
      const r = await i.chunk(migrationToken(upload), live, { entry: entryText, format: "ndjson", ...opts });
      expect(`${name}: ${r.status}`).toBe(`${name}: 403`);
    }
    // The planned backfill chunks, at seq = base.chunk_count + i, epoch = base.epoch + 1.
    const token = migrationToken(upload);
    const sealed: Record<string, unknown>[] = [];
    for (const [n, bytes] of sources.entries()) {
      const r = await i.chunk(token, live, { entry: entryText, seq: base.chunk_count + n, epoch: base.epoch + 1, offset: chunks[n]!.start_offset, bytes, format: "ndjson" });
      expect(r.status).toBe(201);
      sealed.push(((await r.json()) as { entry: Record<string, unknown> }).entry);
    }
    const added = sealed as unknown as { bytes: number; stored_bytes: number; start_offset: number }[];
    const appended = {
      ...liveManifest,
      revision: 2,
      raw: {
        ...liveManifest.raw,
        chunk_count: 3,
        raw_bytes: liveManifest.raw.raw_bytes + added.reduce((n, c) => n + c.bytes, 0),
        stored_bytes: liveManifest.raw.stored_bytes + added.reduce((n, c) => n + c.stored_bytes, 0),
        content_sha256: await computeContentSha256([liveSealed.entry, ...sealed] as never),
        epoch: 1,
        epochs: [...liveManifest.raw.epochs, { epoch: 1, opened_at: AT, reason: "backfill", first_seq: 1, opened_by: `migration:${GRANT}` }],
        next_offset: added.at(-1)!.start_offset + added.at(-1)!.bytes,
        chunks: [liveSealed.entry, ...sealed],
      },
      extensions: {
        ...liveManifest.extensions,
        "client.relayhistory_export": { version: 1, source: "neon.delivery_records", exported_at: AT, grant_id: GRANT, plan_digest: PLAN, source_user_id: USER },
      },
    };
    const post = (t: string, manifest: unknown, previousVersion: unknown = liveVersion) =>
      i.req("/v1/pipeline/manifests", t, {
        method: "POST",
        headers: { "content-type": "application/json", "x-machine-id": live.machineId, "x-session-id": live.sessionId },
        body: JSON.stringify({ key, manifest, previousVersion }),
      });
    // Commit negatives, each with a fresh assertion: a base that is not the stored one, other content, a second epoch.
    const wrongBase = async (b: Partial<BackfillBase>) => migrationToken(await uploadIdentityV2Of(entry, PLAN, { ...base, ...b }));
    for (const [name, t, m, expected] of [
      ["base revision", await wrongBase({ revision: 2 }), appended, 403],
      ["base version", await wrongBase({ version: '"stale"' }), appended, 403],
      ["base chunk_count", await wrongBase({ chunk_count: 0 }), appended, 403],
      ["base owner", await wrongBase({ owner: "machine-other" }), appended, 403],
      // (Inconsistent manifests fail validation first: 400; still refused, nothing consumed.)
      ["a missing backfill chunk", token, { ...appended, raw: { ...appended.raw, chunks: appended.raw.chunks.slice(0, 2), chunk_count: 2 } }, "400|403"],
      ["the epoch not opened by the grant", token, { ...appended, raw: { ...appended.raw, epochs: [liveManifest.raw.epochs[0], { ...appended.raw.epochs[1], opened_by: "machine-a" }] } }, "400|403"],
    ] as const) {
      const r = await post(t, m);
      expect(`${name}: ${String(expected).split("|").includes(String(r.status))}`).toBe(`${name}: true`);
    }
    // The stale CAS version is refused too (409), nothing consumed.
    expect((await post(token, appended, { eTag: '"stale"' })).status).toBe(409);
    expect(await count("sessions.migration_assertion_uses")).toBe(0);

    const commit = await post(token, appended);
    expect(commit.status).toBe(202);
    const workKey = ((await commit.json()) as { workKey: string }).workKey;
    expect(workKey).toBe(`${key}@${upload.entry_sha256}@r2`);

    const sql = pgExecutor();
    const indexed: string[] = [];
    const deadLetters = postgresDeadLetterStore(sql);
    const deps = pipelineQueueDeps({} as Bindings, {
      queue: postgresPipelineQueue(sql), sink: sqlPipelineEventSink(sql), deadLetters,
      index: { upsertRawPointer: async (pointer: { manifestKey: string }) => void indexed.push(pointer.manifestKey) } as never,
    });
    // The live commit and the backfill commit.
    expect(await pollPostgresStage({ sql, stage: "object-landed", deps })).toMatchObject({ claimed: 2, acked: 2, exhausted: 0 });
    expect(await pollPostgresStage({ sql, stage: "session-indexed", deps })).toMatchObject({ claimed: 2, acked: 2, exhausted: 0 });
    expect(indexed).toEqual([key, key]);
    const letters = await deadLetters.list({ stage: "object-landed" });
    expect(letters.filter((l) => JSON.stringify(l).includes("session-straddle"))).toEqual([]);
  });

  test("signed work key and durable audit: posted fields cannot alter the work key; one audit row per commit, no secrets", async () => {
    const { upload, manifest, entryText } = await planned();
    const i = await ingest(MIGRATION_ON);
    const auditBefore = await count("sessions.migration_ingest_audit");
    const token = migrationToken(upload);
    expect((await i.chunk(token, LOCATION, { entry: entryText })).status).toBe(201);
    const commit = await i.req("/v1/pipeline/manifests", token, {
      method: "POST",
      headers: { "content-type": "application/json", "x-machine-id": LOCATION.machineId, "x-session-id": LOCATION.sessionId },
      body: JSON.stringify({ key: manifestKey(LOCATION), manifest, etag: "attacker", version: "attacker", previousVersion: { eTag: "attacker" } }),
    });
    expect(commit.status).toBe(202);
    const { workKey, version } = (await commit.json()) as { workKey: string; version: { eTag?: string; version?: string } };
    // Signed-only: the signed key and entry digest, not the storage version.
    expect(workKey).toBe(`${upload.key}@${upload.entry_sha256}`);
    expect(workKey).not.toContain("attacker");
    expect(workKey).not.toContain(String(version.version ?? version.eTag).replace(/"/g, ""));
    expect(await count("sessions.migration_ingest_audit")).toBe(auditBefore + 1);
    const { rows } = await db.query<Record<string, unknown>>("SELECT * FROM sessions.migration_ingest_audit ORDER BY id DESC LIMIT 1");
    expect(rows[0]).toMatchObject({ grant_id: GRANT, plan_digest: PLAN, entry_sha256: upload.entry_sha256, work_key: workKey, user_id: USER, result: "committed" });
    const text = JSON.stringify(rows[0]);
    for (const secret of [token, token.split(".")[2]!, "eyJ", new TextDecoder().decode(SOURCE).trim()]) expect(text).not.toContain(secret);
  });
});
