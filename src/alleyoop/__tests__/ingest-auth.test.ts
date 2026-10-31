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
import { canonicalJson, uploadIdentityOf, type PlanEntry, type UploadIdentity } from "@rhc/lib/pipeline/migration-assertions.ts";
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
import { runCourierOnce, type CourierConfig } from "../courier.ts";
import { DatabaseSync } from "node:sqlite";
import { writeFile } from "node:fs/promises";
import { indexerReconcileRoot } from "../../hoopdb/reconcile-root.ts";
import { objectKey, prefixAllowed, reconcileManifests, objectStoreManifestReader, orgResolverFromEnv } from "../../shared/vendor.ts";

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

function migrationToken(upload: UploadIdentity, overrides: Record<string, unknown> = {}, meta: Record<string, unknown> = {}) {
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
const liveToken = (machineId = LOCATION.machineId) =>
  sign({ sub: "agent_relayhistory_assertion", org: ORG, wks: WORKSPACE, sponsorId: USER, scopes: ["rth:sync"], token_type: "access", iss: ISSUER, aud: [ORIGIN], iat: now(), exp: now() + 600, meta: { machineId, tokenClass: "relayhistory_assertion" } });

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
  // The legacy machine-claim window: these tokens predate the claim.
  if (extra.CLAIMLESS_WINDOW !== "shut") {
    process.env.MANIFEST_MACHINE_CLAIM = "compat";
    process.env.MANIFEST_MACHINE_CLAIM_COMPAT_UNTIL = "2099-01-01T00:00:00.000Z";
  }
  delete process.env.ALLOW_INTERIM_RELAYAUTH_BEARER;
  if (extra.ALLOW_INTERIM_RELAYAUTH_BEARER) process.env.ALLOW_INTERIM_RELAYAUTH_BEARER = extra.ALLOW_INTERIM_RELAYAUTH_BEARER;
  const store = createFsStore({ root });
  const { app, env } = await buildIngestApp({ db: pgExecutor(), raw: { store, provider: "fs", bucket: "raw" } as never });
  const req = (path: string, token: string, init: RequestInit = {}) =>
    app.request(path, { ...init, headers: { authorization: `Bearer ${token}`, ...(init.headers as Record<string, string>) } }, env);
  /** A chunk request; a migration one carries its plan entry text in front of the bytes. */
  const chunk = (token: string, location: SessionLocation = LOCATION, opts: { entry?: string; seq?: number; bytes?: Uint8Array } = {}) => {
    const bytes = opts.bytes ?? SOURCE;
    const entry = opts.entry === undefined ? undefined : new TextEncoder().encode(opts.entry);
    const body = entry ? Buffer.concat([entry, bytes]) : bytes;
    return req(`/v1/pipeline/chunks?machine=${location.machineId}&harness=${location.harness}&session=${location.sessionId}&date=${location.startDate}&seq=${opts.seq ?? 0}&epoch=0&offset=0&format=jsonl`, token, {
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
  return { req, chunk, commit, app, env, store };
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
  test("the claim switch reaches the route from the real environment, and defaults shut", async () => {
    const shut = await ingest({ ALLOW_INTERIM_RELAYAUTH_BEARER: "true", CLAIMLESS_WINDOW: "shut" });
    const { manifest } = await planned();
    const claimless = sign({ sub: "agent_relayhistory_assertion", org: ORG, wks: WORKSPACE, sponsorId: USER, scopes: ["rth:sync"], token_type: "access", iss: ISSUER, aud: [ORIGIN], iat: now(), exp: now() + 600, meta: { tokenClass: "relayhistory_assertion" } });
    expect((await shut.commit(claimless, manifest)).status).toBe(403);
    expect(await count("sessions.claimless_uses")).toBe(0);
  });

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
    expect((await i.chunk(liveToken(liveLocation.machineId), liveLocation)).status).toBe(201);
    const liveCommit = await i.req("/v1/pipeline/manifests", liveToken(liveLocation.machineId), {
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

  test("E2E under a deployment key prefix: the correct prefix is accepted through session-indexed; a wrong or absent prefix is refused, fail-closed", async () => {
    const PREFIX = "deploy-a";
    const i = await ingest({ ...MIGRATION_ON, ALLOW_INTERIM_RELAYAUTH_BEARER: "true", PIPELINE_KEY_PREFIX: PREFIX });
    const { upload, manifest, entryText } = await planned();
    const prefixedKey = `${PREFIX}/${manifestKey(LOCATION)}`;
    // Negative first: a wrong or extra client-supplied prefix is refused before
    // any write or consume. (An absent prefix is applied by the server.)
    for (const key of [`deploy-b/${manifestKey(LOCATION)}`, `${PREFIX}/extra/${manifestKey(LOCATION)}`, `${PREFIX}/${PREFIX}/${manifestKey(LOCATION)}`]) {
      const refused = await i.commit(migrationToken(upload), manifest, key);
      expect(`${key}: ${refused.status}`).toBe(`${key}: 403`);
    }
    expect(await count("sessions.pipeline_jobs")).toBe(0);
    expect(await count("sessions.migration_assertion_uses")).toBe(0);
    // Positive: chunks land under the prefix (server-chosen), the commit's
    // seal check looks there, and the job is consumed through session-indexed.
    const token = migrationToken(upload);
    const chunk = await i.chunk(token, LOCATION, { entry: entryText });
    expect(chunk.status).toBe(201);
    expect(((await chunk.json()) as { key: string }).key.startsWith(`${PREFIX}/v1/`)).toBe(true);
    const commit = await i.commit(token, manifest, prefixedKey);
    expect(commit.status).toBe(202);
    const workKey = ((await commit.json()) as { workKey: string }).workKey;
    // The signed (unprefixed) key and entry digest; the prefix is deployment config, not identity.
    expect(workKey).toBe(`${upload.key}@${upload.entry_sha256}`);
    // A live session under the same prefix.
    const liveLocation = { ...LOCATION, sessionId: "session-live" };
    const live = await planned(liveLocation, liveLocation.machineId);
    expect((await i.chunk(liveToken(liveLocation.machineId), liveLocation)).status).toBe(201);
    const liveCommit = await i.req("/v1/pipeline/manifests", liveToken(liveLocation.machineId), {
      method: "POST",
      headers: { "content-type": "application/json", "x-machine-id": liveLocation.machineId, "x-session-id": liveLocation.sessionId },
      body: JSON.stringify({ key: `${PREFIX}/${manifestKey(liveLocation)}`, manifest: { ...live.manifest, extensions: undefined } }),
    });
    expect(liveCommit.status).toBe(202);

    const sql = pgExecutor();
    const indexed: string[] = [];
    const deadLetters = postgresDeadLetterStore(sql);
    const deps = pipelineQueueDeps({} as Bindings, {
      queue: postgresPipelineQueue(sql),
      sink: sqlPipelineEventSink(sql),
      deadLetters,
      index: { upsertRawPointer: async (pointer: { manifestKey: string }) => void indexed.push(pointer.manifestKey) } as never,
    });
    expect(await pollPostgresStage({ sql, stage: "object-landed", deps })).toMatchObject({ claimed: 2, acked: 2, exhausted: 0 });
    expect(await pollPostgresStage({ sql, stage: "session-indexed", deps })).toMatchObject({ claimed: 2, acked: 2, exhausted: 0 });
    expect(indexed.sort()).toEqual([prefixedKey, `${PREFIX}/${manifestKey(liveLocation)}`].sort());
    expect(await deadLetters.list({ stage: "object-landed" })).toEqual([]);

    // Consumer fail-closed: a migration job whose signed key is not this
    // object's unprefixed key (here: the prefixed key itself, or another
    // session) is refused (invalid_work_key, dead-lettered), never run as live.
    for (const bad of [prefixedKey, manifestKey({ ...LOCATION, sessionId: "session-other" })]) {
      await postgresPipelineQueue(sql).send({
        v: 1, stage: "object-landed",
        ctx: { traceId: TRACE, orgId: ORG, workspaceId: WORKSPACE },
        workKey: `${bad}@${upload.entry_sha256}`,
        payload: { key: prefixedKey, kind: "manifest", etag: "e1", migration: { key: bad, entry_sha256: upload.entry_sha256 } },
      } as never);
    }
    const refused = await pollPostgresStage({ sql, stage: "object-landed", deps });
    expect(refused.acked + refused.exhausted).toBe(2);
    expect(await count("sessions.pipeline_jobs")).toBe(0);
    expect(indexed).toHaveLength(2);
    const letters = await deadLetters.list({ stage: "object-landed" });
    expect(letters).toHaveLength(2);
  });

  test("prefixed GET manifests/status: the correct prefix reads the signed upload's own status; absent or wrong prefixes are refused", async () => {
    const PREFIX = "deploy-a";
    const i = await ingest({ ...MIGRATION_ON, PIPELINE_KEY_PREFIX: PREFIX });
    const { upload, manifest, entryText } = await planned();
    const prefixedKey = `${PREFIX}/${manifestKey(LOCATION)}`;
    const writer = encodeURIComponent(`migration:${GRANT}`);
    const status = (token: string, key: string) =>
      i.req(`/v1/pipeline/manifests/status?key=${encodeURIComponent(key)}&machine=${LOCATION.machineId}&writer=${writer}`, token);
    // Not yet stored: 404 under the correct prefix.
    expect((await status(migrationToken(upload), prefixedKey)).status).toBe(404);
    const token = migrationToken(upload);
    expect((await i.chunk(token, LOCATION, { entry: entryText })).status).toBe(201);
    expect((await i.commit(token, manifest, prefixedKey)).status).toBe(202);
    // A fresh assertion for the same upload reads its status under the prefix.
    const own = await status(migrationToken(upload), prefixedKey);
    expect(own.status).toBe(200);
    expect(((await own.json()) as { version: { eTag?: string } }).version.eTag).toBeTruthy();
    // The unprefixed key (the courier's shape) is served under the server's prefix.
    expect((await status(migrationToken(upload), manifestKey(LOCATION))).status).toBe(200);
    // A wrong or extra client prefix: refused (403), never served.
    for (const key of [`deploy-b/${manifestKey(LOCATION)}`, `${PREFIX}/extra/${manifestKey(LOCATION)}`]) {
      const refused = await status(migrationToken(upload), key);
      expect(`${key}: ${refused.status}`).toBe(`${key}: 403`);
    }
    // Another session's key under the right prefix: not the signed upload.
    const other = await status(migrationToken(upload), `${PREFIX}/${manifestKey({ ...LOCATION, sessionId: "session-other" })}`);
    expect(other.status).toBe(403);
    expect(await code(other)).toBe("migration_upload_forbidden");
  });

  test("org-derived prefixes (PIPELINE_ORG_FROM_PREFIX through the real composition): the token's own org prefix is accepted end to end; another org's prefix is refused", async () => {
    const i = await ingest({ ...MIGRATION_ON, PIPELINE_ORG_FROM_PREFIX: "true", TENANT_ORG_ID: "" });
    const { upload, manifest, entryText } = await planned();
    const ownKey = `${ORG}/${manifestKey(LOCATION)}`;
    // A token for org A with a key under org B's prefix: refused before any write.
    for (const key of [`org_other/${manifestKey(LOCATION)}`, `x/${ORG}/${manifestKey(LOCATION)}`]) {
      const refused = await i.commit(migrationToken(upload), manifest, key);
      expect(`${key}: ${refused.status}`).toBe(`${key}: 403`);
    }
    // Org B's token (its own identity) cannot write under org A's prefix either.
    const orgBUpload = await uploadIdentityOf(
      { ...(JSON.parse(entryText) as PlanEntry), org_id: "org_other" },
      PLAN,
    );
    const orgB = await i.commit(migrationToken(orgBUpload, { org: "org_other" }), manifest, ownKey);
    expect(orgB.status).toBe(403);
    expect(await count("sessions.pipeline_jobs")).toBe(0);
    // The token's own org prefix: chunk under it, commit, consume through session-indexed.
    const token = migrationToken(upload);
    const chunk = await i.chunk(token, LOCATION, { entry: entryText });
    expect(chunk.status).toBe(201);
    expect(((await chunk.json()) as { key: string }).key.startsWith(`${ORG}/v1/`)).toBe(true);
    const commit = await i.commit(token, manifest, ownKey);
    expect(commit.status).toBe(202);
    const sql = pgExecutor();
    const indexed: string[] = [];
    const deadLetters = postgresDeadLetterStore(sql);
    const deps = pipelineQueueDeps({ PIPELINE_ORG_FROM_PREFIX: "true" } as Bindings, {
      queue: postgresPipelineQueue(sql),
      sink: sqlPipelineEventSink(sql),
      deadLetters,
      index: { upsertRawPointer: async (pointer: { manifestKey: string }) => void indexed.push(pointer.manifestKey) } as never,
    });
    expect(await pollPostgresStage({ sql, stage: "object-landed", deps })).toMatchObject({ claimed: 1, acked: 1, exhausted: 0 });
    expect(await pollPostgresStage({ sql, stage: "session-indexed", deps })).toMatchObject({ claimed: 1, acked: 1, exhausted: 0 });
    expect(indexed).toEqual([ownKey]);
    // No dead letter for this upload (the shared test database may hold
    // letters from the forged jobs of the static-prefix test).
    const letters = await deadLetters.list({ stage: "object-landed" });
    expect(letters.filter((l) => JSON.stringify(l).includes(`${ORG}/v1/`))).toEqual([]);
  });

  test("combined base + org-derived prefix: exactly <base>/<caller org> is accepted end to end; foreign, extra and bare prefixes are refused", async () => {
    const BASE = "deploy-a";
    const i = await ingest({ ...MIGRATION_ON, PIPELINE_KEY_PREFIX: BASE, PIPELINE_ORG_FROM_PREFIX: "true" });
    const { upload, manifest, entryText } = await planned();
    const exact = `${BASE}/${ORG}/${manifestKey(LOCATION)}`;
    for (const key of [
      `deploy-b/${ORG}/${manifestKey(LOCATION)}`, // foreign base
      `${BASE}/extra/${ORG}/${manifestKey(LOCATION)}`, // extra segment
      `${ORG}/${manifestKey(LOCATION)}`, // bare org (no base)
      `${BASE}/org_other/${manifestKey(LOCATION)}`, // another org
      `${BASE}/${manifestKey(LOCATION)}`, // base, no org
    ]) {
      const refused = await i.commit(migrationToken(upload), manifest, key);
      expect(`${key}: ${refused.status}`).toBe(`${key}: 403`);
    }
    expect(await count("sessions.pipeline_jobs")).toBe(0);
    const token = migrationToken(upload);
    const chunk = await i.chunk(token, LOCATION, { entry: entryText });
    expect(((await chunk.json()) as { key: string }).key.startsWith(`${BASE}/${ORG}/v1/`)).toBe(true);
    // The courier's unprefixed key: the server applies exactly <base>/<org>.
    const commit = await i.commit(token, manifest);
    expect(commit.status).toBe(202);
    expect(await i.store.head(objectKey(exact))).toBeTruthy();
    const { rows } = await db.query<{ key: string }>("SELECT message->'payload'->>'key' AS key FROM sessions.pipeline_jobs");
    expect(rows.map((r) => r.key)).toEqual([exact]);
  });

  test("prefixAllowed: exact <base>/<org>, static base, and bare-org rules", () => {
    const org = { PIPELINE_ORG_FROM_PREFIX: "true", PIPELINE_KEY_PREFIX: "deploy-a" };
    expect(prefixAllowed(org, "deploy-a/org_a", "org_a")).toBe(true);
    for (const p of ["deploy-b/org_a", "deploy-a/extra/org_a", "org_a", "deploy-a/org_b", "deploy-a", undefined, "deploy-a/org_a/x"])
      expect(`${p}: ${prefixAllowed(org, p, "org_a")}`).toBe(`${p}: false`);
    const bare = { PIPELINE_ORG_FROM_PREFIX: "true" };
    expect(prefixAllowed(bare, "org_a", "org_a")).toBe(true);
    for (const p of ["x/org_a", "org_b", undefined]) expect(prefixAllowed(bare, p, "org_a")).toBe(false);
    const fixed = { PIPELINE_KEY_PREFIX: "/deploy-a/" };
    expect(prefixAllowed(fixed, "deploy-a", "org_a")).toBe(true);
    for (const p of ["deploy-b", "deploy-a/org_a", undefined]) expect(prefixAllowed(fixed, p, "org_a")).toBe(false);
    expect(prefixAllowed({}, undefined, "org_a")).toBe(true);
    expect(prefixAllowed({}, "deploy-a", "org_a")).toBe(false);
  });

  for (const [mode, extra, prefix] of [
    ["static prefix", { PIPELINE_KEY_PREFIX: "deploy-a" }, "deploy-a"],
    ["org-derived prefix with a base", { PIPELINE_KEY_PREFIX: "deploy-a", PIPELINE_ORG_FROM_PREFIX: "true" }, `deploy-a/${ORG}`],
  ] as const) {
    test(`the REAL courier (runCourierOnce, unprefixed keys) under a ${mode}: commits, reconciles status and is consumed through session-indexed`, async () => {
      const i = await ingest({ ...extra, ALLOW_INTERIM_RELAYAUTH_BEARER: "true" });
      const dir = await mkdtemp(join(tmpdir(), "96bulls-courier-prefix-"));
      try {
        const dbPath = join(dir, "history.db");
        const rawPath = join(dir, "session.jsonl");
        await writeFile(rawPath, '{"type":"user","message":"one"}\n');
        const hist = new DatabaseSync(dbPath);
        hist.exec("CREATE TABLE sessions (session_id TEXT, source TEXT, raw_path TEXT, first_activity_ms INTEGER, last_activity_ms INTEGER, cwd TEXT, agent_version TEXT)");
        hist.prepare("INSERT INTO sessions VALUES (?, ?, ?, ?, ?, ?, ?)").run("session-courier", "claude", rawPath, 1_700_000_000_000, 1_700_000_000_100, null, "1.0.0");
        hist.close();
        const ingestUrl = "http://ingest.test";
        const seenKeys: string[] = [];
        const outer = globalThis.fetch;
        globalThis.fetch = (async (input: RequestInfo | URL, init?: RequestInit) => {
          const url = String(input);
          if (!url.startsWith(ingestUrl)) return outer(input, init);
          const u = new URL(url);
          if (u.searchParams.get("key")) seenKeys.push(u.searchParams.get("key")!);
          if (init?.body && u.pathname.endsWith("/pipeline/manifests")) seenKeys.push((JSON.parse(String(init.body)) as { key: string }).key);
          return i.app.request(`${u.pathname}${u.search}`, init, i.env);
        }) as typeof fetch;
        const config = (): CourierConfig => ({
          dbPath, statePath: join(dir, "state.json"), ingestUrl,
          token: { get: async () => liveToken("machine-c"), invalidate: () => false },
          orgId: ORG, workspaceId: WORKSPACE, userId: USER, machineId: "machine-c",
          harnesses: ["claude"], closeAfterMinutes: 1_000_000, startMode: "all",
        });
        try {
          expect(await runCourierOnce(config())).toMatchObject({ uploaded: 1, chunks: 1 });
          // The courier posts the canonical unprefixed key; the server stored it under its prefix.
          expect(seenKeys.every((k) => k.startsWith("v1/"))).toBe(true);
          const { rows } = await db.query<{ key: string }>("SELECT message->'payload'->>'key' AS key FROM sessions.pipeline_jobs");
          expect(rows).toHaveLength(1);
          expect(rows[0]!.key.startsWith(`${prefix}/v1/`)).toBe(true);
          expect(await i.store.head(objectKey(rows[0]!.key))).toBeTruthy();
          // A restarted courier reconciles its recorded (unprefixed) key: verified, not reset.
          expect(await runCourierOnce({ ...config(), verifiedManifests: new Set() })).toMatchObject({ uploaded: 0 });
          const { rows: still } = await db.query("SELECT 1 FROM sessions.pipeline_jobs");
          expect(still).toHaveLength(1);
        } finally {
          globalThis.fetch = outer;
        }
        const sql = pgExecutor();
        const indexed: string[] = [];
        const deadLetters = postgresDeadLetterStore(sql);
        const deps = pipelineQueueDeps(extra as Bindings, {
          queue: postgresPipelineQueue(sql), sink: sqlPipelineEventSink(sql), deadLetters,
          index: { upsertRawPointer: async (pointer: { manifestKey: string }) => void indexed.push(pointer.manifestKey) } as never,
        });
        expect(await pollPostgresStage({ sql, stage: "object-landed", deps })).toMatchObject({ claimed: 1, acked: 1, exhausted: 0 });
        expect(await pollPostgresStage({ sql, stage: "session-indexed", deps })).toMatchObject({ claimed: 1, acked: 1, exhausted: 0 });
        expect(indexed).toHaveLength(1);
        expect(indexed[0]!.startsWith(`${prefix}/v1/`)).toBe(true);
      } finally {
        await rm(dir, { recursive: true, force: true });
      }
    });
  }

  test("org-derived reconcile scans the <base>/<org> roots and keeps only exact prefixes of a known org", async () => {
    const env = { PIPELINE_KEY_PREFIX: "deploy-a", PIPELINE_ORG_FROM_PREFIX: "true" };
    // The indexer's root (was `<base>/v1` for every mode, which never contains `<base>/<org>/v1`).
    expect(indexerReconcileRoot(env)).toBe("deploy-a");
    expect(indexerReconcileRoot({ PIPELINE_KEY_PREFIX: "deploy-a" })).toBe("deploy-a/v1");
    expect(indexerReconcileRoot({})).toBe("v1");
    expect(indexerReconcileRoot({ PIPELINE_ORG_FROM_PREFIX: "true" })).toBeUndefined();
    const store = createFsStore({ root });
    const { manifest } = await planned();
    const body = new TextEncoder().encode(JSON.stringify(manifest));
    const keys = {
      exact: `deploy-a/${ORG}/${manifestKey(LOCATION)}`,
      foreign: `deploy-b/${ORG}/${manifestKey(LOCATION)}`,
      extra: `deploy-a/extra/${ORG}/${manifestKey(LOCATION)}`,
      bare: `${ORG}/${manifestKey(LOCATION)}`,
    };
    for (const key of Object.values(keys)) await store.put(objectKey(key), body, { contentType: "application/json" });
    const sent: { payload: { key: string } }[] = [];
    const result = await reconcileManifests({
      store,
      reader: objectStoreManifestReader(store, { provider: "fs", bucket: "raw" }),
      orgs: orgResolverFromEnv(env)!,
      allowPrefix: (prefix, orgId) => prefixAllowed(env, prefix, orgId),
      send: async (m) => void sent.push(m as never),
      listPrefix: indexerReconcileRoot(env)!,
      modifiedSince: new Date(0),
    });
    expect(sent.map((m) => m.payload.key)).toEqual([keys.exact]);
    expect(result.enqueued).toBe(1);
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
