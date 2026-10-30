/**
 * RelayAuth migration assertions (relayauth#110) at AlleyOop's own manifest
 * route, `POST /v1/pipeline/manifests` (the route the courier hits), and its
 * status read. The admission check, the ledger insert, enrichMessage and the
 * `pipeline_jobs` insert run in ONE SQL transaction: here on local PGlite with
 * every `supabase/migrations` file applied. The error codes are the same as
 * relayhistory-cloud's (routes/pipeline.ts). Synthetic ids only.
 */
import { afterAll, afterEach, beforeAll, beforeEach, describe, expect, test } from "bun:test";
import { randomBytes } from "node:crypto";
import { mkdtemp, readdir, readFile, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { Hono } from "hono";
import { PGlite } from "../../../packages/pipeline/node_modules/@electric-sql/pglite/dist/index.js";
import { createFsStore } from "../../../packages/object-store/src/fs.ts";
import type { MigrationAssertionLedger, TransactionalSqlExecutor } from "@rhc/lib/pipeline/migration-assertions.ts";
import {
  computeContentSha256,
  manifestKey,
  objectKey,
  objectStoreManifestReader,
  sealRawChunk,
  type AppEnv,
  type ManifestV1,
  type SessionLocation,
  type SqlExecutor,
  type StageMessage,
} from "../../shared/vendor.ts";
import { courierMigrationLedger, createCourierManifestRoutes } from "../manifests.ts";

const ORG = "org_test";
const WORKSPACE = "ws_test";
const USER = "user_a";
const MACHINE = "machine-a";
const SESSION = "session-a";
const TRACE = "019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c18";
const UUID = "019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c19";
const AT = "2026-10-29T10:00:00.000Z";
const ORIGIN = "https://history.example";
const PLAN = "a".repeat(64);
const OTHER_PLAN = "b".repeat(64);
const GRANT = `mgr_${"0".repeat(32)}`;
const SUPABASE = join(import.meta.dir, "../../../supabase");

let db: PGlite;
let root: string;

beforeAll(async () => {
  db = new PGlite();
  await db.exec(await readFile(join(SUPABASE, "tests/supabase_shim.sql"), "utf8"));
  for (const file of (await readdir(join(SUPABASE, "migrations"))).filter((f) => f.endsWith(".sql")).sort())
    await db.exec(await readFile(join(SUPABASE, "migrations", file), "utf8"));
  await db.exec("SET ROLE service_role");
});
afterAll(async () => {
  await db?.close();
});
beforeEach(async () => {
  root = await mkdtemp(join(tmpdir(), "96bulls-migration-"));
  await db.exec("TRUNCATE sessions.migration_assertion_uses; TRUNCATE sessions.pipeline_jobs");
});
afterEach(async () => {
  await rm(root, { recursive: true, force: true });
});

function executor(): TransactionalSqlExecutor {
  const wrap = (q: { query: PGlite["query"] }): SqlExecutor => ({
    query: async (text, params) => ({ rows: (await q.query(text, params)).rows as never[] }),
  });
  return { ...wrap(db), transaction: (work) => db.transaction((tx) => work(wrap(tx))) };
}

const count = async (table: string) =>
  (await db.query<{ n: number }>(`SELECT count(*)::int AS n FROM ${table}`)).rows[0]!.n;

const freshJti = () => `tok_${randomBytes(16).toString("hex")}`;
const freshNonce = () => randomBytes(24).toString("base64url");

type TestAuth = Record<string, unknown>;

/** The auth context the verifier sets for a migration assertion (auth/relayauth.ts). */
function migrationAuth(claims: Record<string, unknown> = {}, migration: Record<string, unknown> = {}): TestAuth {
  return {
    userId: USER,
    orgId: ORG,
    workspaceId: WORKSPACE,
    tokenSubject: "agent_relayhistory_migration",
    scopes: ["rth:sync"],
    historyOrigin: ORIGIN,
    claims: { sub: "agent_relayhistory_migration", wks: WORKSPACE, sponsorId: USER, ...claims },
    migration: {
      jti: freshJti(),
      nonce: freshNonce(),
      grantId: GRANT,
      planDigest: PLAN,
      sourceUserId: USER,
      workspaceId: WORKSPACE,
      expiresAt: Math.floor(Date.now() / 1000) + 65,
      ...migration,
    },
  };
}

const ordinaryAuth: TestAuth = {
  userId: USER,
  orgId: ORG,
  workspaceId: WORKSPACE,
  tokenSubject: "agent_relayhistory_assertion",
  scopes: ["rth:sync"],
  historyOrigin: ORIGIN,
  claims: {},
};

function marker(overrides: Record<string, unknown> = {}) {
  return {
    version: 1,
    source: "neon.delivery_records",
    exported_at: AT,
    grant_id: GRANT,
    plan_digest: PLAN,
    source_user_id: USER,
    ...overrides,
  };
}

async function fixture(options: { ledger?: MigrationAssertionLedger | null } = {}) {
  const store = createFsStore({ root });
  const raw = { store, provider: "fs" as const, bucket: "raw" };
  const reader = objectStoreManifestReader(store, { provider: "fs", bucket: "raw" });
  const location: SessionLocation = { workspaceId: WORKSPACE, machineId: MACHINE, harness: "claude", sessionId: SESSION, startDate: "2026-10-29" };
  const sealed = await sealRawChunk(store, {
    location, format: "jsonl", seq: 0, epoch: 0, startOffset: 0,
    source: new TextEncoder().encode('{"type":"user","text":"hello"}\n'), traceId: TRACE,
  });
  const build = async (exportMarker: Record<string, unknown> | undefined = marker(), ext: Record<string, unknown> = {}): Promise<ManifestV1> => ({
    schema_v: 1, layout: "v1", trace_id: TRACE, revision: 1, created_at: AT, updated_at: AT,
    session: { workspace_id: WORKSPACE, machine_id: MACHINE, harness: "claude", session_id: SESSION, started_at: AT },
    writer: { delivery_instance_id: MACHINE, plugin_version: "test" },
    raw: {
      owner: MACHINE, state: "open", updated_at: AT, format: "jsonl", compression: "zstd", chunk_count: 1,
      raw_bytes: sealed.entry.bytes, stored_bytes: sealed.entry.stored_bytes,
      content_sha256: await computeContentSha256([sealed.entry]), epoch: 0,
      epochs: [{ epoch: 0, opened_at: AT, reason: "initial", first_seq: 0, opened_by: MACHINE }],
      next_offset: sealed.entry.bytes, chunks: [sealed.entry], scrub: sealed.scrub,
    },
    extensions: {
      "96bulls": { session_uuid: UUID, session_uuid_source: "minted", ...ext },
      ...(exportMarker ? { "client.relayhistory_export": exportMarker } : {}),
    },
  });
  const sent: StageMessage[] = [];
  const ledger = options.ledger === null ? undefined : (options.ledger ?? courierMigrationLedger(executor(), reader));
  const routes = createCourierManifestRoutes({
    manifests: reader, raw,
    send: async (_env, _stage, message) => void sent.push(message),
    ...(ledger ? { migrationLedger: ledger } : {}),
  });
  let auth: TestAuth = ordinaryAuth;
  const app = new Hono<AppEnv>();
  app.use("*", async (c, next) => {
    c.set("correlationId", "test");
    c.set("auth", auth as never);
    await next();
  });
  app.route("/v1", routes);
  const env = {
    PIPELINE_ORG_ID: ORG,
    RELAYHISTORY_ORIGIN: ORIGIN,
    RELAYHISTORY_MIGRATION_GRANTS: `${GRANT}=${PLAN}`,
  };
  const key = manifestKey(location);
  const post = async (as: TestAuth, manifest: unknown, at = key, envOverride: Record<string, string | undefined> = {}) => {
    auth = as;
    return app.request("/v1/pipeline/manifests", {
      method: "POST",
      headers: { "content-type": "application/json", "x-machine-id": MACHINE, "x-session-id": SESSION },
      body: JSON.stringify({ key: at, manifest }),
    }, { ...env, ...envOverride });
  };
  const status = async (as: TestAuth, envOverride: Record<string, string | undefined> = {}) => {
    auth = as;
    return app.request(`/v1/pipeline/manifests/status?key=${encodeURIComponent(key)}&machine=${MACHINE}&writer=${MACHINE}`, {}, { ...env, ...envOverride });
  };
  return { store, reader, location, key, build, sent, post, status };
}

async function code(response: Response) {
  return ((await response.json()) as { error?: { code?: string } }).error?.code;
}

describe("AlleyOop manifest route: migration assertions", () => {
  test("commits the ledger row and the enriched pipeline_jobs row together; a replay is 409 migration_assertion_replayed", async () => {
    const f = await fixture();
    const auth = migrationAuth();
    const manifest = await f.build();
    const first = await f.post(auth, manifest);
    expect(first.status).toBe(202);
    expect(await count("sessions.migration_assertion_uses")).toBe(1);
    expect(await count("sessions.pipeline_jobs")).toBe(1);
    // The job carries the enriched ctx (enrichMessage ran inside the transaction).
    const { rows } = await db.query<{ message: { ctx: { session_uuid?: string; tenant_id?: string } } }>("SELECT message FROM sessions.pipeline_jobs");
    expect(rows[0]!.message.ctx.session_uuid).toBe(UUID);
    expect(rows[0]!.message.ctx.tenant_id).toBeTruthy();
    const { rows: uses } = await db.query<Record<string, unknown>>("SELECT grant_id, org_id, workspace_id, user_id, plan_digest FROM sessions.migration_assertion_uses");
    expect(uses).toEqual([{ grant_id: GRANT, org_id: ORG, workspace_id: WORKSPACE, user_id: USER, plan_digest: PLAN }]);
    // The route's own send is not used for a migration commit.
    expect(f.sent).toHaveLength(0);

    const replay = await f.post(auth, manifest);
    expect(replay.status).toBe(409);
    expect(await code(replay)).toBe("migration_assertion_replayed");
    const sameNonce = migrationAuth({}, { nonce: (auth.migration as { nonce: string }).nonce });
    const reused = await f.post(sameNonce, manifest);
    expect(reused.status).toBe(409);
    expect(await code(reused)).toBe("migration_assertion_replayed");
    const late = await f.status(auth);
    expect(late.status).toBe(409);
    expect(await code(late)).toBe("migration_assertion_replayed");
    expect(await count("sessions.migration_assertion_uses")).toBe(1);
    expect(await count("sessions.pipeline_jobs")).toBe(1);

    // A fresh assertion retries idempotently (the identical-manifest branch).
    expect((await f.post(migrationAuth(), manifest)).status).toBe(202);
    expect(await count("sessions.migration_assertion_uses")).toBe(2);
  });

  test("a crash after the pipeline_jobs insert, before COMMIT, rolls back both rows (503, nothing consumed)", async () => {
    let crash = true;
    const holder: { reader?: ReturnType<typeof objectStoreManifestReader> } = {};
    const crashing: MigrationAssertionLedger = {
      consumed: (claims) => courierMigrationLedger(executor(), holder.reader!).consumed(claims),
      consume: (use, stage, message) =>
        courierMigrationLedger(executor(), holder.reader!, async (_stage, _message, work) => {
          await work(); // the real enrich + pipeline_jobs insert on the transaction
          if (crash) throw new Error("crash between the job insert and COMMIT");
        }).consume(use, stage, message),
    };
    const f = await fixture({ ledger: crashing });
    holder.reader = f.reader;
    const auth = migrationAuth();
    const manifest = await f.build();
    const failed = await f.post(auth, manifest);
    expect(failed.status).toBe(503);
    expect(await code(failed)).toBe("migration_ledger_unavailable");
    expect(await count("sessions.migration_assertion_uses")).toBe(0);
    expect(await count("sessions.pipeline_jobs")).toBe(0);
    crash = false;
    // Nothing was spent: the same assertion commits the identical manifest once.
    expect((await f.post(auth, manifest)).status).toBe(202);
    expect(await count("sessions.migration_assertion_uses")).toBe(1);
    expect(await count("sessions.pipeline_jobs")).toBe(1);
  });

  test("fails closed with 503 migration_ledger_unavailable when the ledger is absent or down", async () => {
    const none = await fixture({ ledger: null });
    for (const response of [await none.post(migrationAuth(), await none.build()), await none.status(migrationAuth())]) {
      expect(response.status).toBe(503);
      expect(await code(response)).toBe("migration_ledger_unavailable");
    }
    const down: MigrationAssertionLedger = {
      consumed: async () => { throw new Error("ledger down"); },
      consume: async () => { throw new Error("ledger down"); },
    };
    const f = await fixture({ ledger: down });
    for (const response of [await f.post(migrationAuth(), await f.build()), await f.status(migrationAuth())]) {
      expect(response.status).toBe(503);
      expect(await code(response)).toBe("migration_ledger_unavailable");
    }
    const commitDown: MigrationAssertionLedger = { consumed: async () => false, consume: async () => { throw new Error("tx failed"); } };
    const g = await fixture({ ledger: commitDown });
    const response = await g.post(migrationAuth(), await g.build());
    expect(response.status).toBe(503);
    expect(await code(response)).toBe("migration_ledger_unavailable");
    expect(g.sent).toHaveLength(0);
    // Ordinary tokens are unaffected by the ledger.
    expect((await none.post(ordinaryAuth, await none.build(undefined))).status).toBe(202);
    expect(none.sent).toHaveLength(1);
  });

  test("returns the relayhistory-cloud codes: workspace pair, grant, plan and source user", async () => {
    const f = await fixture();
    const manifest = await f.build();
    const cases: [string, TestAuth, unknown, string?, Record<string, string | undefined>?][] = [
      ["migration_workspace_forbidden", migrationAuth({ wks: "ws_other" }), manifest],
      ["migration_workspace_forbidden", migrationAuth({ sponsorId: "user_b" }), manifest],
      ["migration_workspace_forbidden", migrationAuth({}, { workspaceId: "ws_other" }), manifest],
      ["migration_workspace_forbidden", migrationAuth(), { ...manifest, session: { ...manifest.session, workspace_id: "ws_other" } }, manifestKey({ ...f.location, workspaceId: "ws_other" })],
      ["migration_workspace_forbidden", migrationAuth(), await f.build(marker({ source_user_id: "user_b" }))],
      ["migration_workspace_forbidden", migrationAuth(), await f.build(marker(), { user_id: "user_b" })],
      ["migration_grant_forbidden", migrationAuth(), manifest, undefined, { RELAYHISTORY_MIGRATION_GRANTS: undefined }],
      ["migration_grant_forbidden", migrationAuth(), manifest, undefined, { RELAYHISTORY_MIGRATION_GRANTS: `${GRANT}=${OTHER_PLAN}` }],
      ["migration_grant_forbidden", migrationAuth(), await f.build(marker({ grant_id: "mgr_other" }))],
      ["migration_plan_mismatch", migrationAuth(), await f.build(marker({ plan_digest: OTHER_PLAN }))],
      ["migration_plan_mismatch", migrationAuth(), await f.build(marker({ plan_digest: undefined }))],
    ];
    for (const [expected, auth, body, at, envOverride] of cases) {
      const response = await f.post(auth, body, at ?? f.key, envOverride ?? {});
      expect(response.status).toBe(403);
      expect(await code(response)).toBe(expected);
    }
    expect(await count("sessions.migration_assertion_uses")).toBe(0);
    expect(await count("sessions.pipeline_jobs")).toBe(0);
    expect(await f.store.head(objectKey(f.key))).toBeUndefined();
  });

  test("refuses user A's assertion under a key whose stored manifest names user B", async () => {
    const f = await fixture();
    // User B's session is already stored at this key (and machine).
    const theirs = await f.build(marker({ source_user_id: "user_b" }), { user_id: "user_b" });
    expect((await f.post(ordinaryAuth, theirs)).status).toBe(202);
    const response = await f.post(migrationAuth(), await f.build());
    expect(response.status).toBe(403);
    expect(await code(response)).toBe("migration_workspace_forbidden");
    const read = await f.status(migrationAuth());
    expect(read.status).toBe(403);
    expect(await code(read)).toBe("migration_workspace_forbidden");
    expect(await count("sessions.migration_assertion_uses")).toBe(0);
  });

  test("status: admitted only for the assertion's own grant, workspace, user and plan", async () => {
    const f = await fixture();
    expect((await f.status(migrationAuth())).status).toBe(404);
    expect((await f.post(migrationAuth(), await f.build())).status).toBe(202);
    const own = await f.status(migrationAuth());
    expect(own.status).toBe(200);
    for (const [expected, auth, envOverride] of [
      // A grant accepted here, but the stored manifest commits to another plan.
      ["migration_plan_mismatch", migrationAuth({}, { planDigest: OTHER_PLAN }), { RELAYHISTORY_MIGRATION_GRANTS: `${GRANT}=${OTHER_PLAN}` }],
      // A grant accepted here, but not the grant the stored manifest names.
      ["migration_grant_forbidden", migrationAuth({}, { grantId: "mgr_other" }), { RELAYHISTORY_MIGRATION_GRANTS: `mgr_other=${PLAN}` }],
      ["migration_grant_forbidden", migrationAuth({}, { grantId: "mgr_other" }), {}],
      ["migration_workspace_forbidden", migrationAuth({ wks: "ws_other" }), {}],
    ] as const) {
      const response = await f.status(auth, envOverride);
      expect(response.status).toBe(403);
      expect(await code(response)).toBe(expected);
    }
  });
});
