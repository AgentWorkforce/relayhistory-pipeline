/**
 * E3: the transcript eraser repointed to the C3 Supabase privacy authority, and credentials only via stdin.
 * The Supabase side is the real migration stack plus supabase/migrations/20261105041000_ingest_privacy_authority.sql (PGlite).
 */
import { afterEach, describe, expect, setDefaultTimeout, test } from "bun:test";
import { mkdtemp, writeFile } from "node:fs/promises";
import { openSync, closeSync, readFileSync } from "node:fs";
import { spawn } from "node:child_process";
import type { Queryable } from "../relayhistory-export.ts";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createFsStore } from "../../../packages/object-store/src/fs.ts";
import { objectKey } from "../../shared/vendor.ts";
import { storeEraser } from "../neon-s3-erasure/eraser.ts";
import { DEFAULT_ERASURE_CONFIG, ErasureJob } from "../neon-s3-erasure/job.ts";
import { liveMachineId, liveTargets } from "../neon-s3-erasure/live.ts";
import { supabaseAuthority, type ErasureAuthority } from "../neon-s3-erasure/authority.ts";
import { authorityKind, authorityUrl, eraserFor } from "../neon-s3-erasure/main.ts";
import { SyncState } from "../neon-s3-sync/state.ts";
import { CredentialRefusal, FORBIDDEN_CREDENTIAL_ENV, parseCredentialDocument, readStdinCredentials, refuseCredentialEnv, s3Credentials } from "../stdin-credentials.ts";
import { authorityFixture, epoch } from "../../session-ingest/__tests__/privacy-authority-support.ts";

setDefaultTimeout(60_000);
const open: Array<() => unknown> = [];
afterEach(async () => {
  for (const c of open.splice(0).reverse()) await c();
});
const tmp = () => mkdtemp(join(process.env.TMPDIR ?? tmpdir(), "erasure-authority-"));
const later = () => Date.now() + 10 * 60 * 1000;
const code = (fn: () => unknown) => {
  try {
    fn();
    return null;
  } catch (e) {
    return e instanceof CredentialRefusal ? e.code : String((e as Error).message);
  }
};

/** Runs the probe with its stdin connected to a real OS pipe from `producer` (a shell command). */
function probe(producer: string): Promise<{ out: { ok: boolean; code?: string; ms: number }; wallMs: number }> {
  const started = Date.now();
  const script = new URL("./fixtures/stdin-reader-probe.ts", import.meta.url).pathname;
  return new Promise((resolve, reject) => {
    const child = spawn("sh", ["-c", `${producer} | "${process.execPath}" "${script}"`], { stdio: ["ignore", "pipe", "inherit"] });
    let text = "";
    child.stdout.on("data", (b) => (text += b));
    child.on("error", reject);
    // The probe prints and exits on its own; the producer may still be sleeping, so resolve on the probe's line.
    const tick = setInterval(() => {
      if (text.includes("\n")) {
        clearInterval(tick);
        child.kill("SIGKILL");
        resolve({ out: JSON.parse(text.trim().split("\n")[0]!), wallMs: Date.now() - started });
      }
    }, 20);
  });
}

describe("bounded stdin reads on a real pipe (independent review P2)", () => {
  test("65,537 bytes on a pipe kept open refuse at once, without waiting for EOF", async () => {
    const r = await probe(`(head -c 65537 /dev/zero | tr '\\000' ' '; sleep 30)`);
    expect(r.out).toMatchObject({ ok: false, code: "credentials_too_large" });
    expect(r.wallMs).toBeLessThan(10_000);
  }, 30_000);
  test("a producer that stalls mid-document is refused on the no-progress timeout, not at EOF", async () => {
    const r = await probe(`(printf '{"data'; sleep 40)`);
    expect(r.out).toMatchObject({ ok: false, code: "credentials_stdin_idle_timeout" });
    expect(r.wallMs).toBeLessThan(20_000);
  }, 60_000);
  test("exactly 65,536 bytes are accepted; one more byte is refused", async () => {
    const doc = '{"databaseUrl":"postgres://synthetic@localhost/x"}';
    const pad = 65_536 - doc.length;
    const ok = await probe(`(printf '%s' '${doc}'; head -c ${pad} /dev/zero | tr '\\000' ' ')`);
    expect(ok.out).toMatchObject({ ok: true, keys: ["databaseUrl"] });
    const over = await probe(`(printf '%s' '${doc}'; head -c ${pad + 1} /dev/zero | tr '\\000' ' ')`);
    expect(over.out).toMatchObject({ ok: false, code: "credentials_too_large" });
  }, 30_000);
});

/** Runs the real handoff CLI with `creds` on a real stdin pipe; resolves with its one JSON line, exit code and duration. */
function handoffCli(args: string[], creds: unknown): Promise<{ out: Record<string, unknown>; code: number | null; ms: number }> {
  const started = Date.now();
  const cli = new URL("../../../scripts/privacy-authority-handoff.ts", import.meta.url).pathname;
  return new Promise((resolve, reject) => {
    const env = { ...process.env };
    for (const k of FORBIDDEN_CREDENTIAL_ENV) delete env[k];
    const child = spawn(process.execPath, [cli, ...args], { stdio: ["pipe", "pipe", "inherit"], env });
    let text = "";
    child.stdout.on("data", (b) => (text += b));
    child.on("error", reject);
    child.on("exit", (code) => resolve({ out: JSON.parse(text.trim().split("\n").at(-1) || "{}"), code, ms: Date.now() - started }));
    child.stdin.end(JSON.stringify(creds));
  });
}

describe("handoff CLI: transaction-pooler inputs refused before any connection (independent review P2)", () => {
  const EPOCH = "3".repeat(64);
  // Ports 1 and 9 on loopback: nothing listens, so ANY connection attempt would surface as a connect error instead.
  const directNeon = "postgres://ro@127.0.0.1:9/neondb";
  for (const [name, supabaseUrl] of [
    ["Supabase transaction pooler port 6543", "postgres://postgres.ref:synthetic@127.0.0.1:6543/postgres"],
    ["pgbouncer=true", "postgres://postgres:synthetic@127.0.0.1:1/postgres?pgbouncer=true"],
  ] as const)
    for (const cmd of ["sync-owned", "census", "fence", "activate", "release"])
      test(`${cmd}: ${name} is refused pre-connect`, async () => {
        const r = await handoffCli([cmd, "--epoch", EPOCH, ...(cmd === "activate" ? ["--expect-neon", "a".repeat(64), "--settle-seconds", "30"] : [])], { neonUrl: directNeon, supabaseUrl });
        expect([r.code, r.out.m, r.out.refused]).toEqual([1, "STOPPED", "supabase_url_transaction_pooler"]);
      }, 30_000);
  test("a Neon pooler host is refused pre-connect (session-level read-only settings would not stick)", async () => {
    const r = await handoffCli(["sync-owned", "--epoch", EPOCH], { neonUrl: "postgres://ro@ep-x-pooler.us-east-2.aws.neon.tech:9/neondb", supabaseUrl: "postgres://postgres@127.0.0.1:1/postgres" });
    expect([r.code, r.out.refused]).toEqual([1, "neon_url_pooler"]);
  }, 30_000);
  test("control: a direct URL passes the check and then fails only on the (unreachable) connection", async () => {
    const r = await handoffCli(["fence", "--epoch", EPOCH], { supabaseUrl: "postgres://postgres@127.0.0.1:1/postgres" });
    expect(r.code).toBe(1);
    expect(r.out.refused).not.toBe("supabase_url_transaction_pooler");
  }, 30_000);
});

describe("credentials only through stdin (RAM)", () => {
  test("every on-disk or environment credential name refuses to start, by name only", () => {
    for (const k of FORBIDDEN_CREDENTIAL_ENV) expect(code(() => refuseCredentialEnv({ [k]: "" }))).toBe(`credential_env_refused.${k}`);
    expect(FORBIDDEN_CREDENTIAL_ENV).toContain("NEON_URL_FILE");
    expect(FORBIDDEN_CREDENTIAL_ENV).toContain("S3X_ERASER_CREDS_FILE");
    expect(code(() => refuseCredentialEnv({ S3X_STATE: "/x", S3X_BUCKET: "b" }))).toBeNull();
  });

  test("the stdin document is one bounded object with exactly the expected keys; values never appear in errors", () => {
    expect(code(() => parseCredentialDocument("not json", ["databaseUrl"]))).toBe("credentials_not_json");
    expect(code(() => parseCredentialDocument("[]", ["databaseUrl"]))).toBe("credentials_not_object");
    expect(code(() => parseCredentialDocument('{"databaseUrl":"postgres://u:SECRET@h/d","extra":1}', ["databaseUrl"]))).toBe("credentials_unknown_key.extra");
    expect(code(() => parseCredentialDocument("{}", ["databaseUrl"]))).toBe("credentials_missing_key.databaseUrl");
    expect(code(() => parseCredentialDocument(JSON.stringify({ databaseUrl: "x".repeat(70_000) }), ["databaseUrl"]))).toBe("credentials_too_large");
    expect(code(() => s3Credentials({ accessKeyId: "A", secretAccessKey: "SECRET", region: "x" }, "eraser"))).toBe("credentials_invalid.eraser");
    expect(s3Credentials({ accessKeyId: "A", secretAccessKey: "S", sessionToken: "T" }, "eraser")).toEqual({ accessKeyId: "A", secretAccessKey: "S", sessionToken: "T" });
  });

  test("a regular file on stdin is refused (that is a credential on disk)", async () => {
    const f = join(await tmp(), "synthetic.json");
    await writeFile(f, JSON.stringify({ databaseUrl: "postgres://synthetic@localhost/x" }));
    const fd = openSync(f, "r");
    try {
      expect(await readStdinCredentials(["databaseUrl"], ["databaseUrl"], { fd }).then(() => null, (e) => e.code)).toBe("credentials_stdin_is_a_file");
    } finally {
      closeSync(fd);
    }
  });

  test("authority selection and URL guards; --execute needs stdin eraser credentials and the bucket guard", () => {
    expect(authorityKind(undefined)).toBe("neon");
    expect(authorityKind("supabase")).toBe("supabase");
    expect(code(() => authorityKind("both"))).toContain("must be neon or supabase");
    expect(code(() => authorityUrl("neon", "postgres://u@ep-x-pooler.us-east-2.aws.neon.tech/db"))).toBe("database_url_pooler");
    expect(code(() => authorityUrl("supabase", "postgres://u@aws-0.pooler.supabase.com:6543/postgres"))).toBe("database_url_transaction_pooler");
    expect(code(() => authorityUrl("supabase", "postgres://u@db.ref.supabase.co:5432/postgres?pgbouncer=true"))).toBe("database_url_transaction_pooler");
    expect(code(() => authorityUrl("neon", "postgres://u@ep-x.us-east-2.aws.neon.tech:6543/db"))).toBe("database_url_transaction_pooler");
    expect(authorityUrl("supabase", "postgres://u@aws-0.pooler.supabase.com:5432/postgres")).toContain(":5432");
    expect(code(() => authorityUrl("supabase", "https://example.com"))).toBe("credentials_invalid.databaseUrl");
    // Startup-parameter overrides are refused pre-connect (options=-c session_authorization=... forges the identity).
    expect(code(() => authorityUrl("supabase", "postgres://postgres@db.ref.supabase.co:5432/postgres?options=-c%20session_authorization%3Dbulls96_privacy_eraser"))).toBe("database_url_startup_parameter_refused.options");
    expect(code(() => authorityUrl("supabase", "postgres://u@db.ref.supabase.co:5432/postgres?application_name=x"))).toBe("database_url_startup_parameter_refused.application_name");
    expect(authorityUrl("supabase", "postgres://u@db.ref.supabase.co:5432/postgres?sslmode=verify-full")).toContain("sslmode=verify-full");
    for (const k of ["PGOPTIONS", "PGUSER", "PGPASSFILE", "PGSERVICE"]) expect(code(() => refuseCredentialEnv({ [k]: "" }))).toBe(`credential_env_refused.${k}`);
    const e = (m: Record<string, string>) => (k: string, d?: string) => m[k] ?? d;
    expect(code(() => eraserFor(true, undefined, e({ S3X_BUCKET: "b", S3X_EXPECT_BUCKET: "b" })))).toContain("stdin eraser credentials are required");
    expect(code(() => eraserFor(true, { accessKeyId: "A", secretAccessKey: "S" }, e({ S3X_BUCKET: "b", S3X_EXPECT_BUCKET: "c" })))).toContain("write guard");
    expect(code(() => eraserFor(false, undefined))).toBeNull();
  });
});

const T = { orgId: "org-1", workspaceId: "ws/1", userId: "user-1" };
const M = liveMachineId("ws/1", "origin-1", "user-1");
const DIR = `96bulls/org-1/v1/ws=ws_1/m=${M}/h=claude/d=2026/10/30/s=sess_1/`;
const LIVE = [`${DIR}manifest.json`, `${DIR}raw/0000000000-0-e568b589.ndjson.zst`];
const POINTER = `96bulls/org-1/live-sessions/v1/ws=ws_1/m=${M}/h=claude/s=sess_1.json`;
const selection = (enabled: boolean) => ({ epoch, tenant: T, kind: "selection", payload: { origin_id: "origin-1", sessions: [{ source: "claude", session_id: "sess:1" }], enabled } });

async function world() {
  const f = await authorityFixture();
  open.push(f.close);
  await f.seal();
  const store = createFsStore({ root: await tmp() });
  for (const k of [...LIVE, POINTER]) await store.put(objectKey(k), new TextEncoder().encode("x"));
  const state = new SyncState(join(await tmp(), "s.sqlite"));
  open.push(() => state.close());
  // The job's read-only connection: the same database (an owner session, as the operator's login would be).
  const authority = supabaseAuthority(f.queryable);
  const logs: Record<string, unknown>[] = [];
  const job = (a: ErasureAuthority = authority) => new ErasureJob(a, state, storeEraser(store), (x) => logs.push(x), DEFAULT_ERASURE_CONFIG, later, async () => {});
  const keys = async () => {
    const out: string[] = [];
    let c: string | undefined;
    do {
      const p = await store.list(objectKey("96bulls"), c);
      out.push(...p.items.map((i) => i.key));
      c = p.nextCursor;
    } while (c);
    return out.sort();
  };
  const activate = async () => {
    await f.ownerRpc("ingest_privacy_authority_fence", { controlEpoch: epoch });
    const fp = (await f.db.query("SELECT sessions.ingest_privacy_authority_packet('[]','[]','[]') AS f")).rows[0] as { f: string };
    const owned = (await f.ownerRpc("ingest_privacy_authority_owned_digest")).r;
    await f.ownerRpc("ingest_privacy_authority_activate", { controlEpoch: epoch, neonFingerprintSha256: fp.f, heads: [], deletions: [], resets: [], neonOwned: owned });
  };
  return { f, store, state, job, keys, logs, activate };
}

describe("supabase privacy authority as the eraser's source", () => {
  test("before activation the frozen table is NOT the authority: nothing is erased even with a fence row present", async () => {
    const w = await world();
    // A frozen-only row (Neon may already have re-enabled this session while Neon is still the authority).
    await w.f.db.query("INSERT INTO sessions.ingest_frozen_source_deletions VALUES('org-1','ws/1','user-1','origin-1','claude','sess:1')");
    const j = w.job();
    await j.sweep();
    expect(await w.keys()).toEqual([...LIVE, POINTER].sort());
    expect(w.logs.map((l) => l.message)).toEqual(["sweep refused: privacy authority not ready"]);
    expect(j.counters.liveVersionsDeleted).toBe(0);
  });

  test("after activation an authority disable erases the session's live objects and pointer; no Neon is read", async () => {
    const w = await world();
    await w.activate();
    await w.f.rpc("ingest_privacy_authority_begin", selection(false));
    const j = w.job();
    expect(j.authority.presence).toBeUndefined();
    await j.sweep();
    expect(await w.keys()).toEqual([]);
    expect(j.counters.liveVersionsDeleted).toBe(3);
    expect(w.logs.find((l) => l.message === "erasure sweep")).toMatchObject({ authority: "supabase", gone: 0 });
  });

  test("an exported (Neon-era) group covered by an authority reset is queued for erasure", async () => {
    const w = await world();
    const p = { org_id: "org-1", workspace_id: "ws/1", origin_id: "origin-1", record_id: "r1", user_id: "user-1", source: "claude", session_id: "sess:1" };
    await w.f.db.query("INSERT INTO sessions.ingest_frozen_source_records(org_id,workspace_id,origin_id,record_id,user_id,source,session_id,operation) VALUES($1,$2,$3,$4,$5,$6,$7,'upsert')", Object.values(p));
    // Neon's support identity for that record (the owned copy activation binds; Neon's reset counts these).
    await w.f.db.query("INSERT INTO sessions.ingest_frozen_source_owned_sessions VALUES('org-1','ws/1','user-1','origin-1','claude','sess:1')");
    await w.activate();
    const exported = "96bulls/org-1/v1/ws=ws_1/m=delivery_x/h=claude/d=2026/10/30/s=sess_1/manifest.json";
    w.state.seeRevisions(1, [{ org_id: "org-1", session_id: "sess:1", revision: "1" }]);
    w.state.markExported("org-1", "sess:1", 1, [exported]);
    const head = await w.f.rpc("ingest_privacy_authority_begin", { epoch, tenant: T, kind: "reset", payload: { request_id: "req" } });
    expect(head).toMatchObject({ deleted: "1" });
    expect(await supabaseAuthority(w.f.queryable).fencedGroups()).toEqual([{ org_id: "org-1", session_id: "sess:1" }]);
    await w.job().sweep();
    expect(w.state.nextErasures(Date.now() + 1, 10)).toEqual([expect.objectContaining({ key: exported.slice(0, -"manifest.json".length), kind: "prefix", org_id: "org-1", reason: "fenced" })]);
  });

  test("re-enable race: a tombstone page read before the user re-enabled never erases the session", async () => {
    const w = await world();
    await w.activate();
    await w.f.rpc("ingest_privacy_authority_begin", selection(false));
    const h = (await w.f.db.query("SELECT version::text AS v,digest FROM sessions.ingest_source_control_heads")).rows[0] as { v: string; digest: string };
    await w.f.rpc("ingest_privacy_authority_complete", { epoch, tenant: T, version: h.v, digest: h.digest, kind: "selection", outcome: "applied" });
    const base = supabaseAuthority(w.f.queryable);
    // The page is read, then the user re-enables before the per-tombstone re-check.
    const racing: ErasureAuthority = {
      ...base,
      fencedLivePage: async (after, limit) => {
        const rows = await base.fencedLivePage(after, limit);
        if (rows.length) await w.f.rpc("ingest_privacy_authority_begin", selection(true));
        return rows;
      },
    };
    const j = w.job(racing);
    await j.sweep();
    expect(await w.keys()).toEqual([...LIVE, POINTER].sort());
    expect(j.counters.liveVersionsDeleted).toBe(0);
  });

  test("a release back to Neon mid-sweep stops erasure at the per-tombstone re-check", async () => {
    const w = await world();
    await w.activate();
    await w.f.rpc("ingest_privacy_authority_begin", selection(false));
    // Model the release by removing the authority row under the job (release itself refuses once diverged).
    const base = supabaseAuthority(w.f.queryable);
    const racing: ErasureAuthority = {
      ...base,
      fencedLivePage: async (after, limit) => {
        const rows = await base.fencedLivePage(after, limit);
        if (rows.length) await w.f.db.query("ALTER TABLE sessions.ingest_privacy_authority DISABLE ROW LEVEL SECURITY; DELETE FROM sessions.ingest_privacy_authority");
        return rows;
      },
    };
    const j = w.job(racing);
    await j.sweep();
    expect(await w.keys()).toEqual([...LIVE, POINTER].sort());
    expect(j.counters.liveFenceFailed).toBeGreaterThan(0);
  });
});

/**
 * Independent dogpatch review, P0: a session uploaded AFTER activation is owned only by the target (ingest), never in
 * the frozen copy. A Supabase-authority reset fences it in the target's upload_deletions; the eraser must see that set
 * too, or its live objects survive the reset forever. Runs the REAL ingest upload path and the REAL Supabase eraser.
 */
describe("supabase eraser covers post-cutover sessions (review P0) and runs least-privilege (review P1)", () => {
  const { createIngestApp } = require("../../session-ingest/app.ts") as typeof import("../../session-ingest/app.ts");
  const { deliveryAccount } = require("../../session-ingest/vendor/delivery/prepare.ts") as typeof import("../../session-ingest/vendor/delivery/prepare.ts");
  const { batch, record, TENANT } = require("../../session-ingest/__tests__/support.ts") as typeof import("../../session-ingest/__tests__/support.ts");
  const { fakeNeon } = require("../../session-ingest/__tests__/privacy-authority-support.ts") as typeof import("../../session-ingest/__tests__/privacy-authority-support.ts");
  const handoff = require("../privacy-authority-handoff.ts") as typeof import("../privacy-authority-handoff.ts");
  const { assertEraserIdentity } = require("../neon-s3-erasure/authority.ts") as typeof import("../neon-s3-erasure/authority.ts");

  async function activeWorld() {
    const f = await authorityFixture();
    open.push(f.close);
    await f.seal();
    const n = await fakeNeon();
    open.push(n.close);
    await handoff.fence(f.queryable, epoch);
    await handoff.syncOwned(n.queryable, f.queryable);
    const c = await handoff.census(n.queryable, f.queryable, epoch);
    await handoff.activate(n.queryable, f.queryable, { epoch, expectNeonFingerprint: c.neon.fingerprint, settleMs: 0 });
    const app = (control?: object) => createIngestApp({ identify: () => ({ ...TENANT, scopes: ["rth:sync"], ...(control ? { control } : {}) }) as never, deliver: f.deps, log: () => {}, custodySource: "frozen-supabase", privacyAuthority: "supabase" });
    const post = (a: ReturnType<typeof app>, path: string, body: unknown) => a.request(path, { method: "POST", headers: { "content-type": "application/json" }, body: JSON.stringify(body) });
    const reset = async (request_id: string) => {
      const head = (await (await post(app(), "/v1/custody/authority/begin", { epoch, kind: "reset", payload: { request_id } })).json()) as any;
      const control = { epoch, version: head.version, digest: head.digest, kind: "reset" };
      const r = await post(app(control), "/v1/delivery/sessions/reset", { account_id: await deliveryAccount(TENANT), request_id, confirmation: "DELETE ALL SESSIONS" });
      if (r.status !== 200) throw new Error("reset fence " + r.status);
      await post(app(), "/v1/custody/authority/complete", { ...control, outcome: ((await r.json()) as any).control.outcome });
    };
    /** A superuser session that runs every statement under SET ROLE bulls96_privacy_eraser: the role's privileges, but
     *  NOT its login (a genuine LOGIN is exercised on real PostgreSQL in privacy-authority.postgres.test.ts). */
    const asEraser: Queryable = {
      query: async (t, p) => {
        await f.db.exec("SET ROLE bulls96_privacy_eraser");
        try {
          return (await (p?.length ? f.db.query(t, p as never) : f.db.query(t))) as never;
        } finally {
          await f.db.exec("RESET ROLE");
        }
      },
    };
    return { f, app, post, reset, asEraser };
  }
  const allKeys = async (store: any) => {
    const out: string[] = [];
    let c: string | undefined;
    do {
      const p = await store.list(objectKey("96bulls"), c);
      out.push(...p.items.map((i: any) => i.key));
      c = p.nextCursor;
    } while (c);
    return out;
  };

  test("P0: a post-cutover session's live objects are erased after a Supabase-authority reset", async () => {
    const w = await activeWorld();
    const up = await w.post(w.app(), "/v1/delivery/batches", { protocolVersion: 1, batch: await batch([record({ record_id: "p1", session_id: "POSTCUT" })], { batch_id: "after-activation" }) });
    expect(up.status).toBe(200);
    const store = w.f.deps.target.store as any;
    // Publication (canonical) writes the session's live objects and pointer in the m=srv_ layout; place them exactly
    // there (liveTargets) for the real tenant/origin/session the target now owns.
    const origin = (await (require("../../session-ingest/vendor/delivery/prepare.ts") as any).prepareDelivery(TENANT, await batch([record({ record_id: "x" })]))).originId as string;
    const g = { org_id: TENANT.orgId, workspace_id: TENANT.workspaceId, user_id: TENANT.userId, origin_id: origin, source: "claude", session_id: "POSTCUT" };
    const t = liveTargets("96bulls", g);
    const live = [`${t.base}d=2026/11/06/${t.sessionSegment}/manifest.json`, `${t.base}d=2026/11/06/${t.sessionSegment}/raw/0000000000-0-aa.ndjson.zst`, t.pointerKey];
    for (const k of live) await store.put(objectKey(k), new TextEncoder().encode("x"));
    await w.reset("wipe-1");
    const q = (sql: string) => w.f.db.query(sql).then((r) => (r.rows[0] as any).n as number);
    expect(await q("SELECT count(*)::int AS n FROM sessions.upload_deletions WHERE session_id='POSTCUT'")).toBe(1);
    expect(await q("SELECT count(*)::int AS n FROM sessions.ingest_frozen_source_deletions WHERE session_id='POSTCUT'")).toBe(0);
    const state = new SyncState(join(await tmp(), "s.sqlite"));
    open.push(() => state.close());
    // Past the staged-object redrive window, so staged batch copies are erased in the same sweep.
    const job = new ErasureJob(supabaseAuthority(w.f.queryable), state, storeEraser(store), () => {}, DEFAULT_ERASURE_CONFIG, () => Date.now() + 49 * 60 * 60 * 1000, async () => {});
    await job.sweep();
    const left = await allKeys(store);
    for (const k of live) expect(left).not.toContain(k);
  }, 60_000);

  test("P1: the eraser role reads only through its RPCs; owner, superuser and SET ROLE substitutes are refused", async () => {
    const w = await activeWorld();
    await w.reset("wipe-2");
    const a = supabaseAuthority(w.asEraser);
    await a.ready();
    expect(Array.isArray(await a.fencedGroups())).toBe(true);
    expect(Array.isArray(await a.fencedLivePage(null))).toBe(true);
    // A superuser session that SET ROLE to the eraser role is refused: the login keeps its credential.
    expect(await assertEraserIdentity(w.asEraser).then(() => "ok", (e) => e.code)).toMatch(/^eraser_identity_(not_authenticated_as_privacy_eraser|superuser)$/);
    for (const t of ["sessions.ingest_frozen_source_deletions", "sessions.upload_deletions", "sessions.ingest_privacy_authority", "sessions.ingest_frozen_source_records"])
      expect(await w.asEraser.query(`SELECT 1 FROM ${t} LIMIT 1`).then(() => "read", (e) => String(e.message))).toContain("permission denied");
    for (const fn of ["sessions.ingest_privacy_authority_state()", "sessions.ingest_privacy_authority_owned_digest()"])
      expect(await w.asEraser.query(`SELECT ${fn}`).then(() => "ran", (e) => String(e.message))).toContain("permission denied");
    // The fixture's own connection is a superuser (the owner): refused.
    expect(await assertEraserIdentity(w.f.queryable).then(() => "ok", (e) => e.code)).toMatch(/^eraser_identity_(not_authenticated_as_privacy_eraser|superuser)$/);
    // The generated verifier accepts the bare role exactly as provisioned.
    await w.f.db.exec(readFileSync(new URL("../../../supabase/bootstrap/privacy_eraser_verify.sql", import.meta.url), "utf8"));
  }, 60_000);
});
