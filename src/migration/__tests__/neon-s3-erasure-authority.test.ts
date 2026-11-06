/**
 * E3: the transcript eraser repointed to the C3 Supabase privacy authority, and credentials only via stdin.
 * The Supabase side is the real migration stack plus supabase/migrations/20261105041000_ingest_privacy_authority.sql (PGlite).
 */
import { afterEach, describe, expect, setDefaultTimeout, test } from "bun:test";
import { mkdtemp, writeFile } from "node:fs/promises";
import { openSync, closeSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { createFsStore } from "../../../packages/object-store/src/fs.ts";
import { objectKey } from "../../shared/vendor.ts";
import { storeEraser } from "../neon-s3-erasure/eraser.ts";
import { DEFAULT_ERASURE_CONFIG, ErasureJob } from "../neon-s3-erasure/job.ts";
import { liveMachineId } from "../neon-s3-erasure/live.ts";
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
      expect(code(() => readStdinCredentials(["databaseUrl"], ["databaseUrl"], fd))).toBe("credentials_stdin_is_a_file");
    } finally {
      closeSync(fd);
    }
  });

  test("authority selection and URL guards; --execute needs stdin eraser credentials and the bucket guard", () => {
    expect(authorityKind(undefined)).toBe("neon");
    expect(authorityKind("supabase")).toBe("supabase");
    expect(code(() => authorityKind("both"))).toContain("must be neon or supabase");
    expect(code(() => authorityUrl("neon", "postgres://u@ep-x-pooler.us-east-2.aws.neon.tech/db"))).toContain("direct (unpooled)");
    expect(code(() => authorityUrl("supabase", "postgres://u@aws-0.pooler.supabase.com:6543/postgres"))).toContain("transaction pooler");
    expect(authorityUrl("supabase", "postgres://u@aws-0.pooler.supabase.com:5432/postgres")).toContain(":5432");
    expect(code(() => authorityUrl("supabase", "https://example.com"))).toBe("credentials_invalid.databaseUrl");
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
