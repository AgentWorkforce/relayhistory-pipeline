/**
 * 20261105062380 the session binding, on PGlite with every supabase migration, driven through the real projector
 * (projectSessionDirectory over crafted session_index pointers; synthetic data):
 * - minted once on the first projection; a retry, a re-sent or new revision, a reprojection and a catalog write keep
 *   the exact (id, revision); a caller-written value is overwritten; a second ON CONFLICT insert keeps the first id;
 * - a tombstone retires the binding (kept on the row, never served); a re-sent fenced revision does not revive it;
 *   a genuinely new revision revives the row under a NEW id with a strictly greater revision;
 * - a delayed, first-seen pre-tombstone record (a machine with no fence) lands under a NEW binding, never the old;
 * - a hard erase and a delayed old revision re-creating the row: a new id, a greater revision;
 * - an operator erasure fence: no pointer or directory write revives it; the reader stays empty;
 * - live -> turn-only hidden (062020) -> visible: retired, then rotated (intended, fail closed);
 * - a key change mints a new binding under the new key;
 * - aliases: pointers carrying a canonical session id share the canonical row's one binding;
 * - a live row from before 062380 (no binding) gets one on its next write, and the reader omits it until then;
 * - the reader: read-role and tenant scope (42501), argument bounds (22023), live keys only, deduplicated, ordered.
 */
import { afterEach, describe, expect, test } from "bun:test";
import { readdirSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { PGlite } from "@electric-sql/pglite";
import { btree_gin } from "@electric-sql/pglite/contrib/btree_gin";
import { pg_trgm } from "@electric-sql/pglite/contrib/pg_trgm";
import { projectSessionDirectory } from "../session-directory-projector.ts";
import { eraseSessionDirectory } from "../session-directory-summary.ts";
import { SESSION_BOOTSTRAP_FILES } from "../session-bootstrap-chain.ts";
import { sessionBindings, sqlDirectory } from "../../session-read-api/directory.ts";
import { poolTenantRunner } from "../../session-read-api/liveness.ts";

const root = join(import.meta.dir, "../../..");
const read = (path: string) => readFileSync(join(root, path), "utf8");
const sqlFile = (path: string) => read(path).split("\n").filter((l) => !l.startsWith("\\")).join("\n");
const KEY = { orgId: "o", workspaceId: "w", harness: "codex", sessionId: "s" };
const READER = "sessions.session_directory_bindings_for(text,text[],text[])";

const open: PGlite[] = [];
afterEach(async () => {
  await Promise.all(open.splice(0).map((db) => db.close()));
});

const BINDING_FILE = "20261105062380_session_directory_binding.sql";
/** Every migration; `beforeBinding` runs just before 062380 (a pre-existing default grant, a retained sequence). */
async function database(beforeBinding?: (db: PGlite) => Promise<unknown>): Promise<PGlite> {
  const db = new PGlite({ extensions: { btree_gin, pg_trgm } });
  open.push(db);
  await db.exec(read("supabase/tests/supabase_shim.sql"));
  for (const f of SESSION_BOOTSTRAP_FILES) await db.exec(read(f));
  for (const file of readdirSync(join(root, "supabase/migrations")).sort()) {
    if (file === BINDING_FILE && beforeBinding) await beforeBinding(db);
    await db.exec(read(`supabase/migrations/${file}`));
  }
  await db.exec(sqlFile("supabase/bootstrap/session_read_api_role.sql"));
  return db;
}
/** Non-owner ACL entries on the revision sequence (PUBLIC included). */
const sequenceGrantees = async (db: PGlite) =>
  (await db.query<{ g: string }>(
    `SELECT coalesce(r.rolname, 'PUBLIC') AS g FROM pg_class c
      CROSS JOIN LATERAL aclexplode(coalesce(c.relacl, acldefault('S', c.relowner))) a
      LEFT JOIN pg_roles r ON r.oid = a.grantee
     WHERE c.oid = 'sessions.session_binding_revision'::regclass AND a.grantee <> c.relowner`)).rows.map((r) => r.g);

type Pointer = { machine: string; session?: string; last: string; revision?: number; canonicalSession?: string };
async function upsertPointer(db: PGlite, p: Pointer) {
  await db.query(
    `INSERT INTO sessions.session_index
       (org_id, workspace_id, harness, session_id, machine_id, user_id, created_at, last_activity_at,
        title, state, event_count, input_tokens, storage_provider, bucket, manifest_key, trace_id,
        revision, indexed_at, deleted_at, canonical_session_id)
     VALUES ('o', 'w', 'codex', $1, $2, 'u-' || $2, '2026-10-01T10:00:00Z', $3::timestamptz, 't', 'active', 1, 0,
             's3', 'bucket', 'key/' || $2, '019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c18', $4, now(), NULL, $5)
     ON CONFLICT (org_id, workspace_id, harness, session_id, machine_id) DO UPDATE SET
       last_activity_at = excluded.last_activity_at, revision = excluded.revision, indexed_at = now(),
       deleted_at = NULL, canonical_session_id = excluded.canonical_session_id`,
    [p.session ?? "s", p.machine, p.last, p.revision ?? 1, p.canonicalSession ?? null],
  );
}
const tombstonePointer = (db: PGlite, machine: string, session = "s") =>
  db.query(
    `UPDATE sessions.session_index SET deleted_at = now(), indexed_at = now()
      WHERE org_id = 'o' AND workspace_id = 'w' AND harness = 'codex' AND session_id = $1 AND machine_id = $2`,
    [session, machine],
  );

type Binding = { id: string | null; revision: string | null; createdAt: Date | null; deleted: boolean };
async function binding(db: PGlite, session = "s"): Promise<Binding | undefined> {
  const r = await db.query<Binding>(
    `SELECT session_binding_id AS id, session_binding_revision::text AS revision, session_binding_created_at AS "createdAt",
            deleted_at IS NOT NULL AS deleted
       FROM sessions.session_directory WHERE org_id = 'o' AND workspace_id = 'w' AND harness = 'codex' AND session_id = $1`,
    [session],
  );
  return r.rows[0];
}

type ReaderRow = { harness: string; session_id: string; session_binding_id: string; session_binding_revision: string };
async function asReader(db: PGlite, keys: Array<[string, string]>, scope: [string, string] = ["o", "w"], workspace = scope[1], role = "bulls96_read_api") {
  return db.transaction(async (tx) => {
    await tx.exec(`SET LOCAL ROLE ${role}`);
    await tx.query("SELECT set_config('bulls96_read.org_id',$1,true), set_config('bulls96_read.workspace_id',$2,true)", scope);
    const r = await tx.query<ReaderRow>(
      `SELECT harness, session_id, session_binding_id, session_binding_revision::text AS session_binding_revision
         FROM sessions.session_directory_bindings_for($1, $2::text[], $3::text[])`,
      [workspace, keys.map((k) => k[0]), keys.map((k) => k[1])],
    );
    return r.rows;
  });
}
const code = async (p: Promise<unknown>) =>
  ((await p.then(() => null, (e: unknown) => e)) as { code?: string } | null)?.code ?? null;

describe("062380 session binding: lifecycle through the projector", () => {
  test("minted once on first projection; retries, revisions, reprojection and catalog writes keep (id, revision)", async () => {
    const db = await database();
    await upsertPointer(db, { machine: "m1", last: "2026-10-01T12:00:00Z" });
    await projectSessionDirectory(db, KEY);
    const first = await binding(db);
    expect(first?.id).toMatch(/^[0-9a-f-]{36}$/);
    expect(first?.revision).toMatch(/^\d+$/);
    expect(first?.createdAt).toBeInstanceOf(Date);
    // A retry of the same pointer, a re-projection, a new revision, a second machine, a catalog write.
    await projectSessionDirectory(db, KEY);
    await upsertPointer(db, { machine: "m1", last: "2026-10-01T13:00:00Z", revision: 2 });
    await projectSessionDirectory(db, KEY);
    await upsertPointer(db, { machine: "m2", last: "2026-10-01T14:00:00Z" });
    await projectSessionDirectory(db, KEY);
    await db.query("UPDATE sessions.session_directory SET project_key = 'github.com/acme/api', project_key_method = 'remote' WHERE session_id = 's'");
    expect(await binding(db)).toEqual(first);
    // A caller-written value is overwritten, never stored.
    await db.query("UPDATE sessions.session_directory SET session_binding_id = gen_random_uuid(), session_binding_revision = 1, session_binding_created_at = now() WHERE session_id = 's'");
    expect(await binding(db)).toEqual(first);
  });

  test("a second insert of the same key (the ON CONFLICT path of a concurrent first projection) keeps the first id", async () => {
    const db = await database();
    const insert = () =>
      db.query(
        `INSERT INTO sessions.session_directory (org_id, workspace_id, harness, session_id, created_at, last_activity_at,
           session_binding_id, session_binding_revision, session_binding_created_at)
         VALUES ('o','w','codex','s', now(), now(), '00000000-0000-4000-8000-000000000001', 424242, now())
         ON CONFLICT (org_id, workspace_id, harness, session_id) DO UPDATE SET last_activity_at = excluded.last_activity_at
         RETURNING session_binding_id::text AS id, session_binding_revision::text AS revision`,
      );
    const a = (await insert()).rows[0] as { id: string; revision: string };
    const b = (await insert()).rows[0] as { id: string; revision: string };
    expect(b).toEqual(a);
    // The caller's values were overwritten on insert, not stored.
    expect(a.id).not.toBe("00000000-0000-4000-8000-000000000001");
    expect(a.revision).not.toBe("424242");
  });

  test("a tombstone retires the binding; a re-sent fenced revision does not revive it; a new revision mints a new, greater one", async () => {
    const db = await database();
    await upsertPointer(db, { machine: "m1", last: "2026-10-01T12:00:00Z", revision: 3 });
    await projectSessionDirectory(db, KEY);
    const live = (await binding(db))!;
    expect(await asReader(db, [["codex", "s"]])).toEqual([{ harness: "codex", session_id: "s", session_binding_id: live.id!, session_binding_revision: live.revision! }]);
    await tombstonePointer(db, "m1");
    await projectSessionDirectory(db, KEY);
    expect(await binding(db)).toEqual({ ...live, deleted: true });
    expect(await asReader(db, [["codex", "s"]])).toEqual([]);
    // The same revision re-sent (un-deleted at its fenced revision): stays a tombstone, binding still retired.
    await upsertPointer(db, { machine: "m1", last: "2026-10-01T12:00:00Z", revision: 3 });
    await projectSessionDirectory(db, KEY);
    expect(await binding(db)).toEqual({ ...live, deleted: true });
    expect(await asReader(db, [["codex", "s"]])).toEqual([]);
    // A genuinely new revision above the fence: a new incarnation.
    await upsertPointer(db, { machine: "m1", last: "2026-10-02T12:00:00Z", revision: 4 });
    await projectSessionDirectory(db, KEY);
    const revived = (await binding(db))!;
    expect(revived.deleted).toBe(false);
    expect(revived.id).not.toBe(live.id);
    expect(BigInt(revived.revision!)).toBeGreaterThan(BigInt(live.revision!));
    expect(await asReader(db, [["codex", "s"]])).toEqual([{ harness: "codex", session_id: "s", session_binding_id: revived.id!, session_binding_revision: revived.revision! }]);
  });

  test("a delayed, first-seen pre-tombstone record lands under a NEW binding, never the retired one", async () => {
    const db = await database();
    await upsertPointer(db, { machine: "m1", last: "2026-10-01T12:00:00Z" });
    await projectSessionDirectory(db, KEY);
    const old = (await binding(db))!;
    await tombstonePointer(db, "m1");
    await projectSessionDirectory(db, KEY);
    // A record from a machine never seen before, stamped before the tombstone, arrives late. The projector cannot
    // tell it from a new upload (no fence for that machine); whatever it is, it is not the retired incarnation.
    await upsertPointer(db, { machine: "m-late", last: "2026-10-01T11:00:00Z" });
    await projectSessionDirectory(db, KEY);
    const after = (await binding(db))!;
    expect(after.deleted).toBe(false);
    expect(after.id).not.toBe(old.id);
    expect(BigInt(after.revision!)).toBeGreaterThan(BigInt(old.revision!));
    expect((await asReader(db, [["codex", "s"]])).map((r) => r.session_binding_id)).toEqual([after.id!]);
  });

  test("live -> turn-only hidden -> visible: the hide retires the binding; becoming visible again rotates it (intended)", async () => {
    const db = await database();
    // A registered turn writer and its synthetic turns-origin pointer (20261105062020), plus a transcript pointer.
    await db.query("INSERT INTO sessions.turn_writers (org_id, workspace_id, user_id, source, session_id) VALUES ('o','w','ut','codex','s')");
    await db.query(
      `INSERT INTO sessions.session_index
         (org_id, workspace_id, harness, session_id, machine_id, user_id, created_at, last_activity_at, title, state,
          event_count, input_tokens, storage_provider, bucket, manifest_key, trace_id, revision, indexed_at)
       VALUES ('o','w','codex','s',
               'srv_' || left(encode(sha256(convert_to('w' || chr(31) || 'relayhistory:conversation_turns:v1' || chr(31) || 'ut', 'UTF8')), 'hex'), 32),
               'ut', '2026-10-01T10:00:00Z', '2026-10-01T12:00:00Z', 't', 'active', 1, 0, 's3', 'bucket', 'key/turns',
               '019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c18', 1, now())`,
    );
    await upsertPointer(db, { machine: "m1", last: "2026-10-01T12:30:00Z", revision: 1 });
    await projectSessionDirectory(db, KEY);
    const live = (await binding(db))!;
    expect(live.deleted).toBe(false);
    // The transcript pointer goes; only the turns pointer is live, so the row is hidden (deleted_at set by 062020).
    await tombstonePointer(db, "m1");
    await projectSessionDirectory(db, KEY);
    expect(await binding(db)).toEqual({ ...live, deleted: true });
    expect(await asReader(db, [["codex", "s"]])).toEqual([]);
    // A new transcript revision makes it visible again: a new incarnation as far as the binding goes, so a consumer
    // must re-confirm whatever it attached (fail closed), never inherit it across the hidden interval.
    await upsertPointer(db, { machine: "m1", last: "2026-10-02T12:00:00Z", revision: 2 });
    await projectSessionDirectory(db, KEY);
    const visible = (await binding(db))!;
    expect(visible.deleted).toBe(false);
    expect(visible.id).not.toBe(live.id);
    expect(BigInt(visible.revision!)).toBeGreaterThan(BigInt(live.revision!));
  });

  test("a hard erase then a delayed old revision re-creating the row: a new id and a greater revision", async () => {
    const db = await database();
    await upsertPointer(db, { machine: "m1", last: "2026-10-01T12:00:00Z", revision: 5 });
    await projectSessionDirectory(db, KEY);
    const old = (await binding(db))!;
    expect(await eraseSessionDirectory(db, KEY)).toBe(1);
    expect(await binding(db)).toBeUndefined();
    await projectSessionDirectory(db, KEY);
    const recreated = (await binding(db))!;
    expect(recreated.id).not.toBe(old.id);
    expect(BigInt(recreated.revision!)).toBeGreaterThan(BigInt(old.revision!));
  });

  test("an operator erasure fence: no pointer or directory write revives the session; the reader stays empty", async () => {
    const db = await database();
    await upsertPointer(db, { machine: "m1", last: "2026-10-01T12:00:00Z" });
    await projectSessionDirectory(db, KEY);
    const old = (await binding(db))!;
    await tombstonePointer(db, "m1");
    await projectSessionDirectory(db, KEY);
    await db.query("INSERT INTO sessions.session_erasure_fences (org_id, workspace_id, source, session_id) VALUES ('o','w','codex','s')");
    // A delayed new revision and a direct revival attempt are both dropped by the 050010 guards.
    await upsertPointer(db, { machine: "m1", last: "2026-10-03T12:00:00Z", revision: 9 });
    await upsertPointer(db, { machine: "m-new", last: "2026-10-03T12:00:00Z" });
    await projectSessionDirectory(db, KEY);
    await db.query("UPDATE sessions.session_directory SET deleted_at = NULL WHERE session_id = 's'");
    expect(await binding(db)).toEqual({ ...old, deleted: true });
    expect(await asReader(db, [["codex", "s"]])).toEqual([]);
  });

  test("aliases: pointers carrying a canonical session id share the canonical row's one binding", async () => {
    const db = await database();
    await upsertPointer(db, { machine: "m1", session: "canon", last: "2026-10-01T12:00:00Z" });
    await projectSessionDirectory(db, { ...KEY, sessionId: "canon" });
    const canon = (await binding(db, "canon"))!;
    await upsertPointer(db, { machine: "m2", session: "alias", canonicalSession: "canon", last: "2026-10-01T13:00:00Z" });
    await projectSessionDirectory(db, { ...KEY, sessionId: "alias" });
    expect(await binding(db, "alias")).toBeUndefined();
    expect(await binding(db, "canon")).toEqual(canon);
    expect((await asReader(db, [["codex", "alias"], ["codex", "canon"]])).map((r) => r.session_id)).toEqual(["canon"]);
  });

  test("an UPDATE that changes the row's key mints a new binding under the new key", async () => {
    const db = await database();
    await upsertPointer(db, { machine: "m1", last: "2026-10-01T12:00:00Z" });
    await projectSessionDirectory(db, KEY);
    const old = (await binding(db))!;
    // Memberships reference the key (FOREIGN KEY), so a key change first clears them.
    for (const t of ["users", "machines"]) await db.query(`DELETE FROM sessions.session_directory_${t} WHERE session_id = 's'`);
    await db.query("UPDATE sessions.session_directory SET session_id = 's-moved' WHERE session_id = 's'");
    const moved = (await binding(db, "s-moved"))!;
    expect(moved.id).not.toBe(old.id);
    expect(BigInt(moved.revision!)).toBeGreaterThan(BigInt(old.revision!));
  });

  test("a live row from before 062380 has no binding until its next write; the reader omits it until then", async () => {
    const db = await database();
    await db.exec("ALTER TABLE sessions.session_directory DISABLE TRIGGER zz_session_directory_binding");
    await upsertPointer(db, { machine: "m1", last: "2026-10-01T12:00:00Z" });
    await projectSessionDirectory(db, KEY);
    await db.exec("ALTER TABLE sessions.session_directory ENABLE TRIGGER zz_session_directory_binding");
    expect(await binding(db)).toEqual({ id: null, revision: null, createdAt: null, deleted: false });
    expect(await asReader(db, [["codex", "s"]])).toEqual([]);
    await projectSessionDirectory(db, KEY);
    const minted = (await binding(db))!;
    expect(minted.id).toMatch(/^[0-9a-f-]{36}$/);
    await projectSessionDirectory(db, KEY);
    expect(await binding(db)).toEqual(minted);
  });
});

describe("062380 session binding: the reader and its boundary", () => {
  test("SECURITY DEFINER, pinned search_path, EXECUTE for bulls96_read_api alone; the trigger function has no grantee", async () => {
    const db = await database();
    const fn = (sig: string) =>
      db.query<{ secdef: boolean; config: string[] | null }>("SELECT prosecdef AS secdef, proconfig AS config FROM pg_proc WHERE oid = $1::regprocedure", [sig]).then((r) => r.rows[0]!);
    expect(await fn(READER)).toEqual({ secdef: true, config: ["search_path=pg_catalog, sessions, pg_temp"] });
    expect(await fn("sessions.session_directory_binding()")).toEqual({ secdef: true, config: ["search_path=pg_catalog, pg_temp"] });
    for (const role of ["anon", "authenticated", "service_role", "bulls96_read_api"]) {
      const can = (await db.query<{ c: boolean }>("SELECT has_function_privilege($1, $2, 'EXECUTE') AS c", [role, READER])).rows[0]!.c;
      expect([role, can]).toEqual([role, role === "bulls96_read_api"]);
      const trig = (await db.query<{ c: boolean }>("SELECT has_function_privilege($1, 'sessions.session_directory_binding()', 'EXECUTE') AS c", [role])).rows[0]!.c;
      expect([role, trig]).toEqual([role, false]);
    }
    const seq = (await db.query<{ c: boolean }>("SELECT has_sequence_privilege('bulls96_read_api', 'sessions.session_binding_revision', 'USAGE') AS c")).rows[0]!.c;
    expect(seq).toBe(false);
  });

  test("tenant scope (42501), argument bounds (22023), other tenants absent, keys deduplicated and ordered", async () => {
    const db = await database();
    for (const s of ["b", "a"]) {
      await upsertPointer(db, { machine: "m1", session: s, last: "2026-10-01T12:00:00Z" });
      await projectSessionDirectory(db, { ...KEY, sessionId: s });
    }
    await db.query(
      `INSERT INTO sessions.session_directory (org_id, workspace_id, harness, session_id, created_at, last_activity_at)
       VALUES ('o','w2','codex','a', now(), now()), ('o2','w','codex','a', now(), now())`,
    );
    const rows = await asReader(db, [["codex", "b"], ["codex", "a"], ["codex", "a"], ["codex", "missing"], ["kimi", "a"]]);
    expect(rows.map((r) => r.session_id)).toEqual(["a", "b"]);
    expect(await code(asReader(db, [["codex", "a"]], ["o", "w"], "w2"))).toBe("42501");
    expect(await code(asReader(db, [["codex", "a"]], ["o", "w"], "w", "service_role"))).toBe("42501");
    expect(await code(asReader(db, []))).toBe("22023");
    expect(await code(asReader(db, Array.from({ length: 201 }, (_, i) => ["codex", `s${i}`] as [string, string])))).toBe("22023");
    expect(await asReader(db, Array.from({ length: 200 }, (_, i) => ["codex", `s${i}`] as [string, string]))).toEqual([]);
    const mismatched = db.transaction(async (tx) => {
      await tx.exec("SET LOCAL ROLE bulls96_read_api");
      await tx.query("SELECT set_config('bulls96_read.org_id','o',true), set_config('bulls96_read.workspace_id','w',true)");
      await tx.query("SELECT * FROM sessions.session_directory_bindings_for('w', ARRAY['codex','codex'], ARRAY['a'])");
    });
    expect(await code(mismatched)).toBe("22023");
    const nulls = db.transaction(async (tx) => {
      await tx.exec("SET LOCAL ROLE bulls96_read_api");
      await tx.query("SELECT set_config('bulls96_read.org_id','o',true), set_config('bulls96_read.workspace_id','w',true)");
      await tx.query("SELECT * FROM sessions.session_directory_bindings_for('w', ARRAY['codex'], ARRAY[NULL::text])");
    });
    expect(await code(nulls)).toBe("22023");
  });
});

describe("062380 session binding: sqlDirectory.bindings through the read API's tenant runner", () => {
  test("returns the live bindings inside the tenant-bound transaction, mapped to the route's shape", async () => {
    const db = await database();
    await upsertPointer(db, { machine: "m1", last: "2026-10-01T12:00:00Z" });
    await projectSessionDirectory(db, KEY);
    const live = (await binding(db))!;
    const runner = poolTenantRunner({
      async connect() {
        await db.exec("SET ROLE bulls96_read_api");
        return {
          query: (text: string, params?: unknown[]) => db.query(text, params as unknown[]) as Promise<{ rows: unknown[] }>,
          release: () => void db.exec("RESET ROLE"),
        };
      },
    });
    const keys = [{ harness: "codex", sessionId: "s" }, { harness: "codex", sessionId: "missing" }];
    const rows = await sqlDirectory(runner).bindings!("o", "w", keys);
    expect(sessionBindings(rows!, keys)).toEqual([
      { source: "codex", sessionId: "s", sessionBindingId: live.id!, bindingRevision: live.revision!, bindingCreatedAt: live.createdAt!.toISOString() },
    ]);
  });
});

describe("062380 session binding: the revision sequence stays owner-only and its order survives a re-apply", () => {
  test("a default sequence grant present before the migration is revoked; no non-owner can setval or nextval", async () => {
    const db = await database((d) =>
      d.exec("ALTER DEFAULT PRIVILEGES IN SCHEMA sessions GRANT ALL ON SEQUENCES TO service_role, bulls96_read_api, PUBLIC"));
    expect(await sequenceGrantees(db)).toEqual([]);
    for (const role of ["service_role", "bulls96_read_api", "anon", "authenticated"])
      for (const priv of ["USAGE", "SELECT", "UPDATE"]) {
        const can = (await db.query<{ c: boolean }>("SELECT has_sequence_privilege($1, 'sessions.session_binding_revision', $2) AS c", [role, priv])).rows[0]!.c;
        expect([role, priv, can]).toEqual([role, priv, false]);
      }
    // The trigger (SECURITY DEFINER, owner) still mints.
    await upsertPointer(db, { machine: "m1", last: "2026-10-01T12:00:00Z" });
    await projectSessionDirectory(db, KEY);
    expect((await binding(db))?.revision).toMatch(/^\d+$/);
  });

  test("a retained sequence (a rolled-back earlier install) is re-adopted and continues the order", async () => {
    const db = await database(async (d) => {
      await d.exec("CREATE SEQUENCE sessions.session_binding_revision AS bigint");
      await d.query("SELECT setval('sessions.session_binding_revision', 500)");
      await d.exec("GRANT UPDATE ON SEQUENCE sessions.session_binding_revision TO service_role");
    });
    expect(await sequenceGrantees(db)).toEqual([]);
    await upsertPointer(db, { machine: "m1", last: "2026-10-01T12:00:00Z" });
    await projectSessionDirectory(db, KEY);
    expect((await binding(db))?.revision).toBe("501");
  });

  test("a retained sequence of another shape refuses the migration (55000)", async () => {
    for (const shape of ["AS integer", "AS bigint INCREMENT BY 2", "AS bigint CYCLE", "AS bigint MINVALUE 0"]) {
      const refused = await database((d) => d.exec(`CREATE SEQUENCE sessions.session_binding_revision ${shape}`)).then(
        () => null, (e: { code?: string }) => e.code ?? "no_code");
      expect([shape, refused]).toEqual([shape, "55000"]);
    }
  });
});
