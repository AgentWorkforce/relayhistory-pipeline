/**
 * Migration 20261105054000 on PGlite, as the genuine bulls96_read_api login: the store over the
 * real functions, atomic refresh, identity binding, delete, the sweep, the role guard and the
 * table's refusal of direct access.
 */
import { afterAll, beforeAll, describe, expect, setDefaultTimeout, test } from "bun:test";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { PGlite } from "@electric-sql/pglite";
import { hashToken, sqlTokenStore, type NewTokenRow, type RefreshInput, type TokenQuery } from "../self-issued.ts";

setDefaultTimeout(60_000);
const root = join(import.meta.dir, "../../..");
const now = new Date();
const inMs = (ms: number) => new Date(now.getTime() + ms);
const AUD = "https://read.example.test";

let db: PGlite;
let store: ReturnType<typeof sqlTokenStore>;
const asReader = async <T>(work: () => Promise<T>) => {
  await db.exec("SET SESSION AUTHORIZATION bulls96_read_api");
  try {
    return await work();
  } finally {
    await db.exec("SET SESSION AUTHORIZATION postgres");
  }
};

beforeAll(async () => {
  db = new PGlite();
  await db.exec(`
    CREATE ROLE anon NOLOGIN; CREATE ROLE authenticated NOLOGIN; CREATE ROLE service_role NOLOGIN;
    CREATE ROLE bulls96_read_api LOGIN NOINHERIT;
    CREATE SCHEMA sessions; GRANT USAGE ON SCHEMA sessions TO bulls96_read_api;
  `);
  await db.exec(readFileSync(join(root, "supabase/migrations/20261105054000_read_api_tokens.sql"), "utf8"));
  const client: TokenQuery & { release(): void } = {
    async query(text, params) {
      return (await db.query(text, params as never)) as { rows: unknown[] };
    },
    release() {},
  };
  store = sqlTokenStore({ query: client.query, connect: async () => client });
});
afterAll(() => db.close());

let serial = 0;
async function seed(over: Partial<NewTokenRow> = {}) {
  serial += 1;
  const row: NewTokenRow = {
    id: `id-${serial}`, family: `f-${serial}`, subjectType: "cli", audience: AUD, userId: "u-a", orgId: "org-a", workspaceId: "ws-a",
    accessHash: await hashToken(`a${serial}`), accessExpires: inMs(60_000), refreshHash: await hashToken(`r${serial}`), refreshExpires: inMs(3_600_000),
    label: null, now, ...over,
  };
  await asReader(() => store.issue(row));
  return row;
}
const refreshOf = async (row: NewTokenRow, over: Partial<RefreshInput> = {}): Promise<RefreshInput> => ({
  refreshHash: row.refreshHash, userId: row.userId, orgId: row.orgId, workspaceId: row.workspaceId, audience: row.audience,
  nextAccessHash: await hashToken(`na${serial}${Math.random()}`), accessExpires: inMs(60_000),
  nextRefreshHash: await hashToken(`nr${serial}${Math.random()}`), refreshExpires: inMs(3_600_000), now, ...over,
});

describe("read_api_tokens", () => {
  test("verify returns the row's identity and only until expiry", async () => {
    const row = await seed();
    expect(await asReader(() => store.verify(row.accessHash, now))).toMatchObject({ id: row.id, userId: "u-a", orgId: "org-a", workspaceId: "ws-a", audience: AUD, scopes: ["rth:read"] });
    const expired = await seed({ accessExpires: new Date(Date.now() - 1000) });
    expect(await asReader(() => store.verify(expired.accessHash, now))).toBeNull();
    // An old timestamp from the caller cannot revive it.
    expect(await asReader(() => store.verify(expired.accessHash, new Date(0)))).toBeNull();
    expect(await asReader(() => store.verify("0".repeat(64), now))).toBeNull();
  });

  test("refresh is a compare-and-set bound to the exact identity", async () => {
    const row = await seed();
    for (const other of [{ userId: "u-x" }, { orgId: "org-x" }, { workspaceId: "ws-x" }, { audience: "https://other.example.test" }])
      expect(await asReader(async () => store.refresh(await refreshOf(row, other)))).toBeNull();
    const first = await refreshOf(row);
    expect((await asReader(() => store.refresh(first)))?.id).toBe(row.id);
    // The old refresh hash is spent, and the old access hash stops resolving.
    expect(await asReader(async () => store.refresh(await refreshOf(row)))).toBeNull();
    expect(await asReader(() => store.verify(row.accessHash, now))).toBeNull();
    expect((await asReader(() => store.verify(first.nextAccessHash, now)))?.id).toBe(row.id);
  });

  test("a service token cannot be refreshed", async () => {
    const row = await seed({ subjectType: "service" });
    expect(await asReader(async () => store.refresh(await refreshOf(row)))).toBeNull();
  });

  test("delete by hash and by org, user, workspace or id; another org's id matches nothing", async () => {
    const one = await seed();
    expect(await asReader(() => store.deleteByHash("refresh", one.refreshHash))).toBe(true);
    expect(await asReader(() => store.verify(one.accessHash, now))).toBeNull();
    expect(await asReader(() => store.deleteByHash("access", one.accessHash))).toBe(false);
    const mine = await seed({ orgId: "org-del" });
    expect(await asReader(() => store.deleteFor("org-other", { id: mine.id }))).toBe(0);
    expect((await asReader(() => store.verify(mine.accessHash, now)))?.id).toBe(mine.id);
    const a = await seed({ orgId: "org-del", userId: "gone" });
    const b = await seed({ orgId: "org-del", userId: "stays" });
    expect(await asReader(() => store.deleteFor("org-del", { userId: "gone" }))).toBe(1);
    expect(await asReader(() => store.verify(a.accessHash, now))).toBeNull();
    expect((await asReader(() => store.verify(b.accessHash, now)))?.id).toBe(b.id);
    expect(await asReader(() => store.deleteFor("org-del", {}))).toBe(2);
  });

  test("issue and refresh sweep fully expired rows", async () => {
    const stale = await seed({ accessExpires: new Date(Date.now() - 2000), refreshExpires: new Date(Date.now() - 1000) });
    await seed();
    const { rows } = await db.query("SELECT 1 FROM sessions.read_api_tokens WHERE id = $1", [stale.id]);
    expect(rows.length).toBe(0);
  });

  test("the table refuses a non-rth:read scope", async () => {
    await expect(db.exec(`INSERT INTO sessions.read_api_tokens (id, token_family_id, subject_type, audience, user_id, org_id, workspace_id, scopes, access_token_hash, access_token_expires_at, refresh_token_hash, refresh_token_expires_at)
      VALUES ('x', 'f', 'cli', 'a', 'u', 'o', 'w', '["rth:sync"]', '${"a".repeat(64)}', now(), '${"b".repeat(64)}', now())`)).rejects.toThrow();
  });

  test("the reader has no table access and only the granted functions", async () => {
    await expect(asReader(() => db.query("SELECT * FROM sessions.read_api_tokens"))).rejects.toThrow(/permission denied/);
    await expect(asReader(() => db.query("DELETE FROM sessions.read_api_tokens"))).rejects.toThrow(/permission denied/);
    await expect(asReader(() => db.query("SELECT sessions.read_api_token_sweep_expired(now())"))).rejects.toThrow(/permission denied/);
  });

  test("another login, or SET ROLE into the reader, is refused", async () => {
    await expect(store.verify("0".repeat(64), now)).rejects.toThrow(/not the read api login/);
    await db.exec("GRANT bulls96_read_api TO postgres");
    await db.exec("BEGIN; SET LOCAL ROLE bulls96_read_api");
    await expect(store.verify("0".repeat(64), now)).rejects.toThrow(/not the read api login/);
    await db.exec("ROLLBACK");
  });
});
