/**
 * Causal two-connection proof that owner-parity discovery (scripts/owner-buffered-parity-ci.ts
 * runOwnerParityDiscovery) classifies every owner inside ONE REPEATABLE READ READ ONLY snapshot on the enumerator
 * session: the marker read, the owner enumeration, every G1 and stored-proof comparison and the final marker read.
 * After enumeration, a second connection commits a change to a parity input (a frozen-source reset) or to an
 * owner_buffered_parity row without moving the PASS generation; discovery must still report the pre-change
 * dispositions. The pre-repair code compared each owner in its own autocommit snapshot after enumeration, so it saw
 * the change, counted an eighth missing/stale owner and refused unexpected_scope: this test fails on it.
 *
 * OWNED disposable PostgreSQL only: loopback, user postgres, no password, scratch databases.
 * Run: OWNER_PARITY_TEST_PG_URL=postgres://postgres@127.0.0.1:5432/postgres bun test --timeout 300000 <this file>
 */
import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import pg from "pg";
import { readFileSync, readdirSync } from "node:fs";
import { join } from "node:path";
import {
  ownerParityProofSeedSql,
  PROOF_TENANTS,
  proofTenantLiteral,
  runOwnerParityDiscovery,
  type OwnerParitySessions,
} from "../../../scripts/owner-buffered-parity-ci.ts";
import { tenantSha256 } from "../../../scripts/owner-buffered-parity-operator.ts";
import { writeCurrentParityProof } from "../owner-buffered-parity.ts";
import { SESSION_BOOTSTRAP_FILES } from "../../hoopdb/session-bootstrap-chain.ts";
import type { Queryable } from "../relayhistory-export.ts";

const url = process.env.OWNER_PARITY_TEST_PG_URL;
if (url) {
  const u = new URL(url);
  if (!["postgresql:", "postgres:"].includes(u.protocol) || !["127.0.0.1", "localhost", "[::1]"].includes(u.hostname) ||
    u.username !== "postgres" || u.password || u.pathname !== "/postgres" || u.search || u.hash)
    throw Error("owned_loopback_fixture_required");
}
const root = join(import.meta.dir, "../../..");
const read = (p: string) => readFileSync(join(root, p), "utf8").split("\n").filter((l) => !l.startsWith("\\")).join("\n");
const H = (c: string) => c.repeat(64);
const SHA = "5".repeat(40);
const env = { OWNER_PARITY_MODE: "discovery", OWNER_PARITY_SHA: SHA, OWNER_PARITY_CENSUS_SOURCE: "frozen-supabase" };
const emptyRoster = { contract: "owner-buffered-parity-roster/v1", tenants: [] as string[] };
const BASE = "owner_parity_disc_base_" + crypto.randomUUID().replaceAll("-", "");
const databases: string[] = [];
let admin: pg.Client | undefined;

const dbUrl = (name: string) => { const u = new URL(url!); u.pathname = "/" + name; return u.href; };
async function connect(name: string): Promise<pg.Client> {
  const c = new pg.Client({ connectionString: dbUrl(name) });
  await c.connect();
  return c;
}
const queryable = (c: pg.Client): Queryable => ({ query: async (sql: string, params?: unknown[]) => c.query(sql, params) as never });
/** A frozen-source reset for `index`: moves that owner's fresh proof (reset digest) without touching the generation. */
async function moveSource(c: pg.Client, index: number, request: string): Promise<void> {
  const t = proofTenantLiteral(PROOF_TENANTS[index]!);
  const insert = `INSERT INTO sessions.ingest_frozen_source_resets(org_id,workspace_id,user_id,request_id,deleted) VALUES(${t},'${request}',1)`;
  await c.query(`SET bulls96.privacy_authority_writer='authority'; ${insert}; RESET bulls96.privacy_authority_writer;`)
    .catch(() => c.query(insert));
}

// Before any change: tenants 1 and 3 canonical current, tenant 2 stale, tenants 4-9 missing (exactly seven).
const PRE_CHANGE = [
  { tenantSha256: tenantSha256(PROOF_TENANTS[1]!), disposition: "stale" },
  ...PROOF_TENANTS.slice(3).map((t) => ({ tenantSha256: tenantSha256(t), disposition: "missing" })),
].sort((a, b) => (a.tenantSha256 < b.tenantSha256 ? -1 : 1));

describe.skipIf(!url)("owner parity discovery holds one snapshot across enumeration and comparison (PostgreSQL)", () => {
  beforeAll(async () => {
    admin = new pg.Client({ connectionString: url });
    await admin.connect();
    await admin.query("CREATE DATABASE " + BASE);
    databases.push(BASE);
    const c = await connect(BASE);
    try {
      await c.query(read("supabase/tests/supabase_shim.sql"));
      await c.query("CREATE EXTENSION pg_trgm; CREATE EXTENSION btree_gin");
      for (const f of SESSION_BOOTSTRAP_FILES) await c.query(read(f));
      for (const f of readdirSync(join(root, "supabase/migrations")).filter((n) => n.endsWith(".sql")).sort())
        await c.query(read("supabase/migrations/" + f));
      await c.query(ownerParityProofSeedSql());
    } finally {
      await c.end();
    }
    // Setup only: canonical rows for tenants 1-3 through the real writer (two sessions), then tenant 2's source moves.
    const [s, t] = [await connect(BASE), await connect(BASE)];
    try {
      for (const tenant of PROOF_TENANTS.slice(0, 3))
        await writeCurrentParityProof(queryable(s), queryable(t), tenant, H("4"), "frozen-supabase");
      await moveSource(s, 1, "request-stale");
    } finally {
      await Promise.all([s.end(), t.end()]);
    }
  }, 300_000);

  afterAll(async () => {
    for (const name of databases.splice(0).reverse())
      await admin?.query(`DROP DATABASE IF EXISTS ${name} WITH (FORCE)`).catch(() => undefined);
    await admin?.end();
  }, 60_000);

  async function scenario(change: (b: pg.Client) => Promise<void>) {
    const name = "owner_parity_disc_" + crypto.randomUUID().replaceAll("-", "");
    await admin!.query(`CREATE DATABASE ${name} TEMPLATE ${BASE}`);
    databases.push(name);
    const [a, target, source, b] = [await connect(name), await connect(name), await connect(name), await connect(name)];
    try {
      for (const c of [a, target, source]) await c.query("SET default_transaction_read_only = on");
      const sessions: OwnerParitySessions = { source: queryable(source), enumerator: queryable(a), target: queryable(target) };
      let hooked = 0;
      const out = await runOwnerParityDiscovery(env, sessions, {
        roster: emptyRoster, runId: "707",
        deps: {
          // Connection B, after A enumerated the owners and before any comparison: an autocommit change that the
          // generation fence cannot see (the PASS generation does not move).
          afterEnumeration: async () => {
            hooked++;
            const before = (await b.query("SELECT generation FROM sessions.__96bulls_role_validation")).rows;
            await change(b);
            expect((await b.query("SELECT generation FROM sessions.__96bulls_role_validation")).rows).toEqual(before);
          },
        },
      });
      // A fresh run after the change sees it: the change was causal (an eighth missing/stale owner).
      const fresh = await runOwnerParityDiscovery(env, sessions, { roster: emptyRoster, runId: "708" });
      return { out, fresh, hooked };
    } finally {
      await Promise.all([a, target, source, b].map((c) => c.end()));
    }
  }

  const expectPreChange = (r: Awaited<ReturnType<typeof scenario>>) => {
    expect(r.hooked).toBe(1);
    expect(r.out.exitCode).toBe(0);
    expect(r.out.receipt).toMatchObject({ status: "ok", generation: H("4"),
      counts: { scanned: 9, canonicalCurrent: 2, missing: 6, stale: 1, unparsable: 0 } });
    expect(r.out.receipt.owners).toEqual(PRE_CHANGE);
    expect(r.fresh.receipt).toMatchObject({ status: "refused", code: "unexpected_scope", owners: [] });
  };

  test("a parity input moved on connection B after enumeration: dispositions stay those of the snapshot", async () => {
    // Tenant 1 is canonical current in the snapshot; its frozen source moves (it would read stale outside it).
    expectPreChange(await scenario((b) => moveSource(b, 0, "request-after-enumeration")));
  }, 300_000);

  test("an owner_buffered_parity row deleted on connection B after enumeration: dispositions stay those of the snapshot", async () => {
    // Tenant 3 is canonical current in the snapshot; its stored proof disappears (it would read missing outside it).
    expectPreChange(await scenario(async (b) => {
      const r = await b.query("DELETE FROM sessions.owner_buffered_parity WHERE user_id=$1", [PROOF_TENANTS[2]!.userId]);
      expect(r.rowCount).toBe(1);
    }));
  }, 300_000);
});
