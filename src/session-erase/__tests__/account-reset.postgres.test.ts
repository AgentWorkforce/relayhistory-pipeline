// Two independent sessions on a disposable loopback cluster, never an application DB.
import { afterEach, describe, expect, test } from "bun:test";
import pg from "pg";
import { advanceReset } from "../account-reset-runner.ts";
import {
  alice,
  bob,
  own,
  foreign,
  id,
  base,
  cloudSchema,
  enqueue,
  job,
} from "./account-reset-fixture.ts";

const url = process.env.SESSION_INGEST_TEST_PG_URL;
if (url) {
  const u = new URL(url);
  if (
    !["127.0.0.1", "localhost", "[::1]"].includes(u.hostname) ||
    u.username !== "postgres" ||
    u.password ||
    u.pathname !== "/postgres" ||
    u.search ||
    u.hash
  )
    throw Error("owned_loopback_fixture_required");
}
const close: Array<() => Promise<unknown>> = [];
afterEach(async () => {
  for (const fn of close.splice(0).reverse()) await fn();
});
async function setup() {
  const admin = new pg.Client({ connectionString: url });
  await admin.connect();
  close.push(() => admin.end());
  const name = "account_reset_" + crypto.randomUUID().replaceAll("-", "");
  await admin.query("CREATE DATABASE " + name);
  close.push(() => admin.query("DROP DATABASE " + name + " WITH (FORCE)"));
  const dst = new URL(url!);
  dst.pathname = "/" + name;
  const connect = async () => {
    const c = new pg.Client({ connectionString: dst.href });
    await c.connect();
    close.push(() => c.end());
    await c.query("SET statement_timeout='5s'");
    return c;
  };
  const db = await connect(),
    other = await connect();
  await db.query(base);
  await db.query(cloudSchema);
  await enqueue(db);
  const run = async (ports: Partial<Parameters<typeof advanceReset>[4]> = {}) =>
    advanceReset(db, await job(db), {}, "https://cast.test", {
      findWorkspaces: async () => [],
      ...ports,
    });
  return { db, other, run };
}

describe.skipIf(!url)("account reset on PostgreSQL", () => {
  for (const commit of [true, false])
    test(`membership insert ${commit ? "committed" : "in-flight"} during account reset rolls the account and every target back`, async () => {
      const { db, other, run } = await setup();
      const work = run({
        findWorkspaces: async () => {
          await other.query("BEGIN");
          await other.query(
            "INSERT INTO organization_memberships VALUES ($1,$2,'active',now())",
            [own, bob],
          );
          if (commit) await other.query("COMMIT");
          return [];
        },
      });
      if (commit) await expect(work).rejects.toThrow("membership_changed");
      else await expect(work).rejects.toMatchObject({ code: "55P03" });
      if (!commit) await other.query("COMMIT");
      expect((await job(db)).phase).toBe("account");
      expect(
        (await db.query("SELECT primary_email FROM users WHERE id=$1", [alice]))
          .rows[0].primary_email,
      ).toBe("alice@test");
      expect(
        (await db.query("SELECT * FROM auth_identities")).rows,
      ).toHaveLength(2);
      expect(
        (await db.query("SELECT * FROM admin_reset_retired_workspaces")).rows,
      ).toEqual([]);
    });

  test("an in-flight transfer wins before retirement; no credential or delete reaches the gateway", async () => {
    const { db, other, run } = await setup();
    await run();
    await other.query("BEGIN");
    await other.query(
      "UPDATE workspaces SET relay_workspace_id='r1' WHERE id=$1",
      [id(21)],
    );
    const never = async () => {
      throw Error("outbound DELETE attempted");
    };
    await expect(run({ fetch: never as typeof fetch })).rejects.toMatchObject({
      code: "55P03",
    });
    await other.query("COMMIT");
    await expect(run({ fetch: never as typeof fetch })).rejects.toThrow(
      "relaycast_scope_or_identity_changed",
    );
    expect((await job(db)).relay_targets).toHaveLength(1);
    expect(
      (await db.query("SELECT * FROM admin_reset_retired_workspaces")).rows,
    ).toEqual([]);
  });

  test("retirement wins before a stale repeatable-read rebind; unrelated workspace writes proceed while the external delete is pending", async () => {
    const { db, other, run } = await setup();
    await run();
    await other.query("BEGIN ISOLATION LEVEL REPEATABLE READ");
    expect(
      (
        await other.query(
          "SELECT relay_workspace_id FROM workspaces WHERE id=$1",
          [id(20)],
        )
      ).rows[0].relay_workspace_id,
    ).toBe("r1");
    let deletes = 0;
    await run({
      fetch: (async (url, init) => {
        deletes++;
        expect(String(url)).toBe(
          "https://cast.test/v1/workspaces/provider-alice",
        );
        expect(init?.method).toBe("DELETE");
        await expect(
          other.query(
            "UPDATE workspaces SET relay_workspace_id='r1' WHERE id=$1",
            [id(21)],
          ),
        ).rejects.toMatchObject({ code: "40001" });
        await other.query("ROLLBACK");
        await expect(
          other.query(
            "UPDATE workspaces SET relay_workspace_id='r1' WHERE id=$1",
            [id(21)],
          ),
        ).rejects.toMatchObject({ code: "23514" });
        await other.query("INSERT INTO workspaces VALUES ($1,$2,'r2')", [
          id(99),
          foreign,
        ]);
        return new Response(null, { status: 204 });
      }) as typeof fetch,
    });
    expect(deletes).toBe(1);
    expect(
      (
        await db.query(
          "SELECT relay_workspace_id FROM workspaces WHERE id=$1",
          [id(21)],
        )
      ).rows[0].relay_workspace_id,
    ).toBe("r2");
    expect((await job(db)).phase).toBe("history");
  });
});

test.skipIf(!url)(
  "dashboard grants allow confirmed enqueue and status, while direct writes to targets and receipts are denied",
  async () => {
    const { db, other } = await setup();
    const role = "reset_dashboard_" + crypto.randomUUID().replaceAll("-", "");
    await db.query("CREATE ROLE " + role);
    // The disposable DB is dropped after this test; drop the cluster role after its grants.
    close.push(async () => {
      await other.query("RESET ROLE");
      await db.query("DROP OWNED BY " + role);
      await db.query("DROP ROLE " + role);
    });
    await db.query(
      `GRANT EXECUTE ON FUNCTION public.admin_enqueue_account_reset(uuid,text,text),public.admin_resume_account_reset(uuid) TO ${role}`,
    );
    await db.query(
      "GRANT SELECT(user_id,phase) ON public.admin_account_resets TO " + role,
    );
    await other.query("SET ROLE " + role);
    expect(await enqueue(other, "wrong")).toEqual([]);
    expect(await enqueue(other)).toHaveLength(1);
    expect(
      (await other.query("SELECT phase FROM admin_account_resets")).rows[0]
        .phase,
    ).toBe("account");
    for (const text of [
      "UPDATE admin_account_resets SET phase='complete'",
      "UPDATE admin_account_resets SET history_targets='[]'",
      "SELECT relaycast_api_key FROM relay_workspaces",
      "DELETE FROM users",
      "INSERT INTO admin_reset_retired_workspaces SELECT id,relaycast_workspace_id,owner_user_id FROM relay_workspaces",
    ]) {
      await expect(other.query(text)).rejects.toMatchObject({ code: "42501" });
    }
  },
);
