import { afterEach, expect, test } from "bun:test";
import { PGlite } from "@electric-sql/pglite";
import type pg from "pg";
import { advanceReset, recordResetFailure } from "../account-reset-runner.ts";
import {
  alice,
  bob,
  own,
  shared,
  foreign,
  id,
  base,
  legacyFixture,
  cloudSchema,
  enqueue,
  job,
  verified,
} from "./account-reset-fixture.ts";

let fixture: PGlite | undefined;
afterEach(async () => {
  await fixture?.close();
});
async function setup() {
  fixture = new PGlite();
  await fixture.exec(base + legacyFixture + cloudSchema);
  const db = fixture as unknown as pg.Client;
  const run = async (ports: Partial<Parameters<typeof advanceReset>[4]> = {}) =>
    advanceReset(db, await job(db), {}, "https://cast.example", {
      findWorkspaces: async () => [],
      erase: verified,
      ...ports,
    });
  return { db, run };
}

test("pins confirmation, original membership, provider identity and target user; neighbours survive account reset and every retry", async () => {
  const { db, run } = await setup();
  expect(await enqueue(db, "bob@test")).toEqual([]);
  await enqueue(db);
  await enqueue(db);
  expect((await job(db)).scope_orgs).toEqual([own]);
  expect((await job(db)).relay_targets).toEqual([
    { id: "r1", provider_id: "provider-alice" },
  ]);
  const snapshot = async () =>
    Promise.all(
      [
        "SELECT * FROM users WHERE id=$1",
        "SELECT * FROM organization_memberships WHERE user_id=$1 ORDER BY organization_id",
        "SELECT * FROM auth_identities WHERE user_id=$1",
        "SELECT * FROM api_token_sessions WHERE user_id=$1",
        "SELECT * FROM sessions.auth_sessions WHERE user_id=$1 ORDER BY org_id",
        "SELECT * FROM sessions.probe_connections WHERE user_id=$1 ORDER BY org_id",
        "SELECT * FROM sessions.delivery_records WHERE user_id=$1 ORDER BY org_id",
        "SELECT * FROM relay_workspaces WHERE owner_user_id=$1 ORDER BY id",
      ].map(async (text) => (await db.query(text, [bob])).rows),
    );
  const before = await snapshot();
  // A newly private org and a freshly acquired workspace never widen a queued reset.
  await db.query(
    "DELETE FROM organization_memberships WHERE user_id=$1 AND organization_id=$2",
    [bob, shared],
  );
  const expected = await snapshot();
  await db.query(
    "INSERT INTO relay_workspaces VALUES('new',$1,'later','other-key',now())",
    [alice],
  );
  await expect(
    run({
      findWorkspaces: async () => [{ org_id: foreign, workspace_id: id(21) }],
    }),
  ).rejects.toThrow("workspace_scope_mismatch");
  expect((await job(db)).phase).toBe("account");
  expect(
    (await db.query("SELECT primary_email FROM users WHERE id=$1", [alice]))
      .rows[0].primary_email,
  ).toBe("alice@test");
  await run({
    findWorkspaces: async (orgs) => {
      expect(orgs).toEqual([own]);
      return [{ org_id: own, workspace_id: id(20) }];
    },
  });
  expect(await snapshot()).toEqual(expected);
  const captured = (await job(db)).history_targets;
  expect(captured).toHaveLength(1);
  expect(captured[0]).toMatchObject({
    orgId: own,
    workspaceId: id(20),
    legacy: true,
  });
  const calls: string[] = [];
  await run({
    fetch: (async (url, init) => {
      calls.push(String(url));
      expect(init!.headers).toMatchObject({ authorization: "Bearer key-a" });
      return new Response(null, { status: 204 });
    }) as typeof fetch,
  });
  expect(calls).toEqual(["https://cast.example/v1/workspaces/provider-alice"]);
  expect(
    (await db.query("SELECT * FROM workspaces WHERE relay_workspace_id='r3'"))
      .rows,
  ).toHaveLength(1);
  await expect(
    run({ erase: async () => ({ enabled: false }) }),
  ).rejects.toThrow("history_unverified");
  expect((await job(db)).history_targets).toEqual(captured);
  await run({
    erase: async (_env, body) => {
      expect(JSON.parse(await body())).toEqual({
        org_id: own,
        workspace_id: id(20),
        user_id: alice,
        request_id: captured[0]!.requestId,
        confirmation: "ERASE ALL UPLOADS OF THIS USER",
      });
      return verified();
    },
  });
  expect((await job(db)).phase).toBe("complete");
  expect(
    (
      await db.query(
        "SELECT DISTINCT org_id,ws,owner,request FROM sessions.calls",
      )
    ).rows,
  ).toEqual([
    { org_id: own, ws: id(20), owner: alice, request: captured[0]!.requestId },
  ]);
  expect(await snapshot()).toEqual(expected);
  expect(before[0]).toEqual(expected[0]);
}, 30000);

test("a rebind, ownership transfer, or different provider cannot cause an external DELETE", async () => {
  const { db, run } = await setup();
  await enqueue(db);
  await run();
  let calls = 0;
  const send = (async () => {
    calls++;
    return new Response(null, { status: 204 });
  }) as typeof fetch;
  await db.query("UPDATE workspaces SET relay_workspace_id='r1' WHERE id=$1", [
    id(21),
  ]);
  await expect(run({ fetch: send })).rejects.toThrow(
    "relaycast_scope_or_identity_changed",
  );
  await db.query("UPDATE workspaces SET relay_workspace_id='r2' WHERE id=$1", [
    id(21),
  ]);
  await db.query("UPDATE relay_workspaces SET owner_user_id=$1 WHERE id='r1'", [
    bob,
  ]);
  await expect(run({ fetch: send })).rejects.toThrow(
    "relaycast_scope_or_identity_changed",
  );
  await db.query(
    "UPDATE relay_workspaces SET owner_user_id=$1,relaycast_workspace_id='other' WHERE id='r1'",
    [alice],
  );
  await expect(run({ fetch: send })).rejects.toThrow(
    "relaycast_scope_or_identity_changed",
  );
  expect(calls).toBe(0);
  expect(
    (await db.query("SELECT * FROM admin_reset_retired_workspaces")).rows,
  ).toEqual([]);
}, 30000);

test("crash or ambiguous HTTP result preserves the pinned target, refuses rebinding and never acknowledges completion", async () => {
  const { db, run } = await setup();
  await enqueue(db);
  await run();
  const first = await job(db);
  await expect(
    run({
      fetch: (async () => {
        throw Error("disconnected");
      }) as typeof fetch,
    }),
  ).rejects.toThrow("disconnected");
  await expect(
    db.query("UPDATE workspaces SET relay_workspace_id='r1' WHERE id=$1", [
      id(21),
    ]),
  ).rejects.toThrow("retired");
  await expect(
    db.query("UPDATE relay_workspaces SET owner_user_id=$1 WHERE id='r1'", [
      bob,
    ]),
  ).rejects.toThrow("immutable");
  for (const status of [401, 404]) {
    await expect(
      run({
        fetch: (async () => new Response(null, { status })) as typeof fetch,
      }),
    ).rejects.toThrow("relaycast_deletion_unverified");
  }
  expect((await job(db)).relay_targets).toEqual(first.relay_targets);
  await run({
    fetch: (async () => new Response(null, { status: 204 })) as typeof fetch,
  });
  expect((await job(db)).phase).toBe("history");
}, 30000);

test("older retry requires the scrubbed user, preserves other owners and never enqueues History erasure", async () => {
  const { db } = await setup();
  const retry = async () =>
    (
      await db.query(
        "SELECT * FROM public.admin_enqueue_legacy_relay_cleanup($1,'staff')",
        [alice],
      )
    ).rows;
  expect(await retry()).toEqual([]);
  await db.query("BEGIN");
  await db.query(
    "UPDATE users SET primary_email=NULL,updated_at=now() WHERE id=$1",
    [alice],
  );
  await db.query(
    "UPDATE organization_memberships SET status='revoked',updated_at=now() WHERE user_id=$1",
    [alice],
  );
  await db.query("COMMIT");
  await retry();
  await retry();
  expect(await job(db)).toMatchObject({
    phase: "relaycast",
    legacy_relay_only: true,
    scope_orgs: [own],
    relay_targets: [{ id: "r1", provider_id: "provider-alice" }],
    history_targets: [],
  });
}, 30000);

test("a digest shared with a second contributor stays intact and the native reset is never called", async () => {
  const { db, run } = await setup();
  await enqueue(db);
  await run();
  await run({
    fetch: (async () => new Response(null, { status: 204 })) as typeof fetch,
  });
  await db.query(
    "CREATE TABLE sessions.daily_digests(org_id text,workspace_id text,narrative text)",
  );
  await db.query(
    "INSERT INTO sessions.daily_digests VALUES ($1,$2,'neighbor digest')",
    [own, id(20)],
  );
  await expect(run()).rejects.toThrow("legacy_digest_has_other_contributors");
  expect(
    (await db.query("SELECT narrative FROM sessions.daily_digests")).rows,
  ).toEqual([{ narrative: "neighbor digest" }]);
  expect((await db.query("SELECT * FROM sessions.calls")).rows).toEqual([]);
  expect((await job(db)).phase).toBe("history");
}, 30000);

test("ambiguous Relaycast ack permits independent history cleanup and remains visibly stalled", async () => {
  const { db, run } = await setup();
  await enqueue(db);
  await run();
  try {
    await run({
      fetch: (async () => new Response(null, { status: 401 })) as typeof fetch,
    });
    throw Error("expected refusal");
  } catch (error) {
    await recordResetFailure(db, await job(db), error);
  }
  expect(await job(db)).toMatchObject({
    phase: "history",
    stalled: false,
    deferred_error: "relaycast_deletion_unverified",
  });
  await run();
  expect(await job(db)).toMatchObject({
    phase: "relaycast",
    stalled: true,
    error_code: "relaycast_deletion_unverified",
    history_targets: [],
  });
}, 30000);
