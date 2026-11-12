import { expect, test } from "bun:test";
import { PGlite } from "@electric-sql/pglite";
import type pg from "pg";
import { createS3Store, objectKey } from "../../shared/vendor.ts";
import {
  createS3Eraser,
  type ListedVersion,
} from "../../migration/neon-s3-erasure/eraser.ts";
import { authorityFixture } from "../../session-ingest/__tests__/privacy-authority-support.ts";
import { advanceReset } from "../account-reset-runner.ts";
import {
  runUserErase,
  parseUserEraseRequest,
  userErasePrefixes,
} from "../user-erase.ts";
import {
  base,
  cloudSchema,
  alice,
  bob,
  own,
  foreign,
  id,
  enqueue,
  job,
} from "./account-reset-fixture.ts";

const endpoint = process.env.FRONTIER_TEST_S3_ENDPOINT;
const bucket = process.env.FRONTIER_TEST_S3_BUCKET;
const accessKeyId = process.env.FRONTIER_TEST_S3_ACCESS_KEY,
  secretAccessKey = process.env.FRONTIER_TEST_S3_SECRET_KEY;
if (endpoint) {
  const u = new URL(endpoint);
  if (
    u.protocol !== "http:" ||
    !["127.0.0.1", "localhost", "[::1]"].includes(u.hostname) ||
    u.username ||
    u.password ||
    u.pathname !== "/" ||
    u.search ||
    u.hash
  )
    throw Error("owned_loopback_fixture_required");
}
test.skipIf(!endpoint || !bucket || !accessKeyId || !secretAccessKey)(
  "versioned account reset erases all four original prefixes while preserving other users, organizations and workspaces",
  async () => {
    const conf = {
      endpoint: endpoint!,
      bucket: bucket!,
      region: "us-east-1",
      credentials: {
        accessKeyId: accessKeyId!,
        secretAccessKey: secretAccessKey!,
      },
    };
    const store = createS3Store(conf),
      eraser = createS3Eraser(conf);
    const root = "resetproof" + crypto.randomUUID().replaceAll("-", "");
    const cloud = new PGlite(),
      history = await authorityFixture();
    const authorize = async () => {};
    const prefixes = new Set<string>();
    const inventory = async () => {
      const rows: ListedVersion[] = [];
      for (const prefix of prefixes) {
        const result = await eraser.eraseStep(prefix, {
          scope: root,
          authorize,
          select: (v) => {
            rows.push(v);
            return false;
          },
        });
        expect(result.status).toBe("traversed");
      }
      return rows;
    };
    try {
      await history.seal();
      await cloud.exec(base + cloudSchema);
      const db = cloud as unknown as pg.Client;
      const target = new Set<string>();
      for (const [orgId, workspaceId, userId, origin] of [
        [own, id(20), alice, "device-a"],
        [own, id(20), bob, "device-b"],
        [foreign, id(20), alice, "device-a"],
        [own, id(21), alice, "device-a"],
      ] as const) {
        await history.db.query(
          "INSERT INTO sessions.upload_deletions(org_id,workspace_id,user_id,origin_id,source,session_id) VALUES($1,$2,$3,$4,'claude','same')",
          [orgId, workspaceId, userId, origin],
        );
        for (const prefix of userErasePrefixes(
          root,
          { orgId, workspaceId, userId, requestId: "fixture" },
          [origin],
        )) {
          prefixes.add(prefix);
          const key = objectKey(prefix + "same-session");
          if (orgId === own && workspaceId === id(20) && userId === alice)
            target.add(key);
          for (const version of ["one", "two"])
            await store.put(key, new TextEncoder().encode(key + version));
          await store.delete(key);
        }
      }
      const before = await inventory();
      expect(before).toHaveLength(48);
      await enqueue(db);
      const tick = async () =>
        advanceReset(db, await job(db), {}, "https://cast.test", {
          findWorkspaces: async () => [{ org_id: own, workspace_id: id(20) }],
          fetch: (async () =>
            new Response(null, { status: 204 })) as typeof fetch,
          erase: async (_env, request) => ({
            enabled: true,
            ...(await runUserErase(
              history.db as never,
              parseUserEraseRequest(await request()),
              {
                keyRoot: root,
                eraser,
                sleep: async () => {},
              },
            )),
          }),
        });
      await tick();
      await tick();
      await tick();
      expect((await job(db)).phase).toBe("complete");
      const keep = before.filter((v) => !target.has(v.key));
      expect(keep).toHaveLength(36);
      expect(await inventory()).toEqual(keep);
    } finally {
      try {
        for (const prefix of prefixes)
          await eraser.eraseStep(prefix, { scope: root, authorize });
      } finally {
        await history.close();
        await cloud.close();
      }
    }
  },
  60000,
);
