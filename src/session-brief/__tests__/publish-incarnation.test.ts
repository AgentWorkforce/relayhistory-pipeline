/**
 * T130 slice 1 (b): a brief publishes only over the exact incarnation its input was read from.
 * recordBrief already refuses a different machine set or revision; these fixtures pin that a
 * re-create reusing the SAME revision with different content (manifest key or digest) is refused too.
 */
import { afterEach, describe, expect, setDefaultTimeout, test } from "bun:test";
import type { PGlite } from "@electric-sql/pglite";
import type { ManifestV1 } from "../../shared/vendor.ts";
import { armWakeup, briefSourceBinding, livePointers, recordBrief } from "../store.ts";
import { briefTick } from "../worker.ts";
import { FakeModel, KEY, database, dueNow, hoopDb, mapReader, pointer, tempStore, testConfig, transcript } from "./harness.ts";

setDefaultTimeout(60_000);
const open: PGlite[] = [];
afterEach(async () => {
  await Promise.all(open.splice(0).map((db) => db.close()));
});

const DIGEST_A = "ab".repeat(32); // the harness pointer's content_sha256
const DIGEST_B = "cd".repeat(32);
const ready = {
  status: "ready" as const,
  text: {
    title: "t", description: "d", state: "completed" as const, scrubRuleset: "relayhistory-scrub", scrubVersion: 1,
    fingerprint: "f".repeat(64), promptVersion: "session-summary-v3", engineVersion: "session-summary-v3:96bulls-v1",
    model: "gpt-5.6-luna", messageCount: 1, shortenedMessages: 0, inputTokens: 1, outputTokens: 1, durationMs: 1,
    generatedAt: new Date("2026-10-29T10:00:05.000Z"),
  },
};
const briefRows = async (pg: PGlite) =>
  (await pg.query<{ status: string }>("SELECT status FROM sessions.session_briefs")).rows;
const recreate = (pg: PGlite, digestHex: string, manifestKey?: string) =>
  pg.query(
    `UPDATE sessions.session_index SET content_sha256 = decode($1, 'hex')${manifestKey ? ", manifest_key = $2" : ""}
      WHERE machine_id = 'm-1'`,
    manifestKey ? [digestHex, manifestKey] : [digestHex],
  );

describe("recordBrief binds the source incarnation", () => {
  test("livePointers carries the manifest digest", async () => {
    const pg = await database();
    open.push(pg);
    await pointer(pg, { revision: 3, manifestKey: "manifest/s-1/m-1/r3" });
    expect(await livePointers(hoopDb(pg), KEY)).toEqual([
      { machineId: "m-1", manifestKey: "manifest/s-1/m-1/r3", revision: 3, contentSha256: DIGEST_A },
    ]);
  });

  test("control: a binding that still matches the live row is written", async () => {
    const pg = await database();
    open.push(pg);
    await pointer(pg, { revision: 3, manifestKey: "manifest/s-1/m-1/r3" });
    const binding = briefSourceBinding(await livePointers(hoopDb(pg), KEY));
    expect(await recordBrief(hoopDb(pg), KEY, { "m-1": 3 }, ready, { sourceBinding: binding })).toEqual({ written: true });
    expect(await briefRows(pg)).toEqual([{ status: "ready" }]);
  });

  test("same machine and revision, different digest: refused, nothing written", async () => {
    const pg = await database();
    open.push(pg);
    await pointer(pg, { revision: 3, manifestKey: "manifest/s-1/m-1/r3" });
    const binding = briefSourceBinding(await livePointers(hoopDb(pg), KEY));
    await recreate(pg, DIGEST_B);
    expect(await recordBrief(hoopDb(pg), KEY, { "m-1": 3 }, ready, { sourceBinding: binding })).toEqual({ written: false });
    expect(await briefRows(pg)).toEqual([]);
  });

  test("same machine and revision, different manifest key: refused", async () => {
    const pg = await database();
    open.push(pg);
    await pointer(pg, { revision: 3, manifestKey: "manifest/s-1/m-1/r3" });
    const binding = briefSourceBinding(await livePointers(hoopDb(pg), KEY));
    await recreate(pg, DIGEST_A, "manifest/s-1/m-1/r3-recreated");
    expect(await recordBrief(hoopDb(pg), KEY, { "m-1": 3 }, ready, { sourceBinding: binding })).toEqual({ written: false });
    expect(await briefRows(pg)).toEqual([]);
  });

  test("a binding missing a live machine is refused", async () => {
    const pg = await database();
    open.push(pg);
    await pointer(pg, { revision: 3, manifestKey: "manifest/s-1/m-1/r3" });
    const binding = briefSourceBinding(await livePointers(hoopDb(pg), KEY));
    delete binding["m-1"];
    binding["m-2"] = { manifestKey: "manifest/s-1/m-1/r3", contentSha256: DIGEST_A };
    expect(await recordBrief(hoopDb(pg), KEY, { "m-1": 3 }, ready, { sourceBinding: binding })).toEqual({ written: false });
  });

  test("without a binding the existing machine+revision check is unchanged", async () => {
    const pg = await database();
    open.push(pg);
    await pointer(pg, { revision: 3, manifestKey: "manifest/s-1/m-1/r3" });
    await recreate(pg, DIGEST_B);
    expect(await recordBrief(hoopDb(pg), KEY, { "m-1": 3 }, ready)).toEqual({ written: true });
  });
});

describe("the worker refuses a re-create between pointer read and publish", () => {
  async function run(recreateDuringGeneration: boolean) {
    const pg = await database();
    open.push(pg);
    const store = tempStore();
    const manifests = new Map<string, ManifestV1>();
    const manifestKey = "manifest/s-1/m-1/r1";
    manifests.set(
      manifestKey,
      await transcript(store, [{ type: "user", timestamp: "2026-10-29T10:00:01.000Z", message: { role: "user", content: "hello" } }], {
        machine: "m-1",
        revision: 1,
      }),
    );
    await pointer(pg, { revision: 1, manifestKey });
    await armWakeup(hoopDb(pg), KEY, { idleSeconds: 600, maxWaitSeconds: 3600 });
    await dueNow(pg);
    const model = new FakeModel();
    const complete = model.complete.bind(model);
    model.complete = async (req) => {
      // The pointer row is re-created at the SAME revision while the model runs.
      if (recreateDuringGeneration) await recreate(pg, DIGEST_B);
      return complete(req);
    };
    const counts = await briefTick({ reader: mapReader(manifests), store, transport: model, config: testConfig() })(hoopDb(pg), new Date());
    return { counts, rows: await briefRows(pg) };
  }

  test("control: no re-create, the brief is generated", async () => {
    const { counts, rows } = await run(false);
    expect(counts).toEqual({ generated: 1 });
    expect(rows).toEqual([{ status: "ready" }]);
  });

  test("re-created at the same revision during generation: stale, no brief published", async () => {
    const { counts, rows } = await run(true);
    expect(counts).toEqual({ stale: 1 });
    expect(rows.filter((r) => r.status === "ready")).toEqual([]);
  });
});
