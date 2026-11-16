/**
 * Summaries regeneration, push A: with every SUMMARY_REGEN_* / SUMMARY_RETRY_* flag off
 * (unset, or any value other than exactly "true"), the session brief must be byte-identical
 * to main before the change. The fixture was regenerated (GOLDEN_WRITE=1) on main 4ff67765,
 * the memory-bound base with no regeneration code, and this branch's flags-off run
 * reproduces it byte for byte (the bytes equal the earlier d83752c2 fixture). It pins every
 * query's text and params in order (recordBrief's writes and sweepUnbriefed included) and
 * the structured log lines, over a generated brief, a retry, a final failure and both sweep
 * modes.
 */
import { afterEach, describe, setDefaultTimeout, setSystemTime, test } from "bun:test";
import { join } from "node:path";
import type { PGlite } from "@electric-sql/pglite";
import type { ManifestV1 } from "../../shared/vendor.ts";
import { SessionAnalysisProviderError } from "../engine/transport.ts";
import { armWakeup, sweepUnbriefed } from "../store.ts";
import { briefTick } from "../worker.ts";
import { FakeModel, KEY, database, dueNow, hoopDb, mapReader, pointer, tempStore, testConfig, transcript } from "./harness.ts";
import { captureLogs, expectGolden, normalize, recording } from "../../shared/__tests__/golden.ts";

setDefaultTimeout(120_000);
const FIXTURE = join(import.meta.dir, "fixtures/regen-golden-flags-off.json");
const FLAGS = [
  "SUMMARY_REGEN_TURNS_ENABLED",
  "SUMMARY_RETRY_BACKOFF_ENABLED",
  "SUMMARY_RETRY_CONFIG_RESET_ENABLED",
  "SUMMARY_REGEN_AUDIT_ENABLED",
];

const open: PGlite[] = [];
afterEach(async () => {
  setSystemTime();
  for (const f of FLAGS) delete process.env[f];
  while (open.length) await open.pop()!.close();
});

const user = (text: string) => ({ type: "user", timestamp: "2026-10-29T10:00:01.000Z", message: { role: "user", content: text } });

async function run(flags: Record<string, string>) {
  for (const [k, v] of Object.entries(flags)) process.env[k] = v;
  // A frozen JS clock: durationMs and generatedAt are stable (the database clock is not used in params).
  setSystemTime(new Date());
  const pg = await database();
  open.push(pg);
  const store = tempStore();
  const manifests = new Map<string, ManifestV1>();
  const model = new FakeModel();
  const upload = async (lines: unknown[], revision: number, key = KEY) => {
    const manifestKey = `manifest/${key.sessionId}/m-1/r${revision}`;
    manifests.set(manifestKey, await transcript(store, lines, { key, revision }));
    await pointer(pg, { key, revision, manifestKey });
    await armWakeup(hoopDb(pg), key, { idleSeconds: 600, maxWaitSeconds: 3600 });
  };
  const { db, log: queries } = recording(hoopDb(pg));
  const steps: unknown[] = [];
  const step = async (name: string, run: () => Promise<unknown>) => {
    const start = queries.length;
    const { result, lines } = await captureLogs(run);
    steps.push({ name, result: result ?? null, queries: queries.slice(start), logs: lines });
  };
  const tick = (sweepSeconds = 0) =>
    briefTick({ reader: mapReader(manifests), store, transport: model, config: testConfig({ sweepSeconds }) })(db, new Date());

  await upload([user("fix the build")], 1);
  await dueNow(pg);
  await step("tick generated", () => tick());
  await pg.exec("UPDATE sessions.session_briefs SET attempted_at = now() - interval '10 minutes'");
  await upload([user("fix the build"), user("and the tests")], 2);
  model.next.push(
    new SessionAnalysisProviderError("model_rate_limited", true),
    new SessionAnalysisProviderError("model_unavailable", true),
  );
  await dueNow(pg);
  await step("tick retrying", () => tick());
  await dueNow(pg);
  await step("tick failed", () => tick());
  await pg.exec("UPDATE sessions.session_briefs SET attempted_at = now() - interval '10 minutes'");
  await upload([user("fix the build"), user("and the tests")], 3);
  model.next.push(new SessionAnalysisProviderError("model_auth_failed", false));
  await dueNow(pg);
  await step("tick final non-retryable", () => tick());
  // A second session with no brief and no wakeup: the sweep arms it.
  const other = { ...KEY, sessionId: "s-2" };
  await pointer(pg, { key: other, revision: 1, manifestKey: "manifest/s-2/m-1/r1" });
  await step("sweepUnbriefed default", () => sweepUnbriefed(db, 100));
  await pg.exec("DELETE FROM sessions.session_brief_wakeups");
  await step("sweepUnbriefed canonical", () =>
    sweepUnbriefed(db, 100, { includeChanged: true, refreshCanonicalInput: true, recoverJsonlInputFailures: true,
      recoverRateLimited: true, workspaceIds: ["ws-a"] }));
  await pg.exec("DELETE FROM sessions.session_brief_wakeups");
  await step("tick with sweep", () => tick(60));
  const briefs = (await pg.query("SELECT session_id, status, error, source_revisions FROM sessions.session_briefs ORDER BY session_id")).rows;
  return normalize({ steps, briefs });
}

describe("session brief flags-off golden (summaries regeneration push A)", () => {
  test("flags unset: SQL text and params and log lines match main", async () => {
    expectGolden(FIXTURE, await run({}));
  });

  test("flags set to anything but exactly \"true\" are off: identical to main", async () => {
    if (process.env.GOLDEN_WRITE === "1") return;
    expectGolden(FIXTURE, await run(Object.fromEntries(FLAGS.map((f, i) => [f, ["TRUE", "1", "yes", " true"][i]!]))));
  });
});
