/**
 * Summaries regeneration, push A: with every SUMMARY_REGEN_* / SUMMARY_RETRY_* flag off
 * (unset, or any value other than exactly "true"), Bob Costas must be byte-identical to
 * main before the change. The fixture was regenerated (GOLDEN_WRITE=1) on main 4ff67765, the
 * memory-bound base with no regeneration code, and this branch's flags-off run reproduces it
 * byte for byte (the bytes equal the earlier d83752c2 fixture: none of these cases reaches a
 * path the memory bound changed). It pins every query's text and params in order (the
 * handler's upsert included), the recorder lines and the structured log lines for a fresh
 * write, a current skip, a reconcile, newer and stale revisions, the model_failed and
 * no_api_key fallbacks, a scrub quarantine and the sweep. It does not reach
 * summary_input_incomplete, summary_input_unreadable, input_truncated, input sampling or
 * notCurrentContent; flags-off parity for those rests on handler.test.ts and
 * session-input.test.ts, which run this handler with regen off.
 */
import { afterEach, describe, setDefaultTimeout, setSystemTime, test } from "bun:test";
import { join } from "node:path";
import { zstdCompressSync } from "node:zlib";
import type { PGlite } from "@electric-sql/pglite";
import { rawChunkKey } from "../../shared/vendor.ts";
import { masterKeyring, parseMasterKeys } from "../../shared/keyring.ts";
import { bobCostasHandler } from "../handler.ts";
import { extractiveSummarizer } from "../summarizer.ts";
import { createModelSummarizer, modelSummarizerConfig } from "../model-summarizer.ts";
import { summarySweep } from "../sweep.ts";
import { SessionAnalysisProviderError } from "../../session-brief/engine/transport.ts";
import { database, hoopDb, pointer, tempStore } from "../../session-brief/__tests__/harness.ts";
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
  delete process.env.SUMMARY_SWEEP_BOUNDED;
  while (open.length) await open.pop()!.close();
});

const TRACE = "01a188e1-ef86-7b16-938a-e91b6b78643b";
const manifest = (session: string, revision: number, scrubbed = true) => ({
  revision,
  trace_id: TRACE,
  session: { workspace_id: "w", machine_id: "m", harness: "claude", session_id: session, started_at: "2026-10-29T10:00:00Z" },
  raw: {
    format: "jsonl",
    content_sha256: String(revision).repeat(64),
    chunks: [{ seq: 0, start_offset: 0, sha256_8: "abcdef12" }],
    ...(scrubbed ? { scrub: { ruleset: "r", version: 1 } } : {}),
  },
});
const loc = (session: string) => ({ workspaceId: "w", machineId: "m", harness: "claude", sessionId: session, startDate: "2026-10-29" });
const line = (text: string) => JSON.stringify({ type: "user", message: { role: "user", content: text } });

async function run(flags: Record<string, string>) {
  for (const [k, v] of Object.entries(flags)) process.env[k] = v;
  setSystemTime(new Date("2026-11-01T12:00:00.000Z"));
  const pg = await database();
  open.push(pg);
  const store = tempStore();
  for (const s of ["s1", "s2", "s3", "s4"])
    await store.put(rawChunkKey(loc(s), 0, 0, "abcdef12", "jsonl"),
      zstdCompressSync(Buffer.from([line(`fix the build ${s}`), line("then run the tests")].join("\n") + "\n")));
  const keyring = await masterKeyring(parseMasterKeys(`k1:${"a".repeat(64)}`));
  const { db, log: queries } = recording(hoopDb(pg));
  const manifests = new Map<string, unknown>();
  const reader = { provider: "fs", read: async (k: string) => (manifests.has(k) ? { manifest: manifests.get(k) } : null) } as never;
  const directoryCalls: unknown[] = [];
  const directory = {
    write: async (input: unknown) => { directoryCalls.push({ write: input }); return { written: true }; },
    clear: async (key: unknown) => { directoryCalls.push({ clear: key }); return true; },
  };
  const recorderLines: unknown[] = [];
  const recorder = { log: { info: (msg: string, f: unknown) => recorderLines.push({ msg, f }) } } as never;
  const failing = createModelSummarizer({
    transport: { complete: async () => { throw new SessionAnalysisProviderError("model_unavailable", true); } },
    config: { ...modelSummarizerConfig("gpt-test-mini", {}), maxAttempts: 1 },
    sleep: async () => {},
  });
  const keyless = createModelSummarizer({ transport: null, config: modelSummarizerConfig("gpt-test-mini", {}) });
  const steps: unknown[] = [];
  const volatile = new Set<string>();
  const step = async (name: string, kind: "short" | "long", session: string, revision: number,
    summarizer = extractiveSummarizer, scrubbed = true) => {
    const key = `v1/ws=w/m=m/h=claude/d=2026/10/29/s=${session}/manifest.json`;
    manifests.set(key, manifest(session, revision, scrubbed));
    const handler = bobCostasHandler(kind, { db, reader, store, summarizer, directory, keyring });
    const start = queries.length;
    const { result, lines } = await captureLogs(() =>
      handler({ payload: { manifestKey: key }, ctx: { orgId: "o", session_uuid: "01a188e1-ef86-7892-9389-39149894eeec" } } as never, recorder));
    for (const r of (await pg.query<{ object_sha256: string | null }>("SELECT object_sha256 FROM sessions.session_summaries")).rows)
      if (r.object_sha256) volatile.add(r.object_sha256);
    steps.push({ name, result: result ?? null, queries: queries.slice(start), logs: lines });
  };
  await step("long fresh write", "long", "s1", 2);
  await step("long current skip", "long", "s1", 2);
  await step("short fresh write", "short", "s1", 2);
  await step("short current reconcile", "short", "s1", 2);
  await step("long newer revision", "long", "s1", 3);
  await step("long stale revision", "long", "s1", 2);
  await step("long model_failed fallback", "long", "s2", 2, failing);
  await step("long no_api_key fallback", "long", "s3", 2, keyless);
  await step("long quarantined", "long", "s4", 2, extractiveSummarizer, false);

  // The sweep over session_index: one session with no summary rows, s1 with both kinds.
  for (const session of ["s1", "s9"])
    await pointer(pg, { key: { orgId: "o", workspaceId: "w", harness: "claude", sessionId: session }, machine: "m", revision: 3,
      manifestKey: `v1/ws=w/m=m/h=claude/d=2026/10/29/s=${session}/manifest.json` });
  const sweepStart = queries.length;
  const sweep = await captureLogs(() => summarySweep({ keyring })(db, new Date()));
  steps.push({ name: "summary sweep", queries: queries.slice(sweepStart), logs: sweep.lines });
  const jobs = (await pg.query("SELECT stage, work_key, org_id, workspace_id, session_id, machine_id FROM sessions.pipeline_jobs ORDER BY stage, work_key")).rows;
  return normalize({ steps, directoryCalls, recorderLines, jobs }, volatile);
}

describe("Bob Costas flags-off golden (summaries regeneration push A)", () => {
  test("flags unset: SQL text and params, upsert params, recorder and log lines match main", async () => {
    expectGolden(FIXTURE, await run({}));
  });

  test("flags set to anything but exactly \"true\" are off: identical to main", async () => {
    if (process.env.GOLDEN_WRITE === "1") return;
    expectGolden(FIXTURE, await run(Object.fromEntries(FLAGS.map((f, i) => [f, ["TRUE", "1", "yes", " true"][i]!]))));
  });

  test("SUMMARY_SWEEP_BOUNDED empty or off (its tuning ignored): identical to main", async () => {
    if (process.env.GOLDEN_WRITE === "1") return;
    for (const v of ["", "off"])
      expectGolden(FIXTURE, await run({ SUMMARY_SWEEP_BOUNDED: v, SUMMARY_SWEEP_PAGE_ROWS: "nope", SUMMARY_SWEEP_WINDOW_SECONDS: "1" }));
  });
});
