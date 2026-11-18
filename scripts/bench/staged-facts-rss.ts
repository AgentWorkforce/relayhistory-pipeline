/**
 * Peak RSS of the consumer's side of the staged output facts (slice 2c), for the memory decision before
 * the default flip. Offline and synthetic only: nothing is read from or sent to a service, and no database
 * is used (in a deployment the stage lives in the external Postgres; this measures what the consumer
 * process holds).
 *
 *   bun scripts/bench/staged-stream-rss.ts --export --records N --dir <new-empty-directory>   (the scope)
 *   bun scripts/bench/staged-facts-rss.ts --facts-pass --dir <that-directory>                (a fresh process)
 *   bun scripts/bench/staged-facts-rss.ts --finish --records N                               (a fresh process)
 *   bun scripts/bench/staged-facts-rss.ts --export-legacy --records N [--turn-chars C] [--wide] --dir <new-empty-directory>
 *   bun scripts/bench/staged-facts-rss.ts --whole-run --dir <that-directory>                 (a fresh process)
 *
 * --facts-pass: the streamed read (passes 1-3), then the facts pass in chunks of STAGED_FACTS_CHUNK inputs
 *   and FACTS_CHUNK_MAX_BYTES, each chunk turned into staged items (outputFactsBatchStep) and serialized
 *   as the staged writer sends it, then dropped (as after its transaction commits).
 * --finish: the final step's in-memory part with N legacy turn records of one session (each about the size
 *   of a real turn): the records as read back from the stage (parsed JSON), and outputFactsFinishStep's
 *   legacy fold over them.
 * --export-legacy: a scope of one session event and N legacy turn records (the worst shape for the final
 *   step: every record is a legacy turn it must hold).
 * --whole-run: ONE process doing what a staged run's consumer does with facts on: passes 1-3 (the read is
 *   kept, as the module keeps it to the final step), the facts pass (each chunk's items serialized as sent;
 *   the record items' bodies go to a file standing in for the database stage), then the final step (the
 *   records read back and parsed, as finishStagedOutputFacts reads them, and the fold).
 * Each prints one JSON line: the measurement, the RSS before it, the peak (sampled every 50 ms and the
 * process maxRSS), the growth and the seconds.
 */
import { readFile } from "node:fs/promises";
import { join, resolve } from "node:path";
import * as zlib from "node:zlib";
import { objectKey, parseManifest, readAll } from "../../packages/object-store/src/index.ts";
import { FACTS_CHUNK_MAX_BYTES, streamCanonicalScope } from "../../src/session-read-api/canonical-stream-projection.ts";
import { outputFactsBatchStep, outputFactsFinishStep } from "../../src/session-pipeline/output-facts.ts";
import { STAGED_FACTS_CHUNK } from "../../src/session-pipeline/search-index.ts";
import { addReplayPointer, createFsStore, replayDatabase, REPLAY_FIXTURE_AT, REPLAY_FIXTURE_IDENTITY as identity } from "../../src/session-pipeline/__tests__/replay-fixture.ts";
import { exportUnit } from "../../src/migration/neon-s3-sync/export-session.ts";
import { projectSessionDirectory } from "../../src/hoopdb/session-directory-projector.ts";
import { legacyMachineId } from "../../src/shared/legacy-export-machine.ts";
import { appendFile, mkdir, readdir, writeFile } from "node:fs/promises";
import type { RelayhistoryStreamRecord } from "../../packages/pipeline/src/lib/pipeline/relayhistory-transform.ts";

const args = process.argv.slice(2);
const arg = (name: string) => {
  const i = args.indexOf(name);
  return i < 0 ? undefined : args[i + 1];
};

function sampler() {
  Bun.gc(true);
  const baseline = process.memoryUsage().rss;
  let peak = baseline;
  const timer = setInterval(() => {
    peak = Math.max(peak, process.memoryUsage().rss);
  }, 50);
  const started = performance.now();
  return (extra: Record<string, unknown>) => {
    clearInterval(timer);
    peak = Math.max(peak, process.memoryUsage().rss, process.resourceUsage().maxRSS * 1024);
    console.log(JSON.stringify({ ...extra, baselineRssBytes: baseline, peakRssBytes: peak, growthBytes: peak - baseline, seconds: (performance.now() - started) / 1000 }));
  };
}

if (args.includes("--facts-pass")) {
  const dir = arg("--dir");
  if (!dir) throw Error("requires --dir <directory>");
  const root = resolve(dir);
  const store = createFsStore({ root: join(root, "objects") });
  const { records, keys } = JSON.parse(await readFile(join(root, "manifests.json"), "utf8")) as { records: number; keys: string[] };
  const admitted = [];
  for (const key of keys)
    admitted.push({ key, manifest: await parseManifest(new TextDecoder().decode(await readAll(await store.get(objectKey(key)))), key) });
  const done = sampler();
  const first = admitted[0]!.manifest;
  const scope = { orgId: identity.orgId, workspaceId: identity.workspaceId, harness: identity.source, sessionId: first.session.session_id };
  const read = await streamCanonicalScope(
    scope, admitted,
    {
      store, prefixFor: (org) => `96bulls/${org}`,
      decompress: (stored, max) => new Uint8Array(zlib.zstdDecompressSync(stored, { maxOutputLength: max } as never)),
      perPass: { keyedRecordsCap: Number.MAX_SAFE_INTEGER },
    },
    { maxReadBytes: Number.MAX_SAFE_INTEGER, maxReadEvents: Number.MAX_SAFE_INTEGER },
    Date.now(), 0,
  );
  const fScope = { orgId: scope.orgId, workspaceId: scope.workspaceId, harness: scope.harness, canonicalId: scope.sessionId };
  let chunks = 0;
  let items = 0;
  let maxSent = 0;
  await read.facts(STAGED_FACTS_CHUNK, async (events) => {
    const staged = outputFactsBatchStep(fScope, events, 1);
    // What the writer sends: the items, each body JSON text inside a JSON array.
    const sent = JSON.stringify(staged.map((i) => ({ k: i.kind, b: JSON.stringify(i) })));
    chunks += 1;
    items += staged.length;
    maxSent = Math.max(maxSent, sent.length);
  }, FACTS_CHUNK_MAX_BYTES);
  done({ phase: "facts-pass", records, complete: read.complete, passes: read.meter.passes, chunks, items, maxChunkSentChars: maxSent });
} else if (args.includes("--finish")) {
  const records = Number(arg("--records"));
  if (!Number.isSafeInteger(records) || records < 1) throw Error("requires --records <positive integer>");
  const payload = { org_id: "org_bench", workspace_id: "ws_bench", source: "claude", session_id: "s_bench" };
  const done = sampler();
  // As read back from the stage: one JSON text per record, parsed.
  const read: RelayhistoryStreamRecord[] = [
    JSON.parse(JSON.stringify({ kind: "legacy_scope", origin_id: "legacy:scope", record_id: "legacy:scope:coverage", revision: "1", operation: "upsert", received_at: "2026-11-05T00:00:00.000000Z",
      payload: { ...payload, version: 1, tables: ["conversation_turns", "session_links", "session_outcomes"] } })),
  ];
  for (let n = 0; n < records; n++)
    read.push(JSON.parse(JSON.stringify({
      kind: "legacy_conversation_turn", origin_id: "legacy:conversation_turns", record_id: `legacy:conversation_turns:t-${n}`, revision: "1", operation: "upsert",
      received_at: "2026-11-05T00:00:00.000000Z",
      payload: { ...payload, id: `t-${n}`, turn_index: n, role: n % 2 ? "assistant" : "user", session_owner: "u1", actor_name: "a", actor_role: "owner",
        ts: new Date(Date.parse("2026-11-05T00:00:00Z") + n).toISOString(),
        content: "Synthetic legacy turn content of a typical length for the memory bench. ".repeat(4) + String(n) },
    })));
  const scope = { orgId: payload.org_id, workspaceId: payload.workspace_id, harness: payload.source, sessionIds: [payload.session_id], canonicalId: payload.session_id };
  const out = outputFactsFinishStep(scope, { candidates: [], records: read, notes: [], sourceComplete: true }, 1);
  done({ phase: "finish", records, facts: out.facts.length, complete: out.complete, reasons: out.reasons });
} else if (args.includes("--export-legacy")) {
  const dir = arg("--dir");
  const records = Number(arg("--records"));
  if (!dir || !Number.isSafeInteger(records) || records < 1) throw Error("requires --dir <directory> --records <positive integer>");
  const root = resolve(dir);
  await mkdir(root, { recursive: true });
  if ((await readdir(root)).length) throw Error("--dir must be empty for --export-legacy");
  // --turn-chars: each turn's content length (default 300, the short turn the 2.4 GB figure uses).
  const turnChars = Number(arg("--turn-chars") ?? 300);
  if (!Number.isSafeInteger(turnChars) || turnChars < 20) throw Error("--turn-chars must be an integer of at least 20");
  // --wide: one character above U+00FF per turn (an em dash), so the runtime keeps the text as a 16-bit string.
  const wide = args.includes("--wide");
  const turnText = "Synthetic legacy turn content of a typical length for the memory bench. ".repeat(Math.ceil(turnChars / 72) + 1);
  const store = createFsStore({ root: join(root, "objects") });
  // The session id the reader scopes by: the exported manifest's (the export makes the id path-safe).
  const payload = { org_id: identity.orgId, workspace_id: identity.workspaceId, source: identity.source, session_id: identity.sessionId.replace(/[^A-Za-z0-9_-]/g, "_") };
  const extra: RelayhistoryStreamRecord[] = [{ kind: "legacy_scope", origin_id: "legacy:scope", record_id: "legacy:scope:coverage", revision: "1", operation: "upsert",
    received_at: "2026-11-05T00:00:00.000000Z", payload: { ...payload, version: 1, tables: ["conversation_turns", "session_links", "session_outcomes"] } }];
  for (let n = 0; n < records; n++)
    extra.push({ kind: "legacy_conversation_turn", origin_id: "legacy:conversation_turns", record_id: `legacy:conversation_turns:t-${n}`, revision: "1", operation: "upsert",
      received_at: "2026-11-05T00:00:00.000000Z",
      payload: { ...payload, id: `t-${n}`, turn_index: n, role: n % 2 ? "assistant" : "user", session_owner: "u1", actor_name: "a", actor_role: "owner",
        ts: new Date(Date.parse("2026-11-05T00:00:00Z") + n).toISOString(),
        content: (wide ? "\u2014" : "") + turnText.slice(0, turnChars - String(n).length - (wide ? 1 : 0)) + String(n) } });
  const x = await replayDatabase();
  try {
    // One live record on its machine; the legacy turns on the exporter-owned legacy machine of their user, so
    // they sit in a manifest of their own (legacy-provenance.ts admits legacy records only from such a manifest).
    const user = "user_replay";
    const legacyMachine = legacyMachineId(identity.workspaceId, user);
    const at = REPLAY_FIXTURE_AT;
    const live: RelayhistoryStreamRecord = { kind: "session_event", origin_id: "capacity:0", record_id: "event-000000000000", revision: "1", accepted_seq: "1", operation: "upsert",
      received_at: at, payload: { ts_ms: Date.parse(at), text: "Synthetic live record.", role: "assistant", message_id: "native-0" } };
    const encoder = new TextEncoder();
    const result = await exportUnit({ store, keyRoot: "96bulls", grantId: "replay_fixture", maxChunkBytes: 4 * 1024 * 1024 },
      { orgId: identity.orgId, workspaceId: identity.workspaceId, source: identity.source, sessionId: identity.sessionId, firstMs: Date.parse(at), lastMs: Date.parse(at) + 1, machineIds: [], totals: {} } as never,
      {
        fenced: async () => false,
        async *lines() {
          yield { machine: "machine:0", userId: user, bytes: encoder.encode(JSON.stringify(live) + "\n") };
          for (const r of extra) yield { machine: legacyMachine, userId: user, bytes: encoder.encode(JSON.stringify(r) + "\n") };
        },
      } as never);
    for (const m of result.manifests) {
      const manifest = await parseManifest(new TextDecoder().decode(await readAll(await store.get(objectKey(m.manifestKey)))), m.manifestKey);
      const ext = manifest.extensions!["96bulls"] as { canonical_machine_id: string };
      await addReplayPointer(x.db, { key: m.manifestKey, digest: manifest.raw.content_sha256, physicalSession: manifest.session.session_id, physicalMachine: manifest.session.machine_id,
        canonicalMachine: ext.canonical_machine_id, revision: manifest.revision, trace: manifest.trace_id } as never);
    }
    await projectSessionDirectory(x.db as never, { orgId: identity.orgId, workspaceId: identity.workspaceId, harness: identity.source, sessionId: identity.sessionId });
    await writeFile(join(root, "manifests.json"), JSON.stringify({ records: records + 2, keys: result.manifests.map((m) => m.manifestKey) }) + "\n");
    console.log(JSON.stringify({ phase: "exported", records: records + 2, legacyTurns: records, manifests: result.manifests.length }));
  } finally {
    await x.pg.close();
  }
} else if (args.includes("--whole-run")) {
  const dir = arg("--dir");
  if (!dir) throw Error("requires --dir <directory>");
  const root = resolve(dir);
  const store = createFsStore({ root: join(root, "objects") });
  const { records, keys } = JSON.parse(await readFile(join(root, "manifests.json"), "utf8")) as { records: number; keys: string[] };
  const admitted = [];
  for (const key of keys)
    admitted.push({ key, manifest: await parseManifest(new TextDecoder().decode(await readAll(await store.get(objectKey(key)))), key) });
  const stage = join(root, "stage-records.jsonl");
  await writeFile(stage, "");
  const done = sampler();
  const first = admitted[0]!.manifest;
  const scope = { orgId: identity.orgId, workspaceId: identity.workspaceId, harness: identity.source, sessionId: first.session.session_id };
  // Passes 1-3; `read` stays referenced to the end, as the module holds it to the final step.
  const read = await streamCanonicalScope(
    scope, admitted,
    {
      store, prefixFor: (org) => `96bulls/${org}`,
      decompress: (stored, max) => new Uint8Array(zlib.zstdDecompressSync(stored, { maxOutputLength: max } as never)),
      perPass: { keyedRecordsCap: Number.MAX_SAFE_INTEGER },
    },
    { maxReadBytes: Number.MAX_SAFE_INTEGER, maxReadEvents: Number.MAX_SAFE_INTEGER },
    Date.now(), 0,
  );
  const afterRead = process.memoryUsage().rss;
  const fScope = { orgId: scope.orgId, workspaceId: scope.workspaceId, harness: scope.harness, canonicalId: scope.sessionId };
  let chunks = 0;
  let staged = 0;
  await read.facts(STAGED_FACTS_CHUNK, async (events) => {
    const items = outputFactsBatchStep(fScope, events, 1);
    JSON.stringify(items.map((i) => ({ k: i.kind, b: JSON.stringify(i) }))); // what the writer sends
    const recordLines = items.filter((i) => i.kind === "record").map((i) => JSON.stringify((i as { body: unknown }).body)).join("\n");
    if (recordLines) await appendFile(stage, recordLines + "\n");
    chunks += 1;
    staged += items.length;
  }, FACTS_CHUNK_MAX_BYTES);
  const afterFacts = process.memoryUsage().rss;
  // The final step: the records read back from the stage and parsed, then the fold.
  const back = (await readFile(stage, "utf8")).split("\n").filter(Boolean).map((l) => JSON.parse(l) as RelayhistoryStreamRecord);
  const out = outputFactsFinishStep({ ...fScope, sessionIds: [scope.sessionId] }, { candidates: [], records: back, notes: [], sourceComplete: true }, 1);
  done({
    phase: "whole-run", records, complete: read.complete, passes: read.meter.passes, chunks, staged, recordsBack: back.length, facts: out.facts.length,
    factsComplete: out.complete, reasons: out.reasons, rssAfterReadBytes: afterRead, rssAfterFactsPassBytes: afterFacts, readKept: read.wanted.size >= 0,
  });
} else throw Error("requires --facts-pass, --finish, --export-legacy or --whole-run");
