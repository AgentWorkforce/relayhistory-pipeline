/**
 * End-to-end proof of the OpenSearch / Elasticsearch search backend, in process.
 *
 * upload -> scrub -> index -> search -> erase, against ONE disposable, unauthenticated
 * OpenSearch or Elasticsearch server (CI: the opensearch-proof workflow's container;
 * locally: any throwaway container). No cloud resource is used: the object store is a
 * temp directory, the metadata database is an in-memory fake, and the only network
 * peer is the configured search URL. Each run uses its own index prefix and deletes
 * its index on exit.
 *
 * Chain:
 *   1. upload: transcript records carrying a content marker and placeholder secrets
 *      (src/concourse/seed.ts) pass through the pipeline scrubber (scrubRecords, as
 *      session-ingest does before every put) into a zstd chunk + manifest in the store.
 *   2. index: indexSearchScope projects the scope through a composeSearchService
 *      binding built from the opensearch-capable registry with SEARCH_INDEX_BACKEND=opensearch.
 *   3. search: the marker query returns the event; no placeholder secret is found,
 *      neither by query nor in any stored _source of the index.
 *   4. erase: the scope's live pointers go away, indexSearchScope erases it; the query
 *      is empty and `_count` for the tenant is 0 (then a direct eraseScope, idempotent).
 *
 * Env:
 *   SEARCH_INDEX_OPENSEARCH_TEST_URL      the disposable server (required), e.g. http://127.0.0.1:9200
 *   SEARCH_INDEX_OPENSEARCH_TEST_DIALECT  opensearch (default) | elasticsearch
 *   OPENSEARCH_PROOF_HEAD                 the commit under test (recorded in the results)
 *   OPENSEARCH_PROOF_OUT                  the JSON results file (default opensearch-proof.json)
 *
 * Exit 0 only if every case passes.
 */
import { mkdtemp, rm } from "node:fs/promises";
import { writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { zstdCompressSync } from "node:zlib";
import { createFsStore } from "../../packages/object-store/src/fs.ts";
import { sha256Hex } from "../../packages/object-store/src/crypto.ts";
import { createOpenSearchCapableSearchIndexRegistry } from "../../packages/pipeline/src/adapters/defaults.ts";
import { syntheticDoc } from "../../packages/pipeline/src/search-index/conformance.ts";
import { matchesQuery, positionOf } from "../../packages/pipeline/src/search-index/contracts.ts";
import { scrubRecords } from "../../packages/pipeline/src/lib/scrub-engine/index.ts";
import type { SqlExecutor } from "../../packages/pipeline/src/lib/pipeline/queue.ts";
import { PLANTED_CONTENT_MARKER, PLANTED_SECRETS } from "../../src/concourse/seed.ts";
import { composeSearchService } from "../../src/shared/search-composition.ts";
import {
  computeContentSha256,
  manifestKey,
  objectKey,
  objectStoreManifestReader,
  rawChunkKey,
  type ChunkEntry,
  type ManifestV1,
  type RelayhistoryStreamRecord,
} from "../../src/shared/vendor.ts";
import type { SessionScope } from "../../src/session-pipeline/pointer.ts";
import type { SearchProjectionState } from "../../src/session-pipeline/search-adapter.ts";
import { indexSearchScope } from "../../src/session-pipeline/search-index.ts";

const URL_ = process.env.SEARCH_INDEX_OPENSEARCH_TEST_URL?.trim();
const DIALECT = process.env.SEARCH_INDEX_OPENSEARCH_TEST_DIALECT?.trim() || "opensearch";
const HEAD = process.env.OPENSEARCH_PROOF_HEAD ?? "";
const OUT = process.env.OPENSEARCH_PROOF_OUT ?? "opensearch-proof.json";
if (!URL_) {
  console.error("SEARCH_INDEX_OPENSEARCH_TEST_URL is required (a disposable OpenSearch/Elasticsearch server)");
  process.exit(2);
}
const BASE = URL_.replace(/\/+$/, "");
const PREFIX = `e2e-${Date.now().toString(36)}-${Math.random().toString(36).slice(2, 6)}`;
const INDEX = `${PREFIX}-transcript-events`;

const AT = "2026-11-01T10:00:00.000Z";
const NOW = new Date("2026-11-01T12:00:00.000Z");
const scope: SessionScope = { orgId: "org_e2e", workspaceId: "ws_e2e", harness: "claude", sessionId: "sess_e2e" };
const tenant = { orgId: scope.orgId, workspaceId: scope.workspaceId };
const location = { prefix: `96bulls/${scope.orgId}`, workspaceId: scope.workspaceId, machineId: "machine_e2e", harness: scope.harness, sessionId: scope.sessionId, startDate: "2026-11-01" };

/** Every planted value, as it would have to appear in a stored field to leak. */
const NEEDLES: Record<string, string> = {
  awsKey: PLANTED_SECRETS.awsKey,
  openaiKey: PLANTED_SECRETS.openaiKey,
  slackToken: PLANTED_SECRETS.slackToken,
  email: PLANTED_SECRETS.email,
  pemHeader: "BEGIN RSA PRIVATE KEY",
  pemBody: PLANTED_SECRETS.pem.split("\\n")[1]!,
};

const cases: { name: string; pass: boolean; detail: string }[] = [];
function check(name: string, pass: boolean, detail = "") {
  cases.push({ name, pass, detail });
  console.log(`${pass ? "PASS" : "FAIL"} ${name}${detail ? `: ${detail}` : ""}`);
}

async function raw(path: string, body?: unknown): Promise<any> {
  const res = await fetch(`${BASE}/${path}`, {
    method: body === undefined ? "GET" : "POST",
    headers: body === undefined ? {} : { "content-type": "application/json" },
    body: body === undefined ? undefined : JSON.stringify(body),
    redirect: "error",
  });
  if (!res.ok) throw new Error(`${path}: HTTP ${res.status}`);
  return res.json();
}

const tenantFilter = { bool: { filter: [{ term: { org_id: scope.orgId } }, { term: { workspace_id: scope.workspaceId } }] } };

function record(id: string, tsMs: number, text: string): RelayhistoryStreamRecord {
  return { kind: "session_event", origin_id: "e2e-origin", record_id: id, revision: "1", operation: "upsert", received_at: AT, payload: { ts_ms: tsMs, text, role: "user" } } as RelayhistoryStreamRecord;
}

/** The session-ingest put: scrub, compress, chunk + manifest (raw.scrub declared). */
async function upload(store: ReturnType<typeof createFsStore>, records: RelayhistoryStreamRecord[]): Promise<{ key: string; scrubbed: string }> {
  const plain = new TextEncoder().encode(`${records.map((r) => JSON.stringify(r)).join("\n")}\n`);
  const source = scrubRecords(plain).scrubbed;
  const stored = new Uint8Array(zstdCompressSync(source));
  const sha = await sha256Hex(source);
  const chunk: ChunkEntry = {
    seq: 0, epoch: 0, start_offset: 0, bytes: source.byteLength, stored_bytes: stored.byteLength,
    sha256: sha, sha256_8: sha.slice(0, 8), stored_sha256: await sha256Hex(stored), stored_raw_bytes: source.byteLength,
    raw_sha256: sha, compression: "zstd", records: records.length,
  };
  await store.put(rawChunkKey(location, chunk.seq, 0, sha, "ndjson"), stored, {});
  const manifest = {
    schema_v: 1, layout: "v1", trace_id: "019a1f3c-8e2b-7c41-9d3a-5b6f0e2a7c18", revision: 1, created_at: AT, updated_at: AT,
    session: { workspace_id: scope.workspaceId, machine_id: location.machineId, harness: scope.harness, session_id: scope.sessionId, started_at: AT },
    writer: { delivery_instance_id: location.machineId, plugin_version: "opensearch-proof" },
    raw: {
      owner: location.machineId, state: "open", updated_at: AT, format: "ndjson", compression: "zstd", chunk_count: 1,
      raw_bytes: chunk.bytes, stored_bytes: chunk.stored_bytes, content_sha256: await computeContentSha256([chunk]), epoch: 0,
      epochs: [{ epoch: 0, opened_at: AT, reason: "initial", first_seq: chunk.seq, opened_by: location.machineId }],
      next_offset: source.byteLength, chunks: [chunk], scrub: { ruleset: "relayhistory-scrub", version: 1 },
    },
    extensions: { "96bulls": { user_id: "user_e2e" } },
  } as ManifestV1;
  const key = manifestKey(location);
  await store.put(objectKey(key), new TextEncoder().encode(JSON.stringify(manifest)), { contentType: "application/json" });
  return { key, scrubbed: new TextDecoder().decode(source) };
}

/** The metadata database as the indexer sees it: locks granted, the scope's live pointer keys. */
function fakeMetadata(live: () => string[]) {
  const tx: SqlExecutor = {
    async query<R>(text: string) {
      // The pre-recorded-title catalog schema (044100 absent), as pipeline.test.ts models it:
      // the directory catalog probe needs a boolean row, never an empty reply.
      if (text.includes("pg_catalog.pg_attribute") && text.includes("recorded_catalog_title"))
        return { rows: [{ supported: false }] as R[] };
      if (text.includes("pg_try_advisory_xact_lock")) return { rows: [{ ok: true }] as R[] };
      if (text.includes("event_projection_sources")) return { rows: [{ written: true }] as R[] };
      if (text.includes("FROM sessions.session_index") && text.includes("deleted_at IS NULL"))
        return { rows: live().map((manifest_key) => ({ manifest_key, canonical_id: scope.sessionId })) as R[] };
      if (text.includes("AS canonical_id FROM sessions.session_index")) return { rows: [{ canonical_id: scope.sessionId }] as R[] };
      if (text.includes("upsert_session_index")) return { rows: [{ applied: true, revision: "1" }] as R[] };
      return { rows: [] as R[] };
    },
  };
  return { transact: async <T>(work: (tx: SqlExecutor) => Promise<T>) => work(tx) };
}

let serverVersion = "";
let distribution = "";
const root = await mkdtemp(join(tmpdir(), "96bulls-opensearch-proof-"));
try {
  const rootRes = await fetch(`${BASE}/`, { redirect: "error" });
  if (!rootRes.ok) throw new Error(`GET /: HTTP ${rootRes.status}`);
  const info = (await rootRes.json()) as { version?: { number?: unknown; distribution?: unknown } };
  serverVersion = String(info?.version?.number ?? "");
  // The product signal the adapter checks; anything unlabelled stays "unknown"
  // so the workflow's distribution check cannot pass on a guess.
  distribution =
    typeof info?.version?.distribution === "string"
      ? info.version.distribution
      : rootRes.headers.get("x-elastic-product") === "Elasticsearch"
        ? "elasticsearch"
        : "unknown";
  console.log(`server: ${distribution} ${serverVersion}, dialect ${DIALECT}, index ${INDEX}`);

  const store = createFsStore({ root });
  const reader = objectStoreManifestReader(store, { provider: "s3", bucket: "bucket" });
  let complete = false;
  const state: SearchProjectionState = {
    async fromMs() { return 0; },
    async record(_tx, _scope, value) { complete = value.complete; },
    async partials() { return { sessions: complete ? [] : [scope.sessionId], overflow: false }; },
  };
  const binding = composeSearchService(
    { registry: createOpenSearchCapableSearchIndexRegistry(), state: () => state },
    { env: { SEARCH_INDEX_BACKEND: "opensearch", SEARCH_INDEX_OPENSEARCH_URL: BASE, SEARCH_INDEX_OPENSEARCH_DIALECT: DIALECT, SEARCH_INDEX_OPENSEARCH_INDEX_PREFIX: PREFIX } },
    "writer",
  );
  const index = binding.index;
  check("binding: registry selects the opensearch backend", index.backend === "opensearch", `${index.backend}/${index.id}`);
  check("health before bootstrap is schema_missing", (await index.health()).code === "schema_missing");
  await index.ensureSchema(); // explicit proof bootstrap, never service startup
  check("health after ensureSchema is ok", (await index.health()).ok === true);

  // Repetitive and maximally folded 500-character q against the oracle, in a
  // separate tenant that is deleted again before the upload chain runs.
  const stressTenant = { orgId: "org_e2e_q", workspaceId: scope.workspaceId };
  const runs = `${"a".repeat(600)} ${"İ".repeat(600)} ${"ab".repeat(300)}`;
  const stressDocs = [
    syntheticDoc({ ...stressTenant, eventId: "rep-short", content: "", taskTitle: runs }),
    syntheticDoc({ ...stressTenant, eventId: "rep-long", content: "", taskTitle: `${"x".repeat(3800)}${runs}` }),
    syntheticDoc({ ...stressTenant, eventId: "rep-near", content: `${"a".repeat(499)}b${"İ".repeat(499)}b${"ab".repeat(249)}`, taskTitle: "" }),
  ];
  await index.upsert(stressDocs);
  for (const [name, q] of [["a x 500", "a".repeat(500)], ["İ x 500", "İ".repeat(500)], ["ab x 250", "ab".repeat(250)]] as const) {
    const expected = stressDocs.filter((d) => matchesQuery(d, q)).map((d) => d.eventId).sort().join(",");
    let got: string;
    try {
      got = (await index.query({ tenant: stressTenant, q, order: "asc", limit: 10 })).hits.map((h) => h.eventId).sort().join(",");
    } catch (error) {
      got = error instanceof Error ? `${error.name}: ${error.message}` : String(error);
    }
    check(`search: q ${name} matches the oracle`, expected === "rep-long,rep-short" && got === expected, `expected ${expected}; got ${got}`);
  }
  await index.deleteEvents(stressTenant, stressDocs.map(positionOf));

  // 1. upload (scrub on the way in)
  const t0 = Date.parse(AT);
  const secretText = [
    `Please rotate these before we ship. ${PLANTED_CONTENT_MARKER}`,
    `AWS_ACCESS_KEY_ID=${PLANTED_SECRETS.awsKey}`,
    `OPENAI_API_KEY=${PLANTED_SECRETS.openaiKey}`,
    `SLACK_BOT_TOKEN=${PLANTED_SECRETS.slackToken}`,
    `contact ${PLANTED_SECRETS.email} when done`,
    PLANTED_SECRETS.pem.replace(/\\n/g, "\n"),
  ].join("\n");
  let keys: string[] = [];
  const up = await upload(store, [record("r1", t0, secretText), record("r2", t0 + 1, "Done. Keys rotated.")]);
  keys = [up.key];
  const leakedAtRest = Object.entries(NEEDLES).filter(([, v]) => up.scrubbed.includes(v)).map(([k]) => k);
  check("scrub: no planted secret in the stored chunk", leakedAtRest.length === 0, leakedAtRest.join(","));
  check("scrub: the content marker survives", up.scrubbed.includes(PLANTED_CONTENT_MARKER));

  // 2. index
  const meta = fakeMetadata(() => keys);
  const deps = { transact: meta.transact, reader, store, adapter: binding, prefixFor: (org: string) => `96bulls/${org}`, now: () => NOW };
  const indexed = await indexSearchScope(scope, deps);
  check("index: indexSearchScope completes and inserts both events", indexed.complete === true && indexed.inserted === 2, JSON.stringify(indexed));

  // 3. search
  const hit = await index.query({ tenant, q: PLANTED_CONTENT_MARKER.toLowerCase(), order: "asc", limit: 10 });
  check("search: the marker query returns the event", hit.hits.length === 1 && hit.hits[0]!.content.includes(PLANTED_CONTENT_MARKER), `${hit.hits.length} hit(s)`);
  const sessionPage = await index.query({ tenant, sessionId: scope.sessionId, order: "asc", limit: 10 });
  check("search: the session lists both events in order", sessionPage.hits.length === 2 && sessionPage.hits[0]!.content.includes(PLANTED_CONTENT_MARKER) && sessionPage.hits[1]!.content.includes("Keys rotated"), sessionPage.hits.map((h) => h.eventId).join(","));
  const byQuery: string[] = [];
  for (const [name, needle] of Object.entries(NEEDLES)) {
    if ((await index.query({ tenant, q: needle, order: "asc", limit: 10 })).hits.length) byQuery.push(name);
  }
  check("search: no planted secret is found by query", byQuery.length === 0, byQuery.join(","));
  const docs = await raw(`${INDEX}/_search`, { size: 100, query: tenantFilter });
  const sources = (docs.hits.hits as { _source: unknown }[]).map((h) => JSON.stringify(h._source));
  const inSource = Object.entries(NEEDLES).filter(([, v]) => sources.some((s) => s.includes(v) || s.toLowerCase().includes(v.toLowerCase()))).map(([k]) => k);
  check("index: no planted secret in any stored _source", sources.length === 2 && inSource.length === 0, `${sources.length} doc(s); ${inSource.join(",")}`);
  check("index: the redaction placeholder is what was stored", sources.some((s) => s.includes("[REDACTED]")));

  // 4. erase
  keys = [];
  const erased = await indexSearchScope(scope, deps);
  check("erase: indexSearchScope erases the scope", erased.complete === true && (erased.reasons ?? []).includes("erased"), JSON.stringify(erased));
  check("erase: the marker query is empty", (await index.query({ tenant, q: PLANTED_CONTENT_MARKER.toLowerCase(), order: "asc", limit: 10 })).hits.length === 0);
  check("erase: the tenant query is empty", (await index.query({ tenant, order: "asc", limit: 10 })).hits.length === 0);
  const count = await raw(`${INDEX}/_count`, { query: tenantFilter });
  check("erase: _count for the tenant is 0", count.count === 0, `count=${count.count}`);
  await index.eraseScope({ ...tenant, source: scope.harness, sessionIds: [scope.sessionId] });
  const countAgain = await raw(`${INDEX}/_count`, { query: { match_all: {} } });
  check("erase: a direct eraseScope is idempotent and the index is empty", countAgain.count === 0, `count=${countAgain.count}`);
} catch (error) {
  check("proof ran to completion", false, error instanceof Error ? `${error.name}: ${error.message}` : String(error));
} finally {
  await fetch(`${BASE}/${INDEX}`, { method: "DELETE", redirect: "error" }).catch(() => undefined);
  await rm(root, { recursive: true, force: true });
}

const passed = cases.filter((c) => c.pass).length;
const failed = cases.length - passed;
writeFileSync(OUT, `${JSON.stringify({ head: HEAD, dialect: DIALECT, distribution, server_version: serverVersion, passed, failed, cases }, null, 2)}\n`);
console.log(`${passed} passed, ${failed} failed (${distribution} ${serverVersion}, dialect ${DIALECT})`);
process.exit(failed === 0 && passed > 0 ? 0 : 1);
