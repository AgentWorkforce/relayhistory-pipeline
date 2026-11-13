/**
 * The pipeline e2e steps: one synthetic session driven through every piece of the pipeline, each piece asserted
 * on its own line. Black-box over HTTP where the piece has an HTTP surface, and read back from the real MinIO
 * and Postgres where the piece's output is storage. Bounded waits only; no model calls; no network beyond the
 * stack. The step ids are the contract with flows/lib/classify.mjs (which steps a "targeted" run selects).
 */
import { existsSync } from "node:fs";
import { mkdtempSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { mintToken } from "../../src/alleyoop/auth.ts";
import { runCourierOnce } from "../../src/alleyoop/courier.ts";
import { fixedBearerToken } from "../../src/alleyoop/relay-auth-token.ts";
import { readSummaryObject } from "../../src/bob-costas/summary-objects.ts";
import { createHash } from "node:crypto";
import { createS3Eraser } from "../../src/migration/neon-s3-erasure/eraser.ts";
import { s3Ledger, s3LedgerKeys, fenceAuthorityKey } from "../../src/session-ingest/ledger-s3.ts";
import { sweepRearmMarkers } from "../../src/session-ingest/rearm-sweeper.ts";
import { liveMachineId } from "../../src/session-ingest/writer.ts";
import type { LedgerTenant } from "../../src/session-ingest/ledger.ts";
import { deliveryAccount } from "../../src/session-ingest/vendor/delivery/prepare.ts";
import { masterKeyring, parseMasterKeys } from "../../src/shared/keyring.ts";
import { manifestKey, objectKey, readAll, type ObjectStore } from "../../src/shared/vendor.ts";
import { TENANT_A, TENANT_B, type Tenant } from "../selfhost/stack.ts";
import { COURIER_SESSION, FENCE_AUTHORITY, FIXTURES, SOURCE, STARTED_MS, courierTranscript, deliveryBatch, type Fixture } from "./fixtures.ts";
import { CANONICAL_BUCKET, HOOP_BUCKET, HOOP_SERVICES, HOOP_TENANT, KEY_ROOT, SELFHOST_SERVICES, type PipelineStack } from "./stack.ts";

const ROOT = fileURLToPath(new URL("../..", import.meta.url));
export type Result = "PASS" | "FAIL" | "SKIP";
export interface StepResult { id: string; title: string; result: Result; detail: string; ms: number }
class Failed extends Error {}
class Skipped extends Error {}
const must: (cond: unknown, why: string) => asserts cond = (cond, why) => { if (!cond) throw new Failed(why); };
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));
const sha = (s: string) => createHash("sha256").update(s).digest("hex");

export interface Step {
  id: string;
  title: string;
  /** Which half of the stack the step needs: the self-host canonical path or the courier (AlleyOop) path. */
  leg: "selfhost" | "hoop" | "none";
  run(ctx: Ctx): Promise<string>;
}

/** What the steps share. Later steps read what earlier ones recorded; a step whose input is missing fails, never passes. */
export interface Ctx {
  stack: PipelineStack;
  state: { courierSessionUuid?: string };
}

const ledgerTenant = (t: Tenant): LedgerTenant => ({ orgId: t.org, workspaceId: t.workspace, userId: t.sub });

/** Poll until `probe` returns a value or `ms` passes; the last observation is the failure's detail. */
async function until<T>(ms: number, every: number, probe: () => Promise<{ done: T } | { last: string }>): Promise<T> {
  const deadline = Date.now() + ms;
  let last = "";
  for (;;) {
    const r = await probe();
    if ("done" in r) return r.done;
    last = r.last;
    if (Date.now() > deadline) throw new Failed(`not within ${Math.round(ms / 1000)}s: ${last}`);
    await sleep(every);
  }
}

async function call(base: string, path: string, init: { method?: string; token?: string | null; body?: unknown; headers?: Record<string, string> } = {}) {
  const res = await fetch(`${base}${path}`, {
    method: init.method ?? (init.body === undefined ? "GET" : "POST"),
    headers: { ...(init.token ? { authorization: `Bearer ${init.token}` } : {}), ...(init.body !== undefined ? { "content-type": "application/json" } : {}), ...init.headers },
    body: init.body === undefined ? undefined : JSON.stringify(init.body),
    signal: AbortSignal.timeout(30_000),
  });
  const text = await res.text();
  let json: any = null;
  try { json = JSON.parse(text); } catch {}
  return { status: res.status, json, text };
}

/** The ingest's own log line for one request (it logs counts and the outcome class, never ids or content). */
function ingestLine(stack: PipelineStack, service: string, correlationId: string): Record<string, unknown> | undefined {
  for (const line of stack.logs(service).split("\n").reverse()) {
    if (!line.includes(correlationId)) continue;
    const at = line.indexOf("{");
    try {
      const rec = JSON.parse(line.slice(at)) as Record<string, unknown>;
      if (rec.correlation_id === correlationId && (rec.route === "delivery.batches" || rec.route === "turns.batches")) return rec;
    } catch {}
  }
  return undefined;
}

let seq = 0;
/** POST a delivery batch with a known correlation id, and read back the ingest's outcome for exactly that request. */
async function upload(stack: PipelineStack, which: "sql" | "s3", token: string, body: unknown) {
  const correlationId = `e2e-pipeline-${which}-${++seq}`;
  const base = which === "sql" ? stack.ingestUrl : stack.urls.ingestS3;
  const r = await call(base, "/v1/delivery/batches", { token, body, headers: { "x-correlation-id": correlationId } });
  const service = which === "sql" ? "session-ingest" : "session-ingest-s3";
  // The line is written before the answer, but compose's log driver can lag the socket by a moment.
  let line: Record<string, unknown> | undefined;
  for (let i = 0; i < 10 && !(line = ingestLine(stack, service, correlationId)); i++) await sleep(500);
  return { ...r, line };
}
const outcome = (u: { status: number; line?: Record<string, unknown> }) =>
  `status=${u.status} outcome=${String(u.line?.outcome ?? "?")} manifests=${String(u.line?.manifests ?? "?")} deleted=${String(u.line?.deleted ?? "?")}`;
const admitted = (u: { status: number; line?: Record<string, unknown> }) => u.status === 200 && u.line?.outcome === "accepted" && Number(u.line?.manifests) >= 1;
/** Answered 200 (the Desktop must not retry) but nothing was written: every record stale or of a deleted session. */
const notWritten = (u: { status: number; line?: Record<string, unknown> }) => u.status === 200 && Number(u.line?.manifests) === 0;

async function listAll(store: ObjectStore, prefix: string): Promise<string[]> {
  const keys: string[] = [];
  let cursor: string | undefined;
  do {
    const page = await store.list(objectKey(prefix), cursor);
    keys.push(...page.items.map((i) => i.key));
    cursor = page.nextCursor;
  } while (cursor && keys.length < 20_000);
  return keys;
}

/** The session's canonical objects under the org prefix: its manifest and every chunk the manifest names. */
async function canonical(stack: PipelineStack, t: Tenant, f: Fixture) {
  const store = stack.store(CANONICAL_BUCKET);
  const keys = (await listAll(store, `${KEY_ROOT}/${t.org}`)).filter((k) => k.includes(f.sessionId));
  const manifestKey = keys.find((k) => k.endsWith("/manifest.json"));
  if (!manifestKey) return { keys, manifest: undefined, manifestKey, chunks: [] as string[] };
  const manifest = JSON.parse(Buffer.from(await readAll(await store.get(objectKey(manifestKey)))).toString("utf8")) as { revision: number; raw: { chunks: { key?: string; path?: string }[] } };
  const chunks = keys.filter((k) => k !== manifestKey);
  return { keys, manifest, manifestKey, chunks };
}

const hasEvent = (r: { json: any }, marker: string) => (r.json?.events ?? []).some((e: any) => JSON.stringify(e).includes(marker));
const listed = (r: { json: any }, id: string) => (r.json?.sessions ?? []).some((s: any) => s.sessionId === id);
const ws = (t: Tenant) => `workspace=${t.workspace}`;

export const STEPS: readonly Step[] = [
  {
    id: "upload.http", title: "a. upload over HTTP (SQL ledger): auth refusals, then admitted", leg: "selfhost",
    async run({ stack }) {
      const body = await deliveryBatch(TENANT_A, FIXTURES.http, { batch: "1", revision: 1 });
      const anonymous = await call(stack.ingestUrl, "/v1/delivery/batches", { body });
      must(anonymous.status === 401, `no token: ${anonymous.status}`);
      const readOnly = await call(stack.ingestUrl, "/v1/delivery/batches", { token: stack.tokens.aReadOnly, body });
      must(readOnly.status === 403, `read-only token: ${readOnly.status}`);
      const u = await upload(stack, "sql", stack.tokens.a, body);
      must(admitted(u) && typeof u.json?.receiptId === "string", `upload: ${outcome(u)} ${u.text.slice(0, 160)}`);
      return `no_token=401 read_only=403 ${outcome(u)}`;
    },
  },
  {
    id: "upload.s3_ledger", title: "a. upload over the S3 ledger (INGEST_LEDGER=s3): refused until the fence-authority marker, then admitted", leg: "selfhost",
    async run({ stack }) {
      const body = await deliveryBatch(TENANT_B, FIXTURES.s3, { batch: "1", revision: 1 });
      const store = stack.store(CANONICAL_BUCKET);
      const marker = objectKey(fenceAuthorityKey(KEY_ROOT));
      must(!(await store.head(marker)), "a fence-authority marker exists before the test wrote one");
      const before = await upload(stack, "s3", stack.tokens.b, body);
      must(before.status === 503 && !admitted(before), `without the marker: ${outcome(before)}`);
      const reason = JSON.stringify(before.line ?? {}) + before.text;
      must(/fence_authority_missing/u.test(reason), `refusal reason is not ledger_fence_authority_missing: ${reason.slice(0, 200)}`);
      await store.put(marker, new TextEncoder().encode(JSON.stringify(FENCE_AUTHORITY)), { ifNoneMatch: true });
      const after = await upload(stack, "s3", stack.tokens.b, body);
      must(admitted(after), `with the marker: ${outcome(after)} ${after.text.slice(0, 160)}`);
      const batchKey = s3LedgerKeys(KEY_ROOT, ledgerTenant(TENANT_B)).batch(FIXTURES.s3.originId, body.batch.batch_id);
      const record = JSON.parse(Buffer.from(await readAll(await store.get(objectKey(batchKey)))).toString("utf8")) as { state: string; receiptId: string };
      must(record.state === "committed" && record.receiptId === after.json?.receiptId, `S3 batch record: ${record.state} receipt_match=${record.receiptId === after.json?.receiptId}`);
      const sqlBatches = stack.sql(`SELECT count(*) FROM sessions.ingest_batches WHERE org_id = '${TENANT_B.org}'`)[0]?.[0];
      must(sqlBatches === "0", `the S3 path wrote SQL ingest_batches rows: ${sqlBatches}`);
      return `before_marker=503 ledger_fence_authority_missing; after_marker ${outcome(after)}; s3_batch_record=committed; sql_ingest_batches=0`;
    },
  },
  {
    id: "canonical.objects", title: "b. canonical manifest and chunks written to the object store", leg: "selfhost",
    async run({ stack }) {
      const parts: string[] = [];
      for (const [t, f] of [[TENANT_A, FIXTURES.http], [TENANT_B, FIXTURES.s3]] as const) {
        const c = await canonical(stack, t, f);
        must(c.manifest, `${f.originId}: no manifest.json among ${c.keys.length} object(s) for the session`);
        must(c.manifest.raw.chunks.length >= 1 && c.chunks.length >= c.manifest.raw.chunks.length, `${f.originId}: manifest names ${c.manifest.raw.chunks.length} chunk(s), ${c.chunks.length} stored`);
        const raw = await readAll(await stack.store(CANONICAL_BUCKET).get(objectKey(c.chunks[0]!)));
        must(raw.length > 0 && !Buffer.from(raw).toString("latin1").includes(f.marker), `${f.originId}: a stored chunk is empty or not sealed (marker readable as plain bytes)`);
        parts.push(`${f.originId}: manifest rev ${c.manifest.revision}, ${c.chunks.length} chunk object(s)`);
      }
      return parts.join("; ");
    },
  },
  {
    id: "index.directory", title: "c. the consumer indexes it: session_directory and search_index rows", leg: "selfhost",
    async run({ stack }) {
      const col = stack.sql("SELECT column_name FROM information_schema.columns WHERE table_schema = 'search_index' AND table_name = 'transcript_events' AND column_name IN ('session_id', 'org_id')").map((r) => r[0]);
      must(col.includes("session_id") && col.includes("org_id"), `search_index.transcript_events has no org_id/session_id columns (${col.join(",")})`);
      return until<string>(240_000, 3_000, async () => {
        const counts = [[TENANT_A, FIXTURES.http], [TENANT_B, FIXTURES.s3]].map(([t, f]) => {
          const tt = t as Tenant, ff = f as Fixture;
          const dir = stack.sql(`SELECT count(*) FROM sessions.session_directory WHERE org_id = '${tt.org}' AND session_id = '${ff.sessionId}'`)[0]?.[0];
          const ev = stack.sql(`SELECT count(*) FROM search_index.transcript_events WHERE org_id = '${tt.org}' AND session_id = '${ff.sessionId}'`)[0]?.[0];
          return { name: ff.originId, dir: Number(dir), ev: Number(ev) };
        });
        const text = counts.map((c) => `${c.name}: directory=${c.dir} search_events=${c.ev}`).join("; ");
        return counts.every((c) => c.dir === 1 && c.ev >= 2) ? { done: text } : { last: text };
      });
    },
  },
  {
    id: "search", title: "d. search returns it (and only to its tenant)", leg: "selfhost",
    async run({ stack }) {
      const a = await call(stack.readUrl, `/v1/events?q=${FIXTURES.http.marker}&limit=20&${ws(TENANT_A)}`, { token: stack.tokens.a });
      must(a.status === 200 && hasEvent(a, FIXTURES.http.marker), `A search: ${a.status} ${a.text.slice(0, 160)}`);
      const b = await call(stack.readUrl, `/v1/events?q=${FIXTURES.s3.marker}&limit=20&${ws(TENANT_B)}`, { token: stack.tokens.b });
      must(b.status === 200 && hasEvent(b, FIXTURES.s3.marker), `B search (S3-ledger session): ${b.status} ${b.text.slice(0, 160)}`);
      const miss = await call(stack.readUrl, `/v1/events?q=${FIXTURES.http.marker}zz&limit=20&${ws(TENANT_A)}`, { token: stack.tokens.a });
      must(miss.status === 200 && !hasEvent(miss, FIXTURES.http.marker), `miss: ${miss.status}`);
      const cross = await call(stack.readUrl, `/v1/events?q=${FIXTURES.http.marker}&limit=20&${ws(TENANT_B)}`, { token: stack.tokens.b });
      must(cross.status === 200 && !hasEvent(cross, FIXTURES.http.marker), `B found A's marker: ${cross.status}`);
      return "hit(http)=true hit(s3)=true miss=empty cross_tenant=empty";
    },
  },
  {
    id: "read.api", title: "f. the read API serves the list and the session's events", leg: "selfhost",
    async run({ stack }) {
      const anon = await call(stack.readUrl, `/v1/sessions?${ws(TENANT_A)}`);
      must(anon.status === 401, `anonymous list: ${anon.status}`);
      const list = await call(stack.readUrl, `/v1/sessions?${ws(TENANT_A)}`, { token: stack.tokens.a });
      must(list.status === 200 && listed(list, FIXTURES.http.sessionId), `list: ${list.status} ${list.text.slice(0, 160)}`);
      const events = await until<{ status: number; json: any; text: string }>(120_000, 3_000, async () => {
        const r = await call(stack.readUrl, `/v1/sessions/${FIXTURES.http.sessionId}/events?source=${SOURCE}&${ws(TENANT_A)}`, { token: stack.tokens.a });
        return r.status === 200 && hasEvent(r, FIXTURES.http.marker) ? { done: r } : { last: `${r.status} ${r.json?.error?.reason ?? r.text.slice(0, 120)}` };
      });
      const n = (events.json?.events ?? []).length;
      must(n >= 2, `events: ${n}`);
      return `anonymous=401 list=200 (session listed) events=200 (${n} events, marker present)`;
    },
  },
  {
    id: "read.summary", title: "f. the read API serves the summary route (its brief content is model-only, so empty here)", leg: "selfhost",
    async run({ stack }) {
      // GET /v1/sessions/:id/summary serves session-brief output, which only the model worker produces, and this
      // lane makes no model calls. What it proves is the route's contract for a session with no brief yet: served,
      // tenant-scoped, and an explicit `empty` (never a 5xx, never another tenant's). The extractive summary
      // content itself is proven by summary.extractive.
      const path = `/v1/sessions/${FIXTURES.http.sessionId}/summary?source=${SOURCE}`;
      const anon = await call(stack.readUrl, `${path}&${ws(TENANT_A)}`);
      must(anon.status === 401, `anonymous summary: ${anon.status}`);
      const r = await call(stack.readUrl, `${path}&${ws(TENANT_A)}`, { token: stack.tokens.a });
      must(r.status === 200 && r.json?.summary?.status === "empty" && r.json.summary.kind === "short", `summary: ${r.status} ${r.text.slice(0, 200)}`);
      const wrongKind = await call(stack.readUrl, `${path}&kind=long&${ws(TENANT_A)}`, { token: stack.tokens.a });
      must(wrongKind.status === 501, `kind=long: ${wrongKind.status}`);
      return "anonymous=401; 200 {status: empty, kind: short} (no brief: session-brief is model-only and off in this lane); kind=long=501";
    },
  },
  {
    id: "controls.sql", title: "g. controls on the SQL ledger: equal revision stale, disable, re-upload not admitted", leg: "selfhost",
    async run({ stack }) {
      const f = FIXTURES.http;
      const before = (await canonical(stack, TENANT_A, f)).manifest?.revision;
      const stale = await upload(stack, "sql", stack.tokens.a, await deliveryBatch(TENANT_A, f, { batch: "2", revision: 1 }));
      must(notWritten(stale), `equal-revision re-send was written: ${outcome(stale)}`);
      const account = await deliveryAccount({ orgId: TENANT_A.org, workspaceId: TENANT_A.workspace });
      const disable = await call(stack.ingestUrl, "/v1/delivery/sessions/selection", { token: stack.tokens.a, body: { account_id: account, origin_id: f.originId, sessions: [{ source: SOURCE, session_id: f.sessionId }], enabled: false } });
      must(disable.status === 200, `disable: ${disable.status} ${disable.text.slice(0, 160)}`);
      const again = await upload(stack, "sql", stack.tokens.a, await deliveryBatch(TENANT_A, f, { batch: "3", revision: 2 }));
      must(notWritten(again) && Number(again.line?.deleted) >= 1, `re-upload after disable: ${outcome(again)}`);
      const after = (await canonical(stack, TENANT_A, f)).manifest?.revision;
      must(after === before, `manifest revision moved: ${before} -> ${after}`);
      const list = await call(stack.readUrl, `/v1/sessions?${ws(TENANT_A)}`, { token: stack.tokens.a });
      must(list.status === 200 && !listed(list, f.sessionId), "the disabled session is still listed");
      const reset = await call(stack.ingestUrl, "/v1/delivery/sessions/reset", { token: stack.tokens.a, body: { account_id: account, request_id: "e2e-reset-sql", confirmation: "DELETE ALL SESSIONS" } });
      must(reset.status === 501, `self-host reset route: ${reset.status}`);
      return `stale: ${outcome(stale)}; disable=200; re-upload: ${outcome(again)}; manifest rev ${before} unchanged; unlisted; reset=501 (not served on self-host: proven on the S3 ledger in controls.s3)`;
    },
  },
  {
    id: "controls.s3", title: "g. controls on the S3 ledger: equal revision stale, disable, reset, re-uploads not admitted", leg: "selfhost",
    async run({ stack }) {
      // Self-host under INGEST_LEDGER=s3 serves no selection or reset route (selfhost-config.ts refuses session
      // selection; reset is never routed on self-host), so the controls are applied through ledger-s3.ts itself,
      // against the same bucket and key root the ingest uses. The HTTP upload path proves their effect.
      const tenant = ledgerTenant(TENANT_B);
      const ledger = s3Ledger(stack.store(CANONICAL_BUCKET), KEY_ROOT, { acceptanceMode: "canonical" });
      const lines: string[] = [];
      const stale = await upload(stack, "s3", stack.tokens.b, await deliveryBatch(TENANT_B, FIXTURES.s3, { batch: "2", revision: 1 }));
      must(notWritten(stale), `equal-revision re-send was written: ${outcome(stale)}`);
      lines.push(`stale: ${outcome(stale)}`);
      await ledger.setSessionUploads({ tenant, originId: FIXTURES.s3.originId, sessions: [[SOURCE, FIXTURES.s3.sessionId]], enabled: false });
      const disabled = await upload(stack, "s3", stack.tokens.b, await deliveryBatch(TENANT_B, FIXTURES.s3, { batch: "3", revision: 2 }));
      must(notWritten(disabled) && Number(disabled.line?.deleted) >= 1, `re-upload after disable: ${outcome(disabled)}`);
      lines.push(`after disable: ${outcome(disabled)}`);
      const second = await upload(stack, "s3", stack.tokens.b, await deliveryBatch(TENANT_B, FIXTURES.s3Second, { batch: "s2-1", revision: 1 }));
      must(admitted(second), `a second session before the reset: ${outcome(second)}`);
      const tombstoned = await ledger.resetUploadedSessions(tenant, "e2e-reset-s3");
      must(tombstoned >= 1, `reset tombstoned ${tombstoned} session(s)`);
      const replay = await ledger.resetUploadedSessions(tenant, "e2e-reset-s3");
      must(replay === tombstoned, `a replayed reset answered ${replay}, not its first count ${tombstoned}`);
      const afterReset = await upload(stack, "s3", stack.tokens.b, await deliveryBatch(TENANT_B, FIXTURES.s3Second, { batch: "s2-2", revision: 2 }));
      must(notWritten(afterReset), `re-upload of a reset session: ${outcome(afterReset)}`);
      const fresh = await upload(stack, "s3", stack.tokens.b, await deliveryBatch(TENANT_B, FIXTURES.s3AfterReset, { batch: "s3-1", revision: 1 }));
      must(admitted(fresh), `a new session after the reset: ${outcome(fresh)}`);
      lines.push(`reset tombstoned ${tombstoned} (replay=${replay}); reset session re-upload: ${outcome(afterReset)}; new session: admitted`);
      return lines.join("; ");
    },
  },
  {
    id: "rearm.sweeper", title: "h. a commit a disable reached writes a re-arm marker, and the sweeper erases the session", leg: "selfhost",
    async run({ stack }) {
      const f = FIXTURES.s3Rearm;
      const tenant = ledgerTenant(TENANT_B);
      const store = stack.store(CANONICAL_BUCKET);
      // 1. The session is admitted over HTTP: its canonical objects are written.
      const up = await upload(stack, "s3", stack.tokens.b, await deliveryBatch(TENANT_B, f, { batch: "rearm-1", revision: 1 }));
      must(admitted(up), `upload: ${outcome(up)}`);
      const before = await canonical(stack, TENANT_B, f);
      must(before.manifest && before.keys.length >= 2, `canonical objects before: ${before.keys.length}`);
      // 2. A batch claimed before a disable and committed after it (the race the marker exists for), through the
      //    real ledger: the commit writes the marker.
      const ledger = s3Ledger(store, KEY_ROOT, { acceptanceMode: "canonical" });
      const recordId = sha(`${f.originId}:rearm-record`);
      const records = [{ source: SOURCE, sessionId: f.sessionId, recordId, revisionId: `${recordId}:r2`, revision: 2, digest: sha(`${recordId}:2`) }];
      const sessions: [string, string][] = [[SOURCE, f.sessionId]];
      const input = { tenant, originId: f.originId, batchId: "e2e-rearm-batch", batchDigest: sha("e2e-rearm-batch"), receiptId: `rhr_${sha("e2e-rearm-receipt")}`, sessions, records };
      const claim = await ledger.claim(input);
      must(claim.kind === "claimed" || claim.kind === "resume", `claim: ${claim.kind}`);
      await ledger.setSessionUploads({ tenant, originId: f.originId, sessions, enabled: false });
      const commit = await ledger.commit({ ...input, claimToken: (claim as { claimToken: string }).claimToken, machineId: liveMachineId(tenant.workspaceId, f.originId, tenant.userId) });
      must((commit as { rearmed?: number }).rearmed === 1, `commit: ${JSON.stringify(commit)}`);
      const marker = s3LedgerKeys(KEY_ROOT, tenant).rearm(f.originId, SOURCE, f.sessionId);
      must(await store.head(objectKey(marker)), "no re-arm marker after the fenced commit");
      // 3. The sweeper, as rearm-sweeper-main.ts runs it: the S3 eraser over the same bucket, scoped to the org. Its
      //    clock is set past the skew window, as for anything written moments ago.
      const eraser = createS3Eraser({
        bucket: CANONICAL_BUCKET, region: "us-east-1", endpoint: stack.urls.minio,
        credentials: { accessKeyId: stack.env.E2E_MINIO_USER!, secretAccessKey: stack.env.E2E_MINIO_PASSWORD! },
      });
      const later = () => Date.now() + 10 * 60_000;
      const pass = await sweepRearmMarkers({ store, eraser }, { keyRoot: KEY_ROOT, bucket: CANONICAL_BUCKET, orgs: [TENANT_B.org], now: later });
      must(pass.counts.erased === 1 && pass.counts.failed === 0 && pass.counts.versions > 0, `sweep: ${JSON.stringify(pass.counts)} failures=${JSON.stringify(pass.failures)}`);
      const after = await canonical(stack, TENANT_B, f);
      must(after.keys.length === 0, `${after.keys.length} canonical object(s) left for the session`);
      const others = await canonical(stack, TENANT_B, FIXTURES.s3AfterReset);
      must(others.manifest, "the sweep erased another session's canonical objects");
      const again = await sweepRearmMarkers({ store, eraser }, { keyRoot: KEY_ROOT, bucket: CANONICAL_BUCKET, orgs: [TENANT_B.org], now: later });
      must(again.counts.alreadyDone >= 1 && again.counts.erased === 0, `second sweep: ${JSON.stringify(again.counts)}`);
      return `admitted, then claim/disable/commit rearmed=1; sweep erased 1 session (${pass.counts.versions} versions), ${before.keys.length} -> 0 objects, other sessions intact; second sweep alreadyDone=${again.counts.alreadyDone}`;
    },
  },
  {
    id: "backlog.drain", title: "i. the backlog drain over a buffered stage", leg: "selfhost",
    async run() {
      if (!existsSync(join(ROOT, "src/session-ingest/buffered-drain.ts"))) throw new Skipped("pending #578 (the S3-only buffered backlog drain is not on this branch)");
      throw new Failed("src/session-ingest/buffered-drain.ts is on this branch but this lane does not drive it yet: add the step (docs/pipeline-e2e.md)");
    },
  },
  {
    id: "upload.courier", title: "a. upload over the AlleyOop courier path: chunks and a validated manifest", leg: "hoop",
    async run({ stack }) {
      // The real courier (src/alleyoop/courier.ts runCourierOnce) over a Concourse history.db built here from a
      // fixed fixture, with a token minted from this run's own key, exactly as the local stack's courier.
      const dir = mkdtempSync(join(tmpdir(), "rh-pipe-courier-"));
      try {
        const raw = join(dir, "session.jsonl");
        writeFileSync(raw, courierTranscript());
        const { DatabaseSync } = await import("node:sqlite");
        const db = new DatabaseSync(join(dir, "history.db"));
        db.exec("CREATE TABLE sessions (session_id TEXT, source TEXT, raw_path TEXT, first_activity_ms INTEGER, last_activity_ms INTEGER, cwd TEXT, agent_version TEXT)");
        db.prepare("INSERT INTO sessions VALUES (?, ?, ?, ?, ?, ?, ?)").run(COURIER_SESSION.sessionId, SOURCE, raw, STARTED_MS, STARTED_MS + 15_000, "/e2e/courier", null);
        db.close();
        const token = mintToken(stack.keys, { issuer: "https://96bulls.e2e", audience: "relayhistory", orgId: HOOP_TENANT.orgId, workspaceId: HOOP_TENANT.workspaceId, userId: HOOP_TENANT.userId, machineId: HOOP_TENANT.machineId, ttlSeconds: 900 });
        const result = await runCourierOnce({
          dbPath: join(dir, "history.db"), statePath: join(dir, "courier.json"), ingestUrl: stack.urls.alleyoop, token: fixedBearerToken(token),
          orgId: HOOP_TENANT.orgId, workspaceId: HOOP_TENANT.workspaceId, userId: HOOP_TENANT.userId, machineId: HOOP_TENANT.machineId,
          machineBound: true, closeAfterMinutes: 30, startMode: "all",
        });
        must(result.uploaded >= 1 && result.chunks >= 1, `courier: sessions=${result.sessions} uploaded=${result.uploaded} chunks=${result.chunks}`);
        const mKey = manifestKey({ workspaceId: HOOP_TENANT.workspaceId, machineId: HOOP_TENANT.machineId, harness: SOURCE, sessionId: COURIER_SESSION.sessionId, startDate: new Date(STARTED_MS).toISOString().slice(0, 10) });
        const keys = await listAll(stack.store(HOOP_BUCKET), mKey.replace(/\/manifest\.json$/u, ""));
        must(keys.includes(mKey) && keys.some((k) => k.includes("/raw/")), `locker objects under the session prefix: ${keys.length}`);
        return `courier uploaded ${result.uploaded} session(s), ${result.chunks} chunk(s); manifest and ${keys.filter((k) => k.includes("/raw/")).length} raw chunk object(s) in the Locker`;
      } finally {
        rmSync(dir, { recursive: true, force: true });
      }
    },
  },
  {
    id: "index.hoop", title: "c. the indexer writes session_index and session_index_ext; the referee classifies", leg: "hoop",
    async run({ stack, state }) {
      return until<string>(180_000, 3_000, async () => {
        const row = stack.sql(`SELECT i.state, e.session_uuid::text, e.session_uuid_source FROM sessions.session_index i JOIN sessions.session_index_ext e USING (org_id, workspace_id, harness, session_id, machine_id) WHERE i.org_id = '${HOOP_TENANT.orgId}' AND i.session_id = '${COURIER_SESSION.sessionId}'`)[0];
        if (!row) return { last: "no session_index row" };
        const [st, uuid, src] = row;
        if (st === "unknown") return { last: `indexed (${uuid}) but not classified yet` };
        state.courierSessionUuid = uuid;
        return { done: `session_index state=${st}; session_index_ext uuid=${uuid} (${src})` };
      });
    },
  },
  {
    id: "summary.extractive", title: "e. the summary worker writes extractive summary objects and their pointers", leg: "hoop",
    async run({ stack, state }) {
      const uuid = state.courierSessionUuid;
      must(uuid, "no session uuid: index.hoop did not pass");
      const pointers = await until<string[][]>(180_000, 3_000, async () => {
        const rows = stack.sql(`SELECT kind, status, object_key, object_sha256, workspace_id, revision FROM sessions.session_summaries WHERE session_uuid = '${uuid}'::uuid ORDER BY kind`);
        return rows.length === 2 && rows.every((r) => r[1] === "ready") ? { done: rows } : { last: rows.map((r) => `${r[0]}=${r[1]}`).join(",") || "no pointers" };
      });
      const keyring = await masterKeyring(parseMasterKeys(stack.summaryMasterKeys));
      const store = stack.store(HOOP_BUCKET);
      const parts: string[] = [];
      for (const [kind, , key, sha, workspaceId, revision] of pointers) {
        const rec = await readSummaryObject({ store, keyring, ids: { orgId: HOOP_TENANT.orgId, workspaceId: workspaceId! }, key: key!, sha256: sha!, kind: kind as "short" | "long", revision: Number(revision) });
        must(rec?.status === "ready" && rec.summary && rec.summary.length > 0, `${kind}: object did not decrypt to a ready summary`);
        must(rec.model === "extractive/v1", `${kind}: built by ${rec.model}, not the extractive summariser`);
        const plain = Buffer.from(await readAll(await store.get(objectKey(key!)))).toString("utf8");
        must(!plain.includes(rec.summary.slice(0, 24)), `${kind}: the stored object is not encrypted`);
        parts.push(`${kind}: ${key!.split("/").slice(-2).join("/")} (${rec.summary.length} chars, extractive/v1, encrypted)`);
      }
      return `pointers=2 ready; ${parts.join("; ")}`;
    },
  },
];

export const STEP_IDS = STEPS.map((s) => s.id);

/** The services a selection of steps needs (compose starts their dependencies). */
export function servicesFor(steps: readonly Step[]): string[] {
  const legs = new Set(steps.map((s) => s.leg));
  return [...(legs.has("selfhost") ? SELFHOST_SERVICES : []), ...(legs.has("hoop") ? HOOP_SERVICES : [])];
}

export async function runSteps(ctx: Ctx, steps: readonly Step[], log: (r: StepResult) => void): Promise<StepResult[]> {
  const results: StepResult[] = [];
  for (const step of steps) {
    const started = Date.now();
    let r: StepResult;
    try {
      r = { id: step.id, title: step.title, result: "PASS", detail: await step.run(ctx), ms: 0 };
    } catch (error) {
      r = error instanceof Skipped
        ? { id: step.id, title: step.title, result: "SKIP", detail: error.message, ms: 0 }
        : { id: step.id, title: step.title, result: "FAIL", detail: error instanceof Failed ? error.message : `unexpected: ${(error as Error).message}`, ms: 0 };
    }
    r.ms = Date.now() - started;
    results.push(r);
    log(r);
  }
  return results;
}
