/** Ingest DB-error alert (runbooks/history-620-operations.md, "Ingest DB-error alert"): the SQL/policy body, its
 * predicates against the Worker's real route lines, the read-only plan, the guarded apply, the GitHub gate and the
 * workflow shape. The private-ingest Worker profile is not involved. */
import { afterEach, describe, expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { createIngestApp, SYNC_SCOPE } from "../app.ts";
import { DB_ERROR_CLASSES } from "../db-error-class.ts";
import { DB_REFUSAL_PREFIXES, DB_REFUSALS, GENERATION_REFUSALS } from "../db-refusal-class.ts";
import { readdirSync } from "node:fs";
import { sqlLedger } from "../ledger-sql.ts";
import { prepareDelivery } from "../vendor/delivery/prepare.ts";
import { RECOVERY_CONTENT_TYPE, RECOVERY_PROTOCOL } from "../recovery-protocol.ts";
import { logLine } from "../../shared/log.ts";
import { MemoryLedger, SIGNING_KEY, TENANT, batch, record, scratchStore, signedHeaders } from "./support.ts";
import {
  INGEST_ERROR_ALERT as A,
  EXCLUDED_REFUSALS,
  applyIngestErrorAlert,
  cloudflareAlertingApi,
  countsAsDbErrorDelivery,
  countsAsDelivery,
  ingestErrorAlertAccount,
  ingestErrorAlertConfirmation,
  ingestErrorAlertDigest,
  ingestErrorAlertDispatch,
  ingestErrorAlertPolicy,
  ingestErrorAlertQuery,
  ingestErrorAlertValue,
  planIngestErrorAlert,
  type AlertingApi,
} from "../ingest-error-alert.ts";
import { SESSION_INGEST_WORKER } from "../../../stacks/cloudflare/session-ingest.ts";
import { ingestErrorAlertGithubGate } from "../../../scripts/ingest-error-alert-github.ts";
import { runIngestErrorAlert } from "../../../scripts/ingest-error-alert.ts";
import { SCHEMA_RELEASE as P } from "../../hoopdb/schema-release-contract.ts";
import { PRIVATE_INGEST_RELEASE } from "../private-ingest-release.ts";

const ROOT = new URL("../../../", import.meta.url);
const profile = JSON.parse(readFileSync(new URL("config/cloudflare/private-ingest-production.json", ROOT), "utf8"));
const ACCOUNT: string = profile.sessionIngest.accountId;
const HOOK = "0123456789abcdef0123456789abcdef";
const POLICY_ID = "11111111-2222-4333-8444-555555555555";
const sha = "1".repeat(40);
const code = async (fn: () => unknown) => {
  try { await fn(); return null; } catch (e) { return (e as { code?: string }).code ?? String(e); }
};

// ---- the Worker's real route lines -------------------------------------------------------------------------
const cleanups: (() => void)[] = [];
afterEach(() => cleanups.splice(0).forEach((f) => f()));
const enc = (s: string) => new TextEncoder().encode(s);

async function ledgerError(sqlstate: string, message = "canceling statement"): Promise<unknown> {
  const pgErr = Object.assign(new Error(message), { code: sqlstate });
  try { await sqlLedger(async () => { throw pgErr; }).releaseSlot(TENANT as never, "h"); } catch (x) { return x; }
  throw new Error("expected the ledger to throw");
}

/** One signed buffered-mode recovery POST, as in lock-alert.test.ts; returns the rendered log lines. */
async function recovery(arm: (ledger: MemoryLedger) => void) {
  const s = scratchStore();
  cleanups.push(s.done);
  const ledger = new MemoryLedger();
  const raw = await batch([record({ record_id: "db-error-alert" })], { batch_id: "db-error-alert" });
  const prepared = await prepareDelivery(TENANT, raw);
  const sourceDigest = "d".repeat(64);
  const custody = {
    sourceDigest,
    witness: {
      version: 1 as const, issuer: "relayhistory-cloud" as const, sourceIdentitySha256: "c".repeat(64), controlEpoch: "e".repeat(64), privacyBaselineSha256: "a".repeat(64),
      tenant: TENANT, originId: prepared.originId, batchId: prepared.batchId, batchDigest: prepared.batchDigest, sourceControlVersion: "0", pending: false, pendingControl: null,
      privacyVersion: "0", originalRecordCount: raw.records.length, candidateCount: prepared.records.length,
      records: prepared.records.map((r, preparedOrd) => ({ preparedOrd, processingRank: preparedOrd, candidateDigest: r.digest, recordId: r.record_id, revision: String(r.revision), revisionId: r.revision_id, candidateFenced: false, currentFenced: false, current: null })),
      privacy: { complete: true as const, deletions: [], resets: [] },
    },
  };
  ledger.prepareBufferStage = async () => ({ stageToken: crypto.randomUUID(), expiresAt: new Date(Date.now() + 60_000).toISOString() });
  ledger.commitBuffered = async () => ({ version: 1, sourceDigest, sourceControlVersion: "0", privacySealed: true });
  arm(ledger);
  const logged: Record<string, unknown>[] = [];
  const app = createIngestApp({
    deliver: { ledger, target: { store: s.store, keyRoot: "96bulls", sleep: async () => {} }, acceptanceMode: "buffered" } as never,
    log: (fields) => logged.push(fields),
    signingKey: SIGNING_KEY,
  });
  const body = JSON.stringify({ protocol: RECOVERY_PROTOCOL, prepared, custody });
  const path = "/v1/delivery/recovery";
  const identity = { ...TENANT, scopes: [SYNC_SCOPE] };
  const res = await app.fetch(new Request(`https://x${path}`, { method: "POST", body, headers: { "content-type": RECOVERY_CONTENT_TYPE, ...signedHeaders(identity, "POST", path, enc(body)) } }));
  // Through the real allowlist: what Workers Logs indexes.
  return { status: res.status, lines: logged.map((f) => JSON.parse(logLine("info", "session ingest", f)) as Record<string, unknown>) };
}

describe("signal: the Worker's own route lines", () => {
  test("a DB-error 503 counts in the numerator; the lock-alert line counts nowhere", async () => {
    const thrown = await ledgerError("57014");
    const r = await recovery((l) => { l.commitBuffered = async () => { throw thrown; }; });
    expect(r.status).toBe(503);
    const route = r.lines.filter(countsAsDelivery);
    expect(route).toHaveLength(1);
    expect(route[0]).toMatchObject({ route: "delivery.recovery", outcome: "delivery_unavailable", db_error_class: "timeout" });
    expect(countsAsDbErrorDelivery(route[0]!)).toBe(true);
    expect(r.lines.filter((l) => l.route === "ingest.lock_alert").some(countsAsDelivery)).toBe(false);
  });

  test("55000: a generation-fence refusal counts; a per-user refusal does not", async () => {
    const fence = await ledgerError("55000", "session ingest: generation not ready");
    const f = await recovery((l) => { l.claim = async () => { throw fence; }; });
    expect(f.status).toBe(503);
    const fenceLine = f.lines.filter(countsAsDelivery);
    expect(fenceLine).toHaveLength(1);
    expect(fenceLine[0]).toMatchObject({ outcome: "delivery_unavailable", db_error_code: "55000", db_error_class: "unknown", db_refusal: "generation_not_ready" });
    expect(countsAsDbErrorDelivery(fenceLine[0]!)).toBe(true);
    const user = await ledgerError("55000", "session ingest: user privacy seal missing");
    const u = await recovery((l) => { l.claim = async () => { throw user; }; });
    expect(u.status).toBe(503);
    const userLine = u.lines.filter(countsAsDelivery);
    expect(userLine).toHaveLength(1);
    expect(userLine[0]).toMatchObject({ outcome: "delivery_unavailable", db_error_code: "55000", db_refusal: "user_seal_missing" });
    expect(countsAsDbErrorDelivery(userLine[0]!)).toBe(false);
    // An unrecognised 55000 (a reworded or new fence message) is `other` and counts: a fleet-wide fence pages.
    const reworded = await ledgerError("55000", "session ingest: generation fence not admitted");
    const o = await recovery((l) => { l.claim = async () => { throw reworded; }; });
    expect(o.status).toBe(503);
    const otherLine = o.lines.filter(countsAsDelivery);
    expect(otherLine).toHaveLength(1);
    expect(otherLine[0]).toMatchObject({ outcome: "delivery_unavailable", db_error_code: "55000", db_refusal: "other" });
    expect(countsAsDbErrorDelivery(otherLine[0]!)).toBe(true);
    // Excluded = exactly the known non-fence classes.
    expect(new Set(EXCLUDED_REFUSALS)).toEqual(new Set(DB_REFUSALS.filter((r) => r !== "other" && !GENERATION_REFUSALS.has(r))));
    for (const r of [...GENERATION_REFUSALS, "other"]) expect(EXCLUDED_REFUSALS).not.toContain(r);
  });

  test("every db_refusal prefix is a 55000 MESSAGE in the latest definition of some SQL function (reword guard)", () => {
    // Latest definition per function signature across supabase/migrations (file order): a text that survives only
    // in a superseded definition no longer reaches the Worker, so its prefix would silently stop matching.
    const dir = new URL("supabase/migrations/", ROOT);
    const latest = new Map<string, string>();
    for (const f of readdirSync(dir).filter((f) => f.endsWith(".sql")).sort()) {
      const sql = readFileSync(new URL(f, dir), "utf8");
      const starts = [...sql.matchAll(/create\s+(?:or\s+replace\s+)?function\s+([\w."]+)\s*\(([\s\S]*?)\)\s*returns/giu)];
      starts.forEach((m, i) => {
        const key = (m[1]! + "(" + m[2]! + ")").toLowerCase().replace(/"/gu, "").replace(/\s+/gu, " ");
        latest.set(key, sql.slice(m.index!, i + 1 < starts.length ? starts[i + 1]!.index : sql.length));
      });
    }
    const messages = new Set<string>();
    for (const body of latest.values())
      for (const m of body.matchAll(/ERRCODE\s*=\s*'55000'\s*,\s*MESSAGE\s*=\s*'([^']*)'/giu)) messages.add(m[1]!.toLowerCase());
    expect(messages.size).toBeGreaterThan(50);
    for (const [prefix, refusal] of DB_REFUSAL_PREFIXES)
      expect([...messages].some((m) => m.startsWith(prefix)) ? refusal : `reworded or removed: ${prefix}`).toBe(refusal);
    // Every fence-class text the latest SQL raises is classified as the fence (not `other`).
    for (const m of messages)
      if (/generation not ready|current custody policy unavailable/u.test(m) && m.startsWith("session ingest:"))
        expect(GENERATION_REFUSALS.has(DB_REFUSAL_PREFIXES.find(([p]) => m.startsWith(p))?.[1] ?? ("other" as never))).toBe(true);
  });

  test("a successful delivery counts in the denominator only", async () => {
    const r = await recovery(() => {});
    expect(r.status).toBe(200);
    const route = r.lines.filter(countsAsDelivery);
    expect(route).toHaveLength(1);
    expect(countsAsDbErrorDelivery(route[0]!)).toBe(false);
  });

  test("503 without a DB diagnosis, a 429 busy and capture-policy report lines are not DB-error deliveries", () => {
    const base = { route: "delivery.batches" };
    expect(countsAsDbErrorDelivery({ ...base, outcome: "delivery_unavailable", error_code: "store_unavailable" })).toBe(false);
    expect(countsAsDbErrorDelivery({ ...base, outcome: "delivery_busy", db_error_class: "timeout" })).toBe(false);
    expect(countsAsDelivery({ ...base, outcome: "delivery_busy", db_error_class: "timeout" })).toBe(true);
    for (const outcome of A.reportOutcomes) expect(countsAsDelivery({ ...base, outcome })).toBe(false);
    // A missing outcome reads as '' (the SQL's ifNull): in the denominator, never the numerator.
    expect(countsAsDelivery(base)).toBe(true);
    expect(countsAsDbErrorDelivery({ ...base, db_error_class: "pool" })).toBe(false);
    expect(countsAsDelivery({ route: "delivery.sessions.selection", outcome: "delivery_unavailable", db_error_class: "pool" })).toBe(false);
  });

  test("the three routes are the Worker's delivery routes", () => {
    const app = readFileSync(new URL("src/session-ingest/app.ts", ROOT), "utf8");
    expect(app).toContain(`acceptBatch(c, "delivery.batches"`);
    expect(app).toContain(`acceptBatch(c, "turns.batches"`);
    expect(app).toContain(`route: "delivery.recovery"`);
    expect(A.worker).toBe(SESSION_INGEST_WORKER);
  });

  test("value: ratio above 20% fires; the volume floor keeps a quiet window quiet", () => {
    const ok = { route: "turns.batches", outcome: "accepted" };
    const bad = { route: "delivery.batches", outcome: "delivery_unavailable", db_error_class: "terminated" };
    const lines = (good: number, failed: number) => [...Array(good).fill(ok), ...Array(failed).fill(bad)];
    expect(ingestErrorAlertValue(lines(79, 21))).toBeGreaterThan(A.threshold);
    expect(ingestErrorAlertValue(lines(80, 20))).not.toBeGreaterThan(A.threshold);
    expect(ingestErrorAlertValue(lines(1, 2))).toBe(2 / A.minDeliveries); // 67% of 3 requests does not fire
    expect(ingestErrorAlertValue(lines(0, 5))).toBeGreaterThan(A.threshold); // 5 failures with nothing else does
    expect(ingestErrorAlertValue([])).toBe(0);
  });
});

describe("policy body", () => {
  test("the SQL is one account-scoped, Worker-scoped ratio over 2 minutes with the closed DB classes", () => {
    const q = ingestErrorAlertQuery(ACCOUNT);
    expect(q).toContain(`FROM logs.workersLogs`);
    expect(q).toContain(`accountTag = '${ACCOUNT}'`);
    expect(q).toContain(`scriptName = '96bulls-session-ingest'`);
    expect(q).toContain(`timestamp >= NOW() - INTERVAL '2' MINUTE`);
    expect(q).toContain(`attributes['route'] IN ('delivery.batches', 'turns.batches', 'delivery.recovery')`);
    expect(q).toContain(`SELECT toFloat64(countIf(ifNull(attributes['outcome'], '') = 'delivery_unavailable'`);
    expect(q).toContain(`ifNull(attributes['db_error_class'], '') IN (${DB_ERROR_CLASSES.map((c) => `'${c}'`).join(", ")})`);
    expect(q).toContain(`ifNull(attributes['db_refusal'], '') NOT IN (${EXCLUDED_REFUSALS.map((r) => `'${r}'`).join(", ")})))`);
    expect(q).not.toMatch(/NOT IN \([^)]*'(?:other|generation_not_ready|policy_unavailable)'/u);
    expect(q).toContain(`ifNull(attributes['outcome'], '') NOT IN ('missing', 'stale', 'session_predates')`);
    expect(q).toContain(`/ greatest(count(), 20) AS value`);
    expect(q).not.toContain(";");
    expect(code(() => ingestErrorAlertQuery("x' OR '1'='1"))).resolves.toBe("ingest_error_alert_account");
  });

  test("custom_analytics threshold > 0.2, window 2m, every 1m, one webhook", () => {
    const p = ingestErrorAlertPolicy(ACCOUNT, HOOK);
    expect(p).toMatchObject({ name: A.name, alert_type: "custom_analytics", enabled: true, alert_interval: "30m", mechanisms: { webhooks: [{ id: HOOK }] } });
    expect(p.filters).toEqual({
      source: ["sql_api"], selectors: [ingestErrorAlertQuery(ACCOUNT)], execution_interval: ["1m"],
      alert_trigger_preferences: ["threshold"], alert_trigger_preferences_operator: [">"],
      alert_trigger_preferences_value: ["0.2"], alert_trigger_preferences_short_window: ["2m"],
    });
    expect(code(() => ingestErrorAlertPolicy(ACCOUNT, "https://hook"))).resolves.toBe("ingest_error_alert_webhook_id");
  });

  test("digest is stable and binds the body", () => {
    const p = ingestErrorAlertPolicy(ACCOUNT, HOOK);
    const reordered = JSON.parse(JSON.stringify({ filters: p.filters, mechanisms: p.mechanisms, ...p }));
    expect(ingestErrorAlertDigest(reordered)).toBe(ingestErrorAlertDigest(p));
    expect(ingestErrorAlertDigest(ingestErrorAlertPolicy(ACCOUNT, HOOK.replace("0", "f")))).not.toBe(ingestErrorAlertDigest(p));
  });
});

// ---- plan / apply against a fake alerting API --------------------------------------------------------------
function fakeApi(opts: { live?: Record<string, any>[]; destination?: Record<string, any>; readback?: (p: any) => any } = {}) {
  const calls: { method: string; path: string; body?: any }[] = [];
  let stored: any = opts.live?.find((p) => p.name === A.name);
  const api: AlertingApi = async (method, path, body) => {
    calls.push({ method, path, body });
    if (path.startsWith("/alerting/v3/destinations/webhooks/")) return opts.destination ?? { id: HOOK, name: A.destinationName, type: "generic" };
    if (path === "/alerting/v3/policies" && method === "GET") return opts.live ?? [];
    if (path === "/alerting/v3/policies" && method === "POST") { stored = { ...body, id: POLICY_ID }; return { id: POLICY_ID }; }
    if (method === "PUT") { stored = { ...body, id: POLICY_ID }; return { id: POLICY_ID }; }
    if (method === "GET") return opts.readback ? opts.readback(stored) : stored;
    throw new Error("unexpected " + method + " " + path);
  };
  return { api, calls };
}
const livePolicy = (over: Record<string, any> = {}) => ({ id: POLICY_ID, ...ingestErrorAlertPolicy(ACCOUNT, HOOK), created: "x", modified: "y", ...over });

describe("plan and apply", () => {
  test("plan: create when absent, noop when equal, update with drifted field names", async () => {
    expect((await planIngestErrorAlert(fakeApi().api, ACCOUNT, HOOK)).action).toBe("create");
    const equal = await planIngestErrorAlert(fakeApi({ live: [livePolicy({ filters: { ...livePolicy().filters, extra: ["x"] } })] }).api, ACCOUNT, HOOK);
    expect(equal).toMatchObject({ action: "noop", drift: [], policyId: POLICY_ID });
    const drifted = await planIngestErrorAlert(fakeApi({ live: [livePolicy({ enabled: false, filters: { ...livePolicy().filters, alert_trigger_preferences_value: ["0.5"] } })] }).api, ACCOUNT, HOOK);
    expect(drifted).toMatchObject({ action: "update", drift: ["enabled", "filters.alert_trigger_preferences_value"] });
  });

  test("plan refuses a non-generic or misnamed destination, a duplicate name and a name held by another type", async () => {
    expect(await code(() => planIngestErrorAlert(fakeApi({ destination: { name: A.destinationName, type: "slack" } }).api, ACCOUNT, HOOK))).toBe("ingest_error_alert_destination");
    expect(await code(() => planIngestErrorAlert(fakeApi({ destination: { name: "other", type: "generic" } }).api, ACCOUNT, HOOK))).toBe("ingest_error_alert_destination");
    expect(await code(() => planIngestErrorAlert(fakeApi({ live: [livePolicy(), livePolicy()] }).api, ACCOUNT, HOOK))).toBe("ingest_error_alert_duplicate_policy");
    expect(await code(() => planIngestErrorAlert(fakeApi({ live: [livePolicy({ alert_type: "workers_alert" })] }).api, ACCOUNT, HOOK))).toBe("ingest_error_alert_name_taken");
  });

  test("apply needs the confirmation for this sha and body, writes once, reads back equal", async () => {
    const digest = ingestErrorAlertDigest(ingestErrorAlertPolicy(ACCOUNT, HOOK));
    const wrong = fakeApi();
    expect(await code(() => applyIngestErrorAlert(wrong.api, { accountId: ACCOUNT, webhookId: HOOK, sha, confirmation: ingestErrorAlertConfirmation(sha, "0".repeat(64)) }))).toBe("ingest_error_alert_confirmation");
    expect(wrong.calls.filter((c) => c.method !== "GET")).toEqual([]);
    const ok = fakeApi();
    const r = await applyIngestErrorAlert(ok.api, { accountId: ACCOUNT, webhookId: HOOK, sha, confirmation: ingestErrorAlertConfirmation(sha, digest) });
    expect(r).toMatchObject({ action: "create", readback: "equal", policyId: POLICY_ID });
    expect(ok.calls.filter((c) => c.method !== "GET").map((c) => c.method + " " + c.path)).toEqual(["POST /alerting/v3/policies"]);
    const upd = fakeApi({ live: [livePolicy({ alert_interval: "1h" })] });
    await applyIngestErrorAlert(upd.api, { accountId: ACCOUNT, webhookId: HOOK, sha, confirmation: ingestErrorAlertConfirmation(sha, digest) });
    expect(upd.calls.filter((c) => c.method !== "GET").map((c) => c.method + " " + c.path)).toEqual([`PUT /alerting/v3/policies/${POLICY_ID}`]);
    const noop = fakeApi({ live: [livePolicy()] });
    await applyIngestErrorAlert(noop.api, { accountId: ACCOUNT, webhookId: HOOK, sha, confirmation: ingestErrorAlertConfirmation(sha, digest) });
    expect(noop.calls.filter((c) => c.method !== "GET")).toEqual([]);
    const lying = fakeApi({ readback: (p) => ({ ...p, enabled: false }) });
    expect(await code(() => applyIngestErrorAlert(lying.api, { accountId: ACCOUNT, webhookId: HOOK, sha, confirmation: ingestErrorAlertConfirmation(sha, digest) }))).toBe("ingest_error_alert_readback");
  });

  test("the HTTP client: plan never writes, apply writes only policies, only this account's alerting API", async () => {
    const seen: { url: string; method: string; auth: string | null }[] = [];
    const fetcher = (async (url: string, init: RequestInit) => {
      seen.push({ url, method: String(init.method), auth: new Headers(init.headers).get("authorization") });
      return new Response(JSON.stringify({ success: true, result: [] }));
    }) as unknown as typeof fetch;
    const plan = cloudflareAlertingApi("tok", ACCOUNT, "plan", fetcher);
    await plan("GET", "/alerting/v3/policies");
    expect(seen).toEqual([{ url: `https://api.cloudflare.com/client/v4/accounts/${ACCOUNT}/alerting/v3/policies`, method: "GET", auth: "Bearer tok" }]);
    expect(await code(() => plan("POST", "/alerting/v3/policies", {}))).toBe("ingest_error_alert_plan_write");
    const apply = cloudflareAlertingApi("tok", ACCOUNT, "apply", fetcher);
    expect(await code(() => apply("POST", "/alerting/v3/destinations/webhooks/" + HOOK, {}))).toBe("ingest_error_alert_plan_write");
    expect(await code(() => apply("GET", "/workers/scripts/96bulls-session-ingest"))).toBe("ingest_error_alert_api_path");
    expect(seen).toHaveLength(1);
    const answer = (status: number, body: unknown) =>
      cloudflareAlertingApi("tok", ACCOUNT, "plan", (async () => new Response(JSON.stringify(body), { status })) as unknown as typeof fetch);
    expect(await code(() => answer(404, {})("GET", "/alerting/v3/policies"))).toBe("ingest_error_alert_not_found");
    expect(await code(() => answer(503, { success: true, result: [] })("GET", "/alerting/v3/policies"))).toBe("ingest_error_alert_api");
    expect(await code(() => answer(200, { success: false, errors: [{ code: 1 }] })("GET", "/alerting/v3/policies"))).toBe("ingest_error_alert_api");
    expect(await code(() => cloudflareAlertingApi("tok", ACCOUNT, "plan", (async () => new Response("not json")) as unknown as typeof fetch)("GET", "/alerting/v3/policies"))).toBe("ingest_error_alert_api");
    expect(await code(() => answer(200, { success: true, result: [{}], result_info: { page: 1, total_pages: 2 } })("GET", "/alerting/v3/policies"))).toBe("ingest_error_alert_paginated");
    expect(await code(() => answer(200, { success: true, result: [{}], result_info: { total_count: 3 } })("GET", "/alerting/v3/policies"))).toBe("ingest_error_alert_paginated");
    expect(await answer(200, { success: true, result: [{}], result_info: { page: 1, total_pages: 1, total_count: 1 } })("GET", "/alerting/v3/policies")).toEqual([{}]);
  });

  test("dispatch and account inputs", async () => {
    expect(ingestErrorAlertDispatch({ INGEST_ALERT_MODE: "plan", INGEST_ALERT_SHA: sha })).toEqual({ mode: "plan", sha, confirmation: "" });
    expect(await code(() => ingestErrorAlertDispatch({ INGEST_ALERT_MODE: "deploy", INGEST_ALERT_SHA: sha }))).toBe("ingest_error_alert_mode");
    expect(await code(() => ingestErrorAlertDispatch({ INGEST_ALERT_MODE: "plan", INGEST_ALERT_SHA: sha, INGEST_ALERT_CONFIRMATION: "x" }))).toBe("ingest_error_alert_confirmation");
    expect(await code(() => ingestErrorAlertDispatch({ INGEST_ALERT_MODE: "apply", INGEST_ALERT_SHA: sha, INGEST_ALERT_CONFIRMATION: "APPLY" }))).toBe("ingest_error_alert_confirmation");
    expect(ingestErrorAlertAccount(profile, { CLOUDFLARE_ACCOUNT_ID: ACCOUNT })).toBe(ACCOUNT);
    expect(await code(() => ingestErrorAlertAccount(profile, { CLOUDFLARE_ACCOUNT_ID: "f".repeat(32) }))).toBe("ingest_error_alert_account_mismatch");
    // The checkout must be the dispatched sha, checked before the token or the network is touched.
    let fetched = 0;
    const fetcher = (async () => { fetched++; return new Response("{}"); }) as unknown as typeof fetch;
    expect(await code(() => runIngestErrorAlert({ INGEST_ALERT_MODE: "plan", INGEST_ALERT_SHA: sha, GITHUB_SHA: "2".repeat(40),
      CLOUDFLARE_ACCOUNT_ID: ACCOUNT, CLOUDFLARE_ALERTING_API_TOKEN: "tok", INGEST_ALERT_WEBHOOK_ID: HOOK }, fetcher))).toBe("ingest_error_alert_sha_mismatch");
    expect(fetched).toBe(0);
  });
});

// ---- GitHub gate and workflow ------------------------------------------------------------------------------
function github(over: { dispatcher?: number; approvals?: any[]; reviewers?: boolean; main?: string; ci?: string } = {}) {
  const actor = { id: over.dispatcher ?? P.dispatcher.id, login: P.dispatcher.login, type: "User" };
  const run = { id: 100, head_sha: sha, head_branch: "main", event: "workflow_dispatch", path: A.workflow, run_attempt: 1, actor, triggering_actor: { ...actor } };
  const environment = { id: 7, name: A.environment,
    protection_rules: over.reviewers ? [{ type: "required_reviewers", reviewers: [{ type: "User", reviewer: { type: "User", id: P.dispatcher.id } }] }] : [{ type: "branch_policy" }],
    deployment_branch_policy: { protected_branches: false, custom_branch_policies: true } };
  const jobs = { total_count: 6, jobs: ["checks", "session-read-worker / worker", "unit-tests", "real PostgreSQL 15", "real PostgreSQL 16", "real PostgreSQL 17"]
    .map((name) => ({ name, head_sha: sha, status: "completed", conclusion: "success" })) };
  return async (path: string): Promise<any> => {
    if (path.endsWith("/git/ref/heads/main")) return { object: { sha: over.main ?? sha } };
    if (path.endsWith("/actions/runs/100")) return run;
    if (path.endsWith("/actions/runs/100/approvals")) return over.approvals ?? [];
    if (path.endsWith("/environments/" + A.environment)) return environment;
    if (path.includes("deployment-branch-policies")) return { total_count: 1, branch_policies: [{ name: "main", type: "branch" }] };
    if (path.includes("/actions/workflows/")) return { workflow_runs: [{ id: 10, head_sha: sha, head_branch: "main", event: "push", status: "completed", conclusion: over.ci ?? "success", run_attempt: 1 }] };
    if (path.includes("/runs/10/")) return jobs;
    throw new Error("unexpected " + path);
  };
}
const ctx = (mode: string, confirmation = "") => ({
  GITHUB_REPOSITORY: P.repository, GITHUB_REPOSITORY_ID: PRIVATE_INGEST_RELEASE.repositoryId, GITHUB_EVENT_NAME: "workflow_dispatch",
  GITHUB_REF: "refs/heads/main", GITHUB_SHA: sha, GITHUB_RUN_ATTEMPT: "1", GITHUB_RUN_ID: "100",
  INGEST_ALERT_MODE: mode, INGEST_ALERT_SHA: sha, INGEST_ALERT_CONFIRMATION: confirmation,
});

describe("GitHub gate", () => {
  test("plan and apply pass for the pinned dispatcher on exact main with green CI", async () => {
    expect(await ingestErrorAlertGithubGate(ctx("plan"), github())).toMatchObject({ mode: "plan" });
    expect(await ingestErrorAlertGithubGate(ctx("apply", ingestErrorAlertConfirmation(sha, "a".repeat(64))), github(), true)).toMatchObject({ mode: "apply" });
  });
  test("refuses another dispatcher, another sha in the confirmation, and a missing reviewer approval", async () => {
    expect(await code(() => ingestErrorAlertGithubGate(ctx("plan"), github({ dispatcher: 1 })))).toBe("ingest_error_alert_gate_run");
    expect(await code(() => ingestErrorAlertGithubGate(ctx("apply", ingestErrorAlertConfirmation("2".repeat(40), "a".repeat(64))), github()))).toBe("ingest_error_alert_confirmation");
    expect(await code(() => ingestErrorAlertGithubGate(ctx("plan"), github({ reviewers: true }), true))).toBe("ingest_error_alert_gate_human_approval");
    expect(await code(() => ingestErrorAlertGithubGate({ ...ctx("plan"), GITHUB_RUN_ATTEMPT: "2" }, github()))).toBe("ingest_error_alert_gate_context");
  });
  test("with a required-reviewer rule: a pinned reviewer's approval passes; none or a rejection refuses", async () => {
    const env = { id: 7, name: A.environment };
    const approved = { state: "approved", user: { type: "User", id: P.dispatcher.id }, environments: [env] };
    const rejected = { state: "rejected", user: { type: "User", id: 999 }, environments: [env] };
    expect(await ingestErrorAlertGithubGate(ctx("plan"), github({ reviewers: true, approvals: [approved] }), true)).toMatchObject({ mode: "plan" });
    expect(await code(() => ingestErrorAlertGithubGate(ctx("plan"), github({ reviewers: true, approvals: [] }), true))).toBe("ingest_error_alert_gate_human_approval");
    expect(await code(() => ingestErrorAlertGithubGate(ctx("plan"), github({ reviewers: true, approvals: [approved, rejected] }), true))).toBe("ingest_error_alert_gate_human_rejected");
    expect(await code(() => ingestErrorAlertGithubGate(ctx("plan"), github({ reviewers: true, approvals: [{ ...approved, user: { type: "User", id: 999 } }] }), true))).toBe("ingest_error_alert_gate_human_approval");
  });
  test("without a reviewer rule the human gate is the pinned dispatcher alone (repository plan: no reviewers)", async () => {
    expect(await ingestErrorAlertGithubGate(ctx("apply", ingestErrorAlertConfirmation(sha, "a".repeat(64))), github({ approvals: [] }), true)).toMatchObject({ mode: "apply" });
    expect(await code(() => ingestErrorAlertGithubGate(ctx("apply", ingestErrorAlertConfirmation(sha, "a".repeat(64))), github({ dispatcher: 1 }), true))).toBe("ingest_error_alert_gate_run");
  });
  test("refuses wrong repository, ref, event, dispatched sha, a moved main and red CI", async () => {
    for (const bad of [{ GITHUB_REPOSITORY: "someone/fork" }, { GITHUB_REPOSITORY_ID: "1" }, { GITHUB_REF: "refs/heads/review/x" },
      { GITHUB_EVENT_NAME: "push" }, { GITHUB_SHA: "2".repeat(40) }, { GITHUB_RUN_ID: "0" }])
      expect(await code(() => ingestErrorAlertGithubGate({ ...ctx("plan"), ...bad }, github()))).toBe("ingest_error_alert_gate_context");
    expect(await code(() => ingestErrorAlertGithubGate(ctx("plan"), github({ main: "3".repeat(40) })))).toBe("ingest_error_alert_gate_main_moved");
    expect(await code(() => ingestErrorAlertGithubGate(ctx("plan"), github({ ci: "failure" })))).toBe("ingest_error_alert_gate_ci");
  });
});

describe("workflow", () => {
  const wf = readFileSync(new URL(A.workflow, ROOT), "utf8");
  test("dispatch only; the token reaches one step; the account matches the tracked profile", () => {
    expect(wf).toContain("workflow_dispatch:");
    expect(wf).not.toMatch(/^\s+(push|pull_request|schedule):/mu);
    expect(wf.match(/secrets\.CLOUDFLARE_ALERTING_API_TOKEN/gu)).toHaveLength(1);
    expect(wf).not.toContain("secrets.CLOUDFLARE_API_TOKEN");
    expect(wf).toContain(`CLOUDFLARE_ACCOUNT_ID: ${ACCOUNT}`);
    expect(wf).toContain(`echo "environment=${A.environment}"`);
    expect(wf).toContain("bun scripts/ingest-error-alert-github.ts --approved");
  });
  test("Workers Logs stays on: no observability override, and Alchemy's default keeps logs on", () => {
    expect(readFileSync(new URL("stacks/cloudflare/session-ingest.ts", ROOT), "utf8")).not.toContain("observability");
    // The real default (not a doc comment): WorkerAsyncBindings.ts DEFAULT_OBSERVABILITY, used when props omit it.
    const bindings = readFileSync(new URL("node_modules/@alchemy.run/cloudflare/src/Workers/WorkerAsyncBindings.ts", ROOT), "utf8");
    const block = bindings.match(/const DEFAULT_OBSERVABILITY: WorkerObservability = (\{[\s\S]*?\n\});/u)?.[1];
    expect(block?.replace(/\s+/gu, "")).toBe("{enabled:true,logs:{enabled:true,invocationLogs:true,},}");
    expect(bindings).toContain("news.observability ?? DEFAULT_OBSERVABILITY");
  });
  test("the Worker deploy contract does not reference the alert", () => {
    for (const f of ["stacks/cloudflare/session-ingest.ts", "src/session-ingest/private-ingest-release.ts", ".github/workflows/deploy-private-ingest-worker.yml"])
      expect(readFileSync(new URL(f, ROOT), "utf8")).not.toContain("ingest-error-alert");
  });
});
