/**
 * The end-to-end acceptance checks, black-box over HTTP against the stack in
 * compose.yaml: authenticated upload, processing by the pipeline consumer, the
 * read API (list, turns, search, activity), tenant isolation, durability across
 * a restart, and erase. Every id and token is synthetic. No check prints a token.
 */
import { createHash, randomBytes, randomUUID } from "node:crypto";
import { deliveryAccount } from "../../src/session-ingest/vendor/delivery/prepare.ts";
import { DELIVERY_MAPPING_VERSION } from "../../src/session-ingest/vendor/delivery/delivery-contracts.ts";
import { TURN_KIND, TURN_ORIGIN_ID, turnRecordId } from "../../src/session-ingest/turns.ts";
import { TENANT_A, TENANT_B, type StackSession, type Tenant } from "./stack.ts";

const SOURCE = "claude";
const sha = (s: string) => createHash("sha256").update(s).digest("hex");
export type Result = "PASS" | "FAIL" | "BLOCKED";
export interface Check { name: string; result: Result; detail: string }
class Failed extends Error {}
class Blocked extends Error {}
const must: (cond: unknown, why: string) => asserts cond = (cond, why) => {
  if (!cond) throw new Failed(why);
};
const sleep = (ms: number) => new Promise((r) => setTimeout(r, ms));

interface Upload { sessionId: string; originId: string; marker: string; startedAt: number; body: unknown }

/** One Desktop-shaped delivery batch, marked so reads can find exactly this session. */
async function deliveryBatch(tenant: Tenant, marker: string, startedAt: number): Promise<Upload> {
  const sessionId = randomUUID();
  const originId = `origin-${marker}`;
  const project = `/e2e/${marker}`;
  const base = { git_branch: null, project, session_id: sessionId, source: SOURCE };
  const event = (id: number, role: string, text: string, ts: number) => ({
    ...base, agent_id: null, agent_version: null, call_index: null, cwd: project, error_signal: null,
    event_index: id, event_source: "provider", event_uid: `${sessionId}-message-${id}:0`, id,
    is_meta: 0, is_sidechain: 0, kind: "text", message_id: `${sessionId}-message-${id}`, model: null,
    parent_id: null, payload_bytes: null, payload_hash: null, payload_truncated: 0,
    project_key: null, project_key_method: null, provider: "anthropic", provider_message_id: null,
    raw_facts_version: 2, raw_kind: role, request_id: null, request_span: null,
    result_status: "completed", role, stop_reason: null, subagent_session_id: null, text,
    token_json: null, tool_use_id: null, ts_ms: ts, turn_id: `turn-${id}`,
  });
  const payloads: [string, Record<string, unknown>][] = [
    ["session", {
      agent_version: null, cwd: project, discovery_state: "full", first_activity_ms: startedAt,
      first_prompt: `${marker} first prompt`, git_branch: null, initial_commit: null,
      last_activity_ms: startedAt + 2000, last_assistant_text: `${marker} assistant answer`,
      models_json: null, originator: null, parser_version: 1, project_key: null,
      project_key_method: null, repo_url: null, session_id: sessionId, source: SOURCE,
      source_stamp: null, workspace_roots_json: null,
    }],
    ["history", { ...base, id: 1, prompt: `${marker} first prompt`, prompt_hash: sha(marker).slice(0, 16), timestamp_ms: startedAt }],
    ["session_event", event(1, "user", `${marker} first prompt`, startedAt)],
    ["session_event", event(2, "assistant", `${marker} assistant answer`, startedAt + 1000)],
  ];
  const records = payloads.map(([kind, payload], index) => {
    const recordId = sha(`${originId}:${kind}:${sessionId}:${index}`);
    return {
      kind, operation: "upsert", origin_id: originId, payload, record_id: recordId,
      revision: 1, revision_id: sha(`${recordId}:1:${JSON.stringify(payload)}`), schema_version: 1,
      session_id: sessionId, source: SOURCE,
    };
  });
  const account = await deliveryAccount({ orgId: tenant.org, workspaceId: tenant.workspace, userId: tenant.sub });
  return {
    sessionId, originId, marker, startedAt,
    body: {
      protocolVersion: 1,
      batch: {
        schema_version: 1, origin_id: originId, batch_id: sha(`${originId}:batch:1`).slice(0, 32), job_id: "e2e-job",
        generation: 1, destination_id: "relayhistory", instance_id: "e2e", account_id: account,
        mapping_version: DELIVERY_MAPPING_VERSION, records,
      },
    },
  };
}

async function turnsBatch(tenant: Tenant, upload: Upload) {
  const account = await deliveryAccount({ orgId: tenant.org, workspaceId: tenant.workspace, userId: tenant.sub });
  const turn = (index: number, role: string, content: string) => {
    const payload = {
      id: randomUUID(), org_id: tenant.org, workspace_id: tenant.workspace, source: SOURCE, session_id: upload.sessionId,
      turn_index: index, session_owner: tenant.sub, role, content, actor_name: "E2E", actor_role: "owner",
      metadata_json: JSON.stringify({ nativeCli: SOURCE }), ts: new Date(upload.startedAt + index * 1000).toISOString(), tagged: true,
    };
    const recordId = turnRecordId(tenant.workspace, SOURCE, upload.sessionId, index);
    const revision = 1_793_000_000_000_000 + index;
    return { schema_version: 1, origin_id: TURN_ORIGIN_ID, record_id: recordId, revision_id: `turnrev:${recordId.slice(5)}:${revision}`, revision, kind: TURN_KIND, source: SOURCE, session_id: upload.sessionId, operation: "upsert", payload };
  };
  return {
    protocolVersion: 1,
    batch: {
      schema_version: 1, origin_id: TURN_ORIGIN_ID, batch_id: `turns-${upload.marker}`, job_id: "relayhistory-turns", generation: 1,
      destination_id: "relayhistory", instance_id: "relayhistory-api", account_id: account, mapping_version: DELIVERY_MAPPING_VERSION,
      records: [turn(0, "user", `${upload.marker} user turn`), turn(1, "assistant", `${upload.marker} assistant turn`)],
    },
  };
}

export async function runAcceptance(stack: StackSession, log: (c: Check) => void): Promise<Check[]> {
  const checks: Check[] = [];
  const record = (name: string, result: Result, detail: string) => {
    const c = { name, result, detail };
    checks.push(c);
    log(c);
  };
  const once = async (url: string, method: string, token: string | null, body: unknown, extra: Record<string, string>) => {
    const res = await fetch(url, {
      method,
      headers: { ...(token ? { authorization: `Bearer ${token}` } : {}), ...(body ? { "content-type": "application/json" } : {}), ...extra },
      body: body ? JSON.stringify(body) : undefined,
      signal: AbortSignal.timeout(30_000),
    });
    const text = await res.text();
    let json: any = null;
    try { json = JSON.parse(text); } catch {}
    return { status: res.status, json, text };
  };
  /** A container that restarted gets a new ephemeral host port, so a refused connection re-resolves it. */
  const call = async (base: () => string, path: string, method: string, token: string | null, body?: unknown, extra: Record<string, string> = {}) => {
    const deadline = Date.now() + 90_000;
    for (;;) {
      try {
        return await once(`${base()}${path}`, method, token, body, extra);
      } catch (error) {
        if (Date.now() > deadline) throw error;
        await sleep(2_000);
        try { stack.resolveUrls(); } catch {}
      }
    }
  };
  const ingest = (path: string, token: string | null, body?: unknown, extra?: Record<string, string>) => call(() => stack.ingestUrl, path, "POST", token, body, extra);
  const { a: tokenA, b: tokenB, aReadOnly } = stack.tokens;
  // This backend is workspace-scoped: every read names the caller's own workspace unless a check overrides it.
  const withWorkspace = (path: string, token: string | null) => {
    if (/[?&]workspace=/u.test(path)) return path;
    const workspace = token === tokenB ? TENANT_B.workspace : TENANT_A.workspace;
    return `${path}${path.includes("?") ? "&" : "?"}workspace=${workspace}`;
  };
  const read = (path: string, token: string | null) => call(() => stack.readUrl, withWorkspace(path, token), "GET", token);
  const step = async (name: string, fn: () => Promise<string>) => {
    try {
      record(name, "PASS", await fn());
    } catch (error) {
      if (error instanceof Blocked) record(name, "BLOCKED", error.message);
      else record(name, "FAIL", error instanceof Failed ? error.message : `unexpected: ${(error as Error).message}`);
    }
  };
  const has = (r: { json: any }, id: string) => (r.json?.sessions ?? []).some((s: any) => s.sessionId === id);
  const hasEvent = (r: { json: any }, marker: string) => (r.json?.events ?? []).some((e: any) => JSON.stringify(e).includes(marker));
  const fresh = () => randomBytes(5).toString("hex");
  /** Nothing served: not found, an empty list, or this backend's "no pointer in your scope" refusal. */
  const absent = (r: { status: number; json: any; text: string }, marker: string) =>
    !r.text.includes(marker) &&
    (r.status === 404 ||
      (r.status === 501 && /_no_pointers$/u.test(r.json?.error?.reason ?? "")) ||
      (r.status === 200 && (r.json?.turns ?? []).length === 0));

  const started = Date.now() - 5 * 60_000;
  const uploadA = await deliveryBatch(TENANT_A, `amark${fresh()}`, started);
  const uploadB = await deliveryBatch(TENANT_B, `bmark${fresh()}`, started);
  const day = new Date(started).toISOString().slice(0, 10);
  const activityPath = `/v1/sessions/activity?since=${day}T00:00:00.000Z&until=${new Date(Date.now() + 86_400_000).toISOString().slice(0, 10)}T00:00:00.000Z`;
  let receiptA = "";

  await step("upload.authenticated", async () => {
    const noToken = await ingest("/v1/delivery/batches", null, uploadA.body);
    must(noToken.status === 401, `no token: ${noToken.status}`);
    const readOnly = await ingest("/v1/delivery/batches", aReadOnly, uploadA.body);
    must(readOnly.status === 403, `read-only token: ${readOnly.status}`);
    const spoof = await ingest("/v1/delivery/batches", tokenB, uploadA.body, { "x-relay-ingest-identity": "spoof" });
    must(spoof.status === 403 || spoof.status === 400, `token B on org A's account: ${spoof.status}`);
    const a = await ingest("/v1/delivery/batches", tokenA, uploadA.body);
    must(a.status === 200 && typeof a.json?.receiptId === "string", `upload A: ${a.status} ${a.text.slice(0, 200)}`);
    receiptA = a.json.receiptId;
    const b = await ingest("/v1/delivery/batches", tokenB, uploadB.body);
    must(b.status === 200, `upload B: ${b.status} ${b.text.slice(0, 200)}`);
    const turns = await ingest("/v1/turns/batches", tokenA, await turnsBatch(TENANT_A, uploadA));
    must(turns.status === 200, `turns: ${turns.status} ${turns.text.slice(0, 200)}`);
    return `no_token=401 read_only=403 cross_tenant=${spoof.status} upload=200 turns=200`;
  });

  await step("process.consumer", async () => {
    const deadline = Date.now() + 240_000;
    let last = "";
    for (;;) {
      const list = await read("/v1/sessions", tokenA);
      last = `list=${list.status}`;
      const search = await read(`/v1/events?q=${uploadA.marker}&limit=20`, tokenA);
      last += ` search=${search.status}:${search.json?.error?.reason ?? ""}`;
      // The turns are a second origin, so a second manifest: the session lists before they are indexed.
      const turns = await read(`/v1/sessions/${uploadA.sessionId}/turns?source=${SOURCE}`, tokenA);
      last += ` turns=${turns.status}:${(turns.json?.turns ?? []).length}`;
      if (list.status === 200 && has(list, uploadA.sessionId) && search.status === 200 && hasEvent(search, uploadA.marker) && (turns.json?.turns ?? []).length === 2) return "consumer indexed both uploads into directory, search and turns";
      if (Date.now() > deadline) throw new Failed(`not processed within 240s: ${last}`);
      await sleep(5_000);
    }
  });

  const readA = async (label: string) => {
    const unauth = await read("/v1/sessions", null);
    must(unauth.status === 401, `${label} anonymous list: ${unauth.status}`);
    const list = await read("/v1/sessions", tokenA);
    must(list.status === 200 && has(list, uploadA.sessionId), `${label} list: ${list.status}`);
    const turns = await read(`/v1/sessions/${uploadA.sessionId}/turns?source=${SOURCE}`, tokenA);
    must(turns.status === 200 && turns.json?.turns?.length === 2 && turns.json.turns[0].content.includes(uploadA.marker), `${label} turns: ${turns.status} ${turns.text.slice(0, 200)}`);
    const search = await read(`/v1/events?q=${uploadA.marker}&limit=20`, tokenA);
    must(search.status === 200 && hasEvent(search, uploadA.marker), `${label} search: ${search.status}`);
    const miss = await read(`/v1/events?q=${uploadA.marker}zz&limit=20`, tokenA);
    must(miss.status === 200 && !hasEvent(miss, uploadA.marker), `${label} search miss: ${miss.status}`);
    const activity = await read(activityPath, tokenA);
    const conversations = (activity.json?.days ?? []).reduce((sum: number, d: any) => sum + (d.total ?? 0), 0);
    must(activity.status === 200 && conversations >= 1, `${label} activity: ${activity.status} ${activity.text.slice(0, 200)}`);
    return `list=200 turns=2 search_hit=true search_miss=true activity=${conversations}`;
  };
  await step("read.api", () => readA("reads"));

  await step("tenant.isolation", async () => {
    const list = await read("/v1/sessions", tokenB);
    must(list.status === 200 && !has(list, uploadA.sessionId) && has(list, uploadB.sessionId), `B list: ${list.status}`);
    const turns = await read(`/v1/sessions/${uploadA.sessionId}/turns?source=${SOURCE}`, tokenB);
    must(absent(turns, uploadA.marker), `B turns on A: ${turns.status} ${turns.json?.error?.reason ?? ""}`);
    const search = await read(`/v1/events?q=${uploadA.marker}&limit=20`, tokenB);
    must(search.status === 200 && !hasEvent(search, uploadA.marker), `B search for A's marker: ${search.status}`);
    const timing = await read(`/v1/sessions/${uploadA.sessionId}/timing?source=${SOURCE}`, tokenB);
    must(absent(timing, uploadA.marker), `B timing on A: ${timing.status} ${timing.json?.error?.reason ?? ""}`);
    const catalog = await read(`/v1/sessions/${uploadA.sessionId}/catalog?source=${SOURCE}`, tokenB);
    must(absent(catalog, uploadA.marker), `B catalog on A: ${catalog.status} ${catalog.json?.error?.reason ?? ""}`);
    const otherWs = await read(`/v1/sessions?workspace=${TENANT_A.workspace}`, tokenB);
    must(otherWs.status === 403 || otherWs.status === 401, `B naming A's workspace: ${otherWs.status}`);
    return `b_list_clean=true b_turns=${turns.status} b_search_clean=true b_timing=${timing.status} b_catalog=${catalog.status} b_ws_a=${otherWs.status}`;
  });

  await step("durability.restart", async () => {
    stack.compose(["restart", "db", "minio", "session-ingest", "session-read-api", "session-pipeline-consumer"], { quiet: true });
    // A container still coming back has no published port yet.
    for (let i = 0; ; i++) {
      try { stack.resolveUrls(); break; } catch (error) { if (i >= 30) throw error; await sleep(2_000); }
    }
    const deadline = Date.now() + 120_000;
    for (;;) {
      try {
        if ((await read("/v1/sessions", tokenA)).status === 200) break;
      } catch {}
      if (Date.now() > deadline) throw new Failed("read API did not recover within 120s");
      await sleep(2_000);
    }
    // The object store and pools come back on their own schedule; a 503 here is a retry, not lost data.
    let reads = "";
    const readDeadline = Date.now() + 120_000;
    for (;;) {
      try { reads = await readA("after restart"); break; } catch (error) {
        if (!(error instanceof Failed) || !/: 503/u.test(error.message) || Date.now() > readDeadline) throw error;
        await sleep(3_000);
      }
    }
    // The original upload is idempotent: same receipt, same stored objects.
    let retry = await ingest("/v1/delivery/batches", tokenA, uploadA.body);
    for (let i = 0; i < 12 && retry.status === 503; i++) {
      await sleep(5_000);
      retry = await ingest("/v1/delivery/batches", tokenA, uploadA.body);
    }
    must(retry.status === 200 && retry.json?.receiptId === receiptA, `retry after restart: ${retry.status} same_receipt=${retry.json?.receiptId === receiptA}`);
    // A brand-new session after the restart is uploaded and processed too.
    const later = await deliveryBatch(TENANT_A, `amark${fresh()}`, started + 60_000);
    let up = await ingest("/v1/delivery/batches", tokenA, later.body);
    for (let i = 0; i < 12 && up.status === 503; i++) {
      await sleep(5_000);
      up = await ingest("/v1/delivery/batches", tokenA, later.body);
    }
    must(up.status === 200, `upload after restart: ${up.status} ${up.text.slice(0, 200)}`);
    const pdeadline = Date.now() + 240_000;
    for (;;) {
      const list = await read("/v1/sessions", tokenA);
      if (list.status === 200 && has(list, later.sessionId)) break;
      if (Date.now() > pdeadline) throw new Failed("session uploaded after restart was not processed within 240s");
      await sleep(5_000);
    }
    return `${reads}; idempotent_retry=same_receipt; new_session_after_restart=processed`;
  });

  await step("erase", async () => {
    // A selection is per origin (hosted semantics), and this session has records from two: the desktop upload and the relay turns.
    const account = await deliveryAccount({ orgId: TENANT_A.org, workspaceId: TENANT_A.workspace, userId: TENANT_A.sub });
    for (const origin of [uploadA.originId, TURN_ORIGIN_ID]) {
      const erase = await ingest("/v1/delivery/sessions/selection", tokenA, { account_id: account, origin_id: origin, sessions: [{ source: SOURCE, session_id: uploadA.sessionId }], enabled: false });
      if (erase.status === 501) throw new Blocked("selection answers 501 unsupported_by_backend on this build (needs PR #392)");
      must(erase.status === 200, `selection (${origin}): ${erase.status} ${erase.text.slice(0, 200)}`);
    }
    const list = await read("/v1/sessions", tokenA);
    must(list.status === 200 && !has(list, uploadA.sessionId), "list still returns the erased session");
    const turns = await read(`/v1/sessions/${uploadA.sessionId}/turns?source=${SOURCE}`, tokenA);
    must(absent(turns, uploadA.marker), `turns after erase: ${turns.status} ${turns.json?.error?.reason ?? ""}`);
    const search = await read(`/v1/events?q=${uploadA.marker}&limit=20`, tokenA);
    must(search.status === 200 && !hasEvent(search, uploadA.marker), "search still finds the erased session");
    const stale = await ingest("/v1/delivery/batches", tokenA, uploadA.body);
    must(stale.status === 200, `stale retry: ${stale.status}`);
    const after = await read("/v1/sessions", tokenA);
    must(after.status === 200 && !has(after, uploadA.sessionId), "a stale retry resurrected the erased session");
    const b = await read("/v1/sessions", tokenB);
    must(b.status === 200 && has(b, uploadB.sessionId), "erase affected another tenant's session");
    return "unreadable via list, turns and search; stale retry does not resurrect; other tenant untouched";
  });

  return checks;
}
