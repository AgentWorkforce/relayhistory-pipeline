/**
 * First-upload smoke check for the local single-user stack (docs/selfhost-local.md).
 * Over the public origin only, through the nginx edge:
 *   1. GET /healthz is 404 at the edge (probes stay internal)
 *   2. an upload without a bearer is 401
 *   3. one synthetic Desktop-shaped delivery batch is 200 with a receiptId
 *   4. the pipeline consumer indexes it: GET /v1/sessions lists the session (up to 240 s)
 *
 *   bun deploy/selfhost-local/smoke.ts [--ca .96bulls/edge-tls/ca.crt]
 *
 * Reads RH_ORIGIN, RH_CONTROL_TOKEN, RH_USER_ORG, RH_USER_SUB and RH_WORKSPACE from
 * the environment (the same values compose.yaml uses) and trusts only --ca for TLS.
 * Prints one line per check and never prints the token. Exit 0 all passed, 1 a
 * check failed, 2 bad usage. The uploaded session is synthetic and stays in history;
 * hide it with POST /v1/delivery/sessions/selection (enabled:false) if you like.
 */
import { createHash, randomUUID } from "node:crypto";
import { readFileSync } from "node:fs";
import { deliveryAccount } from "../../src/session-ingest/vendor/delivery/prepare.ts";
import { DELIVERY_MAPPING_VERSION } from "../../src/session-ingest/vendor/delivery/delivery-contracts.ts";

const SOURCE = "claude";
const sha = (s: string) => createHash("sha256").update(s).digest("hex");

export interface SmokeUser { sub: string; org: string; workspace: string }

/** One delivery batch shaped like the Desktop's (e2e/selfhost/acceptance.ts), marked so a read can find it. */
export async function smokeBatch(user: SmokeUser, marker: string, startedAt: number, sessionId: string = randomUUID()) {
  const originId = `origin-${marker}`;
  const project = `/smoke/${marker}`;
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
  const account = await deliveryAccount({ orgId: user.org, workspaceId: user.workspace });
  return {
    sessionId,
    body: {
      protocolVersion: 1,
      batch: {
        schema_version: 1, origin_id: originId, batch_id: sha(`${originId}:batch:1`).slice(0, 32), job_id: "smoke-job",
        generation: 1, destination_id: "relayhistory", instance_id: "smoke", account_id: account,
        mapping_version: DELIVERY_MAPPING_VERSION, records,
      },
    },
  };
}

async function main(argv: string[], env: NodeJS.ProcessEnv): Promise<number> {
  let caPath = ".96bulls/edge-tls/ca.crt";
  for (let i = 0; i < argv.length; i++) {
    if (argv[i] === "--ca" && argv[i + 1]) caPath = argv[++i]!;
    else { console.error("usage: bun deploy/selfhost-local/smoke.ts [--ca <edge ca.crt>]"); return 2; }
  }
  for (const name of ["RH_ORIGIN", "RH_CONTROL_TOKEN", "RH_USER_ORG", "RH_USER_SUB", "RH_WORKSPACE"])
    if (!env[name]?.trim()) { console.error(`smoke: ${name} is required`); return 2; }
  const origin = env.RH_ORIGIN!.trim().replace(/\/+$/, "");
  const token = env.RH_CONTROL_TOKEN!.trim();
  const user = { sub: env.RH_USER_SUB!.trim(), org: env.RH_USER_ORG!.trim(), workspace: env.RH_WORKSPACE!.trim() };
  let ca: string;
  try { ca = readFileSync(caPath, "utf8"); } catch { console.error("smoke: cannot read --ca"); return 2; }
  const call = async (path: string, init: { method?: string; bearer?: boolean; body?: unknown } = {}) => {
    const res = await fetch(`${origin}${path}`, {
      method: init.method ?? "GET",
      headers: { ...(init.bearer ? { authorization: `Bearer ${token}` } : {}), ...(init.body ? { "content-type": "application/json" } : {}) },
      body: init.body ? JSON.stringify(init.body) : undefined,
      signal: AbortSignal.timeout(30_000),
      tls: { ca }, // Bun's fetch: trust only the edge CA
    } as RequestInit);
    const text = await res.text();
    let json: any = null;
    try { json = JSON.parse(text); } catch {}
    return { status: res.status, json };
  };
  let failed = false;
  const check = (name: string, ok: boolean, detail: string) => {
    console.log(`${ok ? "PASS" : "FAIL"} ${name}: ${detail}`);
    if (!ok) failed = true;
    return ok;
  };
  const marker = `smoke${Date.now().toString(36)}`;
  const upload = await smokeBatch(user, marker, Date.now());
  const probe = await call("/healthz");
  check("edge.probes-internal", probe.status === 404, `GET /healthz ${probe.status}`);
  const anon = await call("/v1/delivery/batches", { method: "POST", body: upload.body });
  check("upload.no-token", anon.status === 401, `POST /v1/delivery/batches without a bearer ${anon.status}`);
  const up = await call("/v1/delivery/batches", { method: "POST", bearer: true, body: upload.body });
  if (!check("upload.first", up.status === 200 && typeof up.json?.receiptId === "string", `POST /v1/delivery/batches ${up.status}`)) return 1;
  const deadline = Date.now() + 240_000;
  for (;;) {
    const list = await call(`/v1/sessions?workspace=${encodeURIComponent(user.workspace)}`, { bearer: true });
    const listed = (list.json?.sessions ?? []).some((s: any) => s?.sessionId === upload.sessionId);
    if (listed) { check("read.listed", true, `session ${upload.sessionId} listed`); break; }
    if (Date.now() > deadline) { check("read.listed", false, `not listed within 240 s (last GET /v1/sessions ${list.status})`); break; }
    await new Promise((r) => setTimeout(r, 5_000));
  }
  return failed ? 1 : 0;
}

if (import.meta.main) {
  main(process.argv.slice(2), process.env).then(
    (code) => process.exit(code),
    // fetch errors name the URL and cause, never a header, so the token is not printed.
    (error) => { console.error(`smoke: ${error instanceof Error ? error.message : "request failed"}`); process.exit(1); },
  );
}
