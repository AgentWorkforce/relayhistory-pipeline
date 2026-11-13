/**
 * Fixed, synthetic fixtures for the pipeline e2e. Every id, timestamp and text is a constant, so two runs send
 * byte-identical batches; isolation between runs comes from a fresh compose project with fresh volumes, never
 * from random ids. Nothing here is real data.
 */
import { createHash } from "node:crypto";
import { deliveryAccount } from "../../src/session-ingest/vendor/delivery/prepare.ts";
import { DELIVERY_MAPPING_VERSION } from "../../src/session-ingest/vendor/delivery/delivery-contracts.ts";
import type { Tenant } from "../selfhost/stack.ts";

export const SOURCE = "claude";
/** 2026-11-01T09:00:00Z: inside any read window the stack applies, and fixed. */
export const STARTED_MS = 1_793_523_600_000;
const sha = (s: string) => createHash("sha256").update(s).digest("hex");

export interface Fixture { sessionId: string; originId: string; marker: string }

/** The sessions the lane drives. The marker is a word the search tokenizer keeps whole. */
export const FIXTURES = {
  http: { sessionId: "0195f000-0000-7000-8000-00000000a001", originId: "origin-e2e-http", marker: "pipehttpmarker" },
  s3: { sessionId: "0195f000-0000-7000-8000-00000000b001", originId: "origin-e2e-s3", marker: "pipesthreemarker" },
  s3Second: { sessionId: "0195f000-0000-7000-8000-00000000b002", originId: "origin-e2e-s3", marker: "pipesthreesecond" },
  s3AfterReset: { sessionId: "0195f000-0000-7000-8000-00000000b003", originId: "origin-e2e-s3", marker: "pipesthreeafter" },
  s3Rearm: { sessionId: "0195f000-0000-7000-8000-00000000b004", originId: "origin-e2e-s3", marker: "pipesthreerearm" },
} as const satisfies Record<string, Fixture>;

/**
 * One Desktop-shaped delivery batch (the shape e2e/selfhost/acceptance.ts sends), with an explicit batch id and
 * record revision so a test can send the same records again under a new batch (stale) or a newer revision.
 */
export async function deliveryBatch(tenant: Tenant, f: Fixture, opts: { batch: string; revision: number }) {
  const project = `/e2e/${f.marker}`;
  const base = { git_branch: null, project, session_id: f.sessionId, source: SOURCE };
  const event = (id: number, role: string, text: string, ts: number) => ({
    ...base, agent_id: null, agent_version: null, call_index: null, cwd: project, error_signal: null,
    event_index: id, event_source: "provider", event_uid: `${f.sessionId}-message-${id}:0`, id,
    is_meta: 0, is_sidechain: 0, kind: "text", message_id: `${f.sessionId}-message-${id}`, model: null,
    parent_id: null, payload_bytes: null, payload_hash: null, payload_truncated: 0,
    project_key: null, project_key_method: null, provider: "anthropic", provider_message_id: null,
    raw_facts_version: 2, raw_kind: role, request_id: null, request_span: null,
    result_status: "completed", role, stop_reason: null, subagent_session_id: null, text,
    token_json: null, tool_use_id: null, ts_ms: ts, turn_id: `turn-${id}`,
  });
  const payloads: [string, Record<string, unknown>][] = [
    ["session", {
      agent_version: null, cwd: project, discovery_state: "full", first_activity_ms: STARTED_MS,
      first_prompt: `${f.marker} first prompt`, git_branch: null, initial_commit: null,
      last_activity_ms: STARTED_MS + 2000, last_assistant_text: `${f.marker} assistant answer`,
      models_json: null, originator: null, parser_version: 1, project_key: null,
      project_key_method: null, repo_url: null, session_id: f.sessionId, source: SOURCE,
      source_stamp: null, workspace_roots_json: null,
    }],
    ["history", { ...base, id: 1, prompt: `${f.marker} first prompt`, prompt_hash: sha(f.marker).slice(0, 16), timestamp_ms: STARTED_MS }],
    ["session_event", event(1, "user", `${f.marker} first prompt`, STARTED_MS)],
    ["session_event", event(2, "assistant", `${f.marker} assistant answer`, STARTED_MS + 1000)],
  ];
  const records = payloads.map(([kind, payload], index) => {
    const recordId = sha(`${f.originId}:${kind}:${f.sessionId}:${index}`);
    return {
      kind, operation: "upsert", origin_id: f.originId, payload, record_id: recordId,
      revision: opts.revision, revision_id: sha(`${recordId}:${opts.revision}:${JSON.stringify(payload)}`), schema_version: 1,
      session_id: f.sessionId, source: SOURCE,
    };
  });
  const account = await deliveryAccount({ orgId: tenant.org, workspaceId: tenant.workspace });
  return {
    protocolVersion: 1,
    batch: {
      schema_version: 1, origin_id: f.originId, batch_id: sha(`${f.originId}:batch:${opts.batch}`).slice(0, 32), job_id: "e2e-pipeline",
      generation: 1, destination_id: "relayhistory", instance_id: "e2e", account_id: account,
      mapping_version: DELIVERY_MAPPING_VERSION, records,
    },
  };
}

/** The courier leg's transcript: Claude-shaped JSONL, the records the extractive summariser digests. */
export const COURIER_SESSION = { sessionId: "e2e-courier-session-0001", marker: "pipecouriermarker" } as const;
export function courierTranscript(): string {
  const ts = (s: number) => new Date(STARTED_MS + s * 1000).toISOString();
  const lines = [
    { type: "user", sessionId: COURIER_SESSION.sessionId, timestamp: ts(0), message: { role: "user", content: `${COURIER_SESSION.marker}: add a retry to the upload loop` } },
    { type: "assistant", sessionId: COURIER_SESSION.sessionId, timestamp: ts(5), message: { role: "assistant", model: "fixture-model", content: [{ type: "text", text: "I will read the loop first." }, { type: "tool_use", name: "Read", input: { path: "src/loop.ts" } }] } },
    { type: "user", sessionId: COURIER_SESSION.sessionId, timestamp: ts(9), message: { role: "user", content: "Also add a test for the retry." } },
    { type: "assistant", sessionId: COURIER_SESSION.sessionId, timestamp: ts(15), message: { role: "assistant", model: "fixture-model", content: [{ type: "text", text: "Added the retry and its test." }] } },
  ];
  return lines.map((l) => JSON.stringify(l)).join("\n") + "\n";
}

/** The deployment-wide marker an operator writes (ledger-s3.ts FenceAuthorityDoc): a test value, every gate asserted. */
export const FENCE_AUTHORITY = {
  v: 1, fenceBackfill: "complete", revisionBackfill: "complete", ownershipBackfill: "complete",
  controlMirror: "on", rearmSweeper: "live", workerHttpAdmission: "off", revisionLayout: "4096", writtenAtMs: STARTED_MS,
} as const;
