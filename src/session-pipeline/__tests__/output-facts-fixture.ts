/**
 * One session scope's canonical read, as readCanonicalScope returns it, covering
 * every Output rule: message qualification and dedup, tool-result commit matching
 * (accepted, a non-commit call, two logical calls), a commit link whose full sha
 * resolves an abbreviated tool-result sha, a reverted legacy outcome, and a legacy
 * turn whose session already has window messages.
 */
import type { RelayhistoryStreamRecord } from "../../../packages/pipeline/src/lib/pipeline/relayhistory-transform.ts";
import type { OutputFactsScope } from "../output-facts.ts";

export const OUTPUT_FIXTURE_SCOPE: OutputFactsScope = {
  orgId: "org-f4",
  workspaceId: "ws-f4",
  harness: "codex",
  sessionIds: ["s1"],
  canonicalId: "s1",
};

export const LINK_SHA = "abc1234" + "f".repeat(33);
export const OUTCOME_SHA = "9".repeat(40);

type Row = { table: string; row: Record<string, unknown> };

let n = 0;
function event(fields: Record<string, unknown>, payload: Record<string, unknown> = {}): Row {
  n += 1;
  return {
    table: "convergence_events",
    row: {
      machine_id: "m1",
      source: "codex",
      session_id: "s1",
      event_id: `m1:e${n}`,
      kind: "session_event",
      type: "assistant",
      ts: `2026-11-05T10:00:${String(n).padStart(2, "0")}.000001Z`,
      actor_role: "assistant",
      content: "text",
      request_key: null,
      delivery_record_id: `rec-${n}`,
      record: { deliveryRecordId: `rec-${n}`, payload },
      tool_name: null,
      task_ref: { id: "acme/app@main" },
      project_id: "acme/app",
      files_touched: ["src/a.ts"],
      model: "gpt-5",
      provider: "openai",
      input_tokens: 10,
      output_tokens: 5,
      reasoning_tokens: 0,
      cache_read_tokens: 0,
      cache_create_tokens: 0,
      cache_create_5m_tokens: null,
      cache_create_1h_tokens: null,
      cost_usd_micros: 100,
      ...fields,
    },
  };
}

const call = (toolUse: string, message: string, command: string) =>
  event({ kind: "tool_call", type: "tool_call", actor_role: null, tool_name: "Bash", content: null }, { tool_use_id: toolUse, message_id: message, args_json: JSON.stringify({ command }) });
const result = (toolUse: string, content: string) =>
  event({ actor_role: "tool_result", type: "tool_result", content }, { tool_use_id: toolUse });

const legacy = (kind: string, table: string, key: string, payload: Record<string, unknown>): RelayhistoryStreamRecord => ({
  kind,
  origin_id: `legacy:${table}`,
  record_id: `legacy:${table}:${key}`,
  revision: "1",
  operation: "upsert",
  received_at: "2026-11-05T00:00:00.000000Z",
  payload: { org_id: "org-f4", workspace_id: "ws-f4", source: "codex", session_id: "s1", ...payload },
});

export function outputFixture(): { projectionRows: Row[]; records: RelayhistoryStreamRecord[]; complete: boolean } {
  n = 0;
  const projectionRows: Row[] = [
    event({}, { kind: "text", message_id: "msg-1" }),
    event({}, { kind: "text", message_id: "msg-1" }),
    event({ kind: "assistant" }, { message_id: "msg-2" }),
    event({ kind: "text", type: "text", request_key: "req-9" }, { kind: "text", message_id: "msg-9" }),
    event({ content: "   " }, { kind: "text", message_id: "blank" }),
    event({}, { kind: "thinking", message_id: "thought" }),
    call("tu1", "m-call-1", "git commit -m fix"),
    result("tu1", "[main abc1234] fix\n 1 file changed, 5 insertions(+), 3 deletions(-)"),
    call("tu2", "m-call-2", "git add . && git commit -m y"),
    result("tu2", "[main def5678] y\n 1 file changed, 2 insertions(+)"),
    call("tu3", "m-call-3", "ls -la"),
    result("tu3", "[main 7777777] z\n 1 file changed"),
    call("tu4", "m-call-4a", "git commit -m a"),
    call("tu4", "m-call-4b", "git commit -m b"),
    result("tu4", "[main 8888888] w\n 1 file changed"),
    {
      table: "session_commit_links",
      row: {
        source: "codex", record_id: "link-1", origin_id: "o1", received_at: "2026-11-05T09:00:00.000000Z", session_id: "s1",
        commit_sha: LINK_SHA.toUpperCase(), numstat: [{ path: "a", additions: 10, deletions: 1 }], linked_at: "2026-11-05T09:00:00.000000Z",
      },
    },
  ];
  const records = [
    legacy("legacy_conversation_turn", "conversation_turns", "t-1", {
      id: "t-1", turn_index: 0, role: "assistant", session_owner: "u1", content: "old answer", actor_name: "a", actor_role: "owner", ts: "2026-11-05T08:00:00.000000Z",
    }),
    legacy("legacy_session_outcome", "session_outcomes", OUTCOME_SHA, {
      commit_sha: OUTCOME_SHA, reverted: true, shipped_at: "2026-11-05T12:00:00.000000Z", created_at: "2026-11-05T11:00:00.000000Z", numstat_json: { added: "1", deleted: "1" },
    }),
  ];
  return { projectionRows, records, complete: true };
}
