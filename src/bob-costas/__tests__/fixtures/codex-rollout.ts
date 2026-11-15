/**
 * A Codex rollout transcript in the harness's real record shapes (session_meta,
 * turn_context, developer and injected-context messages, event_msg duplicates,
 * reasoning, function_call and custom_tool_call with their outputs). Text is synthetic.
 */
const r = (timestamp: string, type: string, payload: Record<string, unknown>) => JSON.stringify({ timestamp, type, payload });

export const CODEX_FIRST_PROMPT = "Fix the flaky retry test in the queue worker";
export const CODEX_SECOND_PROMPT = "Now run the whole suite";

export const codexRollout: string[] = [
  r("2026-11-07T12:49:08.000Z", "session_meta", { id: "019a0000-0000-7000-8000-000000000001", cwd: "/work/repo", originator: "codex_cli_rs", cli_version: "0.80.0" }),
  r("2026-11-07T12:49:08.100Z", "response_item", { type: "message", role: "developer", content: [{ type: "input_text", text: "You are Codex, a coding agent." }] }),
  r("2026-11-07T12:49:08.200Z", "response_item", { type: "message", role: "user", content: [{ type: "input_text", text: "# AGENTS.md instructions\n\n<INSTRUCTIONS>\nUse bun.\n</INSTRUCTIONS>" }] }),
  r("2026-11-07T12:49:08.300Z", "response_item", { type: "message", role: "user", content: [{ type: "input_text", text: "<environment_context>\n  <cwd>/work/repo</cwd>\n</environment_context>" }] }),
  r("2026-11-07T12:49:09.000Z", "turn_context", { cwd: "/work/repo", model: "gpt-5-codex", approval_policy: "on-request", effort: "medium" }),
  r("2026-11-07T12:49:09.100Z", "response_item", { type: "message", role: "user", content: [{ type: "input_text", text: CODEX_FIRST_PROMPT }] }),
  r("2026-11-07T12:49:09.100Z", "event_msg", { type: "user_message", message: CODEX_FIRST_PROMPT }),
  r("2026-11-07T12:49:10.000Z", "response_item", { type: "reasoning", summary: [{ type: "summary_text", text: "Look at the test." }], encrypted_content: "x" }),
  r("2026-11-07T12:49:11.000Z", "response_item", { type: "message", role: "assistant", content: [{ type: "output_text", text: "I'll read the worker test first." }] }),
  r("2026-11-07T12:49:12.000Z", "response_item", { type: "function_call", name: "shell", arguments: "{\"command\":[\"cat\",\"worker.test.ts\"]}", call_id: "call_1" }),
  r("2026-11-07T12:49:12.500Z", "response_item", { type: "function_call_output", call_id: "call_1", output: "test body" }),
  r("2026-11-07T12:49:13.000Z", "response_item", { type: "custom_tool_call", name: "apply_patch", input: "*** Begin Patch", call_id: "call_2", status: "completed" }),
  r("2026-11-07T12:49:13.500Z", "response_item", { type: "custom_tool_call_output", call_id: "call_2", output: "Done" }),
  r("2026-11-07T12:49:14.000Z", "response_item", { type: "message", role: "assistant", content: [{ type: "output_text", text: "Fixed: the test now awaits the lease." }] }),
  r("2026-11-07T12:49:14.100Z", "event_msg", { type: "agent_message", message: "Fixed: the test now awaits the lease." }),
  r("2026-11-07T12:49:14.200Z", "event_msg", { type: "token_count", info: null }),
  r("2026-11-07T12:50:00.000Z", "turn_context", { cwd: "/work/repo", model: "gpt-5-codex" }),
  r("2026-11-07T12:50:00.100Z", "response_item", { type: "message", role: "user", content: [{ type: "input_text", text: CODEX_SECOND_PROMPT }] }),
  r("2026-11-07T12:50:01.000Z", "response_item", { type: "function_call", name: "shell", arguments: "{\"command\":[\"bun\",\"test\"]}", call_id: "call_3" }),
  r("2026-11-07T12:50:05.000Z", "response_item", { type: "message", role: "assistant", content: [{ type: "output_text", text: "All tests pass." }] }),
];
