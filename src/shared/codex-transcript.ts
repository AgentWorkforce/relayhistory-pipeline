/**
 * Codex rollout records, as the harness writes them to its JSONL transcript. Shared by
 * the brief's transcript reader (`session-brief/input.ts`) and Bob Costas's digest
 * (`bob-costas/summarizer.ts`), so both read Codex the same way.
 *
 *   {"type":"response_item","payload":{"type":"message","role":"user","content":[{"type":"input_text","text":"..."}]}}
 *   {"type":"response_item","payload":{"type":"message","role":"assistant","content":[{"type":"output_text","text":"..."}]}}
 *   {"type":"response_item","payload":{"type":"function_call","name":"shell","arguments":"..."}}
 *   {"type":"response_item","payload":{"type":"custom_tool_call","name":"exec","input":"..."}}
 *   {"type":"turn_context","payload":{"model":"...", ...}}
 *
 * Role `developer` messages are the harness's own instructions. `event_msg` records
 * repeat the same turns for the UI and are not read here.
 */

type Row = Record<string, unknown>;

const isObject = (value: unknown): value is Row =>
  value !== null && typeof value === "object" && !Array.isArray(value);

/** Text content part types across Claude (`text`) and Codex (`input_text`, `output_text`). */
export const TRANSCRIPT_TEXT_PART_TYPES: readonly string[] = ["text", "input_text", "output_text"];

/** A Codex message record's role and content, or null when the record is not one. */
export function codexMessage(record: Row): { role: unknown; content: unknown } | null {
  const payload = isObject(record.payload) ? record.payload : null;
  if (!payload || payload.type !== "message") return null;
  return { role: payload.role, content: payload.content };
}

/** The tool name of a Codex tool-call record (`function_call`, `custom_tool_call`), or null. */
export function codexToolCall(record: Row): string | null {
  if (record.type !== "response_item" || !isObject(record.payload)) return null;
  const { type, name } = record.payload;
  if (type !== "function_call" && type !== "custom_tool_call") return null;
  return typeof name === "string" && name ? name : null;
}

/** The model a Codex `turn_context` record names, or null. */
export function codexTurnModel(record: Row): string | null {
  if (record.type !== "turn_context" || !isObject(record.payload)) return null;
  const model = record.payload.model;
  return typeof model === "string" && model ? model : null;
}

/**
 * Codex injects context as user-role messages before the person's first prompt
 * (AGENTS.md instructions, environment context). They are not prompts.
 */
export function isCodexInjectedContext(text: string): boolean {
  const t = text.trimStart();
  return (
    t.startsWith("<environment_context>") ||
    t.startsWith("<user_instructions>") ||
    t.startsWith("# AGENTS.md instructions")
  );
}
