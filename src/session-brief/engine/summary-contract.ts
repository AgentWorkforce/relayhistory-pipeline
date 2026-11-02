/** Browser/Worker-safe contract. No provider history or Node dependencies. */
export const SESSION_SUMMARY_VERSION = 1 as const;
export const SESSION_SUMMARY_PROMPT_VERSION = "session-summary-v2";
export const SESSION_SUMMARY_TIMEOUT_MS = 20_000;
export const SESSION_SUMMARY_MAX_MESSAGES = 4_000;
export const SESSION_SUMMARY_TEXT_BUDGET = 64_000;

export interface SessionSummaryInput {
  source: string;
  sessionId: string;
  messages: { role: "user" | "assistant"; text: string; truncated: boolean }[];
}

export interface SessionSummaryText {
  title: string;
  description: string;
}

export const SESSION_SUMMARY_JSON_SCHEMA = {
  type: "object",
  additionalProperties: false,
  required: ["title", "description"],
  properties: {
    title: { type: "string", maxLength: 90 },
    description: { type: "string", maxLength: 320 },
  },
} as const;

export function validateSummaryText(value: unknown): SessionSummaryText {
  if (
    !record(value) ||
    Object.keys(value).some((key) => !["title", "description"].includes(key))
  )
    throw new TypeError("Invalid summary output");
  for (const [key, max] of [
    ["title", 90],
    ["description", 320],
  ] as const) {
    if (
      typeof value[key] !== "string" ||
      !value[key].trim() ||
      value[key].length > max ||
      /[\r\n]/.test(value[key])
    )
      throw new TypeError(`Invalid summary ${key}`);
  }
  return {
    title: (value.title as string).trim(),
    description: (value.description as string).trim(),
  };
}

/** Every history source RelayHistory captures; a session from any of them is briefable. */
export const SESSION_SUMMARY_SOURCES: readonly string[] = [
  "codex",
  "claude",
  "cursor",
  "grok",
  "opencode",
  "muse",
  "devin",
  "capy",
];

export function validateSummaryInput(value: unknown): SessionSummaryInput {
  if (
    !record(value) ||
    Object.keys(value).some(
      (key) => !["source", "sessionId", "messages"].includes(key),
    ) ||
    typeof value.source !== "string" ||
    !SESSION_SUMMARY_SOURCES.includes(value.source) ||
    typeof value.sessionId !== "string" ||
    !value.sessionId.trim() ||
    value.sessionId.length > 512 ||
    !Array.isArray(value.messages) ||
    !value.messages.length ||
    value.messages.length > SESSION_SUMMARY_MAX_MESSAGES
  )
    throw new TypeError("Invalid summary input");
  let textSize = 0;
  for (const message of value.messages) {
    if (
      !record(message) ||
      Object.keys(message).some(
        (key) => !["role", "text", "truncated"].includes(key),
      ) ||
      !["user", "assistant"].includes(String(message.role)) ||
      typeof message.text !== "string" ||
      !message.text.trim() ||
      typeof message.truncated !== "boolean"
    )
      throw new TypeError("Invalid summary message");
    textSize += message.text.length;
  }
  if (textSize > SESSION_SUMMARY_TEXT_BUDGET)
    throw new TypeError("Summary text exceeds its budget");
  return value as unknown as SessionSummaryInput;
}

function record(value: unknown): value is Record<string, unknown> {
  return Boolean(value) && typeof value === "object" && !Array.isArray(value);
}
