/**
 * Bob Costas summarisers. `extractive` runs with no model and is the default
 * (Khaliq's self-host decision: model calls off unless switched on). A model
 * summariser plugs in behind the same interface; its telemetry carries model
 * id and token counts only, never prompts or outputs.
 */
import { scrubText } from "../shared/vendor.ts";

export interface SessionDigest {
  harness: string;
  records: number;
  userTurns: string[];
  /** Total turns; userTurns retains only the first bounded prompt samples. */
  userTurnCount?: number;
  assistantTurns: number;
  toolCalls: Record<string, number>;
  models: string[];
  firstTs?: string;
  lastTs?: string;
}

export interface Summary {
  text: string;
  modelId: string;
  tokensIn: number;
  tokensOut: number;
}

export interface Summarizer {
  readonly id: string;
  summarize(kind: "short" | "long", digest: SessionDigest): Promise<Summary>;
}

/** Parses scrubbed JSONL transcript records into a digest. Harness-agnostic best effort. */
export function newDigest(harness: string): SessionDigest {
  return { harness, records: 0, userTurns: [], userTurnCount: 0, assistantTurns: 0, toolCalls: Object.create(null), models: [] };
}

/** Constant retained space: at most 10 prompts, 64 models and 128 tool names. */
export function addDigestRecord(d: SessionDigest, rec: Record<string, unknown>): void {
    const payload = rec.payload as Record<string, unknown> | undefined;
    // Native Codex response_item/event_msg and Claude message records.
    const msg = (rec.message ?? (rec.type === "response_item" ? payload : undefined)) as Record<string, unknown> | undefined;
    const ts = typeof rec.timestamp === "string" ? rec.timestamp : undefined;
    if (ts) {
      d.firstTs ??= ts;
      d.lastTs = ts;
    }
    const role = (msg?.role ?? rec.role ?? (payload?.type === "user_message" ? "user" : rec.type)) as string | undefined;
    const content = msg?.content ?? rec.content ?? (payload?.type === "user_message" ? payload.message : undefined);
    if (role === "user") {
      const text = textOf(content);
      if (text) {
        d.userTurnCount = (d.userTurnCount ?? d.userTurns.length) + 1;
        if (d.userTurns.length < 10) d.userTurns.push(text.slice(0, 2000));
      }
    } else if (role === "assistant") {
      d.assistantTurns += 1;
      const model = msg?.model ?? rec.model;
      if (typeof model === "string" && d.models.length < 64 && !d.models.includes(model)) d.models.push(model.slice(0, 128));
      if (Array.isArray(content))
        for (const part of content as Record<string, unknown>[])
          if (part?.type === "tool_use" && typeof part.name === "string") addTool(d, part.name);
    }
    if (rec.type === "tool_call" && typeof rec.name === "string") addTool(d, rec.name);
}

/** Shared mapping for authoritative canonical projections, including replay pages. */
export function addProjectedDigestRecord(d: SessionDigest, row: Record<string, unknown>): void {
  d.records++;
  addDigestRecord(d, {
    role: row.actor_role, type: row.kind, content: row.content,
    timestamp: row.ts, model: row.model,
    ...(row.kind === "tool_call" ? { name: (row.tool_call as Record<string, unknown> | null)?.name } : {}),
  });
}

function addTool(d: SessionDigest, name: string) {
  const bounded = name.slice(0, 128);
  if (Object.hasOwn(d.toolCalls, bounded) || Object.keys(d.toolCalls).length < 128)
    d.toolCalls[bounded] = (d.toolCalls[bounded] ?? 0) + 1;
}

export function digestRecords(harness: string, lines: Iterable<string>, d = newDigest(harness)): SessionDigest {
  for (const line of lines) {
    if (!line.trim()) continue;
    d.records += 1;
    let rec: Record<string, unknown>;
    try {
      rec = JSON.parse(line) as Record<string, unknown>;
    } catch {
      continue;
    }
    if (rec && typeof rec === "object" && !Array.isArray(rec)) addDigestRecord(d, rec);
  }
  return d;
}

function textOf(content: unknown): string | undefined {
  if (typeof content === "string") return content;
  if (Array.isArray(content)) {
    let out = "";
    for (const p of content as Record<string, unknown>[]) {
      if (["text", "input_text", "output_text"].includes(String(p?.type)) && typeof p.text === "string")
        out += (out ? " " : "") + p.text.slice(0, 2000 - out.length);
      if (out.length >= 2000) break;
    }
    return out || undefined;
  }
  return undefined;
}

const clip = (s: string, n: number) => (s.length > n ? `${s.slice(0, n - 1)}…` : s);

export const extractiveSummarizer: Summarizer = {
  id: "extractive/v1",
  async summarize(kind, d) {
    const first = d.userTurns[0] ? clip(d.userTurns[0].replace(/\s+/g, " ").trim(), 160) : "(no user prompt)";
    const tools = Object.entries(d.toolCalls).sort((a, b) => b[1] - a[1]);
    const toolLine = tools.length ? tools.map(([n, c]) => `${n}×${c}`).join(", ") : "none";
    if (kind === "short") {
      const text = `${d.harness}: "${first}" — ${d.userTurnCount ?? d.userTurns.length} user turns, ${d.assistantTurns} assistant turns, tools: ${toolLine}.`;
      return { text: scrubText(clip(text, 280)), modelId: this.id, tokensIn: 0, tokensOut: 0 };
    }
    const prompts = d.userTurns.slice(0, 10).map((t, i) => `${i + 1}. ${clip(t.replace(/\s+/g, " ").trim(), 200)}`).join("\n");
    const text = [
      `# Session summary (${d.harness})`,
      "",
      `- Records: ${d.records}`,
      `- User turns: ${d.userTurnCount ?? d.userTurns.length}; assistant turns: ${d.assistantTurns}`,
      `- Models: ${d.models.length ? d.models.join(", ") : "unknown"}`,
      `- Tools: ${toolLine}`,
      `- Span: ${d.firstTs ?? "?"} → ${d.lastTs ?? "?"}`,
      "",
      "## Prompts",
      prompts || "(none)",
    ].join("\n");
    return { text: scrubText(text), modelId: this.id, tokensIn: 0, tokensOut: 0 };
  },
};

/** Chosen by BOB_COSTAS_MODEL: `off` (default) is extractive; anything else is not wired yet. */
export function summarizerFromEnv(): Summarizer {
  const model = (process.env.BOB_COSTAS_MODEL ?? "off").trim();
  if (model === "" || model === "off" || model === "extractive") return extractiveSummarizer;
  throw new Error(`96bulls: BOB_COSTAS_MODEL=${model} is not wired; only the extractive summariser ships in the local stack`);
}
