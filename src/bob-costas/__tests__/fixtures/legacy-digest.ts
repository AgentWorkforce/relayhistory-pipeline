/**
 * FROZEN copy of the digest and extractive summariser as they were before the streaming
 * input (origin/main f905699c, src/bob-costas/summarizer.ts). The equivalence test holds
 * the new DigestBuilder (sampling off) to this output, byte for byte. Do not edit.
 */
import { scrubText } from "../../../shared/vendor.ts";
import {
  TRANSCRIPT_TEXT_PART_TYPES,
  codexMessage,
  codexToolCall,
  codexTurnModel,
  isCodexInjectedContext,
} from "../../../shared/codex-transcript.ts";
import {
  normalizeRelayhistoryRecords,
  parseRelayhistoryRecord,
  relayhistoryEventTime,
  type RelayhistoryStreamRecord,
} from "@rhc/lib/pipeline/relayhistory-transform.ts";

export interface SessionDigest {
  harness: string;
  records: number;
  userTurns: string[];
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

/** Who the summary is for: the model summariser's budget and caps key on it. Ids only. */
export interface SummaryContext {
  orgId: string;
  /** The logical session within the org (workspace, harness, session id). */
  sessionKey: string;
  traceId?: string;
}

export interface Summarizer {
  readonly id: string;
  summarize(kind: "short" | "long", digest: SessionDigest, ctx?: SummaryContext): Promise<Summary>;
}

/**
 * Parses scrubbed JSONL transcript records into a digest. Harness-agnostic best effort.
 * Lines are raw transcript records (Claude Code, Codex rollout) or delivery envelopes
 * (`writer.ts` liveLine); envelopes are settled as a set before they are counted.
 */
export function legacyDigestRecords(harness: string, lines: string[]): SessionDigest {
  const d: SessionDigest = { harness, records: 0, userTurns: [], assistantTurns: 0,
    // No prototype: a tool named "constructor" or "__proto__" is counted like any other.
    toolCalls: Object.create(null) as Record<string, number>, models: [] };
  const envelopes: RelayhistoryStreamRecord[] = [];
  for (const line of lines) {
    if (!line.trim()) continue;
    let rec: Record<string, unknown>;
    try {
      rec = JSON.parse(line) as Record<string, unknown>;
    } catch {
      d.records += 1;
      continue;
    }
    if (isDeliveryEnvelope(rec)) {
      // Counted in `records` once settled; a malformed envelope counts as a line, like bad JSON.
      try {
        envelopes.push(parseRelayhistoryRecord(line));
      } catch {
        d.records += 1;
      }
      continue;
    }
    d.records += 1;
    if (rec === null || typeof rec !== "object" || Array.isArray(rec)) continue;
    const ts = typeof rec.timestamp === "string" ? rec.timestamp : undefined;
    if (ts) {
      d.firstTs ??= ts;
      d.lastTs = ts;
    }
    if (rec.type === "response_item" || rec.type === "turn_context") {
      digestCodexRecord(d, rec);
      continue;
    }
    const msg = rec.message as Record<string, unknown> | undefined;
    const role = (msg?.role ?? rec.role ?? rec.type) as string | undefined;
    const content = msg?.content ?? rec.content;
    if (role === "user") {
      const text = textOf(content);
      if (text) d.userTurns.push(text);
    } else if (role === "assistant") {
      d.assistantTurns += 1;
      if (typeof msg?.model === "string" && !d.models.includes(msg.model)) d.models.push(msg.model);
      if (Array.isArray(content))
        for (const part of content as Record<string, unknown>[])
          if (part?.type === "tool_use" && typeof part.name === "string") d.toolCalls[part.name] = (d.toolCalls[part.name] ?? 0) + 1;
    }
  }
  if (envelopes.length) digestEnvelopes(d, envelopes);
  return d;
}

/**
 * A delivery envelope as the ingest writer stores it in a raw chunk (`session-ingest/writer.ts`
 * liveLine, the alleyoop feed's `envelope`): `{ kind, origin_id, record_id, revision, operation,
 * received_at, payload }`, payload `null` on a delete. Transcript lines carry none of
 * record_id/revision, and a Codex record's `payload` comes with `type`, never `kind`.
 */
function isDeliveryEnvelope(rec: unknown): boolean {
  if (rec === null || typeof rec !== "object" || Array.isArray(rec)) return false;
  const r = rec as Record<string, unknown>;
  return typeof r.kind === "string" && typeof r.record_id === "string" && r.revision !== undefined &&
    (r.payload === null || (typeof r.payload === "object" && !Array.isArray(r.payload)));
}

/**
 * The read path's settle (`normalizeRelayhistoryRecords`, what the projection and the search
 * feed fold with): the highest revision of each record wins, a delete drops it, survivors in
 * event-time order. That fold refuses inputs it cannot order (two different records at one
 * revision, one record from two origins with no acceptance seq); a digest must not fail the
 * summary for that, so it falls back to the highest revision per (kind, record_id), ties to the
 * later receipt, deletes dropped.
 */
function settleEnvelopes(records: RelayhistoryStreamRecord[]): RelayhistoryStreamRecord[] {
  try {
    return normalizeRelayhistoryRecords(records);
  } catch {
    const latest = new Map<string, RelayhistoryStreamRecord>();
    for (const record of records) {
      const key = JSON.stringify([record.kind, record.record_id]);
      const held = latest.get(key);
      const rev = BigInt(record.revision);
      if (!held || rev > BigInt(held.revision) ||
          (rev === BigInt(held.revision) && Date.parse(record.received_at) >= Date.parse(held.received_at)))
        latest.set(key, record);
    }
    return [...latest.values()]
      .filter((record) => record.operation === "upsert" && record.payload !== null)
      .sort((a, b) => relayhistoryEventTime(a) - relayhistoryEventTime(b));
  }
}

/**
 * Settled delivery records into the digest, as the projection reads them
 * (`relayhistory-projection.ts`): tool_call `name` is a tool; a session_event `text` row is a
 * turn by `role`, except control rows (`control_kind`, `is_meta`) and sidechain user rows
 * (subagent prompts), which the projection skips too; `model` from any record.
 */
function digestEnvelopes(d: SessionDigest, envelopes: RelayhistoryStreamRecord[]): void {
  const settled = settleEnvelopes(envelopes);
  d.records += settled.length;
  let first: number | undefined;
  let last: number | undefined;
  for (const record of settled) {
    const p = record.payload!;
    const ms = relayhistoryEventTime(record);
    if (Number.isFinite(ms)) {
      if (first === undefined || ms < first) first = ms;
      if (last === undefined || ms > last) last = ms;
    }
    if (typeof p.model === "string" && p.model && !d.models.includes(p.model)) d.models.push(p.model);
    if (record.kind === "tool_call") {
      if (typeof p.name === "string" && p.name.trim()) d.toolCalls[p.name] = (d.toolCalls[p.name] ?? 0) + 1;
      continue;
    }
    if (record.kind !== "session_event" || p.kind !== "text" || flagged(p.control_kind) || truthy(p.is_meta)) continue;
    if (p.role === "user") {
      if (truthy(p.is_sidechain)) continue;
      if (typeof p.text === "string" && p.text.trim()) d.userTurns.push(p.text);
    } else if (p.role === "assistant") {
      d.assistantTurns += 1;
    }
  }
  if (first !== undefined && (d.firstTs === undefined || first < Date.parse(d.firstTs))) d.firstTs = new Date(first).toISOString();
  if (last !== undefined && (d.lastTs === undefined || last > Date.parse(d.lastTs))) d.lastTs = new Date(last).toISOString();
}

/** The projection's `control_kind` test: any non-empty value. */
const flagged = (v: unknown) => v !== null && v !== undefined && String(v) !== "";
/** The projection's `is_meta` / `is_sidechain` test: 1 or true, as number, boolean or text. */
const truthy = (v: unknown) => ["1", "true"].includes(String(v ?? ""));

/**
 * A Codex rollout record (`shared/codex-transcript.ts`): user and assistant messages,
 * `function_call` and `custom_tool_call` tool names, and the model from `turn_context`.
 * Developer messages and the context Codex injects as user messages are not turns.
 */
function digestCodexRecord(d: SessionDigest, rec: Record<string, unknown>): void {
  const model = codexTurnModel(rec);
  if (model) {
    if (!d.models.includes(model)) d.models.push(model);
    return;
  }
  const tool = codexToolCall(rec);
  if (tool) {
    d.toolCalls[tool] = (d.toolCalls[tool] ?? 0) + 1;
    return;
  }
  const message = codexMessage(rec);
  if (message?.role === "user") {
    const text = textOf(message.content, TRANSCRIPT_TEXT_PART_TYPES);
    if (text?.trim() && !isCodexInjectedContext(text)) d.userTurns.push(text);
  } else if (message?.role === "assistant") {
    d.assistantTurns += 1;
  }
}

function textOf(content: unknown, partTypes: readonly string[] = ["text"]): string | undefined {
  if (typeof content === "string") return content;
  if (Array.isArray(content)) {
    const parts = (content as Record<string, unknown>[])
      .filter((p) => partTypes.includes(String(p?.type)) && typeof p.text === "string")
      .map((p) => p.text as string);
    return parts.length ? parts.join(" ") : undefined;
  }
  return undefined;
}

const clip = (s: string, n: number) => (s.length > n ? `${s.slice(0, n - 1)}…` : s);

export const legacyExtractiveSummarizer: Summarizer = {
  id: "extractive/v1",
  async summarize(kind, d) {
    const first = d.userTurns[0] ? clip(d.userTurns[0].replace(/\s+/g, " ").trim(), 160) : "(no user prompt)";
    const tools = Object.entries(d.toolCalls).sort((a, b) => b[1] - a[1]);
    const toolLine = tools.length ? tools.map(([n, c]) => `${n}×${c}`).join(", ") : "none";
    if (kind === "short") {
      const text = `${d.harness}: "${first}" — ${d.userTurns.length} user turns, ${d.assistantTurns} assistant turns, tools: ${toolLine}.`;
      return { text: scrubText(clip(text, 280)), modelId: this.id, tokensIn: 0, tokensOut: 0 };
    }
    const prompts = d.userTurns.slice(0, 10).map((t, i) => `${i + 1}. ${clip(t.replace(/\s+/g, " ").trim(), 200)}`).join("\n");
    const text = [
      `# Session summary (${d.harness})`,
      "",
      `- Records: ${d.records}`,
      `- User turns: ${d.userTurns.length}; assistant turns: ${d.assistantTurns}`,
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

// Frozen from src/bob-costas/model-summarizer.ts at the same commit (LIMITS, clean, buildPromptInput).
const LEGACY_LIMITS = {
  short: { turns: 5, turnChars: 400, outputChars: 280 },
  long: { turns: 20, turnChars: 800, outputChars: 4_000 },
} as const;

/** Scrub first, then bound: clipping first could cut a secret below a rule's match. */
const clean = (s: string, n: number) => {
  const scrubbed = scrubText(s).replace(/\s+/g, " ").trim();
  return scrubbed.length > n ? `${scrubbed.slice(0, n - 1)}…` : scrubbed;
};

/** The request input: digest fields only, every string scrubbed. */
export function legacyBuildPromptInput(kind: "short" | "long", d: SessionDigest): string {
  const lim = LEGACY_LIMITS[kind];
  const tools = Object.entries(d.toolCalls)
    .sort((a, b) => b[1] - a[1])
    .slice(0, 30)
    .map(([name, count]) => ({ name: clean(name, 80), count }));
  return JSON.stringify({
    harness: clean(d.harness, 64),
    records: d.records,
    user_turn_count: d.userTurns.length,
    assistant_turn_count: d.assistantTurns,
    user_turns: d.userTurns.slice(0, lim.turns).map((t) => clean(t, lim.turnChars)),
    tool_calls: tools,
    models: d.models.slice(0, 10).map((m) => clean(m, 128)),
    first_ts: d.firstTs ? clean(d.firstTs, 40) : null,
    last_ts: d.lastTs ? clean(d.lastTs, 40) : null,
  });
}
