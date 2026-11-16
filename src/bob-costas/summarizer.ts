/**
 * Bob Costas summarisers. `extractive` runs with no model and is the default
 * (Khaliq's self-host decision: model calls off unless switched on). The model
 * summariser (`model-summarizer.ts`, chosen by BOB_COSTAS_MODEL there in
 * `summarizerFromEnv`) plugs in behind the same interface; its telemetry carries
 * model id and token counts only, never prompts or outputs.
 */
import { createHash } from "node:crypto";
import { scrubText } from "../shared/vendor.ts";
import {
  TRANSCRIPT_TEXT_PART_TYPES,
  codexMessage,
  codexToolCall,
  codexTurnModel,
  isCodexInjectedContext,
} from "../shared/codex-transcript.ts";
import {
  chooseRelayhistoryRevision,
  compareRelayhistoryOrder,
  parseRelayhistoryRecord,
  relayhistoryEventTime,
  relayhistoryNewestAcrossOrigins,
  relayhistoryRecordKey,
  relayhistoryRevisionContent,
  type RelayhistoryStreamRecord,
} from "@rhc/lib/pipeline/relayhistory-transform.ts";

export interface SessionDigest {
  harness: string;
  records: number;
  /**
   * The user turns, in session order. Every turn in full, unless the digest was built with
   * sampling on: then the first ones, the last ones and an evenly spaced sample between
   * (`userTurnsSampled`), each clipped to TURN_TEXT_CHARS.
   */
  userTurns: string[];
  /** Every user turn the digest read, including any not kept in `userTurns`. */
  userTurnCount: number;
  /** True when `userTurns` holds a sample of the `userTurnCount` turns (sampling on only). */
  userTurnsSampled?: boolean;
  assistantTurns: number;
  toolCalls: Record<string, number>;
  models: string[];
  firstTs?: string;
  lastTs?: string;
  /** Present only when a cap fired: the digest describes part of the session. */
  input?: DigestInputTruncation;
}

/**
 * The caps that fired and make the digest's counts (records, turns, tools) lower bounds for
 * the whole session: chunks not read, or tool names and models not tracked. Counts only,
 * never content. Turn sampling alone (`userTurnsSampled`) is not truncation: every turn
 * is still counted.
 */
export interface DigestInputTruncation {
  chunksTotal: number;
  chunksRead: number;
  /** Decompressed bytes read. */
  bytesRead: number;
  /** Tool names or models not tracked (past their caps). */
  namesDropped: number;
}

/** Sampling on: the user turns kept, the first, the last, and an evenly spaced sample between. */
export const RETAINED_HEAD_TURNS = 32;
export const RETAINED_MIDDLE_TURNS = 32;
export const RETAINED_TAIL_TURNS = 16;
export const RETAINED_USER_TURNS = RETAINED_HEAD_TURNS + RETAINED_MIDDLE_TURNS + RETAINED_TAIL_TURNS;
/**
 * Sampling on: a kept turn's text is scrubbed, its whitespace runs collapsed, and then
 * clipped to this many characters (`clipTurn`). The summarisers read at most 800 (model)
 * and 200 (extractive) characters of it.
 */
export const TURN_TEXT_CHARS = 8_192;
/** Sampling on: distinct tool names and models tracked; further names are not counted. */
export const MAX_TOOL_NAMES = 1_000;
export const MAX_MODELS = 64;
/** Sampling on: a model or tool name longer than this is not tracked. */
const MAX_NAME_CHARS = 1_024;
/** Sampling on: user text (characters) the held envelopes may carry in total; later turns count without text. */
export const ENVELOPE_TEXT_BUDGET_CHARS = 16 * 1024 * 1024;

export interface Summary {
  text: string;
  modelId: string;
  tokensIn: number;
  tokensOut: number;
  /** Set by the model summariser when it fell back to extractive text (a closed reason code). */
  fallbackReason?: string;
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

/** The model summariser's `clean` before its bound: scrub, then collapse whitespace runs. */
const scrubCollapse = (t: string) => scrubText(t).replace(/\s+/g, " ");
/**
 * The first `n` UTF-16 units of `s`, one fewer when the last kept unit would be a high
 * surrogate: a cut never splits a surrogate pair, which the scrubber refuses as
 * `scrub_input_not_well_formed` (dead-lettering the summary).
 */
export function cutAt(s: string, n: number): string {
  if (s.length <= n) return s;
  const code = n > 0 ? s.charCodeAt(n - 1) : 0;
  return s.slice(0, code >= 0xd800 && code <= 0xdbff ? n - 1 : n);
}
const bound = (t: string) => cutAt(t, TURN_TEXT_CHARS);
/** Rounds of scrub-and-collapse a turn may take to settle before it is given up. */
const SETTLE_ROUNDS = 8;

/**
 * A fresh string holding `s` scrubbed, whitespace collapsed and clipped to TURN_TEXT_CHARS,
 * and settled: the result is a fixed point of scrub-then-collapse, the very step the model
 * summariser's `clean` applies before it bounds to 400/800. So that step changes nothing
 * in it, and a value the 8,192 clip cuts (too short for its rule to match any more) stays
 * where the clip left it, thousands of characters past anything a summariser reads,
 * whatever a scrub redacted or a collapse glued together before it. Settling runs both
 * before the clip (a collapse can glue a value that a second scrub then matches) and after
 * it (a re-scrub of the clipped text must not shrink it). A turn that does not settle in
 * SETTLE_ROUNDS rounds, before the clip or after it, is dropped to "". The copy (Buffer
 * round trip) keeps a slice from holding its parent alive. `step` is the scrub-and-collapse
 * step (a parameter only so a test can force a turn that never settles).
 */
export function clipTurn(s: string, step: (t: string) => string = scrubCollapse): string {
  let t = step(s);
  let settled = false;
  for (let round = 0; round < SETTLE_ROUNDS; round++) {
    const next = step(t);
    if (next === t) {
      settled = true;
      break;
    }
    t = next;
  }
  if (!settled) return "";
  t = bound(t);
  for (let round = 0; round < SETTLE_ROUNDS; round++) {
    const next = step(t);
    if (next === t) return Buffer.from(t, "utf16le").toString("utf16le");
    t = bound(next);
  }
  return "";
}

/** Where the digest keeps user turns: all of them, or a bounded sample. */
interface TurnSink {
  count: number;
  add(text: string): void;
  turns(): string[];
}

class AllTurns implements TurnSink {
  private readonly kept: string[] = [];
  get count(): number {
    return this.kept.length;
  }
  add(text: string): void {
    this.kept.push(text);
  }
  turns(): string[] {
    return this.kept;
  }
}

/**
 * Keeps a bounded, order-preserving sample of a stream of user turns: the first
 * RETAINED_HEAD_TURNS, the last RETAINED_TAIL_TURNS, and between them a systematic
 * sample (every `stride`-th turn, the stride doubling whenever the sample outgrows
 * RETAINED_MIDDLE_TURNS), so the kept middle stays evenly spaced whatever the total.
 * Memory is O(RETAINED_USER_TURNS × TURN_TEXT_CHARS) for any number of turns.
 */
export class TurnSampler implements TurnSink {
  count = 0;
  private head: string[] = [];
  private middle: { i: number; text: string }[] = [];
  private tail: { i: number; text: string }[] = [];
  private stride = 1;

  add(text: string): void {
    const i = this.count++;
    const t = clipTurn(text);
    if (i < RETAINED_HEAD_TURNS) {
      this.head.push(t);
      return;
    }
    this.tail.push({ i, text: t });
    if (this.tail.length <= RETAINED_TAIL_TURNS) return;
    const out = this.tail.shift()!;
    if ((out.i - RETAINED_HEAD_TURNS) % this.stride !== 0) return;
    this.middle.push(out);
    if (this.middle.length > RETAINED_MIDDLE_TURNS) {
      this.stride *= 2;
      this.middle = this.middle.filter((e) => (e.i - RETAINED_HEAD_TURNS) % this.stride === 0);
    }
  }

  /** Counts a turn whose text was not kept (an envelope past the text budget). */
  skip(): void {
    this.count += 1;
  }

  turns(): string[] {
    return [...this.head, ...this.middle.map((e) => e.text), ...this.tail.map((e) => e.text)];
  }
}

export interface DigestBuilderOptions {
  /**
   * BOB_COSTAS_INPUT_SAMPLING. Off (the default): the digest is exactly the one the
   * whole-session read built (every turn in full, every tool and model). On: user turns
   * are sampled and clipped, tool names and models capped, envelope text budgeted.
   */
  sampling?: boolean;
}

/** A held envelope: its revision seat and settle metadata, the digest fields, never the full payload. */
interface HeldEnvelope extends RelayhistoryStreamRecord {
  /** SHA-256 of the full record's exact same-revision content (`relayhistoryRevisionContent`). */
  content: string;
}

/**
 * Builds a digest from scrubbed JSONL transcript lines fed one at a time, so a caller can
 * stream a session chunk by chunk and let each chunk go once fed. Harness-agnostic best
 * effort. Lines are raw transcript records (Claude Code, Codex rollout) or delivery
 * envelopes (`writer.ts` liveLine).
 *
 * Envelopes are settled as the read path settles them (`normalizeRelayhistoryRecords`),
 * folded as they arrive instead of held: one seat per record, every revision and tombstone
 * folded whatever the caps (a deleted or superseded revision is never shown). A seat keeps
 * the record's metadata, the SHA-256 of its exact content (so equal-revision copies
 * compare as the full records did) and the few payload fields the digest reads.
 */
export class DigestBuilder {
  private readonly d: SessionDigest;
  private readonly sampling: boolean;
  private readonly turns: TurnSink;
  /** The strict per-seat fold; null once it refused an input (then only the fallback holds). */
  private strict: Map<string, HeldEnvelope> | null = new Map();
  /** The fallback fold: highest revision per (kind, record_id), ties to the later receipt. */
  private readonly fallback = new Map<string, HeldEnvelope>();
  private sawEnvelopes = false;
  private envelopeTextChars = 0;
  textDropped = 0;
  namesDropped = 0;

  constructor(harness: string, options: DigestBuilderOptions = {}) {
    this.d = { harness, records: 0, userTurns: [], userTurnCount: 0, assistantTurns: 0,
      // No prototype: a tool named "constructor" or "__proto__" is counted like any other.
      toolCalls: Object.create(null) as Record<string, number>, models: [] };
    this.sampling = options.sampling === true;
    this.turns = this.sampling ? new TurnSampler() : new AllTurns();
  }

  /** True once a delivery envelope was folded: the session must be read whole. */
  get hasEnvelopes(): boolean {
    return this.sawEnvelopes;
  }

  addLine(line: string): void {
    const d = this.d;
    if (!line.trim()) return;
    let rec: Record<string, unknown>;
    try {
      rec = JSON.parse(line) as Record<string, unknown>;
    } catch {
      d.records += 1;
      return;
    }
    if (isDeliveryEnvelope(rec)) {
      // Counted in `records` once settled; a malformed envelope counts as a line, like bad JSON.
      let parsed: RelayhistoryStreamRecord;
      try {
        parsed = parseRelayhistoryRecord(line);
      } catch {
        d.records += 1;
        return;
      }
      this.fold(this.hold(parsed));
      return;
    }
    d.records += 1;
    if (rec === null || typeof rec !== "object" || Array.isArray(rec)) return;
    const ts = typeof rec.timestamp === "string" ? rec.timestamp : undefined;
    if (ts) {
      d.firstTs ??= ts;
      d.lastTs = ts;
    }
    if (rec.type === "response_item" || rec.type === "turn_context") {
      this.codexRecord(rec);
      return;
    }
    const msg = rec.message as Record<string, unknown> | undefined;
    const role = (msg?.role ?? rec.role ?? rec.type) as string | undefined;
    const content = msg?.content ?? rec.content;
    if (role === "user") {
      const text = textOf(content);
      if (text) this.turns.add(text);
    } else if (role === "assistant") {
      d.assistantTurns += 1;
      if (typeof msg?.model === "string") this.addModel(msg.model);
      if (Array.isArray(content))
        for (const part of content as Record<string, unknown>[])
          if (part?.type === "tool_use" && typeof part.name === "string") this.countTool(part.name);
    }
  }

  /**
   * A Codex rollout record (`shared/codex-transcript.ts`): user and assistant messages,
   * `function_call` and `custom_tool_call` tool names, and the model from `turn_context`.
   * Developer messages and the context Codex injects as user messages are not turns.
   */
  private codexRecord(rec: Record<string, unknown>): void {
    const model = codexTurnModel(rec);
    if (model) {
      this.addModel(model);
      return;
    }
    const tool = codexToolCall(rec);
    if (tool) {
      this.countTool(tool);
      return;
    }
    const message = codexMessage(rec);
    if (message?.role === "user") {
      const text = textOf(message.content, TRANSCRIPT_TEXT_PART_TYPES);
      if (text?.trim() && !isCodexInjectedContext(text)) this.turns.add(text);
    } else if (message?.role === "assistant") {
      this.d.assistantTurns += 1;
    }
  }

  private addModel(model: string): void {
    const d = this.d;
    if (d.models.includes(model)) return;
    if (this.sampling && (d.models.length >= MAX_MODELS || model.length > MAX_NAME_CHARS)) {
      this.namesDropped += 1;
      return;
    }
    d.models.push(model);
  }

  private countTool(name: string): void {
    const calls = this.d.toolCalls;
    if (calls[name] !== undefined) {
      calls[name] += 1;
      return;
    }
    if (this.sampling && (Object.keys(calls).length >= MAX_TOOL_NAMES || name.length > MAX_NAME_CHARS)) {
      this.namesDropped += 1;
      return;
    }
    calls[name] = 1;
  }

  /**
   * The envelope as held: its seat and settle metadata unchanged, the SHA-256 of its exact
   * content, and its payload cut to what the digest reads, each value bounded without
   * changing what the digest makes of it (`compactPayload`).
   */
  private hold(r: RelayhistoryStreamRecord): HeldEnvelope {
    this.sawEnvelopes = true;
    const content = createHash("sha256").update(relayhistoryRevisionContent(r)).digest("hex");
    return { ...r, payload: r.payload === null ? null : this.compactPayload(r.kind, r.payload), content };
  }

  private compactPayload(kind: string, p: Record<string, unknown>): Record<string, unknown> {
    const out: Record<string, unknown> = {};
    // relayhistoryEventTime's fields, in its order: only values it would accept.
    for (const f of EVENT_TIME_FIELDS) {
      const v = p[f];
      if ((typeof v === "number" && Number.isFinite(v) && v > 0) || (typeof v === "string" && /^\d{10,16}$/.test(v))) out[f] = v;
    }
    // Compared against short literals only.
    if (typeof p.kind === "string" && p.kind.length <= 64) out.kind = p.kind;
    if (typeof p.role === "string" && p.role.length <= 64) out.role = p.role;
    if (flagged(p.control_kind)) out.control_kind = true;
    if (truthy(p.is_meta)) out.is_meta = true;
    if (truthy(p.is_sidechain)) out.is_sidechain = true;
    // Sampling off: kept whole, so the digest stays byte-identical. Sampling on: a value past
    // MAX_NAME_CHARS is never tracked (addModel, countTool), so only that fact is held.
    if (typeof p.model === "string" && p.model) {
      if (this.sampling && p.model.length > MAX_NAME_CHARS) out.model_too_long = true;
      else out.model = p.model;
    }
    if (kind === "tool_call" && typeof p.name === "string") {
      if (this.sampling && p.name.length > MAX_NAME_CHARS) out.name_too_long = true;
      else out.name = p.name;
    }
    if (kind === "session_event" && p.kind === "text" && p.role === "user" && typeof p.text === "string") {
      if (!this.sampling) out.text = p.text;
      else if (p.text.trim()) {
        const text = clipTurn(p.text);
        if (this.envelopeTextChars + text.length <= ENVELOPE_TEXT_BUDGET_CHARS) {
          this.envelopeTextChars += text.length;
          out.text = text;
        } else {
          out.text_dropped = true;
        }
      }
    }
    return out;
  }

  /**
   * Folds one envelope into both settles, in arrival order, exactly as the whole-list
   * settle did: the strict fold (`normalizeRelayhistoryRecords`'s per-seat revision choice,
   * the content compared by hash) until it refuses an input, and the fallback fold always.
   */
  private fold(held: HeldEnvelope): void {
    if (this.strict) {
      const key = relayhistoryRecordKey(held);
      try {
        this.strict.set(key, chooseRelayhistoryRevision(this.strict.get(key), held, sameContent));
      } catch {
        this.strict = null;
      }
    }
    const key = JSON.stringify([held.kind, held.record_id]);
    const prior = this.fallback.get(key);
    const rev = BigInt(held.revision);
    if (!prior || rev > BigInt(prior.revision) ||
        (rev === BigInt(prior.revision) && Date.parse(held.received_at) >= Date.parse(prior.received_at)))
      this.fallback.set(key, held);
  }

  /**
   * The settled survivors: the strict fold's seats, newest across origins, deletes dropped,
   * in the fold's order; or, when either step refused, the fallback fold's, deletes dropped,
   * in event-time order.
   */
  private settled(): RelayhistoryStreamRecord[] {
    if (this.strict) {
      try {
        return relayhistoryNewestAcrossOrigins(this.strict.values())
          .filter((record) => record.operation === "upsert" && record.payload !== null)
          .sort((left, right) => compareRelayhistoryOrder(
            relayhistoryEventTime(left), left, relayhistoryEventTime(right), right));
      } catch {
        // Fall through to the fallback fold.
      }
    }
    return [...this.fallback.values()]
      .filter((record) => record.operation === "upsert" && record.payload !== null)
      .sort((a, b) => relayhistoryEventTime(a) - relayhistoryEventTime(b));
  }

  /**
   * Settled delivery records into the digest, as the projection reads them
   * (`relayhistory-projection.ts`): tool_call `name` is a tool; a session_event `text` row is
   * a turn by `role`, except control rows (`control_kind`, `is_meta`) and sidechain user rows
   * (subagent prompts), which the projection skips too; `model` from any record.
   */
  private digestEnvelopes(): void {
    const d = this.d;
    const settled = this.settled();
    this.strict = null;
    this.fallback.clear();
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
      if (typeof p.model === "string" && p.model) this.addModel(p.model);
      else if (p.model_too_long === true) this.namesDropped += 1;
      if (record.kind === "tool_call") {
        if (typeof p.name === "string" && p.name.trim()) this.countTool(p.name);
        else if (p.name_too_long === true) this.namesDropped += 1;
        continue;
      }
      if (record.kind !== "session_event" || p.kind !== "text" || flagged(p.control_kind) || truthy(p.is_meta)) continue;
      if (p.role === "user") {
        if (truthy(p.is_sidechain)) continue;
        if (typeof p.text === "string" && p.text.trim()) this.turns.add(p.text);
        else if (p.text_dropped === true) {
          // Sampling on, past the text budget: counted, not kept.
          (this.turns as TurnSampler).skip();
          this.textDropped += 1;
        }
      } else if (p.role === "assistant") {
        d.assistantTurns += 1;
      }
    }
    if (first !== undefined && (d.firstTs === undefined || first < Date.parse(d.firstTs))) d.firstTs = new Date(first).toISOString();
    if (last !== undefined && (d.lastTs === undefined || last > Date.parse(d.lastTs))) d.lastTs = new Date(last).toISOString();
  }

  finish(): SessionDigest {
    const d = this.d;
    if (this.sawEnvelopes) this.digestEnvelopes();
    d.userTurns = this.turns.turns();
    d.userTurnCount = this.turns.count;
    if (d.userTurns.length < d.userTurnCount || this.textDropped > 0) d.userTurnsSampled = true;
    return d;
  }
}

const sameContent = (left: HeldEnvelope, right: HeldEnvelope) => left.content === right.content;

const EVENT_TIME_FIELDS = [
  "ts_ms", "timestamp_ms", "created_ms", "spawned_at_ms", "updated_ms", "first_activity_ms", "last_activity_ms",
] as const;

/** Parses scrubbed JSONL transcript records into a digest: `DigestBuilder` over a list. */
export function digestRecords(harness: string, lines: Iterable<string>, options?: DigestBuilderOptions): SessionDigest {
  const builder = new DigestBuilder(harness, options);
  for (const line of lines) builder.addLine(line);
  return builder.finish();
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

/** The projection's `control_kind` test: any non-empty value. */
const flagged = (v: unknown) => v !== null && v !== undefined && String(v) !== "";
/** The projection's `is_meta` / `is_sidechain` test: 1 or true, as number, boolean or text. */
const truthy = (v: unknown) => ["1", "true"].includes(String(v ?? ""));

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

/** `s` bounded to `n` characters with an ellipsis; never splits a surrogate pair (`cutAt`). */
const clip = (s: string, n: number) => (s.length > n ? `${cutAt(s, n - 1)}…` : s);

/** The long summary's note on a capped digest: which part of the session it describes. */
function inputNote(input: DigestInputTruncation | undefined): string[] {
  if (!input) return [];
  return input.chunksRead < input.chunksTotal
    ? [`- Input: sampled ${input.chunksRead} of ${input.chunksTotal} chunks; counts are lower bounds`]
    : ["- Input: capped; counts are lower bounds"];
}

export const extractiveSummarizer: Summarizer = {
  id: "extractive/v1",
  async summarize(kind, d) {
    const first = d.userTurns[0] ? clip(d.userTurns[0].replace(/\s+/g, " ").trim(), 160) : "(no user prompt)";
    const tools = Object.entries(d.toolCalls).sort((a, b) => b[1] - a[1]);
    const toolLine = tools.length ? tools.map(([n, c]) => `${n}×${c}`).join(", ") : "none";
    if (kind === "short") {
      const text = `${d.harness}: "${first}" — ${d.userTurnCount} user turns, ${d.assistantTurns} assistant turns, tools: ${toolLine}.`;
      return { text: scrubText(clip(text, 280)), modelId: this.id, tokensIn: 0, tokensOut: 0 };
    }
    const prompts = d.userTurns.slice(0, 10).map((t, i) => `${i + 1}. ${clip(t.replace(/\s+/g, " ").trim(), 200)}`).join("\n");
    const text = [
      `# Session summary (${d.harness})`,
      "",
      `- Records: ${d.records}`,
      `- User turns: ${d.userTurnCount}; assistant turns: ${d.assistantTurns}`,
      `- Models: ${d.models.length ? d.models.join(", ") : "unknown"}`,
      `- Tools: ${toolLine}`,
      `- Span: ${d.firstTs ?? "?"} → ${d.lastTs ?? "?"}`,
      ...inputNote(d.input),
      "",
      "## Prompts",
      prompts || "(none)",
    ].join("\n");
    return { text: scrubText(text), modelId: this.id, tokensIn: 0, tokensOut: 0 };
  },
};
