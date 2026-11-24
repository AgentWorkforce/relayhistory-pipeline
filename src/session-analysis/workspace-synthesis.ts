/**
 * The offline workspace-synthesis job and its stored artifact (docs/session-intel-workspace-synthesis.md, "Storage").
 * Next to the per-session analysis results (results.ts), in the same analysis-result object family, one artifact per
 * workspace and window:
 *
 *   {keyRoot}/{org}/v1/ws={workspace}/normalized/session-analysis-v2/workspace-synthesis-v1/w={sinceMs}-{untilMs}/g={generatedAtMs, 13 digits}-{sha256}.json
 *
 * The per-session results sit under each session's manifest directory (`.../m=/h=/d=/s=/normalized/session-analysis-v2/`).
 * A workspace aggregate has no session to anchor to, so it sits under the workspace directory instead, still inside
 * the bucket's analysis-result glob (stacks ANALYSIS_RESULT_OBJECT_GLOB: `{keyRoot}/{any}/v1/{any}/normalized/session-analysis-v2/{any}`), and never ends in
 * `/manifest.json` (so it is not notified) nor parses as a session key (so the pointer stage ignores it). The per-user
 * erase (src/session-erase/user-erase.ts userErasePrefixes) deletes the workspace's whole synthesis directory.
 * Keys sort by generation, so the newest is last.
 *
 * The artifact is an envelope around the unchanged workspace-synthesis/1 document: `considered` (every session it
 * considered) and `related` (every other session the analyzed results' evidence belongs to, children included), so
 * a reader refuses it when any of them has been erased (read API: GET /v1/session-intel/synthesis). Writes are
 * create-only and read back byte for byte.
 * No clock, environment or network here: the store and `now` are injected.
 */
import { createHash } from "node:crypto";
import { isObjectStoreError, objectKey, type ObjectStore } from "../../packages/object-store/src/index.ts";
import { validateWorkspaceSynthesis } from "../session-intel/synthesis/schema.ts";
import {
  analyzedSessions, SESSION_ID, SOURCE_ID, synthesisJson, synthesisProvenance, synthesize,
  type SessionRef, type SynthesisOptions, type WorkspaceSynthesis,
} from "../session-intel/synthesis/synthesize.ts";
import {
  runStage2, stage1Of, stage2FlagOn, STAGE2_PROMPT_VERSION_SLUG, Stage2ValidationError, SYNTHESIS_CONTRACT_V2, synthesisV2Json, validateStoredStage2,
  type Stage2Deps, type WorkspaceSynthesisV2,
} from "../session-intel/synthesis/stage2.ts";
import { ORG_SEGMENT, SYNTHESIS_PREFIX_SEGMENT, workspaceSynthesisDirectory, workspaceSynthesisPrefix } from "./workspace-synthesis-layout.ts";

export { SYNTHESIS_PREFIX_SEGMENT, workspaceSynthesisDirectory, workspaceSynthesisPrefix };

export const SYNTHESIS_ARTIFACT_FORMAT = "workspace-synthesis-artifact/1";
/** The envelope (synthesis plus its provenance lists); the synthesis inside is itself capped at 256 KiB. */
export const SYNTHESIS_ARTIFACT_MAX_BYTES = 4 * 1024 * 1024;
/** Related sessions one artifact may record; more refuses the job (never a silently partial erase check). */
export const SYNTHESIS_MAX_RELATED = 10_000;
/** A lookup reads at most this many of the newest artifacts of one window before it gives up. */
export const SYNTHESIS_LOOKUP_MAX_CANDIDATES = 5;
/** A lookup lists at most this many keys of one window. */
export const SYNTHESIS_LOOKUP_MAX_KEYS = 1_000;

export class SynthesisArtifactError extends Error {
  /**
   * `detail` (optional, additive) names the rule that failed, e.g. `envelope.window` or the stored-v2 rule
   * (`stage2: sessions: order`). Diagnostic only: it is never part of the message, and the read route serves `code`.
   */
  constructor(readonly code: string, readonly detail?: string) {
    super(code);
    this.name = "SynthesisArtifactError";
  }
}

export interface SynthesisArtifact {
  format: typeof SYNTHESIS_ARTIFACT_FORMAT;
  orgId: string;
  workspace: string;
  window: { since: string; until: string };
  generatedAt: string;
  /** Every session the synthesis considered (analyzed or skipped), sorted. */
  considered: SessionRef[];
  /** Every other session the analyzed results' evidence belongs to (children included), sorted. */
  related: SessionRef[];
  /** The workspace-synthesis/1 document, unchanged; the only part the route serves. */
  synthesis: WorkspaceSynthesis;
}

/** The narrow store the job and the reader need. `objectStoreArtifacts` adapts an ObjectStore; tests use memory. */
export interface SynthesisArtifactStore {
  /** Keys under `prefix`, at most `max` (more is an error, never a silent cut). */
  list(prefix: string, max: number): Promise<string[]>;
  /** The bytes, or undefined when absent. More than `maxBytes` is an error. */
  get(key: string, maxBytes: number): Promise<Uint8Array | undefined>;
  /** Create only; identical bytes already there is `exists`. */
  putNew(key: string, bytes: Uint8Array, sha256: string): Promise<"created" | "exists">;
}

const sha256 = (bytes: Uint8Array) => createHash("sha256").update(bytes).digest("hex");
const KEY_TAIL = /^g=(\d{13})-([0-9a-f]{64})\.json$/u;
export function workspaceSynthesisKey(prefix: string, generatedAt: string, digest: string): string {
  const ms = Date.parse(generatedAt);
  if (!Number.isSafeInteger(ms) || ms < 0 || ms > 9_999_999_999_999 || !/^[0-9a-f]{64}$/u.test(digest)) throw new SynthesisArtifactError("synthesis_key_invalid");
  return `${prefix}g=${String(ms).padStart(13, "0")}-${digest}.json`;
}

export const artifactJson = (a: SynthesisArtifact) => `${JSON.stringify(a)}\n`;

/**
 * The offline job: synthesize the cohort, then write one artifact for its workspace and window. Same cohort and
 * same `now` give the same bytes and key, so a rerun is `exists`, never a second artifact.
 */
export async function runWorkspaceSynthesisJob(input: {
  store: SynthesisArtifactStore;
  keyRoot: string;
  orgId: string;
  cohort: unknown;
  options: SynthesisOptions;
  /** Stage 2, default off: it runs only when `flag` (WORKSPACE_SYNTHESIS_STAGE2_ENABLED) is exactly `true`. */
  stage2?: Stage2JobOptions;
}): Promise<{ key: string; sha256: string; bytes: number; outcome: "created" | "exists"; synthesis: WorkspaceSynthesis; stage2?: Stage2JobResult }> {
  if (!ORG_SEGMENT.test(input.orgId) || input.orgId.startsWith(".")) throw new SynthesisArtifactError("synthesis_org_invalid");
  const synthesis = synthesize(input.cohort, input.options);
  const provenance = synthesisProvenance(input.cohort, input.options);
  if (provenance.related.length > SYNTHESIS_MAX_RELATED) throw new SynthesisArtifactError("synthesis_related_bound");
  const artifact: SynthesisArtifact = {
    format: SYNTHESIS_ARTIFACT_FORMAT,
    orgId: input.orgId,
    workspace: synthesis.workspace,
    window: synthesis.window,
    generatedAt: synthesis.generatedAt,
    considered: provenance.considered,
    related: provenance.related,
    synthesis,
  };
  const bytes = new TextEncoder().encode(artifactJson(artifact));
  if (bytes.length > SYNTHESIS_ARTIFACT_MAX_BYTES) throw new SynthesisArtifactError("synthesis_artifact_bound");
  const digest = sha256(bytes);
  const key = workspaceSynthesisKey(workspaceSynthesisPrefix(input.keyRoot, input.orgId, synthesis.workspace, synthesis.window), synthesis.generatedAt, digest);
  const outcome = await input.store.putNew(key, bytes, digest);
  const back = await input.store.get(key, SYNTHESIS_ARTIFACT_MAX_BYTES);
  if (!back || back.length !== bytes.length || sha256(back) !== digest) throw new SynthesisArtifactError("synthesis_write_verification_failed");
  const v1 = { key, sha256: digest, bytes: bytes.length, outcome, synthesis };
  // Flag off (the default): exactly the Stage 1 job, nothing else read, called or written.
  if (!stage2FlagOn(input.stage2?.flag)) return v1;
  return { ...v1, stage2: await runStage2Step(input, artifact, key, digest) };
}

export const SYNTHESIS_ARTIFACT_FORMAT_V2 = "workspace-synthesis-artifact/2";
/**
 * The v2 envelope: the v1 envelope's fields, `analyzed` (the Stage 1 cohort's analyzed sessions, sorted: the set the
 * kept sessions must come from; the v1 drilldowns can be cut, so they are not an exact list), `v1Sha256` (the Stage 1
 * artifact's digest, binding it to those bytes),
 * `stage2Sha256` (sha256 of the v2 document's two-space JSON, verified on every read) and the
 * workspace-synthesis/2 document as `synthesis`. Served behind READ_API_WORKSPACE_SYNTHESIS_V2_ENABLED (latestWorkspaceSynthesisV2).
 */
export interface SynthesisArtifactV2 extends Omit<SynthesisArtifact, "format" | "synthesis"> {
  format: typeof SYNTHESIS_ARTIFACT_FORMAT_V2;
  /** The Stage 1 cohort's analyzed sessions, sorted by (source, sessionId); a subset of `considered`. */
  analyzed: SessionRef[];
  v1Sha256: string;
  stage2Sha256: string;
  synthesis: WorkspaceSynthesisV2;
}
const V2_KEYS = "analyzed,considered,format,generatedAt,orgId,related,stage2Sha256,synthesis,v1Sha256,window,workspace";

export interface Stage2JobOptions extends Omit<Stage2Deps, "enabled"> {
  /** The raw WORKSPACE_SYNTHESIS_STAGE2_ENABLED value; the caller reads it, the job reads no environment. */
  flag: string | undefined;
}
/**
 * `created`: this run wrote the v2 artifact; `exists`: one was already at the key (a rerun, or the winner of a race),
 * its document returned and no call (or no second write) made; `not_written`: stage2 is null; `failed`: the step
 * failed with `code` (the v1 artifact is written and returned regardless).
 */
export type Stage2JobResult =
  | { outcome: "created" | "exists" | "not_written"; key: string; document: WorkspaceSynthesisV2; called: boolean; usage: { inputTokens: number; outputTokens: number } | null }
  | { outcome: "failed"; key: string | null; code: string; called: boolean };

/**
 * The Stage 2 key of one Stage 1 artifact and prompt version: fixed, so a run computes it before the call.
 *   {workspace synthesis directory}stage2/w={sinceMs}-{untilMs}/{v1 file name without .json}.{prompt version slug}.json
 * Outside the window's v1 prefix (`.../w=.../`), so the v1 lookup never lists it; inside the workspace's synthesis
 * directory (the erase) and the analysis-result glob. One object per (Stage 1 artifact, prompt version).
 */
export function workspaceSynthesisStage2Key(keyRoot: string, orgId: string, workspace: string, window: { since: string; until: string }, v1Key: string): string {
  const v1Prefix = workspaceSynthesisPrefix(keyRoot, orgId, workspace, window);
  const name = v1Key.slice(v1Prefix.length);
  if (!v1Key.startsWith(v1Prefix) || !KEY_TAIL.test(name)) throw new SynthesisArtifactError("synthesis_key_invalid");
  return `${workspaceSynthesisDirectory(keyRoot, orgId, workspace)}stage2/w=${Date.parse(window.since)}-${Date.parse(window.until)}/${name.slice(0, -".json".length)}.${STAGE2_PROMPT_VERSION_SLUG}.json`;
}

const refKey = (r: { source: unknown; sessionId: unknown }) => `${r.source}\u0000${r.sessionId}`;
const v2Bytes = (a: SynthesisArtifactV2) => new TextEncoder().encode(`${JSON.stringify(a)}\n`);
const errorCode = (e: unknown) => (e instanceof SynthesisArtifactError ? e.code : "synthesis_stage2_failed");

async function runStage2Step(
  input: { store: SynthesisArtifactStore; keyRoot: string; orgId: string; cohort: unknown; options: SynthesisOptions; stage2?: Stage2JobOptions },
  v1Artifact: SynthesisArtifact,
  v1Key: string,
  v1Digest: string,
): Promise<Stage2JobResult> {
  let key: string | null = null;
  let called = false;
  // Once the v1 artifact is written the job returns it: a Stage 2 failure is reported here, never thrown.
  try {
    const { flag: _flag, ...deps } = input.stage2!;
    const k = workspaceSynthesisStage2Key(input.keyRoot, input.orgId, v1Artifact.workspace, v1Artifact.window, v1Key);
    key = k;
    const read = async () => {
      const bytes = await input.store.get(k, SYNTHESIS_ARTIFACT_MAX_BYTES);
      return bytes ? parseStage2Artifact(bytes, v1Artifact, v1Digest) : null;
    };
    // A rerun of the same Stage 1 artifact and prompt version finds it: no call.
    const prior = await read();
    if (prior) return { outcome: "exists", key: k, document: prior.synthesis, called: false, usage: null };
    const run = await runStage2({ v1: v1Artifact.synthesis, cohort: input.cohort, options: input.options, orgId: input.orgId, deps: { ...deps, enabled: true } });
    called = run.called;
    const base = { key: k, document: run.document, called: run.called, usage: run.usage };
    // Only a complete Stage 2 is stored; a null one is returned, so a later run (within the caps) may still try.
    if (run.document.stage2 === null) return { ...base, outcome: "not_written" };
    const { format: _f, synthesis: _s, ...envelope } = v1Artifact;
    const analyzed = analyzedSessions(input.cohort, input.options).map((a) => ({ source: a.source, sessionId: a.sessionId }))
      .sort((x, y) => (refKey(x) < refKey(y) ? -1 : refKey(x) > refKey(y) ? 1 : 0));
    const artifact: SynthesisArtifactV2 = {
      format: SYNTHESIS_ARTIFACT_FORMAT_V2, ...envelope, analyzed, v1Sha256: v1Digest,
      stage2Sha256: sha256(new TextEncoder().encode(synthesisV2Json(run.document))), synthesis: run.document,
    };
    const bytes = v2Bytes(artifact);
    if (bytes.length > SYNTHESIS_ARTIFACT_MAX_BYTES) throw new SynthesisArtifactError("synthesis_artifact_bound");
    // Pre-write self-check: the strict reader over our own bytes. It cannot fail for a runStage2 result (that parser
    // already enforced the same rules), so it is untested by design; it guards against the two drifting apart.
    parseStage2Artifact(bytes, v1Artifact, v1Digest);
    const digest = sha256(bytes);
    let outcome: "created" | "exists";
    try {
      outcome = await input.store.putNew(k, bytes, digest);
    } catch {
      outcome = "exists";
    }
    // Lost a race (or the put failed): the winner's object, if it is a valid one, is the answer.
    const back = await input.store.get(k, SYNTHESIS_ARTIFACT_MAX_BYTES);
    if (!back) throw new SynthesisArtifactError("synthesis_write_verification_failed");
    if (outcome === "created" || sha256(back) === digest) {
      if (back.length !== bytes.length || sha256(back) !== digest) throw new SynthesisArtifactError("synthesis_write_verification_failed");
      return { ...base, outcome: outcome === "created" ? "created" : "exists" };
    }
    const winner = parseStage2Artifact(back, v1Artifact, v1Digest);
    return { outcome: "exists", key: k, document: winner.synthesis, called: run.called, usage: run.usage };
  } catch (e) {
    return { outcome: "failed", key, code: errorCode(e), called };
  }
}

/**
 * The STRICT reader of a stored v2 artifact, checked against the Stage 1 artifact it extends (and its digest). Any
 * failure is a typed SynthesisArtifactError: `synthesis_cross_tenant` (another org's envelope), `synthesis_stage2_digest`
 * (stage2Sha256 or v1Sha256 does not match), `synthesis_stage2_stage1_mismatch` (the Stage 1 part is not the v1
 * document byte for byte), `synthesis_stage2_invalid` (anything else: bytes, envelope keys, the `analyzed` list, the
 * stage2 rules, the kept sessions checked against the ANALYZED set). `detail` names the rule that failed.
 */
export function parseStage2Artifact(bytes: Uint8Array, v1: SynthesisArtifact, v1Digest: string): SynthesisArtifactV2 {
  const invalid = (detail: string) => new SynthesisArtifactError("synthesis_stage2_invalid", detail);
  let a: SynthesisArtifactV2;
  try {
    a = JSON.parse(new TextDecoder("utf-8", { fatal: true, ignoreBOM: false }).decode(bytes)) as SynthesisArtifactV2;
  } catch {
    throw invalid("bytes");
  }
  if (!a || typeof a !== "object" || Array.isArray(a)) throw invalid("envelope: not an object");
  if (Object.keys(a).sort().join(",") !== V2_KEYS) throw invalid("envelope: keys");
  if (a.format !== SYNTHESIS_ARTIFACT_FORMAT_V2) throw invalid("envelope.format");
  if (a.orgId !== v1.orgId) throw new SynthesisArtifactError("synthesis_cross_tenant", "envelope.orgId");
  const same = (x: unknown, y: unknown) => JSON.stringify(x) === JSON.stringify(y);
  if (a.workspace !== v1.workspace) throw invalid("envelope.workspace");
  if (!same(a.window, v1.window)) throw invalid("envelope.window");
  if (a.generatedAt !== v1.generatedAt) throw invalid("envelope.generatedAt");
  if (!same(a.considered, v1.considered)) throw invalid("envelope.considered");
  if (!same(a.related, v1.related)) throw invalid("envelope.related");
  if (a.v1Sha256 !== v1Digest) throw new SynthesisArtifactError("synthesis_stage2_digest", "v1Sha256");
  const d = a.synthesis;
  if (!d || typeof d !== "object" || Array.isArray(d)) throw invalid("synthesis: not an object");
  if (d.contract !== SYNTHESIS_CONTRACT_V2) throw invalid("synthesis.contract");
  if (d.stage2Reason !== null) throw invalid("synthesis.stage2Reason");
  if (typeof a.stage2Sha256 !== "string" || a.stage2Sha256 !== sha256(new TextEncoder().encode(synthesisV2Json(d)))) throw new SynthesisArtifactError("synthesis_stage2_digest", "stage2Sha256");
  let stage1: string;
  try {
    stage1 = synthesisJson(stage1Of(d));
  } catch {
    throw invalid("synthesis: stage1");
  }
  if (stage1 !== synthesisJson(v1.synthesis)) throw new SynthesisArtifactError("synthesis_stage2_stage1_mismatch", "synthesis: stage1");
  const analyzed = analyzedSet(a.analyzed, v1);
  try {
    validateStoredStage2(d.stage2, analyzed, v1.synthesis.inputs.analyzed);
  } catch (e) {
    throw invalid(`stage2: ${e instanceof Stage2ValidationError ? e.where : "unknown"}`);
  }
  return a;
}

/**
 * The envelope's `analyzed` list, checked against the v1 artifact: exact refs, strictly sorted, as many as
 * inputs.analyzed, each among `considered`, and covering every session a category drilldown names; when no drilldown
 * was cut it must equal the drilldowns' union exactly (every analyzed session sits in exactly one category).
 */
function analyzedSet(list: unknown, v1: SynthesisArtifact): Set<string> {
  const invalid = (detail: string) => new SynthesisArtifactError("synthesis_stage2_invalid", `envelope.analyzed: ${detail}`);
  if (!Array.isArray(list) || list.length !== v1.synthesis.inputs.analyzed) throw invalid("count");
  const considered = new Set(v1.considered.map(refKey));
  const out = new Set<string>();
  let prev: string | null = null;
  for (const r of list as unknown[]) {
    if (!r || typeof r !== "object" || Array.isArray(r) || Object.keys(r).sort().join(",") !== "sessionId,source") throw invalid("ref");
    const { source, sessionId } = r as Record<string, unknown>;
    if (typeof source !== "string" || !SOURCE_ID.test(source) || typeof sessionId !== "string" || !SESSION_ID.test(sessionId)) throw invalid("ref");
    const k = refKey({ source, sessionId });
    if (prev !== null && !(prev < k)) throw invalid("order");
    if (!considered.has(k)) throw invalid("not considered");
    prev = k;
    out.add(k);
  }
  const drilled = new Set(v1.synthesis.categories.flatMap((c) => c.sessions.map(refKey)));
  for (const k of drilled) if (!out.has(k)) throw invalid("misses a drilldown session");
  if (!v1.synthesis.inputs.caps.drilldown.truncated && drilled.size !== out.size) throw invalid("not the drilldowns");
  return out;
}

/** Every session the synthesis names in a drilldown, an issue or its evidence. */
export function namedSessions(s: WorkspaceSynthesis): SessionRef[] {
  const out = new Map<string, SessionRef>();
  const add = (r: SessionRef) => out.set(`${r.source}\u0000${r.sessionId}`, { source: r.source, sessionId: r.sessionId });
  for (const c of s.categories) c.sessions.forEach(add);
  for (const i of s.issues) { i.sessions.forEach(add); i.evidence.forEach(add); }
  return [...out.values()];
}

/** Parse and check one stored artifact against the lookup; null when it is not a complete, matching artifact. */
export function parseArtifact(bytes: Uint8Array, expect: { orgId: string; workspace: string; window: { since: string; until: string }; generatedAtMs: number; sha256: string }): SynthesisArtifact | null {
  if (sha256(bytes) !== expect.sha256) return null;
  let a: SynthesisArtifact;
  try {
    a = JSON.parse(new TextDecoder("utf-8", { fatal: true, ignoreBOM: false }).decode(bytes)) as SynthesisArtifact;
  } catch {
    return null;
  }
  if (!a || typeof a !== "object" || a.format !== SYNTHESIS_ARTIFACT_FORMAT) return null;
  const keys = Object.keys(a).sort().join(",");
  if (keys !== "considered,format,generatedAt,orgId,related,synthesis,window,workspace") return null;
  // Another tenant's artifact under this tenant's prefix is never served, and is loud.
  if (a.orgId !== expect.orgId) throw new SynthesisArtifactError("synthesis_cross_tenant");
  if (a.workspace !== expect.workspace || a.window?.since !== expect.window.since || a.window?.until !== expect.window.until) return null;
  if (Date.parse(a.generatedAt) !== expect.generatedAtMs) return null;
  const refs = (list: unknown, max: number): list is SessionRef[] => Array.isArray(list) && list.length <= max
    && list.every((r) => r && typeof r === "object" && !Array.isArray(r) && Object.keys(r).length === 2
      && typeof r.source === "string" && typeof r.sessionId === "string" && SOURCE_ID.test(r.source) && SESSION_ID.test(r.sessionId));
  if (!refs(a.considered, 500) || !refs(a.related, SYNTHESIS_MAX_RELATED)) return null;
  try {
    validateWorkspaceSynthesis(a.synthesis);
  } catch {
    return null;
  }
  const s = a.synthesis;
  if (s.workspace !== a.workspace || s.window.since !== a.window.since || s.window.until !== a.window.until || s.generatedAt !== a.generatedAt) return null;
  if (a.considered.length !== s.inputs.considered) return null;
  // Every session a drilldown or an issue names must be one the erase check covers.
  const listed = new Set(a.considered.map((r) => `${r.source}\u0000${r.sessionId}`));
  if (!namedSessions(s).every((r) => listed.has(`${r.source}\u0000${r.sessionId}`))) return null;
  return a;
}

/**
 * The authoritative artifact of a lookup: the newest complete one for the exact workspace and window (newest by its
 * key's generation; an unreadable, altered or mismatched object is passed over, up to SYNTHESIS_LOOKUP_MAX_CANDIDATES).
 * Null when there is none. The caller still runs the erase check before serving it.
 */
export async function latestWorkspaceSynthesis(
  store: SynthesisArtifactStore,
  lookup: { keyRoot: string; orgId: string; workspace: string; window: { since: string; until: string } },
): Promise<SynthesisArtifact | null> {
  return (await latestWorkspaceSynthesisEntry(store, lookup))?.artifact ?? null;
}

/** latestWorkspaceSynthesis with the artifact's key and digest (the Stage 2 lookup needs both). */
export async function latestWorkspaceSynthesisEntry(
  store: SynthesisArtifactStore,
  lookup: { keyRoot: string; orgId: string; workspace: string; window: { since: string; until: string } },
): Promise<{ artifact: SynthesisArtifact; key: string; sha256: string } | null> {
  const prefix = workspaceSynthesisPrefix(lookup.keyRoot, lookup.orgId, lookup.workspace, lookup.window);
  const keys = (await store.list(prefix, SYNTHESIS_LOOKUP_MAX_KEYS))
    .filter((k) => k.startsWith(prefix) && KEY_TAIL.test(k.slice(prefix.length)))
    .sort()
    .reverse()
    .slice(0, SYNTHESIS_LOOKUP_MAX_CANDIDATES);
  for (const key of keys) {
    const [, ms, digest] = KEY_TAIL.exec(key.slice(prefix.length))!;
    const bytes = await store.get(key, SYNTHESIS_ARTIFACT_MAX_BYTES);
    if (!bytes) continue;
    const a = parseArtifact(bytes, { orgId: lookup.orgId, workspace: lookup.workspace, window: lookup.window, generatedAtMs: Number(ms), sha256: digest! });
    if (a) return { artifact: a, key, sha256: digest! };
  }
  return null;
}

/**
 * The Stage 2 lookup: the authoritative (newest complete) Stage 1 artifact of the window, and the v2 artifact stored
 * for exactly that artifact and the current prompt version (workspaceSynthesisStage2Key), read through the strict
 * reader (parseStage2Artifact). Null when there is no Stage 1 artifact; `v2: null` when that artifact has no v2 (an
 * older generation's v2 is never served in its place). A stored v2 that does not verify throws its typed
 * SynthesisArtifactError (never a silent skip). The caller still runs the erase check before serving it.
 */
export async function latestWorkspaceSynthesisV2(
  store: SynthesisArtifactStore,
  lookup: { keyRoot: string; orgId: string; workspace: string; window: { since: string; until: string } },
): Promise<{ v1: SynthesisArtifact; v2: SynthesisArtifactV2 | null } | null> {
  const entry = await latestWorkspaceSynthesisEntry(store, lookup);
  if (!entry) return null;
  const key = workspaceSynthesisStage2Key(lookup.keyRoot, lookup.orgId, entry.artifact.workspace, entry.artifact.window, entry.key);
  const bytes = await store.get(key, SYNTHESIS_ARTIFACT_MAX_BYTES);
  if (!bytes) return { v1: entry.artifact, v2: null };
  return { v1: entry.artifact, v2: parseStage2Artifact(bytes, entry.artifact, entry.sha256) };
}

/** The body, refused as soon as it passes `maxBytes` (the reader is cancelled; the rest is never read). */
export async function readBounded(stream: ReadableStream<Uint8Array>, maxBytes: number): Promise<Uint8Array> {
  const reader = stream.getReader();
  const chunks: Uint8Array[] = [];
  let length = 0;
  for (;;) {
    const { done, value } = await reader.read();
    if (done) break;
    length += value.length;
    if (length > maxBytes) {
      await reader.cancel().catch(() => {});
      throw new SynthesisArtifactError("synthesis_artifact_bound");
    }
    chunks.push(value);
  }
  const out = new Uint8Array(length);
  let offset = 0;
  for (const c of chunks) { out.set(c, offset); offset += c.length; }
  return out;
}

/** An ObjectStore as a SynthesisArtifactStore: paged list, bounded get, create-only put with the digest checked. */
export function objectStoreArtifacts(store: ObjectStore): SynthesisArtifactStore {
  return {
    async list(prefix, max) {
      const keys: string[] = [];
      let cursor: string | undefined;
      do {
        // An object key has no trailing `/`; the listing is on a `/` boundary, and only keys under `prefix` count.
        const page = await store.list(objectKey(prefix.replace(/\/$/u, "")), cursor);
        for (const item of page.items) if (String(item.key).startsWith(prefix)) keys.push(String(item.key));
        if (keys.length > max) throw new SynthesisArtifactError("synthesis_list_bound");
        cursor = page.nextCursor;
      } while (cursor);
      return keys;
    },
    async get(key, maxBytes) {
      const head = await store.head(objectKey(key));
      if (!head) return undefined;
      if (head.size > maxBytes) throw new SynthesisArtifactError("synthesis_artifact_bound");
      try {
        return await readBounded(await store.get(objectKey(key)), maxBytes);
      } catch (error) {
        if (isObjectStoreError(error, "not_found")) return undefined;
        throw error;
      }
    },
    async putNew(key, bytes, digest) {
      const ack = await store.put(objectKey(key), bytes, { ifNoneMatch: true, contentSha256: digest, contentType: "application/json" });
      if (ack.size !== bytes.length || ack.contentSha256 !== digest) throw new SynthesisArtifactError("synthesis_write_ack_invalid");
      return ack.created ? "created" : "exists";
    },
  };
}

/** An in-memory SynthesisArtifactStore (tests, local runs). */
export function memoryArtifactStore(): SynthesisArtifactStore & { objects: Map<string, Uint8Array> } {
  const objects = new Map<string, Uint8Array>();
  return {
    objects,
    async list(prefix, max) {
      const keys = [...objects.keys()].filter((k) => k.startsWith(prefix)).sort();
      if (keys.length > max) throw new SynthesisArtifactError("synthesis_list_bound");
      return keys;
    },
    async get(key, maxBytes) {
      const b = objects.get(key);
      if (b && b.length > maxBytes) throw new SynthesisArtifactError("synthesis_artifact_bound");
      return b ? new Uint8Array(b) : undefined;
    },
    async putNew(key, bytes, digest) {
      const prior = objects.get(key);
      if (prior) {
        if (sha256(prior) !== digest) throw new SynthesisArtifactError("synthesis_key_conflict");
        return "exists";
      }
      objects.set(key, new Uint8Array(bytes));
      return "created";
    },
  };
}
