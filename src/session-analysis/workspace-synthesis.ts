/**
 * The offline workspace-synthesis job and its stored artifact (docs/session-intel-workspace-synthesis.md, "Storage").
 * Next to the per-session analysis results (results.ts), under its own prefix: one artifact per workspace and window.
 *
 *   {root}derived/workspace-synthesis-v1/ws=<workspace>/w=<sinceMs>-<untilMs>/g=<generatedAtMs, 13 digits>-<sha256>.json
 *
 * The per-session results live under each session's manifest directory (`normalized/session-analysis-v2/`); a
 * workspace aggregate has no session to anchor to, so it gets a workspace-level prefix of its own, outside the raw
 * `v1/` layout that raw-layout readers parse. Keys sort by generation, so the newest is last.
 *
 * The artifact names every session it considered (`sessions`), so a reader can refuse it when any of them has been
 * erased since (read API: GET /v1/session-intel/synthesis). Writes are create-only and read back byte for byte.
 * No clock, environment or network here: the store and `now` are injected.
 */
import { createHash } from "node:crypto";
import { isObjectStoreError, objectKey, readAll, type ObjectStore } from "../../packages/object-store/src/index.ts";
import { validateWorkspaceSynthesis } from "../session-intel/synthesis/schema.ts";
import {
  consideredSessions, SESSION_ID, SOURCE_ID, synthesize, WORKSPACE_ID,
  type SessionRef, type SynthesisOptions, type WorkspaceSynthesis,
} from "../session-intel/synthesis/synthesize.ts";

export const SYNTHESIS_ARTIFACT_FORMAT = "workspace-synthesis-artifact/1";
export const SYNTHESIS_PREFIX_SEGMENT = "derived/workspace-synthesis-v1/";
/** The artifact (synthesis plus its considered-session list); the synthesis inside is itself capped at 256 KiB. */
export const SYNTHESIS_ARTIFACT_MAX_BYTES = 1024 * 1024;
/** A lookup reads at most this many of the newest artifacts of one window before it gives up. */
export const SYNTHESIS_LOOKUP_MAX_CANDIDATES = 5;
/** A lookup lists at most this many keys of one window. */
export const SYNTHESIS_LOOKUP_MAX_KEYS = 1_000;

export class SynthesisArtifactError extends Error {
  constructor(readonly code: string) {
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
  /** Every session the synthesis considered (analyzed or skipped), sorted: the erase check's input. */
  sessions: SessionRef[];
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
const ROOT = /^(?:[A-Za-z0-9][A-Za-z0-9_.=-]*\/)*$/u;
const KEY_TAIL = /^g=(\d{13})-([0-9a-f]{64})\.json$/u;

/** The prefix of one workspace and window. `root` is "" or `/`-terminated plain segments. */
export function workspaceSynthesisPrefix(root: string, workspace: string, window: { since: string; until: string }): string {
  if (!ROOT.test(root)) throw new SynthesisArtifactError("synthesis_root_invalid");
  if (!WORKSPACE_ID.test(workspace) || workspace.includes("/")) throw new SynthesisArtifactError("synthesis_workspace_invalid");
  const since = Date.parse(window.since), until = Date.parse(window.until);
  if (!Number.isSafeInteger(since) || !Number.isSafeInteger(until) || since < 0 || since >= until) throw new SynthesisArtifactError("synthesis_window_invalid");
  return `${root}${SYNTHESIS_PREFIX_SEGMENT}ws=${workspace}/w=${since}-${until}/`;
}

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
  root: string;
  orgId: string;
  cohort: unknown;
  options: SynthesisOptions;
}): Promise<{ key: string; sha256: string; bytes: number; outcome: "created" | "exists"; synthesis: WorkspaceSynthesis }> {
  if (!/^[A-Za-z0-9][A-Za-z0-9_.:-]{0,511}$/u.test(input.orgId)) throw new SynthesisArtifactError("synthesis_org_invalid");
  const synthesis = synthesize(input.cohort, input.options);
  const artifact: SynthesisArtifact = {
    format: SYNTHESIS_ARTIFACT_FORMAT,
    orgId: input.orgId,
    workspace: synthesis.workspace,
    window: synthesis.window,
    generatedAt: synthesis.generatedAt,
    sessions: consideredSessions(input.cohort, input.options),
    synthesis,
  };
  const bytes = new TextEncoder().encode(artifactJson(artifact));
  if (bytes.length > SYNTHESIS_ARTIFACT_MAX_BYTES) throw new SynthesisArtifactError("synthesis_artifact_bound");
  const digest = sha256(bytes);
  const key = workspaceSynthesisKey(workspaceSynthesisPrefix(input.root, synthesis.workspace, synthesis.window), synthesis.generatedAt, digest);
  const outcome = await input.store.putNew(key, bytes, digest);
  const back = await input.store.get(key, SYNTHESIS_ARTIFACT_MAX_BYTES);
  if (!back || back.length !== bytes.length || sha256(back) !== digest) throw new SynthesisArtifactError("synthesis_write_verification_failed");
  return { key, sha256: digest, bytes: bytes.length, outcome, synthesis };
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
  if (keys !== "format,generatedAt,orgId,sessions,synthesis,window,workspace") return null;
  // Another tenant's artifact under this tenant's prefix is never served, and is loud.
  if (a.orgId !== expect.orgId) throw new SynthesisArtifactError("synthesis_cross_tenant");
  if (a.workspace !== expect.workspace || a.window?.since !== expect.window.since || a.window?.until !== expect.window.until) return null;
  if (Date.parse(a.generatedAt) !== expect.generatedAtMs) return null;
  if (!Array.isArray(a.sessions) || a.sessions.length > 500 || !a.sessions.every((r) => r && typeof r === "object" && Object.keys(r).length === 2 && SOURCE_ID.test(r.source) && SESSION_ID.test(r.sessionId))) return null;
  try {
    validateWorkspaceSynthesis(a.synthesis);
  } catch {
    return null;
  }
  const s = a.synthesis;
  if (s.workspace !== a.workspace || s.window.since !== a.window.since || s.window.until !== a.window.until || s.generatedAt !== a.generatedAt) return null;
  if (a.sessions.length !== s.inputs.considered) return null;
  // Every session a drilldown or an issue names must be one the erase check covers.
  const listed = new Set(a.sessions.map((r) => `${r.source}\u0000${r.sessionId}`));
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
  lookup: { root: string; orgId: string; workspace: string; window: { since: string; until: string } },
): Promise<SynthesisArtifact | null> {
  const prefix = workspaceSynthesisPrefix(lookup.root, lookup.workspace, lookup.window);
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
    if (a) return a;
  }
  return null;
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
        const bytes = await readAll(await store.get(objectKey(key)));
        if (bytes.length > maxBytes) throw new SynthesisArtifactError("synthesis_artifact_bound");
        return bytes;
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
