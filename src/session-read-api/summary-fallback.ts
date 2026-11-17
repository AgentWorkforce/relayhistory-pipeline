/**
 * The Bob Costas summary behind GET /v1/sessions/:id/summary when the session has no
 * native brief (READ_API_SUMMARY_FALLBACK_ENABLED, exactly "true"; default off).
 *
 * Bob Costas (src/bob-costas) stores each summary only as an encrypted object,
 * `<session prefix>/summary/{short,long}.json.enc` (summary-objects.ts: AES-256-GCM under
 * the workspace key, associated data `<object key>\n<kind>\n<revision>`), and keeps a
 * pointer row in `sessions.session_summaries`. The read role (`bulls96_read_api`) has no
 * grant on that table, so this module does not read it: it needs no migration.
 *
 * Authority, in order, every step fail closed:
 *   1. The caller's route has already run `sessions.session_brief_summary_for` for the same
 *      key and got rows back (not a refusal): the tenant scope
 *      (session_directory_read_api_scope), the single-physical-alias and canonical-mapping
 *      checks and the managed-history refusal all passed.
 *   2. The session's live pointer rows are read as the read role inside the request's
 *      READ ONLY, tenant-bound transaction (the same table and RLS the timing and turns
 *      routes read the transcript through), `deleted_at IS NULL` only. No live row (erased,
 *      tombstoned or unknown) = no summary. More than one live physical row (one session
 *      uploaded from several machines) = no summary: each machine's summary covers only its
 *      own raw, and none is presented as the whole session.
 *   3. The row must name this tenant, this Worker's own bucket and provider, and a manifest
 *      key whose parsed location is this row's workspace, machine, harness and session, under
 *      a prefix allowed for the caller's org (prefixAllowed, as canonical reads). The object
 *      key is derived from that location (summaryObjectKey), never taken from a caller.
 *   4. The object opens only under the caller's workspace key and only at exactly the
 *      pointer's current revision: a summary built for an older revision, another kind,
 *      another key or another workspace is not served (it fails the AES-GCM tag). Its
 *      recorded source_content_sha256 must equal the pointer's content_sha256 (revisions
 *      restart on a re-upload). A text-free record (opt-out, quarantine, empty) is not
 *      served either.
 *
 * Nothing here logs; errors carry reason codes only, never ids, keys, paths or text.
 */
import type { SummaryKind, SummaryRecord, WorkspaceKeyring } from "../bob-costas/summary-objects.ts";
import { readSummaryObjectAt, summaryObjectKey } from "../bob-costas/summary-objects.ts";
import { prefixAllowed } from "../../packages/pipeline/src/lib/pipeline/adapter.ts";
import { isManifestRest } from "../../packages/object-store/src/index.ts";
import { parseSessionKey, type ObjectStore } from "../shared/vendor.ts";
import type { TenantRunner } from "./liveness.ts";

export type SummaryFallbackReason =
  | "not_configured"
  | "tenant_mismatch"
  | "pointer_invalid"
  | "parse"
  | "unavailable";

export class SummaryFallbackError extends Error {
  constructor(readonly reason: SummaryFallbackReason) {
    super(`summary fallback: ${reason}`);
    this.name = "SummaryFallbackError";
  }
}

export interface SummaryFallbackKey {
  orgId: string;
  workspaceId: string;
  source: string;
  sessionId: string;
}

/** A served Bob Costas summary; text is bounded and already scrubbed by its producer. */
export interface SessionSummaryReady {
  status: "ready";
  kind: SummaryKind;
  summary: string;
  model: string;
  /** The stored record's built_at. */
  generatedAt: string;
  /** The stored record's revision (equal to the live pointer's). */
  revision: number;
  /** Whether the RETURNED text was cut (the stored text was longer than SUMMARY_FALLBACK_MAX_CHARS[kind]). */
  truncated: boolean;
}

export interface SummaryFallback {
  /** null = no servable summary (missing, stale, text-free, multi-machine or erased). */
  read(key: SummaryFallbackKey, kind: SummaryKind): Promise<SessionSummaryReady | null>;
}

export interface SummaryPointerRow {
  org_id: string;
  workspace_id: string;
  harness: string;
  session_id: string;
  logical_session_id: string;
  machine_id: string;
  storage_provider: string;
  bucket: string;
  manifest_key: string;
  revision: number | string;
  /** The indexed raw's content SHA-256, lowercase hex (null when the row has none). */
  content_sha256: string | null;
}

/**
 * The live physical pointers of one logical session, as the read role (RLS binds the
 * tenant; every predicate binds it again). Same logical-id rule as
 * session_brief_summary_for: coalesce(canonical_session_id, session_id). LIMIT 2: one
 * row is servable, a second only proves there is more than one.
 */
export const SUMMARY_POINTERS_SQL = `SELECT p.org_id, p.workspace_id, p.harness, p.session_id,
       coalesce(p.canonical_session_id, p.session_id) AS logical_session_id,
       p.machine_id, p.storage_provider, p.bucket, p.manifest_key, p.revision,
       encode(p.content_sha256, 'hex') AS content_sha256
  FROM sessions.session_index AS p
 WHERE p.org_id = $1 AND p.workspace_id = $2 AND p.harness = $3
   AND coalesce(p.canonical_session_id, p.session_id) = $4
   AND p.deleted_at IS NULL AND p.manifest_key IS NOT NULL
 ORDER BY p.session_id, p.machine_id
 LIMIT 2`;

/**
 * Code-point bounds on served text. short = the brief ready variant's 320 (live consumers
 * parse it strictly); long = a documented higher bound. Longer text is cut on a word
 * boundary and flagged `truncated`, never served over the bound.
 */
export const SUMMARY_FALLBACK_MAX_CHARS: Readonly<Record<SummaryKind, number>> = { short: 320, long: 16_000 };
const MAX_MODEL_CHARS = 128;

/** `text` cut to at most `max` code points, on a word boundary when one is near, ending in an ellipsis. */
export function truncateOnWord(text: string, max: number): { text: string; truncated: boolean } {
  const points = [...text];
  if (points.length <= max) return { text, truncated: false };
  let cut = points.slice(0, max - 1).join("");
  const space = cut.search(/\s\S*$/u);
  // Prefer a word boundary unless it would discard most of the text.
  if (space > (max - 1) / 2) cut = cut.slice(0, space);
  return { text: `${cut.trimEnd()}\u2026`, truncated: true };
}

export interface SummaryFallbackDeps {
  run: TenantRunner;
  /** This deployment's raw store (the bucket canonical reads use); undefined = not configured. */
  raw: () => { store: ObjectStore; provider: string; bucket: string } | undefined;
  /** The summary keyring; rejects when no key material is configured. */
  keyring: () => Promise<WorkspaceKeyring>;
  /** PIPELINE_KEY_PREFIX / PIPELINE_ORG_FROM_PREFIX, as canonical reads check them. */
  prefix: { PIPELINE_KEY_PREFIX?: string; PIPELINE_ORG_FROM_PREFIX?: string };
}

/** The object key for one checked pointer row, or a refusal. Exported for tests. */
export function summaryKeyForPointer(
  row: SummaryPointerRow,
  scope: SummaryFallbackKey,
  raw: { provider: string; bucket: string },
  prefix: SummaryFallbackDeps["prefix"],
  kind: SummaryKind,
): { key: string; revision: number; contentSha256: string } {
  if (
    row.org_id !== scope.orgId ||
    row.workspace_id !== scope.workspaceId ||
    row.harness !== scope.source ||
    row.logical_session_id !== scope.sessionId
  )
    throw new SummaryFallbackError("tenant_mismatch");
  const revision = Number(row.revision);
  if (
    row.storage_provider !== raw.provider ||
    row.bucket !== raw.bucket ||
    typeof row.manifest_key !== "string" ||
    !Number.isSafeInteger(revision) ||
    revision < 1 ||
    typeof row.content_sha256 !== "string" ||
    !/^[0-9a-f]{64}$/.test(row.content_sha256)
  )
    throw new SummaryFallbackError("pointer_invalid");
  const at = parseSessionKey(row.manifest_key);
  if (
    !at ||
    !isManifestRest(at.rest) ||
    at.workspaceId !== row.workspace_id ||
    at.machineId !== row.machine_id ||
    at.harness !== row.harness ||
    at.sessionId !== row.session_id ||
    !prefixAllowed(prefix, at.prefix, scope.orgId)
  )
    throw new SummaryFallbackError("pointer_invalid");
  let key: string;
  try {
    key = String(
      summaryObjectKey(
        {
          ...(at.prefix ? { prefix: at.prefix } : {}),
          workspaceId: at.workspaceId,
          machineId: at.machineId,
          harness: at.harness,
          sessionId: at.sessionId,
          startDate: at.startDate,
        },
        kind,
      ),
    );
  } catch {
    throw new SummaryFallbackError("pointer_invalid");
  }
  return { key, revision, contentSha256: row.content_sha256 };
}

/** The served fields of a decrypted record, or null when it holds no servable text. */
export function servableSummary(
  record: SummaryRecord | null,
  kind: SummaryKind,
  revision: number,
  contentSha256: string,
): SessionSummaryReady | null {
  if (!record || record.status !== "ready") return null;
  // Bound to the exact raw the live pointer indexes, not only its revision: revisions restart
  // on a re-upload (an erased then re-uploaded session), so a leftover object at the same key
  // and revision but built from other content is never served.
  if (record.source_content_sha256 !== contentSha256) return null;
  const builtAt = new Date(record.built_at);
  if (
    record.kind !== kind ||
    record.revision !== revision ||
    typeof record.summary !== "string" ||
    !record.summary.trim() ||
    typeof record.built_at !== "string" ||
    !Number.isFinite(builtAt.getTime())
  )
    throw new SummaryFallbackError("parse");
  // The model is the one Bob Costas recorded inside the encrypted record with the text
  // (SummaryRecord.model, the summariser's model id). Never a configured default: a ready
  // record without a usable recorded model is not served.
  const model = record.model;
  if (typeof model !== "string" || !model.trim() || model.length > MAX_MODEL_CHARS) return null;
  const cut = truncateOnWord(record.summary.trim(), SUMMARY_FALLBACK_MAX_CHARS[kind]);
  return {
    status: "ready",
    kind,
    summary: cut.text,
    model,
    generatedAt: builtAt.toISOString(),
    revision,
    truncated: cut.truncated,
  };
}

export function pointerSummaryFallback(deps: SummaryFallbackDeps): SummaryFallback {
  return {
    async read(scope, kind) {
      const raw = deps.raw();
      if (!raw) throw new SummaryFallbackError("not_configured");
      let keyring: WorkspaceKeyring;
      try {
        keyring = await deps.keyring();
      } catch {
        throw new SummaryFallbackError("not_configured");
      }
      const rows = await deps.run(scope.orgId, scope.workspaceId, async (q) =>
        (await q.query<SummaryPointerRow>(SUMMARY_POINTERS_SQL, [scope.orgId, scope.workspaceId, scope.source, scope.sessionId])).rows,
      );
      // Any row outside the scope is a tenant failure, whatever the count.
      for (const row of rows)
        if (row.org_id !== scope.orgId || row.workspace_id !== scope.workspaceId)
          throw new SummaryFallbackError("tenant_mismatch");
      if (rows.length !== 1) return null;
      const { key, revision, contentSha256 } = summaryKeyForPointer(rows[0]!, scope, raw, deps.prefix, kind);
      let record: SummaryRecord | null;
      try {
        record = await readSummaryObjectAt({
          store: raw.store,
          keyring,
          ids: { orgId: scope.orgId, workspaceId: scope.workspaceId },
          key,
          kind,
          revision,
        });
      } catch {
        // A store, deadline or keyring failure (e.g. a key id this deployment does not hold).
        throw new SummaryFallbackError("unavailable");
      }
      return servableSummary(record, kind, revision, contentSha256);
    },
  };
}

/**
 * The route body for a served Bob Costas summary (docs/session-read-api-design.md). The inner
 * `summary` object has exactly the brief ready variant's keys (live consumers parse it with a
 * strict object schema); provenance is outside it.
 */
export function sessionSummaryBody(ready: SessionSummaryReady) {
  return {
    summary: {
      status: "ready" as const,
      kind: ready.kind,
      summary: ready.summary,
      model: ready.model,
      builtAt: ready.generatedAt,
      state: null,
    },
    provenance: {
      source: "session_summary" as const,
      kind: ready.kind,
      generatedAt: ready.generatedAt,
      revision: ready.revision,
      truncated: ready.truncated,
    },
  };
}
