/**
 * Guarded CREATE INDEX CONCURRENTLY prebuild for migration 20261105058000 (operator user erase indexes), so the
 * schema release can adopt its sixteen indexes on populated tables instead of refusing (55000). Workflow:
 * .github/workflows/index-prebuild-058000.yml; entry point scripts/index-prebuild-058000.ts; GitHub gate
 * scripts/index-prebuild-058000-github.ts; operator notes docs/packets/operator-user-erase-index-packet.md.
 *
 * The sixteen names, tables, definitions and their order are ADOPTABLE_INDEXES[step] (src/hoopdb/migration-sentinels.ts),
 * which a test keeps byte-identical to the migration. Nothing here holds index SQL of its own: the only DDL it can
 * emit is concurrentStatement(spec), the declared definition with CONCURRENTLY inserted after CREATE INDEX. It never
 * emits a blocking CREATE INDEX, never DROPs, never sets the blocking-build opt-in and never applies 058000 (it does
 * not import the migrator's runner or read the migration file).
 *
 * Modes:
 *   census    READ ONLY (READ COMMITTED, so the second read of the binding sees fresh rows), under the migrator's
 *             advisory lock taken SHARED and non-blocking. Per index: table present/empty, absent/exact/drifted,
 *             indisvalid/indisready/indislive, owner, schema, definition digest and the matching
 *             pg_stat_progress_create_index row, then an action: present, deferred (absent on an empty table: the
 *             migration builds it), prebuild (absent on a populated table) or refused (drifted, invalid, in progress,
 *             wrong owner, table missing, or a same-named index in another schema). The binding (live readiness marker
 *             generation and status, the ledger tail, prefix step count and prefix digest, whether 058000 is ledgered)
 *             is read before and after and must not move. A marker whose status is not `pass` (e.g. `pending`: readers
 *             non-ready, possibly a crashed migration's residue) is reported but makes the verdict blocked. Emits the
 *             census artifact with a descriptor digest over every field.
 *   prebuild  Dormant while INDEX_PREBUILD_ENABLED is false. Needs a census artifact under 24h old with verdict
 *             prebuild_required and the byte-exact typed confirmation built from it (indexPrebuildConfirmation).
 *             Holds the dedicated session advisory lock INDEX_PREBUILD_LOCK (try, never wait), re-censuses and
 *             refuses any drift from the artifact, then for each required index, one at a time, in declared order:
 *             re-inspects it (an already exact valid/ready/live index is resumed, nothing else is), runs one
 *             CREATE INDEX CONCURRENTLY in autocommit with a finite statement and lock budget, reads it back and
 *             records a per-index receipt. The marker status must be `pass` at the re-census and before every build.
 *             A statement whose outcome is unknown (no SQLSTATE, a connection-fatal SQLSTATE such as 57P01, or a
 *             session that fails right after the statement) is read back on a fresh session only, never on the
 *             failed one, and never retried; without a fresh readback the receipt records outcome_unknown and the run
 *             stops. An invalid, in-progress, drifted, wrong-owner or name-colliding object is refused and left in
 *             place: nothing is dropped automatically.
 *   readback  READ ONLY. Complete only when all sixteen are exact, valid, ready and live and no create-index
 *             progress row exists on their tables (normally after the release applies 058000, which creates the
 *             indexes on empty tables itself).
 *
 * Locks. Neither the census nor the prebuild ever waits on an advisory lock. A CONCURRENTLY build waits for every
 * transaction with an older snapshot, so a session waiting on a lock this run holds while the build runs would
 * deadlock against it; the prebuild therefore holds only its own dedicated lock (taken with pg_try_advisory_lock,
 * so no waiter exists) and takes the migrator lock shared and try-only inside each short census transaction. The
 * workflow's concurrency group serializes it with the schema release and the other guarded operators.
 *
 * Receipts and artifacts carry public schema names, booleans, digests, the owner role name, phases, SQLSTATE codes
 * and counts only; never a connection string, host, error message or row value.
 */
import { createHash } from "node:crypto";
import { ADOPTABLE_INDEXES, ADOPTABLE_INDEX_STATE_SQL, type AdoptableIndex } from "./migration-sentinels.ts";
import { admissionRenewalLedger } from "./ingest-admission-renewal-operator.ts";
import { MIGRATION_LEDGER } from "./migrate.ts";
import { ROLE_VALIDATION_NAME, ROLE_VALIDATION_TABLE } from "../shared/db.ts";

/**
 * Writes (prebuild) stay refused until a separate reviewed one-line change enables them. Before that change: a
 * guarded disposable/dev long-statement proof through the session pooler (or a proven direct owner endpoint), since
 * TCP keepalive alone cannot tell a pooler statement/lifetime cap from a dead peer, and a guarded read-only
 * census/readback run proving the protected-environment stdin handoff.
 */
export const INDEX_PREBUILD_ENABLED: boolean = false;

export const INDEX_PREBUILD_STEP = "upstream:20261105058000_operator_user_erase_indexes.sql";
export const INDEX_PREBUILD_COUNT = 16;
export const INDEX_PREBUILD_WORKFLOW = ".github/workflows/index-prebuild-058000.yml";
export const INDEX_PREBUILD_MODES = ["census", "prebuild", "readback"] as const;
export type IndexPrebuildMode = (typeof INDEX_PREBUILD_MODES)[number];
/** Census artifact (retention 1 day) and its single file; downloaded by prebuild from the census run. */
export const INDEX_PREBUILD_CENSUS_ARTIFACT = "index-prebuild-058000-census";
export const INDEX_PREBUILD_CENSUS_FILE_NAME = "index-prebuild-058000-census.json";
/** Sanitized receipt (retention 30 days), rewritten after every state change of a prebuild. */
export const INDEX_PREBUILD_RECEIPT_ARTIFACT = "index-prebuild-058000";
export const INDEX_PREBUILD_RECEIPT_FILE_NAME = "index-prebuild-058000.json";
export const INDEX_PREBUILD_CENSUS_CONTRACT = "index-prebuild-058000-census/v1";
export const INDEX_PREBUILD_RECEIPT_CONTRACT = "index-prebuild-058000-receipt/v1";
/** A census older than this (its run's created_at and its database capturedAt) cannot feed a prebuild. */
export const INDEX_PREBUILD_CENSUS_MAX_AGE_MS = 24 * 60 * 60 * 1000;
/** The migrator's advisory lock (migrate.ts), taken here only shared, only try, only inside a census transaction. */
export const MIGRATOR_LOCK = [961996, 1] as const;
/** The prebuild's dedicated session lock (no other user of key 58000 under 961996). */
export const INDEX_PREBUILD_LOCK = [961996, 58000] as const;
/** Per CONCURRENTLY build: finite, never 0. */
export const INDEX_PREBUILD_STATEMENT_TIMEOUT_MS = 60 * 60 * 1000;
/** Lock waits inside a build (its SHARE UPDATE EXCLUSIVE does not block DML while queued; a timeout leaves INVALID). */
export const INDEX_PREBUILD_LOCK_TIMEOUT_MS = 10 * 60 * 1000;
/** No new build starts unless it can finish within this run budget (the job's own timeout is above it). */
export const INDEX_PREBUILD_RUN_BUDGET_MS = 300 * 60 * 1000;

const SHA40 = /^[0-9a-f]{40}$/u;
const HEX64 = /^[0-9a-f]{64}$/u;
const RUN_ID = /^[1-9][0-9]{0,19}$/u;
const STEP = /^(upstream|96bulls):[0-9A-Za-z_.-]{1,200}\.sql$/u;
const IDENT = /^[a-z][a-z0-9_]{0,62}$/u;
const CANONICAL_MS = /^[0-9]{4}-[0-9]{2}-[0-9]{2}T[0-9]{2}:[0-9]{2}:[0-9]{2}\.[0-9]{3}Z$/u;
const SQLSTATE = /^[0-9A-Z]{5}$/u;

export class IndexPrebuildRefusal extends Error {
  constructor(readonly code: string) {
    super(code);
    this.name = "IndexPrebuildRefusal";
  }
}
export function refuseIndexPrebuild(code: string): never {
  throw new IndexPrebuildRefusal(code);
}
function refuse(code: string): never {
  throw new IndexPrebuildRefusal(code);
}
const sha256 = (text: string) => createHash("sha256").update(text).digest("hex");

/** The minimal client surface (node-postgres Client, a PGlite adapter in tests). */
export interface PrebuildDb {
  query<R = Record<string, unknown>>(sql: string, params?: unknown[]): Promise<{ rows: R[] }>;
}

/** The declared sixteen, from the migration packet's ADOPTABLE_INDEXES entry; refuses an unexpected shape. */
export function indexPrebuildSpec(): readonly AdoptableIndex[] {
  const spec = ADOPTABLE_INDEXES[INDEX_PREBUILD_STEP];
  if (!spec || spec.length !== INDEX_PREBUILD_COUNT || new Set(spec.map((s) => s.name)).size !== spec.length)
    refuse("index_prebuild_spec");
  for (const s of spec) concurrentStatement(s);
  return spec;
}

/** sha256 over the canonical JSON of the declared [name, table, def] triples, in order. */
export function indexPrebuildSpecSha256(spec: readonly AdoptableIndex[] = indexPrebuildSpec()): string {
  return sha256(JSON.stringify(spec.map((s) => [s.name, s.table, s.def])));
}

/**
 * The one DDL statement this module can produce: the declared definition with CONCURRENTLY inserted after
 * CREATE INDEX. Refuses anything that is not exactly `CREATE INDEX <name> ON sessions.<table> USING btree (...)`
 * with plain identifiers, so nothing but a declared single-statement btree build can reach the database.
 */
export function concurrentStatement(spec: AdoptableIndex): string {
  const prefix = `CREATE INDEX ${spec.name} ON ${spec.table} USING btree (`;
  const table = spec.table.startsWith("sessions.") ? spec.table.slice("sessions.".length) : "";
  if (!IDENT.test(spec.name) || !IDENT.test(table) || !spec.def.startsWith(prefix) || !spec.def.endsWith(")") ||
      /;|--|\/\*|\bCONCURRENTLY\b|\bIF\s+NOT\s+EXISTS\b/iu.test(spec.def))
    refuse("index_prebuild_spec");
  return `CREATE INDEX CONCURRENTLY ${spec.name} ON ${spec.table} USING btree (${spec.def.slice(prefix.length)}`;
}

// ---------------------------------------------------------------------------------------------------------------
// Dispatch inputs (env PREBUILD_*): checked before any credential or GitHub read.
// ---------------------------------------------------------------------------------------------------------------

export interface IndexPrebuildDispatch {
  mode: IndexPrebuildMode;
  sha: string;
  /** prebuild only: the census run id; null otherwise. */
  censusRun: string | null;
  confirmation: string;
}

export function indexPrebuildDispatch(
  env: Record<string, string | undefined>,
  enabled: boolean = INDEX_PREBUILD_ENABLED,
): IndexPrebuildDispatch {
  const mode = env.PREBUILD_MODE ?? "", sha = env.PREBUILD_SHA ?? "", censusRun = env.PREBUILD_CENSUS_RUN ?? "",
    confirmation = env.PREBUILD_CONFIRMATION ?? "";
  if (!(INDEX_PREBUILD_MODES as readonly string[]).includes(mode) || !SHA40.test(sha)) refuse("index_prebuild_inputs");
  if (mode === "prebuild" && enabled !== true) refuse("index_prebuild_dormant");
  if (mode === "prebuild" ? !RUN_ID.test(censusRun) : censusRun !== "") refuse("index_prebuild_census_run");
  if (mode === "prebuild" ? !confirmation.startsWith("PREBUILD-058000 ") : confirmation !== "")
    refuse("index_prebuild_confirmation");
  return { mode: mode as IndexPrebuildMode, sha, censusRun: mode === "prebuild" ? censusRun : null, confirmation };
}

// ---------------------------------------------------------------------------------------------------------------
// Census
// ---------------------------------------------------------------------------------------------------------------

export type IndexPresence = "absent" | "exact" | "drifted";
export type IndexAction = "present" | "deferred" | "prebuild" | "refused";
export type IndexRefusalReason = "table_missing" | "drifted" | "invalid" | "in_progress" | "wrong_owner" | "name_collision";
const REFUSAL_REASONS: readonly IndexRefusalReason[] = ["table_missing", "drifted", "invalid", "in_progress", "wrong_owner", "name_collision"];
/** The only readiness-marker status under which prebuild may write (as in the admission-renewal operator). */
export const INDEX_PREBUILD_MARKER_PASS = "pass";

export interface IndexProgress {
  command: string | null;
  phase: string | null;
}

export interface CensusIndex {
  name: string;
  table: string;
  tableExists: boolean;
  /** null when the table does not exist. */
  tableNonEmpty: boolean | null;
  presence: IndexPresence;
  valid: boolean | null;
  ready: boolean | null;
  live: boolean | null;
  owner: string | null;
  tableOwner: string | null;
  schema: string | null;
  defSha256: string | null;
  expectedDefSha256: string;
  /** Indexes named `name` in any schema (1 when present and unambiguous). */
  sameNameCount: number;
  /** The pg_stat_progress_create_index row building this index, if any. */
  progress: IndexProgress | null;
  /** Create-index progress rows on this index's table (any index). */
  tableBuilds: number;
  action: IndexAction;
  reason: IndexRefusalReason | null;
}

export interface IndexPrebuildBinding {
  /** The live readiness marker's generation (discovered, never an input or recomputed). */
  generation: string;
  markerStatus: string;
  ledgerTail: string;
  stepCount: number;
  ledgerPrefixDigest: string;
  /** Whether 058000 itself is already ledgered (prebuild refuses then: there is nothing to prebuild for). */
  stepLedgered: boolean;
}

export interface IndexPrebuildSnapshot extends IndexPrebuildBinding {
  owner: string;
  capturedAt: string;
  progressRows: number;
  /** Progress rows in this database whose target the session may not see (relid hidden): never assumed harmless. */
  unattributableProgressRows: number;
  indexes: CensusIndex[];
  required: string[];
  verdict: "prebuild_required" | "nothing_to_prebuild" | "blocked";
}

export interface IndexPrebuildCensusBody extends IndexPrebuildSnapshot {
  contract: typeof INDEX_PREBUILD_CENSUS_CONTRACT;
  step: typeof INDEX_PREBUILD_STEP;
  specSha256: string;
  sourceSha: string;
  censusRun: string;
}
export interface IndexPrebuildCensusArtifact extends IndexPrebuildCensusBody {
  descriptorSha256: string;
}

const INDEX_SQL = `WITH adopt AS (${ADOPTABLE_INDEX_STATE_SQL})
SELECT adopt.state, adopt.table_exists,
  c.oid::text AS index_oid, n.nspname AS schema,
  i.indisvalid AS valid, i.indisready AS ready, i.indislive AS live,
  pg_get_userbyid(c.relowner) AS owner,
  pg_get_indexdef(c.oid) AS def,
  to_regclass($2)::oid::text AS table_oid,
  (SELECT pg_get_userbyid(t.relowner) FROM pg_class t WHERE t.oid = to_regclass($2)) AS table_owner,
  (SELECT count(*) FROM pg_class x WHERE x.relname = $1 AND x.relkind IN ('i', 'I'))::int AS same_name
FROM adopt
LEFT JOIN pg_class c ON c.relnamespace = to_regnamespace('sessions') AND c.relname = $1
LEFT JOIN pg_namespace n ON n.oid = c.relnamespace
LEFT JOIN pg_index i ON i.indexrelid = c.oid`;

/** This database's create-index progress rows (pg_stat_progress_create_index is not MVCC; read it as late as possible). */
const PROGRESS_SQL = `SELECT p.relid::text AS relid, p.index_relid::text AS index_relid, p.command, p.phase
  FROM pg_stat_progress_create_index p
 WHERE p.datid IS NULL OR p.datid = (SELECT oid FROM pg_database WHERE datname = current_database())`;

const NOW_SQL = `SELECT to_char(date_trunc('milliseconds', clock_timestamp()) AT TIME ZONE 'UTC',
  'YYYY-MM-DD"T"HH24:MI:SS.MS"Z"') AS now, current_user AS me`;

interface ProgressRow { relid: string | null; index_relid: string | null; command: string | null; phase: string | null }

function text(v: unknown): string | null {
  return typeof v === "string" ? v : null;
}
function bool(v: unknown): boolean | null {
  return typeof v === "boolean" ? v : null;
}

async function readProgress(db: PrebuildDb): Promise<ProgressRow[]> {
  return (await db.query<ProgressRow>(PROGRESS_SQL)).rows.map((r) => ({
    relid: text(r.relid), index_relid: text(r.index_relid), command: text(r.command), phase: text(r.phase),
  }));
}

/** One index, read in the caller's transaction or in autocommit; `progress` from readProgress. */
export async function inspectIndex(
  db: PrebuildDb,
  spec: AdoptableIndex,
  me: string,
  progress: readonly ProgressRow[],
): Promise<CensusIndex & { tableOid: string | null }> {
  concurrentStatement(spec);
  const r = (await db.query<Record<string, unknown>>(INDEX_SQL, [spec.name, spec.table, spec.def])).rows[0];
  if (!r) refuse("index_prebuild_catalog");
  const tableExists = r.table_exists === true;
  const tableOid = text(r.table_oid);
  let tableNonEmpty: boolean | null = null;
  if (tableExists) {
    const e = (await db.query<{ nonempty: unknown }>(`SELECT EXISTS (SELECT 1 FROM ${spec.table} LIMIT 1) AS nonempty`)).rows[0];
    tableNonEmpty = e?.nonempty === true;
  }
  const state = text(r.state);
  const presence: IndexPresence = state === "absent" ? "absent" : state === "adoptable" || state === "not_ready" ? "exact" : "drifted";
  const indexOid = text(r.index_oid);
  const own = indexOid !== null ? progress.find((p) => p.index_relid === indexOid) : undefined;
  const tableBuilds = tableOid !== null ? progress.filter((p) => p.relid === tableOid).length : 0;
  const def = text(r.def);
  const valid = bool(r.valid), ready = bool(r.ready), live = bool(r.live);
  const owner = text(r.owner), tableOwner = text(r.table_owner);
  const usable = presence === "exact" && valid === true && ready === true && live === true;
  const sameNameCount = typeof r.same_name === "number" ? r.same_name : Number(r.same_name ?? 0);
  let action: IndexAction;
  let reason: IndexRefusalReason | null = null;
  if (!tableExists) reason = "table_missing";
  else if (own || (!usable && tableBuilds > 0)) reason = "in_progress";
  // Any other index of this name (another schema, whatever its owner or definition): a build here would leave two,
  // which the migration's adoption check refuses, so refuse before any DDL (and for the migration's own build too).
  else if (!Number.isSafeInteger(sameNameCount) || sameNameCount > (presence === "absent" ? 0 : 1)) reason = "name_collision";
  else if (presence === "drifted") reason = "drifted";
  else if (tableOwner !== me || (presence !== "absent" && owner !== me)) reason = "wrong_owner";
  else if (presence === "exact" && !usable) reason = "invalid";
  if (reason) action = "refused";
  else if (usable) action = "present";
  else action = tableNonEmpty ? "prebuild" : "deferred";
  return {
    name: spec.name, table: spec.table, tableExists, tableNonEmpty, presence, valid, ready, live, owner, tableOwner,
    schema: text(r.schema), defSha256: def === null ? null : sha256(def), expectedDefSha256: sha256(spec.def),
    sameNameCount,
    progress: own ? { command: own.command, phase: own.phase } : null, tableBuilds, action, reason, tableOid,
  };
}

/** The census entry without the table oid (which never leaves the process). */
function strip(i: CensusIndex & { tableOid: string | null }): CensusIndex {
  const { tableOid: _oid, ...entry } = i;
  return entry;
}

/**
 * One index read back in its own short READ ONLY transaction (row_security off, as in the census), with this
 * database's unattributable progress row count. Used before and after every build, and on a fresh session after an
 * unknown outcome.
 */
export async function readbackIndex(db: PrebuildDb, spec: AdoptableIndex, owner: string): Promise<{ index: CensusIndex; unattributable: number }> {
  await db.query("BEGIN ISOLATION LEVEL READ COMMITTED READ ONLY");
  try {
    await db.query("SET LOCAL row_security = off");
    const progress = await readProgress(db);
    const index = strip(await inspectIndex(db, spec, owner, progress));
    await db.query("COMMIT");
    return { index, unattributable: progress.filter((p) => p.relid === null).length };
  } catch (error) {
    await db.query("ROLLBACK").catch(() => undefined);
    throw error;
  }
}

/** The binding: marker generation and status, the ledger proof, whether 058000 is ledgered. */
export async function readBinding(db: PrebuildDb): Promise<IndexPrebuildBinding> {
  const marker = (await db.query<{ generation: unknown; status: unknown }>(
    `SELECT generation, status FROM ${ROLE_VALIDATION_TABLE} WHERE name = $1`, [ROLE_VALIDATION_NAME])).rows;
  if (marker.length !== 1 || typeof marker[0]!.generation !== "string" || !HEX64.test(marker[0]!.generation) ||
      typeof marker[0]!.status !== "string" || !/^[a-z_]{1,32}$/u.test(marker[0]!.status))
    refuse("index_prebuild_generation_unavailable");
  let ledger: Awaited<ReturnType<typeof admissionRenewalLedger>>;
  try {
    ledger = await admissionRenewalLedger(db as never);
  } catch (error) {
    const code = (error as { code?: unknown })?.code;
    // The shared ledger proof's own codes are renamed for this operator; the migrator's guard codes pass through.
    if (typeof code === "string" && code.startsWith("admission_renewal_")) refuse("index_prebuild_" + code.slice("admission_renewal_".length));
    throw error;
  }
  const ledgered = (await db.query<{ n: unknown }>(`SELECT count(*)::int AS n FROM ${MIGRATION_LEDGER} WHERE name = $1`,
    [INDEX_PREBUILD_STEP])).rows[0];
  return {
    generation: marker[0]!.generation as string, markerStatus: marker[0]!.status as string,
    ledgerTail: ledger.ledgerTail, stepCount: ledger.stepCount, ledgerPrefixDigest: ledger.ledgerPrefixDigest,
    stepLedgered: Number(ledgered?.n ?? 0) > 0,
  };
}

const sameBinding = (a: IndexPrebuildBinding, b: IndexPrebuildBinding) =>
  a.generation === b.generation && a.markerStatus === b.markerStatus && a.ledgerTail === b.ledgerTail &&
  a.stepCount === b.stepCount && a.ledgerPrefixDigest === b.ledgerPrefixDigest && a.stepLedgered === b.stepLedgered;

function verdictOf(indexes: readonly CensusIndex[], progressRows: number, unattributable: number, markerStatus: string) {
  const required = indexes.filter((i) => i.action === "prebuild").map((i) => i.name);
  const blocked = indexes.some((i) => i.action === "refused") || unattributable > 0 || progressRows > 0 ||
    markerStatus !== INDEX_PREBUILD_MARKER_PASS;
  return { required, verdict: blocked ? "blocked" as const : required.length ? "prebuild_required" as const : "nothing_to_prebuild" as const };
}

/**
 * One READ ONLY snapshot of all sixteen. READ COMMITTED so the closing binding read sees rows committed meanwhile;
 * the migrator lock is taken shared and try-only (refuses index_prebuild_migrator_busy while a migration holds it).
 * row_security is off for the emptiness probes, so a policy that would filter rows errors instead.
 */
export async function indexPrebuildSnapshot(db: PrebuildDb): Promise<IndexPrebuildSnapshot> {
  const spec = indexPrebuildSpec();
  await db.query("BEGIN ISOLATION LEVEL READ COMMITTED READ ONLY");
  try {
    const lock = (await db.query<{ ok: unknown }>(
      `SELECT pg_try_advisory_xact_lock_shared(${MIGRATOR_LOCK[0]}, ${MIGRATOR_LOCK[1]}) AS ok`)).rows[0];
    if (lock?.ok !== true) refuse("index_prebuild_migrator_busy");
    await db.query("SET LOCAL row_security = off");
    const before = await readBinding(db);
    const head = (await db.query<{ now: unknown; me: unknown }>(NOW_SQL)).rows[0];
    if (typeof head?.now !== "string" || !CANONICAL_MS.test(head.now) || typeof head.me !== "string") refuse("index_prebuild_catalog");
    const me = head.me;
    const progress = await readProgress(db);
    const indexes: CensusIndex[] = [];
    const tables = new Set<string>();
    for (const s of spec) {
      const inspected = await inspectIndex(db, s, me, progress);
      if (inspected.tableOid) tables.add(inspected.tableOid);
      indexes.push(strip(inspected));
    }
    // Read progress again after the catalog reads: a build that started meanwhile is not missed.
    const late = await readProgress(db);
    const all = [...progress, ...late];
    const progressRows = new Set(all.filter((p) => p.relid !== null && tables.has(p.relid)).map((p) => JSON.stringify(p))).size;
    const unattributable = Math.max(progress.filter((p) => p.relid === null).length, late.filter((p) => p.relid === null).length);
    for (const i of indexes) {
      const oid = (await db.query<{ oid: unknown }>("SELECT to_regclass($1)::oid::text AS oid", [i.table])).rows[0]?.oid;
      const lateBuilds = late.filter((p) => p.relid !== null && p.relid === oid).length;
      if (lateBuilds > i.tableBuilds) {
        i.tableBuilds = lateBuilds;
        if (i.action !== "present" && i.action !== "refused") { i.action = "refused"; i.reason = "in_progress"; }
      }
    }
    const after = await readBinding(db);
    if (!sameBinding(before, after)) refuse("index_prebuild_binding_moved");
    await db.query("COMMIT");
    return { ...before, owner: me, capturedAt: head.now, progressRows, unattributableProgressRows: unattributable, indexes,
      ...verdictOf(indexes, progressRows, unattributable, before.markerStatus) };
  } catch (error) {
    await db.query("ROLLBACK").catch(() => undefined);
    throw error;
  }
}

// ---------------------------------------------------------------------------------------------------------------
// Census artifact, descriptor and the typed confirmation
// ---------------------------------------------------------------------------------------------------------------

/** Deterministic JSON: object keys sorted recursively, arrays in order. */
export function canonicalJson(value: unknown): string {
  if (Array.isArray(value)) return "[" + value.map(canonicalJson).join(",") + "]";
  if (value !== null && typeof value === "object")
    return "{" + Object.keys(value).sort().filter((k) => (value as Record<string, unknown>)[k] !== undefined)
      .map((k) => JSON.stringify(k) + ":" + canonicalJson((value as Record<string, unknown>)[k])).join(",") + "}";
  return JSON.stringify(value);
}

export function indexPrebuildDescriptor(body: IndexPrebuildCensusBody): string {
  const { descriptorSha256: _ignored, ...rest } = body as IndexPrebuildCensusBody & { descriptorSha256?: unknown };
  return sha256("index-prebuild-058000-census-descriptor/v1\0" + canonicalJson(rest));
}

export function indexPrebuildCensusArtifact(snapshot: IndexPrebuildSnapshot, sourceSha: string, censusRun: string): IndexPrebuildCensusArtifact {
  if (!SHA40.test(sourceSha) || !RUN_ID.test(censusRun)) refuse("index_prebuild_inputs");
  const body: IndexPrebuildCensusBody = {
    contract: INDEX_PREBUILD_CENSUS_CONTRACT, step: INDEX_PREBUILD_STEP, specSha256: indexPrebuildSpecSha256(),
    sourceSha, censusRun, ...snapshot,
  };
  return { ...body, descriptorSha256: indexPrebuildDescriptor(body) };
}

/** The exact typed confirmation for prebuild, built from the census artifact. */
export function indexPrebuildConfirmation(c: IndexPrebuildCensusArtifact): string {
  return `PREBUILD-058000 ${c.sourceSha} GENERATION ${c.generation} TAIL ${c.ledgerTail} LEDGER ${c.ledgerPrefixDigest}` +
    ` DESCRIPTOR ${c.descriptorSha256} CENSUS ${c.censusRun} INDEXES ${c.required.join(",")}`;
}

function canonicalMs(value: unknown): number | null {
  if (typeof value !== "string" || !CANONICAL_MS.test(value)) return null;
  const ms = Date.parse(value);
  return Number.isFinite(ms) && new Date(ms).toISOString() === value ? ms : null;
}

const ARTIFACT_KEYS = ["contract", "step", "specSha256", "sourceSha", "censusRun", "generation", "markerStatus", "ledgerTail",
  "stepCount", "ledgerPrefixDigest", "stepLedgered", "owner", "capturedAt", "progressRows", "unattributableProgressRows",
  "indexes", "required", "verdict", "descriptorSha256"].sort();
const INDEX_KEYS = ["name", "table", "tableExists", "tableNonEmpty", "presence", "valid", "ready", "live", "owner",
  "tableOwner", "schema", "defSha256", "expectedDefSha256", "sameNameCount", "progress", "tableBuilds", "action", "reason"].sort();
const sameKeys = (o: Record<string, unknown>, keys: readonly string[]) => {
  const k = Object.keys(o).sort();
  return k.length === keys.length && k.every((x, i) => x === keys[i]);
};
const nullable = (v: unknown, t: "string" | "boolean") => v === null || typeof v === t;

/** Strict census artifact: exact keys, the declared sixteen in order, a consistent required list, the descriptor. */
export function parseIndexPrebuildCensus(value: unknown): IndexPrebuildCensusArtifact {
  const bad = () => refuse("index_prebuild_census_invalid");
  if (value === null || typeof value !== "object" || Array.isArray(value)) bad();
  const doc = value as Record<string, unknown>;
  if (!sameKeys(doc, ARTIFACT_KEYS) || doc.contract !== INDEX_PREBUILD_CENSUS_CONTRACT || doc.step !== INDEX_PREBUILD_STEP ||
      typeof doc.sourceSha !== "string" || !SHA40.test(doc.sourceSha) || typeof doc.censusRun !== "string" ||
      !RUN_ID.test(doc.censusRun) || typeof doc.generation !== "string" || !HEX64.test(doc.generation) ||
      typeof doc.markerStatus !== "string" || typeof doc.ledgerTail !== "string" || !STEP.test(doc.ledgerTail) ||
      !Number.isSafeInteger(doc.stepCount) || (doc.stepCount as number) < 1 || typeof doc.ledgerPrefixDigest !== "string" ||
      !HEX64.test(doc.ledgerPrefixDigest) || typeof doc.stepLedgered !== "boolean" || typeof doc.owner !== "string" ||
      canonicalMs(doc.capturedAt) === null || !Number.isSafeInteger(doc.progressRows) ||
      !Number.isSafeInteger(doc.unattributableProgressRows) || typeof doc.specSha256 !== "string" ||
      typeof doc.descriptorSha256 !== "string" || !HEX64.test(doc.descriptorSha256) || !Array.isArray(doc.indexes) ||
      !Array.isArray(doc.required) || !["prebuild_required", "nothing_to_prebuild", "blocked"].includes(doc.verdict as string))
    bad();
  const spec = indexPrebuildSpec();
  const indexes = doc.indexes as unknown[];
  if (indexes.length !== spec.length) bad();
  indexes.forEach((entry, n) => {
    if (entry === null || typeof entry !== "object" || Array.isArray(entry)) bad();
    const e = entry as Record<string, unknown>;
    const p = e.progress as Record<string, unknown> | null;
    if (!sameKeys(e, INDEX_KEYS) || e.name !== spec[n]!.name || e.table !== spec[n]!.table ||
        typeof e.tableExists !== "boolean" || !nullable(e.tableNonEmpty, "boolean") ||
        !["absent", "exact", "drifted"].includes(e.presence as string) || !nullable(e.valid, "boolean") ||
        !nullable(e.ready, "boolean") || !nullable(e.live, "boolean") || !nullable(e.owner, "string") ||
        !nullable(e.tableOwner, "string") || !nullable(e.schema, "string") || !nullable(e.defSha256, "string") ||
        e.expectedDefSha256 !== sha256(spec[n]!.def) || !Number.isSafeInteger(e.sameNameCount) ||
        !Number.isSafeInteger(e.tableBuilds) || !["present", "deferred", "prebuild", "refused"].includes(e.action as string) ||
        !(e.reason === null || (REFUSAL_REASONS as readonly string[]).includes(e.reason as string)) ||
        (e.action === "refused") !== (e.reason !== null) ||
        !(p === null || (typeof p === "object" && !Array.isArray(p) && sameKeys(p, ["command", "phase"]) &&
          nullable(p.command, "string") && nullable(p.phase, "string"))))
      bad();
  });
  const artifact = doc as unknown as IndexPrebuildCensusArtifact;
  const expected = verdictOf(artifact.indexes, artifact.progressRows, artifact.unattributableProgressRows, artifact.markerStatus);
  if (JSON.stringify(artifact.required) !== JSON.stringify(expected.required) || artifact.verdict !== expected.verdict) bad();
  if (indexPrebuildDescriptor(artifact) !== artifact.descriptorSha256) refuse("index_prebuild_census_descriptor");
  return artifact;
}

/**
 * Everything a prebuild can check before it reads a credential: the census is the dispatched run's, at the
 * dispatched sha, for the declared sixteen (spec digest), under 24h old, 058000 not ledgered, verdict
 * prebuild_required, and the typed confirmation equals the phrase built from it byte for byte.
 */
export function verifyIndexPrebuildInputs(
  dispatch: IndexPrebuildDispatch,
  census: IndexPrebuildCensusArtifact,
  now: number = Date.now(),
): void {
  if (dispatch.mode !== "prebuild") refuse("index_prebuild_inputs");
  if (census.sourceSha !== dispatch.sha) refuse("index_prebuild_census_sha_mismatch");
  if (census.censusRun !== dispatch.censusRun) refuse("index_prebuild_census_run_mismatch");
  if (census.specSha256 !== indexPrebuildSpecSha256()) refuse("index_prebuild_census_spec_mismatch");
  const captured = canonicalMs(census.capturedAt)!;
  if (captured - now > 5 * 60 * 1000 || now - captured >= INDEX_PREBUILD_CENSUS_MAX_AGE_MS) refuse("index_prebuild_census_stale");
  if (census.stepLedgered) refuse("index_prebuild_step_already_applied");
  if (census.markerStatus !== INDEX_PREBUILD_MARKER_PASS) refuse("index_prebuild_marker_not_pass");
  if (census.verdict === "blocked") refuse("index_prebuild_census_blocked");
  if (census.verdict !== "prebuild_required" || census.required.length === 0) refuse("index_prebuild_nothing_required");
  if (dispatch.confirmation !== indexPrebuildConfirmation(census)) refuse("index_prebuild_confirmation");
}

// ---------------------------------------------------------------------------------------------------------------
// Receipt
// ---------------------------------------------------------------------------------------------------------------

export type IndexReceiptStatus =
  | "present" | "deferred" | "pending" | "building" | "built" | "resumed_exact" | "committed_after_error"
  | "refused" | "not_attempted"
  /** The statement's outcome could not be read back on a fresh session: a human must inspect the index. */
  | "outcome_unknown";

export interface IndexReceiptEntry {
  name: string;
  status: IndexReceiptStatus;
  action: IndexAction | null;
  reason: IndexRefusalReason | "absent_after_build" | "budget" | null;
  /** SQLSTATE of a failed statement, or "unknown" when it failed without one (the connection failed). */
  error: string | null;
  readback: Pick<CensusIndex, "presence" | "valid" | "ready" | "live" | "defSha256" | "progress" | "tableBuilds"> | null;
  startedAt: string | null;
  finishedAt: string | null;
}

export interface IndexPrebuildReceipt {
  contract: typeof INDEX_PREBUILD_RECEIPT_CONTRACT;
  status: "ok" | "running" | "complete" | "incomplete" | "blocked" | "partial_budget" | "refused";
  code?: string;
  mode: IndexPrebuildMode | "refused";
  runId: string;
  sourceSha: string | null;
  censusRun: string | null;
  generation: string | null;
  ledgerTail: string | null;
  ledgerPrefixDigest: string | null;
  verdict: IndexPrebuildSnapshot["verdict"] | null;
  counts: { declared: number; present: number; deferred: number; required: number; built: number; resumed: number; refused: number };
  indexes: IndexReceiptEntry[];
  statements: number;
  at: string;
  credentialsPersisted: false;
}

export interface IndexPrebuildOutcome {
  receipt: IndexPrebuildReceipt;
  census: IndexPrebuildCensusArtifact | null;
  exitCode: number;
}

/** Fixed codes only; driver text (which could name a host) collapses to a constant. */
export function indexPrebuildCode(error: unknown): string {
  const code = (error as { code?: unknown } | null)?.code;
  if (error instanceof IndexPrebuildRefusal && typeof code === "string" && /^[a-z0-9_.]{1,80}$/u.test(code)) return code;
  if (typeof code === "string" && /^[a-z][a-z0-9_.]{1,80}$/u.test(code)) return code;
  if (typeof code === "string" && SQLSTATE.test(code)) return "index_prebuild_sqlstate_" + code;
  return "index_prebuild_failed";
}

function counts(entries: readonly IndexReceiptEntry[], required: number) {
  return {
    declared: INDEX_PREBUILD_COUNT,
    present: entries.filter((e) => e.status === "present").length,
    deferred: entries.filter((e) => e.status === "deferred").length,
    required,
    built: entries.filter((e) => e.status === "built" || e.status === "committed_after_error").length,
    resumed: entries.filter((e) => e.status === "resumed_exact").length,
    refused: entries.filter((e) => e.status === "refused").length,
  };
}

const readbackOf = (i: CensusIndex): IndexReceiptEntry["readback"] => ({
  presence: i.presence, valid: i.valid, ready: i.ready, live: i.live, defSha256: i.defSha256, progress: i.progress, tableBuilds: i.tableBuilds,
});

// ---------------------------------------------------------------------------------------------------------------
// Runner
// ---------------------------------------------------------------------------------------------------------------

export interface IndexPrebuildSessions {
  /** The owner session: read-only for census and readback, autocommit (not read-only) for prebuild. */
  db: PrebuildDb;
  /** A fresh owner session, used only to read back a statement whose outcome is unknown. */
  fresh?: () => Promise<{ db: PrebuildDb; close(): Promise<void> }>;
}

export interface IndexPrebuildRunOptions {
  runId: string;
  /** prebuild only: the downloaded census artifact, raw. */
  census?: unknown;
  now?: () => number;
  /** Test injection only; production never passes it and the workflow cannot set it. */
  enabled?: boolean;
  /** Called with the receipt after every per-index state change (the entry point writes it to disk). */
  persist?: (receipt: IndexPrebuildReceipt) => void;
}

/** The SQLSTATE of a server error, or null (a client-side or connection failure). */
function sqlstateOf(error: unknown): string | null {
  const code = (error as { code?: unknown } | null)?.code;
  return typeof code === "string" && SQLSTATE.test(code) ? code : null;
}

/**
 * Whether the session can no longer be used after this error: no SQLSTATE at all (a client-side or connection
 * failure), a FATAL or PANIC severity, or a SQLSTATE after which the server has ended (or is ending) the session:
 * connection exceptions (08), the 57P01-57P05 terminations (admin/crash shutdown, cannot connect now, database
 * dropped, idle session timeout), idle-in-transaction and transaction timeouts (25P03, 25P04), system errors (58) and
 * internal errors (XX). Erring towards fatal is safe: it only moves the readback to a fresh session and stops the run.
 */
export function sessionFatalError(error: unknown): boolean {
  const severity = (error as { severity?: unknown } | null)?.severity;
  if (severity === "FATAL" || severity === "PANIC") return true;
  const code = sqlstateOf(error);
  return code === null || /^(08|57P0[1-5]|25P0[34]|58|XX)/u.test(code);
}

/**
 * The credential-free part: mode, dormant switch, sha, and for prebuild the census artifact and the typed
 * confirmation. Runs in the parent before the secret is touched and again in the credential child.
 */
export function indexPrebuildPreflight(
  env: Record<string, string | undefined>,
  census: unknown,
  enabled: boolean = INDEX_PREBUILD_ENABLED,
  now: number = Date.now(),
): { dispatch: IndexPrebuildDispatch; census: IndexPrebuildCensusArtifact | null } {
  const dispatch = indexPrebuildDispatch(env, enabled);
  indexPrebuildSpec();
  if (dispatch.mode !== "prebuild") return { dispatch, census: null };
  const artifact = parseIndexPrebuildCensus(census);
  verifyIndexPrebuildInputs(dispatch, artifact, now);
  return { dispatch, census: artifact };
}

/** The live state may differ from the census only where a required index has since become exact (resume). */
function requireNoDrift(census: IndexPrebuildCensusArtifact, live: IndexPrebuildSnapshot): void {
  if (live.generation !== census.generation || live.markerStatus !== census.markerStatus || live.ledgerTail !== census.ledgerTail ||
      live.stepCount !== census.stepCount || live.ledgerPrefixDigest !== census.ledgerPrefixDigest)
    refuse("index_prebuild_binding_moved");
  if (live.markerStatus !== INDEX_PREBUILD_MARKER_PASS) refuse("index_prebuild_marker_not_pass");
  if (live.stepLedgered) refuse("index_prebuild_step_already_applied");
  if (live.owner !== census.owner) refuse("index_prebuild_owner_changed");
  if (live.unattributableProgressRows > 0) refuse("index_prebuild_progress_unattributable");
  if (live.progressRows > 0) refuse("index_prebuild_in_progress");
  live.indexes.forEach((i, n) => {
    const was = census.indexes[n]!;
    if (i.action === "refused") refuse("index_prebuild_refused_" + i.reason);
    if (census.required.includes(i.name)) {
      if (i.action !== "prebuild" && i.action !== "present") refuse("index_prebuild_census_drift");
    } else if (i.action !== was.action) refuse("index_prebuild_census_drift");
  });
}

export async function runIndexPrebuild(
  env: Record<string, string | undefined>,
  sessions: IndexPrebuildSessions,
  options: IndexPrebuildRunOptions,
): Promise<IndexPrebuildOutcome> {
  const now = options.now ?? Date.now;
  const started = now();
  const iso = () => new Date(now()).toISOString();
  let dispatch: IndexPrebuildDispatch | null = null;
  let snapshot: IndexPrebuildSnapshot | null = null;
  let census: IndexPrebuildCensusArtifact | null = null;
  let entries: IndexReceiptEntry[] = [];
  let required = 0;
  let statements = 0;
  const receipt = (status: IndexPrebuildReceipt["status"], code?: string): IndexPrebuildReceipt => ({
    contract: INDEX_PREBUILD_RECEIPT_CONTRACT, status, ...(code === undefined ? {} : { code }),
    mode: dispatch?.mode ?? "refused", runId: RUN_ID.test(options.runId) ? options.runId : "0",
    sourceSha: dispatch?.sha ?? null, censusRun: census?.censusRun ?? null,
    generation: snapshot?.generation ?? census?.generation ?? null, ledgerTail: snapshot?.ledgerTail ?? census?.ledgerTail ?? null,
    ledgerPrefixDigest: snapshot?.ledgerPrefixDigest ?? census?.ledgerPrefixDigest ?? null, verdict: snapshot?.verdict ?? null,
    counts: counts(entries, required), indexes: entries.map((e) => ({ ...e })), statements, at: iso(), credentialsPersisted: false,
  });
  const persist = () => options.persist?.(receipt("running"));
  const fromSnapshot = (s: IndexPrebuildSnapshot): IndexReceiptEntry[] => s.indexes.map((i) => ({
    name: i.name, status: i.action === "present" ? "present" : i.action === "deferred" ? "deferred" : i.action === "prebuild" ? "pending" : "refused",
    action: i.action, reason: i.reason, error: null, readback: readbackOf(i), startedAt: null, finishedAt: null,
  }));
  try {
    const pre = indexPrebuildPreflight(env, options.census, options.enabled ?? INDEX_PREBUILD_ENABLED, now());
    dispatch = pre.dispatch;
    census = pre.census;
    if (dispatch.mode === "census") {
      snapshot = await indexPrebuildSnapshot(sessions.db);
      entries = fromSnapshot(snapshot);
      required = snapshot.required.length;
      const artifact = indexPrebuildCensusArtifact(snapshot, dispatch.sha, options.runId);
      return { receipt: receipt(snapshot.verdict === "blocked" ? "blocked" : "ok"), census: artifact,
        exitCode: snapshot.verdict === "blocked" ? 3 : 0 };
    }
    if (dispatch.mode === "readback") {
      snapshot = await indexPrebuildSnapshot(sessions.db);
      entries = fromSnapshot(snapshot);
      required = snapshot.required.length;
      const complete = snapshot.indexes.every((i) => i.action === "present" && i.presence === "exact" && i.progress === null &&
        i.tableBuilds === 0) && snapshot.progressRows === 0 && snapshot.unattributableProgressRows === 0;
      return { receipt: receipt(complete ? "complete" : "incomplete"), census: null, exitCode: complete ? 0 : 3 };
    }

    // prebuild
    const c = census!;
    const spec = indexPrebuildSpec();
    required = c.required.length;
    const lock = (await sessions.db.query<{ ok: unknown }>(
      `SELECT pg_try_advisory_lock(${INDEX_PREBUILD_LOCK[0]}, ${INDEX_PREBUILD_LOCK[1]}) AS ok`)).rows[0];
    if (lock?.ok !== true) refuse("index_prebuild_busy");
    let unlock = true;
    try {
      snapshot = await indexPrebuildSnapshot(sessions.db);
      entries = fromSnapshot(snapshot).map((e) => c.required.includes(e.name) ? { ...e, status: e.action === "present" ? "resumed_exact" : "pending" } : e);
      requireNoDrift(c, snapshot);
      persist();
      for (const name of c.required) {
        const s = spec.find((x) => x.name === name)!;
        const entry = entries.find((e) => e.name === name)!;
        // Before each build: the binding has not moved, the owner is unchanged and nothing builds on the table.
        const binding = await readBinding(sessions.db);
        if (binding.markerStatus !== INDEX_PREBUILD_MARKER_PASS) refuse("index_prebuild_marker_not_pass");
        if (!sameBinding(binding, c)) refuse("index_prebuild_binding_moved");
        const me = (await sessions.db.query<{ me: unknown }>("SELECT current_user AS me")).rows[0]?.me;
        if (me !== c.owner) refuse("index_prebuild_owner_changed");
        const { index: one, unattributable } = await readbackIndex(sessions.db, s, c.owner);
        if (unattributable > 0) refuse("index_prebuild_progress_unattributable");
        entry.readback = readbackOf(one);
        entry.action = one.action;
        if (one.action === "present") {
          entry.status = "resumed_exact";
          entry.finishedAt = iso();
          persist();
          continue;
        }
        if (one.action !== "prebuild") {
          entry.status = "refused";
          entry.reason = one.reason;
          persist();
          refuse("index_prebuild_refused_" + (one.reason ?? "state"));
        }
        if (now() - started + INDEX_PREBUILD_STATEMENT_TIMEOUT_MS > INDEX_PREBUILD_RUN_BUDGET_MS) {
          entry.status = "not_attempted";
          entry.reason = "budget";
          for (const rest of entries) if (rest.status === "pending") { rest.status = "not_attempted"; rest.reason = "budget"; }
          persist();
          return { receipt: receipt("partial_budget"), census: null, exitCode: 4 };
        }
        // One autocommit statement with a finite budget; never inside BEGIN.
        await sessions.db.query(`SET statement_timeout = ${INDEX_PREBUILD_STATEMENT_TIMEOUT_MS}`);
        await sessions.db.query(`SET lock_timeout = ${INDEX_PREBUILD_LOCK_TIMEOUT_MS}`);
        entry.status = "building";
        entry.startedAt = iso();
        persist();
        let error: string | null = null;
        // Whether the build session can no longer be trusted: then the outcome is read back on a fresh session only.
        let unknown = false;
        statements++;
        try {
          await sessions.db.query(concurrentStatement(s));
        } catch (caught) {
          error = sqlstateOf(caught) ?? "unknown";
          unknown = sessionFatalError(caught);
        }
        entry.error = error;
        let after: CensusIndex | null = null;
        if (!unknown) {
          // A definite outcome: read back on the same session. If that session fails now, it is broken after all.
          try {
            await sessions.db.query("RESET statement_timeout");
            await sessions.db.query("RESET lock_timeout");
            after = (await readbackIndex(sessions.db, s, c.owner)).index;
          } catch {
            unknown = true;
          }
        }
        if (unknown) {
          // Never on the failed session, never a retry: one fresh session reads the outcome, then the run stops.
          unlock = false;
          after = null;
          const fresh = sessions.fresh ? await sessions.fresh().catch(() => null) : null;
          if (fresh) {
            try {
              after = (await readbackIndex(fresh.db, s, c.owner)).index;
            } catch {
              after = null;
            } finally {
              await fresh.close().catch(() => undefined);
            }
          }
          if (!after) {
            entry.status = "outcome_unknown";
            entry.finishedAt = iso();
            persist();
            refuse("index_prebuild_unknown_commit_unverified");
          }
        }
        entry.readback = readbackOf(after!);
        entry.action = after!.action;
        entry.finishedAt = iso();
        if (after!.action === "present") {
          entry.status = error === null ? "built" : "committed_after_error";
          persist();
          // The session that failed is gone; nothing more runs on this dispatch, the next one resumes.
          if (unknown) refuse("index_prebuild_unknown_commit_verified_stop");
          continue;
        }
        entry.status = "refused";
        entry.reason = after!.action === "refused" ? after!.reason : after!.presence === "absent" ? "absent_after_build" : null;
        persist();
        refuse(unknown ? "index_prebuild_unknown_commit_" + (entry.reason ?? "state") : "index_prebuild_build_failed");
      }
      // Final readback: every required index exact, valid, ready and live; the rest as the census found them.
      snapshot = await indexPrebuildSnapshot(sessions.db);
      if (!sameBinding(snapshot, c)) refuse("index_prebuild_binding_moved");
      snapshot.indexes.forEach((i, n) => {
        const ok = c.required.includes(i.name) ? i.action === "present" : i.action === c.indexes[n]!.action;
        if (!ok) refuse("index_prebuild_final_readback");
        entries[n]!.readback = readbackOf(i);
      });
      if (snapshot.progressRows > 0 || snapshot.unattributableProgressRows > 0) refuse("index_prebuild_final_readback");
      return { receipt: receipt("ok"), census: null, exitCode: 0 };
    } finally {
      if (unlock)
        await sessions.db.query(`SELECT pg_advisory_unlock(${INDEX_PREBUILD_LOCK[0]}, ${INDEX_PREBUILD_LOCK[1]})`).catch(() => undefined);
    }
  } catch (error) {
    const out = receipt("refused", indexPrebuildCode(error));
    options.persist?.(out);
    return { receipt: out, census: null, exitCode: 1 };
  }
}
