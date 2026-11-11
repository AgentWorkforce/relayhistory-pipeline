/**
 * Core of the P1 frozen-source sealer. scripts/frozen-source-copy.ts is the thin driver (argv, the Neon/Supabase
 * clients, the copy loop, the per-org page scans). Everything here is either pure or takes a minimal `Queryable`, so
 * it is tested without a production database (src/migration/__tests__/frozen-source-copy-seal*.test.ts).
 *
 * 1. Direct-owner credential adapter. The Supabase side is the existing direct owner connection (the schema owner
 *    `postgres` through the pinned session pooler), received ONLY in the stdin credential document
 *    {"neonOwnerUrl": ..., "supabaseOwnerUrl": ...}. It is validated against the fixed schema-release pins
 *    (releaseConnection: host, port, database, login `postgres.<project>`, no startup overrides), connected with the
 *    pinned CA + rejectUnauthorized + servername, and the connected identity must then be exactly the pinned owner
 *    (requireReleaseOwner: current_user = session_user = postgres, pinned attribute shape). Nothing mints, refreshes,
 *    stores or logs a credential, and nothing in this path changes the session role.
 * 2. Parity. Records and receipts per org; deletions, resets and control heads globally (and privacy per org for the
 *    org seals). Each mismatch has its own refusal code. The manifest names exactly what was verified.
 * 3. Seal fence. One Supabase SERIALIZABLE transaction, opened only after this session holds the privacy-authority
 *    key EXCLUSIVELY, which every writer of the mirror and the frozen privacy/record tables joins (statement guard of
 *    20261105041000_ingest_privacy_authority.sql). Inside it: a Neon REPEATABLE READ READ ONLY snapshot, both sides'
 *    privacy rows and control heads, the live policy; any movement since the pre-verification read refuses. The
 *    convergence argument is in docs/p1-frozen-source-custody.md ("Seal fence").
 */
import { createHash } from "node:crypto";
import { SCHEMA_RELEASE, releaseConnection } from "../hoopdb/schema-release-contract.ts";
import { requireReleaseOwner } from "../hoopdb/schema-release-inspect.ts";
import { SUPABASE_PROD_CA_2021 } from "../session-pipeline/supabase-ca.ts";
import { CredentialRefusal, postgresUrl } from "./stdin-credentials.ts";
import { SUPA_CONTROL_HEADS, SUPA_DELETIONS, SUPA_RESETS } from "./frozen-source-copy-queries.ts";

/** A refusal with a stable code. `detail` carries counts and org ids only, never a credential. */
export class FrozenCopyRefusal extends Error {
  constructor(readonly code: string, readonly detail: Record<string, string> = {}) {
    super(code);
    this.name = "FrozenCopyRefusal";
  }
}
const refuse = (code: string, detail?: Record<string, string>): never => {
  throw new FrozenCopyRefusal(code, detail);
};

export interface Queryable {
  query(sql: string, params?: unknown[]): Promise<{ rows: any[]; rowCount?: number | null }>;
}

// ---------------------------------------------------------------------------------------------------------------
// 1. Credential document and the direct-owner adapter
// ---------------------------------------------------------------------------------------------------------------

export type FrozenCopyMode = "plan" | "copy" | "seal" | "repair-head";
export const FROZEN_COPY_CREDENTIAL_KEYS = ["neonOwnerUrl", "supabaseOwnerUrl"] as const;

/** Keys of the stdin document: --plan never connects to Supabase, every other mode needs the owner URL. */
export function frozenCopyCredentialKeys(mode: FrozenCopyMode): { required: string[]; allowed: string[] } {
  return { required: mode === "plan" ? ["neonOwnerUrl"] : [...FROZEN_COPY_CREDENTIAL_KEYS], allowed: [...FROZEN_COPY_CREDENTIAL_KEYS] };
}

export interface OwnerConnection {
  host: string;
  port: number;
  database: string;
  user: string;
  password: string;
}

/**
 * The Supabase owner URL against the FIXED schema-release target (no environment input): host and port of the pinned
 * session pooler, database `postgres`, login exactly `postgres.<pinned project>`, a non-empty password, and at most
 * `sslmode=require|verify-full` / `channel_binding=require`. Anything else (another host, the transaction pooler
 * port, a CLI temporary login, `sslmode=disable`/`no-verify`, `options=`) refuses before any client exists.
 */
export function frozenCopyOwnerConnection(v: unknown): OwnerConnection {
  if (typeof v !== "string") refuse("frozen_copy_owner_url_refused");
  try {
    return releaseConnection(v as string);
  } catch (error) {
    const code = (error as { code?: unknown })?.code;
    if (code === "release_connection_options") refuse("frozen_copy_owner_url_options_refused");
    return refuse("frozen_copy_owner_url_refused");
  }
}

/** The validated stdin document. The Neon URL keeps its existing production-owner pattern check in the driver. */
export function frozenCopyCredentials(doc: Record<string, unknown>, mode: FrozenCopyMode): { neonOwnerUrl: string; owner: OwnerConnection | null } {
  const neonOwnerUrl = postgresUrl(doc.neonOwnerUrl, "neonOwnerUrl");
  if (mode === "plan" && doc.supabaseOwnerUrl === undefined) return { neonOwnerUrl, owner: null };
  if (doc.supabaseOwnerUrl === undefined) throw new CredentialRefusal("credentials_missing_key.supabaseOwnerUrl");
  return { neonOwnerUrl, owner: frozenCopyOwnerConnection(doc.supabaseOwnerUrl) };
}

export const FROZEN_COPY_APPLICATION_NAME = "p1-frozen-copy";

/** Discrete pg config (never a connectionString): TLS verified against the pinned CA for the pinned pooler name. */
export function frozenCopyOwnerClientConfig(c: OwnerConnection) {
  return {
    host: c.host,
    port: c.port,
    database: c.database,
    user: c.user,
    password: c.password,
    ssl: { ca: SUPABASE_PROD_CA_2021, rejectUnauthorized: true as const, servername: SCHEMA_RELEASE.host },
    options: "-c statement_timeout=60000 -c lock_timeout=5000",
    application_name: FROZEN_COPY_APPLICATION_NAME,
    connectionTimeoutMillis: 10000,
  };
}

/**
 * After connect (and again inside the seal transaction): the session must BE the pinned owner. A CLI login that
 * switched role (session_user differs), any other login, another database, or a different attribute shape refuses.
 */
export async function requireFrozenCopyOwner(client: Queryable): Promise<void> {
  try {
    await requireReleaseOwner(client as never);
  } catch (error) {
    if ((error as { code?: unknown })?.code === "release_owner") refuse("frozen_copy_owner_identity");
    throw error;
  }
}

// ---------------------------------------------------------------------------------------------------------------
// 2. Canonical row text, digests and parity
// ---------------------------------------------------------------------------------------------------------------

export interface Digest {
  n: bigint;
  sum: bigint;
}
export const emptyDigest = (): Digest => ({ n: 0n, sum: 0n });
const MOD = 1n << 64n;
/** Order-independent: count plus the sum (mod 2^64) of each row text's first 64 sha256 bits. */
export function addDigest(acc: Digest, text: string): void {
  acc.n += 1n;
  acc.sum = (acc.sum + BigInt("0x" + createHash("sha256").update(text).digest("hex").slice(0, 16))) % MOD;
}
export const sameDigest = (a: Digest, b: Digest) => a.n === b.n && a.sum === b.sum;
const digestOf = (texts: Iterable<string>) => {
  const d = emptyDigest();
  for (const t of texts) addDigest(d, t);
  return d;
};

type Row = Record<string, unknown>;
const s = (v: unknown) => (v === null || v === undefined ? null : String(v));
export const RECORD_COLS = ["org_id", "workspace_id", "origin_id", "record_id", "revision_id", "revision", "digest", "kind", "source", "session_id", "operation", "user_id", "projection_accepted_seq", "catalog_accepted_seq"] as const;
/** One row's canonical text: identical on both sides (built in JS from fetched values). */
export const rowText = (r: Row) => JSON.stringify(RECORD_COLS.map((k) => s(r[k])));
export const receiptText = (r: Row) => JSON.stringify([r.org_id, r.workspace_id, r.origin_id, r.batch_id, r.digest, r.receipt]);
export const deletionText = (r: Row) => JSON.stringify([r.org_id, r.workspace_id, r.user_id, r.origin_id, r.source, r.session_id]);
export const resetText = (r: Row) => JSON.stringify([r.org_id, r.workspace_id, r.user_id, r.request_id, s(r.deleted)]);
export const headText = (r: Row) => JSON.stringify([r.org_id, r.workspace_id, r.user_id, s(r.v), r.digest, s(r.pending)]);

export const NEON_DELETIONS = "SELECT org_id,workspace_id,user_id,origin_id,source,session_id FROM sessions.deleted_upload_sessions";
export const NEON_RESETS = "SELECT org_id,workspace_id,user_id,request_id,deleted::text AS deleted FROM sessions.session_resets";
export const NEON_CONTROL_HEADS = "SELECT org_id,workspace_id,user_id,version::text v,digest,pending FROM sessions.delivery_cutover_control_heads ORDER BY 1,2,3";

/** One side's privacy state and control heads, as digested by the sealer. */
export interface PrivacySide {
  deletions: Digest;
  resets: Digest;
  /** deletions + resets of each org (the org seal's privacy_rows/privacy_sum). */
  byOrg: Map<string, Digest>;
  /** headText of every control head, sorted. */
  heads: string[];
  /** Reset rows by native key [org,workspace,user,request_id] -> `deleted` as text (what the witness consumes). */
  resetRows: Map<string, string | null>;
}

export function privacySide(deletions: Row[], resets: Row[], heads: Row[]): PrivacySide {
  const byOrg = new Map<string, Digest>();
  const org = (o: unknown) => {
    const k = String(o);
    let d = byOrg.get(k);
    if (!d) byOrg.set(k, (d = emptyDigest()));
    return d;
  };
  const del = emptyDigest(), res = emptyDigest();
  for (const r of deletions) {
    const t = deletionText(r);
    addDigest(del, t);
    addDigest(org(r.org_id), t);
  }
  const resetRows = new Map<string, string | null>();
  for (const r of resets) {
    const t = resetText(r);
    addDigest(res, t);
    addDigest(org(r.org_id), t);
    resetRows.set(JSON.stringify([r.org_id, r.workspace_id, r.user_id, r.request_id]), s(r.deleted));
  }
  return { deletions: del, resets: res, byOrg, heads: heads.map(headText).sort(), resetRows };
}

export async function readPrivacySide(c: Queryable, side: "neon" | "supabase"): Promise<PrivacySide> {
  const [d, r, h] = side === "neon" ? [NEON_DELETIONS, NEON_RESETS, NEON_CONTROL_HEADS] : [SUPA_DELETIONS, SUPA_RESETS, SUPA_CONTROL_HEADS];
  return privacySide((await c.query(d)).rows, (await c.query(r)).rows, (await c.query(h)).rows);
}

const sameList = (a: string[], b: string[]) => a.length === b.length && a.every((x, i) => x === b[i]);

/**
 * Reset rows exactly as ingest_frozen_source_privacy consumes them (keyed by org/workspace/user/request_id, value
 * `deleted::text`): a Neon reset absent from the copy, a copied reset Neon does not have, or the same key with a
 * different `deleted` each refuse with their own code.
 */
export function resetParityRefusal(neon: PrivacySide, supa: PrivacySide): string | null {
  for (const k of neon.resetRows.keys()) if (!supa.resetRows.has(k)) return "frozen_copy_reset_missing";
  for (const k of supa.resetRows.keys()) if (!neon.resetRows.has(k)) return "frozen_copy_reset_extra";
  for (const [k, v] of neon.resetRows) if (supa.resetRows.get(k) !== v) return "frozen_copy_reset_changed";
  return null;
}

/** Neon vs Supabase privacy/heads parity: the first difference, as its own refusal code, or null. */
export function privacyParityRefusal(neon: PrivacySide, supa: PrivacySide): string | null {
  if (!sameDigest(neon.deletions, supa.deletions)) return "frozen_copy_deletion_mismatch";
  const reset = resetParityRefusal(neon, supa);
  if (reset) return reset;
  // Backstop (duplicate native keys cannot exist on either side; the digest still binds the exact row texts).
  if (!sameDigest(neon.resets, supa.resets)) return "frozen_copy_reset_mismatch";
  if (!sameList(neon.heads, supa.heads)) return "frozen_copy_control_head_mismatch";
  return null;
}

export interface OrgVerification {
  org: string;
  records: Digest;
  receipts: Digest;
}

/** One org's records + receipts parity. A Supabase org with no rows at all is a partial copy (org missing). */
export function orgParityRefusal(org: string, neon: { records: Digest; receipts: Digest }, supa: { records: Digest; receipts: Digest }): FrozenCopyRefusal | null {
  const detail = (a: Digest, b: Digest) => ({ org, neonRows: String(a.n), supabaseRows: String(b.n) });
  if (!sameDigest(neon.records, supa.records)) {
    const code = supa.records.n === 0n && supa.receipts.n === 0n && neon.records.n > 0n ? "frozen_copy_org_missing" : "frozen_copy_record_mismatch";
    return new FrozenCopyRefusal(code, detail(neon.records, supa.records));
  }
  if (!sameDigest(neon.receipts, supa.receipts)) return new FrozenCopyRefusal("frozen_copy_receipt_mismatch", detail(neon.receipts, supa.receipts));
  return null;
}

/** Orgs in the Supabase copy (records or receipts) that Neon does not name. */
export const SUPA_EXTRA_ORGS = `SELECT count(*)::int AS n FROM (
 SELECT DISTINCT org_id FROM sessions.ingest_frozen_source_records WHERE NOT (org_id = ANY($1))
 UNION SELECT DISTINCT org_id FROM sessions.ingest_frozen_source_receipts WHERE NOT (org_id = ANY($1))) q`;
export function extraOrgRefusal(extra: number): FrozenCopyRefusal | null {
  return extra ? new FrozenCopyRefusal("frozen_copy_extra_org", { extraOrgs: String(extra) }) : null;
}

// ---------------------------------------------------------------------------------------------------------------
// Policy / descriptor binding, manifest, convergence decision
// ---------------------------------------------------------------------------------------------------------------

export interface LivePolicy {
  source_identity_sha256: string;
  control_epoch: string;
  privacy_baseline_sha256: string;
}
export interface CutoverStatus {
  phase?: unknown;
  epoch?: unknown;
  privacyBaselineSha256?: unknown;
  [k: string]: unknown;
}
export const LIVE_POLICY = "SELECT source_identity_sha256,control_epoch,privacy_baseline_sha256 FROM sessions.ingest_lazy_policy WHERE singleton";

export async function neonFrozenStatus(n: Queryable): Promise<CutoverStatus> {
  const st = (await n.query("SELECT sessions.delivery_cutover_status() AS s")).rows[0]?.s as CutoverStatus | undefined;
  if (st?.phase !== "active") refuse("frozen_copy_neon_not_frozen");
  return st as CutoverStatus;
}

/**
 * The live Supabase policy must equal the Neon cutover descriptor (epoch, privacy baseline) and, when `pre` is given,
 * the policy and descriptor read before verification (no rebind/re-epoch/new source identity mid-seal). The privacy
 * baseline is compared for EQUALITY only; the sealer never recomputes it.
 */
export function policyRefusal(policy: LivePolicy | undefined, status: CutoverStatus, pre?: { policy: LivePolicy; status: CutoverStatus }): string | null {
  if (!policy) return "frozen_copy_policy_missing";
  if (policy.control_epoch !== status.epoch || (pre && (pre.policy.control_epoch !== policy.control_epoch || pre.status.epoch !== status.epoch))) return "frozen_copy_stale_epoch";
  if (policy.privacy_baseline_sha256 !== status.privacyBaselineSha256 || (pre && (pre.policy.privacy_baseline_sha256 !== policy.privacy_baseline_sha256 || pre.status.privacyBaselineSha256 !== status.privacyBaselineSha256)))
    return "frozen_copy_stale_privacy_baseline";
  if (pre && pre.policy.source_identity_sha256 !== policy.source_identity_sha256) return "frozen_copy_stale_source_identity";
  return null;
}

/** What a completeness seal verified, in manifest order. The privacy baseline is NOT in this list (equality only). */
export const SEAL_VERIFIED = ["records", "receipts", "deletions", "resets", "control_heads"] as const;
export const MANIFEST_VERSION = "frozen-source-manifest-v2";

export interface Manifest {
  text: string;
  sha256: string;
  orgPrivacy: Map<string, Digest>;
  totals: { orgs: number; records: bigint; receipts: bigint; deletions: Digest; resets: Digest; controlHeads: Digest };
}

export function buildManifest(orgs: OrgVerification[], side: PrivacySide): Manifest {
  const seen = new Set<string>();
  for (const o of orgs) {
    if (seen.has(o.org)) refuse("frozen_copy_duplicate_org", { org: o.org });
    seen.add(o.org);
  }
  const orgPrivacy = new Map<string, Digest>();
  const lines = [MANIFEST_VERSION];
  for (const o of orgs) {
    const p = side.byOrg.get(o.org) ?? emptyDigest();
    orgPrivacy.set(o.org, p);
    lines.push(["org", JSON.stringify(o.org), "records", o.records.n, o.records.sum, "receipts", o.receipts.n, o.receipts.sum, "privacy", p.n, p.sum].join("\t"));
  }
  const controlHeads = digestOf(side.heads);
  lines.push(`deletions\t${side.deletions.n}\t${side.deletions.sum}`, `resets\t${side.resets.n}\t${side.resets.sum}`, `control_heads\t${controlHeads.n}\t${controlHeads.sum}`);
  const text = lines.join("\n") + "\n";
  return {
    text,
    sha256: createHash("sha256").update(text).digest("hex"),
    orgPrivacy,
    totals: {
      orgs: orgs.length,
      records: orgs.reduce((a, o) => a + o.records.n, 0n),
      receipts: orgs.reduce((a, o) => a + o.receipts.n, 0n),
      deletions: side.deletions,
      resets: side.resets,
      controlHeads,
    },
  };
}

/** The completeness row's neon_snapshot: exactly what was verified, and what was only compared. */
export function sealSnapshot(status: CutoverStatus, m: Manifest) {
  return {
    cutover: status,
    sealedFrom: "neon-frozen",
    manifestVersion: MANIFEST_VERSION,
    verified: [...SEAL_VERIFIED],
    receiptRows: String(m.totals.receipts),
    deletionRows: String(m.totals.deletions.n),
    resetRows: String(m.totals.resets.n),
    privacyRows: String(m.totals.deletions.n + m.totals.resets.n),
    controlHeads: String(m.totals.controlHeads.n),
    privacyBaseline: "equal to the Neon cutover descriptor; not recomputed",
  };
}

export interface PreRead {
  status: CutoverStatus;
  policy: LivePolicy;
  neon: PrivacySide;
  supa: PrivacySide;
}

/**
 * Convergence decision inside the fence. Supabase heads, Neon heads and both pre-verification reads must be identical
 * (any difference: the mirror or its Neon source advanced since verification began); Neon and Supabase privacy must
 * agree, and equal the pre-verification read.
 */
export function fenceRefusal(pre: PreRead, neon: PrivacySide, supa: PrivacySide): string | null {
  if (!sameList(supa.heads, neon.heads) || !sameList(supa.heads, pre.supa.heads) || !sameList(neon.heads, pre.neon.heads)) return "frozen_copy_mirror_advanced";
  const parity = privacyParityRefusal(neon, supa);
  if (parity) return parity;
  if (!sameDigest(neon.deletions, pre.neon.deletions) || !sameDigest(neon.resets, pre.neon.resets)) return "frozen_copy_privacy_advanced";
  return null;
}

// ---------------------------------------------------------------------------------------------------------------
// 3. Pre-verification read and the seal fence
// ---------------------------------------------------------------------------------------------------------------

/** Read before any per-org verification: Neon in one REPEATABLE READ READ ONLY snapshot, then Supabase. */
export async function preRead(neon: Queryable, supa: Queryable): Promise<PreRead> {
  await neon.query("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY");
  let status: CutoverStatus, n: PrivacySide;
  try {
    status = await neonFrozenStatus(neon);
    n = await readPrivacySide(neon, "neon");
  } finally {
    await neon.query("ROLLBACK");
  }
  const sp = await readPrivacySide(supa, "supabase");
  const policy = (await supa.query(LIVE_POLICY)).rows[0] as LivePolicy | undefined;
  const parity = privacyParityRefusal(n, sp);
  if (parity) refuse(parity);
  const pol = policyRefusal(policy, status);
  if (pol) refuse(pol);
  return { status, policy: policy!, neon: n, supa: sp };
}

/** The privacy-authority key: every guarded writer takes it SHARED per statement (20261105041000). */
export const AUTHORITY_LOCK = [1919249529, 96] as const;
export const FENCED_TABLES = [
  "sessions.ingest_source_control_heads",
  "sessions.ingest_frozen_source_deletions",
  "sessions.ingest_frozen_source_resets",
  "sessions.ingest_frozen_source_records",
] as const;
/** Each fenced table must carry an enabled guard trigger whose function takes exactly the key the sealer holds. */
export const FENCE_GUARD_CHECK = `SELECT count(DISTINCT t.tgrelid)::int AS n FROM pg_trigger t JOIN pg_proc p ON p.oid=t.tgfoid
 WHERE t.tgname='ingest_privacy_authority_guard' AND NOT t.tgisinternal AND t.tgenabled IN ('O','A')
 AND p.prosrc LIKE '%pg_advisory_xact_lock_shared(${AUTHORITY_LOCK[0]},${AUTHORITY_LOCK[1]})%'
 AND t.tgrelid = ANY(ARRAY[${FENCED_TABLES.map((t) => `to_regclass('${t}')::oid`).join(",")}])`;

const ORG_SEAL = `INSERT INTO sessions.ingest_frozen_source_org_seals(org_id,record_rows,record_sum,privacy_rows,privacy_sum) VALUES($1,$2,$3,$4,$5)
 ON CONFLICT (org_id) DO UPDATE SET record_rows=EXCLUDED.record_rows,record_sum=EXCLUDED.record_sum,privacy_rows=EXCLUDED.privacy_rows,privacy_sum=EXCLUDED.privacy_sum`;
const COMPLETENESS = `INSERT INTO sessions.ingest_frozen_source_completeness(source_identity_sha256,control_epoch,privacy_baseline_sha256,org_count,record_rows,manifest_sha256,neon_snapshot)
 VALUES($1,$2,$3,$4,$5,$6,$7) ON CONFLICT (singleton) DO NOTHING RETURNING manifest_sha256`;

/** SQLSTATEs the fence turns into refusals (never retried). */
export function fenceErrorRefusal(error: unknown): FrozenCopyRefusal | null {
  if (error instanceof FrozenCopyRefusal) return error;
  const code = (error as { code?: unknown })?.code;
  if (code === "40001" || code === "40P01") return new FrozenCopyRefusal("frozen_copy_serialization_failure");
  if (code === "55P03") return new FrozenCopyRefusal("frozen_copy_fence_busy");
  return null;
}

export interface SealResult {
  manifestSha256: string;
  inserted: boolean;
  orgs: number;
  records: string;
  deletions: string;
  resets: string;
  controlHeads: string;
}

/**
 * Writes the org seals and the completeness seal in ONE transaction, or nothing. Order:
 *   1. session-level EXCLUSIVE lock on the authority key (waits for in-flight guarded writers; lock_timeout refuses);
 *   2. Supabase BEGIN SERIALIZABLE (its snapshot is taken after the lock, so it sees every committed writer);
 *   3. owner identity, guard presence/key, authority not 'active';
 *   4. Neon BEGIN REPEATABLE READ READ ONLY: phase active, privacy rows and heads (one snapshot);
 *   5. Supabase privacy rows, heads and policy; policy == descriptor == pre-read; fenceRefusal;
 *   6. org seals + completeness; Neon ROLLBACK (read only); Supabase COMMIT.
 * Any refusal or error: ROLLBACK both, release the key, throw. A serialization failure is a refusal, not a retry.
 */
export async function sealFence(
  neon: Queryable,
  supa: Queryable,
  input: { orgs: OrgVerification[]; pre: PreRead },
  deps: { requireOwner?: (c: Queryable) => Promise<void>; budget?: () => void } = {},
): Promise<SealResult> {
  const requireOwner = deps.requireOwner ?? requireFrozenCopyOwner;
  const budget = deps.budget ?? (() => {});
  let locked = false, neonOpen = false;
  try {
    budget();
    await supa.query("SELECT pg_advisory_lock($1,$2)", [...AUTHORITY_LOCK]);
    locked = true;
    await supa.query("BEGIN ISOLATION LEVEL SERIALIZABLE");
    await requireOwner(supa);
    if ((await supa.query(FENCE_GUARD_CHECK)).rows[0]?.n !== FENCED_TABLES.length) refuse("frozen_copy_fence_unavailable");
    if ((await supa.query("SELECT state FROM sessions.ingest_privacy_authority WHERE singleton")).rows[0]?.state === "active") refuse("frozen_copy_privacy_authority_active");

    await neon.query("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY");
    neonOpen = true;
    const status = await neonFrozenStatus(neon);
    const n = await readPrivacySide(neon, "neon");
    const sp = await readPrivacySide(supa, "supabase");
    const policy = (await supa.query(LIVE_POLICY)).rows[0] as LivePolicy | undefined;
    const pol = policyRefusal(policy, status, input.pre);
    if (pol) refuse(pol);
    const conv = fenceRefusal(input.pre, n, sp);
    if (conv) refuse(conv);

    const m = buildManifest(input.orgs, sp);
    for (const o of input.orgs) {
      const p = m.orgPrivacy.get(o.org)!;
      await supa.query(ORG_SEAL, [o.org, String(o.records.n), String(o.records.sum), String(p.n), String(p.sum)]);
    }
    const ins = await supa.query(COMPLETENESS, [policy!.source_identity_sha256, policy!.control_epoch, policy!.privacy_baseline_sha256, m.totals.orgs, String(m.totals.records), m.sha256, JSON.stringify(sealSnapshot(status, m))]);
    const inserted = ins.rows.length === 1;
    if (!inserted) {
      // An existing completeness seal is immutable evidence: it must be for exactly this manifest.
      const prior = (await supa.query("SELECT manifest_sha256 FROM sessions.ingest_frozen_source_completeness WHERE singleton")).rows[0]?.manifest_sha256;
      if (prior !== m.sha256) refuse("frozen_copy_completeness_conflict");
    }
    await neon.query("ROLLBACK");
    neonOpen = false;
    // Last budget check: past this point the job either commits or reports that it does not know.
    budget();
    try {
      await supa.query("COMMIT");
    } catch (error) {
      if (!hasSqlState(error)) throw new FrozenCopyUnknownCommit({ phase: "seal", dataset: null, checkpointSeq: null, manifestSha256: m.sha256, detectedAt: Date.now() });
      throw error;
    }
    return {
      manifestSha256: m.sha256,
      inserted,
      orgs: m.totals.orgs,
      records: String(m.totals.records),
      deletions: String(m.totals.deletions.n),
      resets: String(m.totals.resets.n),
      controlHeads: String(m.totals.controlHeads.n),
    };
  } catch (error) {
    if (error instanceof FrozenCopyUnknownCommit) throw error;
    if (locked) await supa.query("ROLLBACK").catch(() => {});
    if (neonOpen) await neon.query("ROLLBACK").catch(() => {});
    throw fenceErrorRefusal(error) ?? error;
  } finally {
    if (locked) await supa.query("SELECT pg_advisory_unlock($1,$2)", [...AUTHORITY_LOCK]).catch(() => {});
  }
}

// ---------------------------------------------------------------------------------------------------------------
// 4. Unknown commits and the wall-clock budget (receipt: frozen-source-copy-progress.ts)
// ---------------------------------------------------------------------------------------------------------------

/** What an in-doubt COMMIT was about (in memory only, for the readback; the receipt drops `org`). */
export interface InDoubt {
  phase: "copy" | "seal";
  dataset: "records" | "receipts" | null;
  checkpointSeq: number | null;
  org?: string;
  manifestSha256?: string;
  detectedAt: number;
}

/**
 * A COMMIT (page + checkpoint, or the seal) was sent and no server answer came back: it may or may not have committed.
 * Never retried in-process; the receipt says UNKNOWN_COMMIT until the target readback reconciles it.
 */
export class FrozenCopyUnknownCommit extends Error {
  readonly code = "frozen_copy_unknown_commit";
  readonly during: "copy" | "seal";
  constructor(readonly context: InDoubt) {
    super("frozen_copy_unknown_commit");
    this.name = "FrozenCopyUnknownCommit";
    this.during = context.phase;
  }
}

/** A server answer carries a SQLSTATE; a lost connection / socket error / timeout does not. */
export const hasSqlState = (error: unknown) => typeof (error as { code?: unknown })?.code === "string" && /^[0-9A-Z]{5}$/.test((error as { code: string }).code);

/** Run one write; an error WITHOUT a server answer becomes unknown-commit (a SQLSTATE means the server refused it). */
export async function guardedWrite<T>(context: Omit<InDoubt, "detectedAt">, write: () => Promise<T>, now: () => number = Date.now): Promise<T> {
  try {
    return await write();
  } catch (error) {
    if (hasSqlState(error)) throw error;
    throw new FrozenCopyUnknownCommit({ ...context, detectedAt: now() });
  }
}

export const DEFAULT_BUDGET_MS = 6 * 60 * 60 * 1000;
/** Wall-clock budget. `check()` refuses (frozen_copy_budget_exhausted) once the deadline passed; callers check it at
 * every org and page boundary and before the seal's lock and COMMIT, so the abort point is deterministic. */
export function jobBudget(budgetMs: number, now: () => number = Date.now) {
  if (!Number.isSafeInteger(budgetMs) || budgetMs < 0) refuse("frozen_copy_budget_invalid");
  const startedAt = now(), deadline = startedAt + budgetMs;
  return {
    startedAt,
    deadline,
    check() {
      if (now() >= deadline) refuse("frozen_copy_budget_exhausted", { budgetMs: String(budgetMs) });
    },
  };
}
