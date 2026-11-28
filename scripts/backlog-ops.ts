/**
 * The one credential-scoped step of .github/workflows/backlog-ops.yml (contract: src/session-ingest/backlog-ops.ts).
 *
 * Before reading any credential it re-parses the dispatch (dormant switch, typed bindings), checks the checkout is
 * the dispatched commit, checks the protected target variables (bucket, region, KMS key, the two role ARNs) and
 * checks every bound run's downloaded receipt (backfill complete; export failed = fence_late = orgs.incomplete = 0).
 * Only then does it exchange the job's GitHub OIDC token for a role session (the read-only role for a dry run, the
 * writer role for an execute; STS AssumeRoleWithWebIdentity, nothing on disk) and run the tool as a child with fixed
 * argv and an allowlisted environment: the AWS session in the child's environment (the tools read AWS_* only from
 * there), the database URL only on the child's stdin pipe as {"dbUrl": ...} (backfill and export; the drain gets no
 * database), never argv, a file or a log. The owner URL is the environment's SUPABASE_MIGRATION_OWNER_URL, checked
 * against the pinned release connection (releaseConnection) and handed to the tool in one normal form
 * (sslmode=verify-full; ownerClientConfig pins TLS to the Supabase CA itself and never forwards channel_binding).
 *
 * presence-dry (dry only) also reads the dry environment's secret SESSION_PRESENCE_SESSION (backlogOpsPresenceSession:
 * one lowercase UUID or [A-Za-z0-9_-]{8,128} token, refused with fixed codes otherwise) and hands it to the tool only
 * in the same stdin payload, {"dbUrl": ..., "session": ...}. The value is never printed, masked by an echoed command,
 * put in argv or written to the receipt; every other mode refuses a non-empty value.
 *
 * A catch-up (backfill-catchup-*, or a drain's BACKFILL that is one) also reads BACKLOG_OPS_REVISIONS_SINCE_MS, the
 * gate's step output t (FULL's created_at minus the margin; backlogOpsRevisionsSince), passes it as
 * --revisions-since-ms, and refuses unless every bound catch-up receipt recorded the same t (it bound the same FULL).
 *
 * Every S3 tool child (not presence-dry) also gets INGEST_LEDGER_HOME_GUARD=on, always (the export never reads the marker,
 * so it has no effect there); the backfill and the drain also get INGEST_LEDGER_BUCKET /
 * INGEST_LEDGER_REGION from the protected variables BACKLOG_OPS_LEDGER_BUCKET / BACKLOG_OPS_LEDGER_REGION when set
 * (backlogOpsLedgerEnv; docs/ledger-relocation.md): a relocated ledger is written in its own bucket, and a run without
 * those variables refuses (ledger_home_moved) rather than writing the Locker bucket's stale copy. The receipt records
 * the ledger target as a hash (backlogOpsLedgerId, never the name), and a backfill or drain execute refuses unless the
 * backfill / drain runs it binds recorded the same one (backlog_ops_bound_<kind>_ledger).
 *
 * Path B (backlogOpsNoHeadParity; default off): the export modes read the secret BACKLOG_OPS_NO_HEAD_PARITY_ORGS and the
 * variable BACKLOG_OPS_NO_HEAD_PARITY_MAX_AGE_SECONDS; the list reaches the export only in its stdin payload
 * ({"dbUrl": ..., "noHeadParityOrgs": [...]}, each org also masked), the age as --no-head-parity-max-age. The receipt
 * records their hash (`no_head_parity`, only when set). Before any credential, an export-execute refuses unless its DRY
 * recorded the same hash, and a drain-execute unless its EXPORT recorded the hash of its own environment's values
 * (refusal only: the drain tool gets neither value and is unchanged).
 *
 * The child's single stdout summary line becomes a counts-only receipt (any non-code string refuses it), written to
 * $RUNNER_TEMP/backlog-ops-out and echoed to the log. The step's exit code is the tool's.
 */
import { spawn } from "node:child_process";
import { createHash } from "node:crypto";
import { existsSync, mkdirSync, readFileSync, renameSync, writeFileSync } from "node:fs";
import { dirname, join } from "node:path";
import { fileURLToPath } from "node:url";
import { releaseConnection } from "../src/hoopdb/schema-release-contract.ts";
import {
  BACKLOG_OPS_CATCHUP_MARGIN_MS,
  BACKLOG_OPS_RECEIPT_FILE,
  BACKLOG_OPS_SESSION_SECONDS,
  backlogOpsBindingMaxAgeMs,
  backlogOpsBindings,
  backlogOpsDispatch,
  BACKLOG_OPS_LEDGER_ID_TOOLS,
  BACKLOG_OPS_NO_HEAD_PARITY_NONE,
  backlogOpsCloudflareToken,
  backlogOpsAuthorityReceipt,
  backlogOpsHomeId,
  backlogOpsNoHeadParity,
  backlogOpsLedgerEnv,
  backlogOpsLedgerId,
  backlogOpsLedgerTarget,
  backlogOpsPresenceSession,
  backlogOpsReceipt,
  backlogOpsRevisionsSince,
  backlogOpsRole,
  backlogOpsScope,
  backlogOpsSessionName,
  backlogOpsTarget,
  backlogOpsToolArgs,
  checkBoundReceipt,
  countsOnly,
  checkRefreshFingerprint,
  checkStsExpiration,
  isBridge,
  isCatchup,
  isMarker,
  isExecute,
  parseStsCredentials,
  toolOf,
  refuseBacklogOps as refuse,
  type BacklogOpsDispatch,
} from "../src/session-ingest/backlog-ops.ts";
import { checkAuthorityDryReceipt, checkBaselineReceipt, checkReshardReceipt } from "../src/session-ingest/backlog-authority.ts";
import { authorityReceiptSummary, type AuthorityReceiptKind } from "../src/session-ingest/backlog-authority-schema.ts";
import { checkMarkerDryReceipt, markerBoundFrom, markerReceiptSummary, receiptSha256, type MarkerBound } from "../src/session-ingest/marker-publication.ts";
import { isWaveReceipt, mergeBaselines, orgScopeId, waveConcurrencyFor, waveReceiptSummary, waveSummary, waveView } from "../src/session-ingest/backlog-wave.ts";
import { runWave } from "../src/session-ingest/org-wave.ts";
import { evidenceHash } from "../src/session-ingest/authority-evidence.ts";

const ROOT = fileURLToPath(new URL("..", import.meta.url));
/** The only parent variables a child inherits (plus the AWS session the parent adds). */
export const BACKLOG_OPS_CHILD_ENV = ["PATH", "HOME", "LANG", "TMPDIR"] as const;
const STDOUT_LIMIT = 1024 * 1024;

export function backlogOpsPaths(env: Record<string, string | undefined>) {
  const temp = env.RUNNER_TEMP ?? "";
  if (!temp.startsWith("/")) refuse("backlog_ops_runner_temp");
  return {
    bound: (kind: string) => join(temp, "backlog-ops-bound", kind, BACKLOG_OPS_RECEIPT_FILE),
    receipt: join(temp, "backlog-ops-out", BACKLOG_OPS_RECEIPT_FILE),
  };
}

/** The tool's database URL: the pinned release login, re-encoded with only sslmode=verify-full. */
export function toolDbUrl(ownerUrl: string | undefined): string {
  const c = releaseConnection(ownerUrl);
  return `postgresql://${encodeURIComponent(c.user)}:${encodeURIComponent(c.password)}@${c.host}:${c.port}/${encodeURIComponent(c.database)}?sslmode=verify-full`;
}

/** Spawn spec: fixed argv, allowlisted env plus the AWS session, the URL only in the stdin payload. */
export function backlogOpsChildSpawn(
  env: Record<string, string | undefined>,
  d: BacklogOpsDispatch,
  aws: { accessKeyId: string; secretAccessKey: string; sessionToken: string },
  dbUrl: string | null,
  session: string | null = null,
  bound: BoundAuthority = {},
) {
  const since = isCatchup(d.mode) || isBridge(d.mode) ? backlogOpsRevisionsSince(env, d) : null;
  const scope = backlogOpsScope(env);
  // Path B: the export's only (the drain's binding of it is in checkBoundReceipts).
  const nhp = backlogOpsNoHeadParity(env, d, scope), exportNhp = toolOf(d.mode) === "export" ? nhp : null;
  const t = backlogOpsTarget(env), spec = backlogOpsToolArgs(d, t, scope, since, exportNhp);
  if (spec.needsDb !== (dbUrl !== null)) refuse("backlog_ops_db_binding");
  // The session reaches the presence tool, and only it, beside the URL on stdin.
  if ((d.mode === "presence-dry") !== (session !== null) || (session !== null && dbUrl === null)) refuse("backlog_ops_session_binding");
  const childEnv: Record<string, string> = {};
  for (const key of BACKLOG_OPS_CHILD_ENV) if (typeof env[key] === "string") childEnv[key] = env[key]!;
  // INGEST_LEDGER_HOME_GUARD=on for every S3 tool, and the ledger bucket for the ledger tools (backlogOpsLedgerEnv):
  // set here, not inherited, so neither the workflow file nor the allowlist can drop the guard.
  Object.assign(childEnv, backlogOpsLedgerEnv(d, backlogOpsLedgerTarget(env, t)));
  childEnv.AWS_ACCESS_KEY_ID = aws.accessKeyId;
  childEnv.AWS_SECRET_ACCESS_KEY = aws.secretAccessKey;
  childEnv.AWS_SESSION_TOKEN = aws.sessionToken;
  // The authority modes: the Cloudflare token (and, for the execute, the bound baseline and the ledger bucket's name
  // for the sweeper's home check) on stdin. A catch-up bound to an authority-dry: that run's evidence on stdin.
  const cfToken = backlogOpsCloudflareToken(env, d);
  const wantsEvidence = isCatchup(d.mode) && d.authorityRun !== null, wantsBaseline = d.mode === "authority-execute" || isMarker(d.mode), wantsFull = isBridge(d.mode);
  // A marker mode: what the bound receipts proved (and marker-execute's approved wave), beside the baseline.
  const marker = isMarker(d.mode);
  if (marker !== (bound.marker !== undefined) || (d.mode === "marker-execute") !== (bound.wave !== undefined)) refuse("backlog_ops_authority_binding");
  if (wantsFull !== (bound.fullEvidence !== undefined) || (wantsFull && !/^[0-9a-f]{64}$/u.test(bound.fullEvidence!))) refuse("backlog_ops_authority_binding");
  if (wantsEvidence !== (bound.evidence !== undefined) || wantsBaseline !== (bound.baseline !== undefined) || (cfToken !== null && dbUrl === null))
    refuse("backlog_ops_authority_binding");
  let payload: Record<string, unknown> = session !== null ? { dbUrl, session } : exportNhp !== null ? { dbUrl, noHeadParityOrgs: exportNhp.orgs } : { dbUrl };
  if (cfToken !== null) payload = { dbUrl, cloudflareToken: cfToken,
    ...(wantsBaseline ? { baseline: bound.baseline, ledgerBucket: backlogOpsLedgerTarget(env, t)?.bucket ?? null } : {}),
    ...(marker ? { bound: bound.marker, ...(bound.wave ? { wave: bound.wave } : {}) } : {}) };
  if (wantsEvidence) payload = { dbUrl, authorityEvidence: bound.evidence };
  if (wantsFull) payload = { dbUrl, fullEvidence: bound.fullEvidence };
  return {
    command: process.execPath,
    args: [join(ROOT, spec.script), ...spec.args],
    env: childEnv,
    input: dbUrl === null ? null : JSON.stringify(payload) + "\n",
  };
}

/**
 * Every bound run's receipt, from the artifacts the workflow downloaded, before any credential: same org scope, the
 * binding's mode (a refresh or a catch-up only where the binding is one). With FULL, the drain's refresh must carry
 * FULL's input fingerprint (checkRefreshFingerprint, backlog_ops_backfill_stale); a catch-up (the drain's BACKFILL, or
 * a catch-up execute's DRY) must have recorded the gate's t (`revisionsSinceMs`), so it bound the same FULL.
 */
/** What the authority bindings hand the tool: an authority-dry's evidence (a catch-up), a catch-up's baseline (authority-execute). */
export interface BoundAuthority { evidence?: unknown; baseline?: unknown; /** A bridge mode: sha256 of the bound FULL receipt's exact bytes. */ fullEvidence?: string;
  /** A marker mode: the sections, reshard verdicts and catch-up evidence the bound receipts proved. */ marker?: MarkerBound;
  /** marker-execute: the approved wave its marker-dry recorded. */ wave?: { id: string; orgs: { orgHash: string; epochHash: string }[] } }
export function checkBoundReceipts(env: Record<string, string | undefined>, d: BacklogOpsDispatch, read = (p: string) => readFileSync(p, "utf8"),
  revisionsSinceMs: number | null = backlogOpsRevisionsSince(env, d), readBytes: (p: string) => Uint8Array = (p: string) => readFileSync(p)): BoundAuthority {
  const paths = backlogOpsPaths(env), scopeFull = backlogOpsScope(env), scope = scopeFull.id;
  // Path B's id from this run's environment: an export-execute's DRY and a drain-execute's EXPORT must have recorded it.
  const nhpId = backlogOpsNoHeadParity(env, d, scopeFull)?.id ?? BACKLOG_OPS_NO_HEAD_PARITY_NONE;
  // The ledger target this run's tool gets (read only when a binding needs it): a backfill or drain must have it equal
  // to the backfill / drain runs it binds (its dry run, the drain's BACKFILL and FULL).
  const ledgerTool = BACKLOG_OPS_LEDGER_ID_TOOLS.includes(toolOf(d.mode));
  // This run's store-home pair (authority bindings only; backlogOpsRun validated the target before this runs).
  const homeId = () => backlogOpsHomeId({ bucket: env.BACKLOG_OPS_BUCKET ?? "", region: env.BACKLOG_OPS_REGION ?? "" },
    backlogOpsLedgerTarget(env, { bucket: env.BACKLOG_OPS_BUCKET ?? "" }));
  // Which bound receipts must attest this run's home: every authority-execute binding (FULL included: every backfill
  // receipt records it), a catch-up's AUTHORITY, and a reshard-dry's catch-up.
  const authorityRun = (b: { kind: string }, dd: BacklogOpsDispatch) => dd.mode === "authority-execute" || b.kind === "authority" || dd.mode === "reshard-dry" || isBridge(dd.mode) || isMarker(dd.mode);
  const authority: BoundAuthority = {};
  // The per-org bindings need the scope's org list (their receipts carry org hashes, never ids).
  const orgs = scopeFull.orgs;
  if ((d.mode === "authority-execute" || d.authorityRun !== null || isMarker(d.mode)) && orgs === null) refuse("backlog_ops_scope_required");
  let ledgerId: string | undefined;
  // (Only the Locker bucket's name is needed here; backlogOpsRun validates the whole target before this runs.)
  const ledger = () => (ledgerId ??= backlogOpsLedgerId(d, backlogOpsLedgerTarget(env, { bucket: env.BACKLOG_OPS_BUCKET ?? "" })));
  const raws: Partial<Record<string, string>> = {};
  const modes: Partial<Record<string, string>> = {};
  let catchupEvidence: string | undefined;
  for (const b of backlogOpsBindings(d)) {
    const path = paths.bound(b.kind);
    let raw: string;
    try { raw = read(path); } catch { return refuse(`backlog_ops_bound_${b.kind}_missing`); }
    // The drain's BACKFILL and FULL, and a catch-up's FULL: exactly the run's scope (never an unscoped run standing in).
    const exact = (d.mode === "drain-execute" && (b.kind === "backfill" || b.kind === "full")) || (isCatchup(d.mode) && b.kind === "full") ||
      d.mode === "authority-execute" || b.kind === "authority" || d.mode === "reshard-dry" || isBridge(d.mode) || isMarker(d.mode);
    // The drain's BACKFILL with FULL: a refresh or a catch-up, whichever the receipt says (the gate checked its run-name).
    let mode = b.mode;
    if (b.altMode !== undefined) {
      try { if (JSON.parse(raw)?.mode === b.altMode) mode = b.altMode; } catch { /* checkBoundReceipt refuses it */ }
    }
    const bindsLedger = ledgerTool && BACKLOG_OPS_LEDGER_ID_TOOLS.includes(toolOf(mode));
    const bindsNhp = (d.mode === "export-execute" && b.kind === "dry") || (d.mode === "drain-execute" && b.kind === "export");
    checkBoundReceipt(raw, { mode, sha: d.sha, run: b.run, scope, kind: b.kind, exact, ...(bindsLedger ? { ledger: ledger() } : {}),
      ...(bindsNhp ? { noHeadParity: nhpId } : {}) });
    // (A reshard-dry binds a catch-up without its FULL, so without t: it checks the catch-up's completion, not its t.)
    if (isCatchup(mode) && d.mode !== "reshard-dry" && (revisionsSinceMs === null || JSON.parse(raw).summary.revisions_since_ms !== revisionsSinceMs))
      refuse(`backlog_ops_bound_${b.kind}_since`);
    raws[b.kind] = raw;
    modes[b.kind] = mode;
    // The authority bindings were produced against this run's store-home pair (Locker and ledger), exactly.
    if (authorityRun(b, d)) {
      let home: unknown;
      try { home = JSON.parse(raw).home; } catch { home = undefined; }
      if (home !== homeId()) refuse(`backlog_ops_bound_${b.kind}_home`);
    }
    // The per-org content of the authority bindings (backlog-authority.ts).
    // (A marker mode checks the reshard per org below: one org's failed verify refuses that org only.)
    if (b.kind === "reshard" && !isMarker(d.mode)) checkReshardReceipt(raw, orgs!, d.backfillRun!);
    if (b.kind === "authority" && isMarker(d.mode)) checkAuthorityDryReceipt(raw, orgs!);
    else if (b.kind === "authority") authority.evidence = checkAuthorityDryReceipt(raw, orgs!);
    if (isMarker(d.mode) && b.kind === "backfill") {
      authority.baseline = checkBaselineReceipt(raw, orgs!, d.authorityRun!, d.fullBackfillRun!);
      // The marker attempt's evidence: the catch-up receipt's exact downloaded bytes, which must be what was checked.
      const bytes = readBytes(path);
      if (Buffer.compare(Buffer.from(bytes), Buffer.from(raw, "utf8")) !== 0) refuse("backlog_ops_bound_backfill_receipt");
      catchupEvidence = receiptSha256(bytes);
    }
    if (d.mode === "authority-execute" && b.kind === "dry") checkAuthorityDryReceipt(raw, orgs!);
    if (d.mode === "authority-execute" && b.kind === "backfill") authority.baseline = checkBaselineReceipt(raw, orgs!, d.dryRun!, d.fullBackfillRun!);
    // A bridge mode: the bound FULL receipt's sha256 (over its exact downloaded bytes), compared by the tool to the
    // 062340 binding's selected_full.evidence_sha256 (the seal's evidence).
    if (isBridge(d.mode) && b.kind === "full") {
      // Over the file's exact bytes (read without decoding), and those bytes must be what was parsed and checked.
      const bytes = readBytes(path);
      if (Buffer.compare(Buffer.from(bytes), Buffer.from(raw, "utf8")) !== 0) refuse("backlog_ops_bound_full_receipt");
      authority.fullEvidence = createHash("sha256").update(bytes).digest("hex");
    }
  }
  if (isMarker(d.mode)) {
    if (raws.full === undefined || raws.backfill === undefined || raws.reshard === undefined || catchupEvidence === undefined) refuse("backlog_ops_bound_full_missing");
    authority.marker = markerBoundFrom(raws.full, raws.backfill, raws.reshard, orgs!, d.backfillRun!, catchupEvidence);
    if (d.mode === "marker-execute") {
      if (raws.dry === undefined) refuse("backlog_ops_bound_dry_missing");
      authority.wave = checkMarkerDryReceipt(raws.dry, orgs!,
        { authority: d.authorityRun!, backfill: d.backfillRun!, full: d.fullBackfillRun!, reshard: d.reshardRun! }, d.waveId!);
    }
  }
  if (d.mode === "drain-execute" && d.fullBackfillRun !== null) {
    if (raws.full === undefined || raws.backfill === undefined) refuse("backlog_ops_bound_full_missing");
    // A catch-up's fingerprint differs from FULL's by design (the org uploaded during FULL); it re-read what changed.
    if (modes.backfill !== "backfill-catchup-execute") checkRefreshFingerprint(raws.full, raws.backfill);
  }
  return authority;
}

/** GitHub OIDC token (audience sts.amazonaws.com) for STS: in memory only. */
async function oidcToken(env: Record<string, string | undefined>, fetcher: typeof fetch): Promise<string> {
  const url = env.ACTIONS_ID_TOKEN_REQUEST_URL, bearer = env.ACTIONS_ID_TOKEN_REQUEST_TOKEN;
  if (!url || !bearer || !url.startsWith("https://")) refuse("backlog_ops_oidc_unavailable");
  const res = await fetcher(url + (url.includes("?") ? "&" : "?") + "audience=sts.amazonaws.com", {
    headers: { Authorization: `bearer ${bearer}`, Accept: "application/json" },
  });
  if (!res.ok) refuse("backlog_ops_oidc_failed");
  const value = ((await res.json()) as { value?: unknown }).value;
  if (typeof value !== "string" || value.split(".").length !== 3) refuse("backlog_ops_oidc_failed");
  return value;
}

export async function assumeRole(
  env: Record<string, string | undefined>,
  roleArn: string,
  region: string,
  sessionName: string,
  fetcher: typeof fetch = fetch,
) {
  const token = await oidcToken(env, fetcher);
  const body = new URLSearchParams({
    Action: "AssumeRoleWithWebIdentity", Version: "2011-06-15", RoleArn: roleArn,
    RoleSessionName: sessionName, WebIdentityToken: token, DurationSeconds: String(BACKLOG_OPS_SESSION_SECONDS),
  });
  const res = await fetcher(`https://sts.${region}.amazonaws.com/`, {
    method: "POST", headers: { "Content-Type": "application/x-www-form-urlencoded" }, body,
  });
  if (!res.ok) refuse("backlog_ops_sts_refused");
  const creds = parseStsCredentials(await res.text());
  checkStsExpiration(creds.expiration);
  return creds;
}

function writePrivate(path: string, value: unknown): void {
  mkdirSync(dirname(path), { recursive: true, mode: 0o700 });
  const temp = path + ".tmp";
  writeFileSync(temp, JSON.stringify(value, null, 2) + "\n", { mode: 0o600 });
  renameSync(temp, path);
}

export type ChildSpec = ReturnType<typeof backlogOpsChildSpawn>;
/** Run the tool child: stdin from the spec, stdout collected (bounded), stderr to the step log. */
function runChild(child: ChildSpec): Promise<{ code: number; stdout: string }> {
  return new Promise((resolve) => {
    const p = spawn(child.command, child.args, { cwd: ROOT, env: child.env, stdio: [child.input === null ? "ignore" : "pipe", "pipe", "inherit"] });
    let out = "";
    p.stdout!.on("data", (chunk: Buffer) => { if (out.length < STDOUT_LIMIT) out += chunk.toString("utf8"); });
    if (child.input !== null) { p.stdin!.on("error", () => undefined); p.stdin!.end(child.input); }
    p.on("error", () => resolve({ code: 1, stdout: "" }));
    p.on("close", (c) => resolve({ code: c ?? 1, stdout: out }));
  });
}

export async function backlogOpsRun(
  env: Record<string, string | undefined>,
  deps: { fetcher?: typeof fetch; runChild?: (child: ChildSpec) => Promise<{ code: number; stdout: string }> } = {},
) {
  const d = backlogOpsDispatch(env);
  if (env.GITHUB_SHA !== d.sha || !/^[1-9][0-9]*$/u.test(env.GITHUB_RUN_ID ?? "")) refuse("backlog_ops_checkout");
  // presence-dry's session id (and, for every other mode, that none was passed), before any credential. Never logged:
  // GitHub masks the secret's value, and no ::add-mask:: line is echoed for it.
  const session = backlogOpsPresenceSession(env, d);
  const scope = backlogOpsScope(env);
  // GitHub masks the secret's whole value; mask each org too, so a tool line naming one org never shows it.
  for (const org of scope.orgs ?? []) console.log(`::add-mask::${org}`);
  // Path B's allow-list and age (shape, scope), before any credential; its orgs masked too.
  const nhp = backlogOpsNoHeadParity(env, d, scope);
  for (const org of nhp?.orgs ?? []) console.log(`::add-mask::${org}`);
  const since = backlogOpsRevisionsSince(env, d);
  // The authority modes' Cloudflare token (shape; and empty for every other mode), before any credential.
  backlogOpsCloudflareToken(env, d);
  const t = backlogOpsTarget(env), spec = backlogOpsToolArgs(d, t, scope, isCatchup(d.mode) || isBridge(d.mode) ? since : null, toolOf(d.mode) === "export" ? nhp : null);
  // The ledger target's shape, before any credential (backlog_ops_target_ledger_bucket / _region), and its receipt id.
  const ledger = backlogOpsLedgerId(d, backlogOpsLedgerTarget(env, t));
  const paths = backlogOpsPaths(env);
  // A per-org wave run (backlog-wave.ts): every org's bindings on their own views; a marker or a reshard-dry that binds a
  // wave receipt checks it per org too. Everything else: the bindings as before.
  const waveRun = d.perOrg === true;
  if (waveRun && scope.orgs === null) refuse("backlog_ops_scope_required");
  if (waveRun && nhp !== null) refuse("backlog_ops_wave_no_head_parity");
  const readText = (p: string) => readFileSync(p, "utf8");
  const boundIsWave = (kind: string) => { try { return isWaveReceipt(readText(paths.bound(kind))); } catch { return false; } };
  let bound: BoundAuthority = {};
  let perOrgBound: Map<string, { bound: BoundAuthority; since: number | null } | string> | null = null;
  // A per-org retry: the orgs its previous run completed are carried, with that run's creation time.
  const carried = waveRun && d.previousRun ? previousSlices(env, d, scope, ledgerIdFor(env, d), since) : new Map<string, { summary: unknown; carriedMs: number }>();
  // The drain's max_objects is a per-RUN cap: a wave splits it across its outstanding orgs (the sum never exceeds it; every org
  // gets at least one object, so a budget below the org count refuses).
  const budgets = waveRun ? waveBudgets(d.maxObjects, scope.orgs!.filter((o) => !carried.has(o))) : null;
  if (waveRun) {
    perOrgBound = new Map(scope.orgs!.filter((o) => !carried.has(o)).map((o): [string, { bound: BoundAuthority; since: number | null } | string] => {
      try { return [o, checkOrgBindingsAt(env, d, o, since)]; } catch (e) { return [o, refusalCode(e)]; }
    }));
  } else if (isMarker(d.mode) && (boundIsWave("backfill") || boundIsWave("full"))) {
    bound = markerWaveBound(env, d, since);
  } else if (d.mode === "reshard-dry" && d.backfillRun !== null && boundIsWave("backfill")) {
    // The catch-up it verifies after is a wave: it must be this wave's, and at least one org's slice must bind (each
    // org's own completion is checked again by whatever binds this reshard-dry).
    let ok = 0;
    for (const o of scope.orgs ?? []) { try { checkOrgBindings(env, d, o, since); ok++; } catch { /* that org only */ } }
    if (ok === 0) refuse("backlog_ops_wave_no_org");
  } else {
    bound = checkBoundReceipts(env, d, undefined, since);
  }
  const nhpId = nhp?.id ?? BACKLOG_OPS_NO_HEAD_PARITY_NONE;
  // Every ledger-id tool's receipt attests the store-home pair (a FULL backfill-execute included).
  const home = BACKLOG_OPS_LEDGER_ID_TOOLS.includes(toolOf(d.mode)) ? backlogOpsHomeId(t, backlogOpsLedgerTarget(env, t)) : null;
  if (existsSync(paths.receipt)) refuse("backlog_ops_receipt_exists");
  // Credentials from here on: the database URL (backfill, export, presence) and the role session.
  const dbUrl = spec.needsDb ? toolDbUrl(env.SUPABASE_MIGRATION_OWNER_URL) : null;
  const aws = await assumeRole(env, backlogOpsRole(d, t), t.region, backlogOpsSessionName(d.mode, env.GITHUB_RUN_ID!), deps.fetcher);
  const run = deps.runChild ?? runChild;
  let receipt;
  let exit: number;
  if (waveRun) {
    const orgs = scope.orgs!;
    const outstanding = orgs.filter((o) => !carried.has(o));
    const settled = outstanding.length === 0 ? [] : await runWave(outstanding, waveConcurrencyFor(d.mode), async (org) => {
      const b = perOrgBound!.get(org)!;
      // A binding refused for this org: its slice records the code, and no child runs for it.
      if (typeof b === "string") return { org, exitCode: 1, summary: { refused: b } };
      // The org's own t (a carried FULL slice moves it), handed to its child exactly as the gate's t would be.
      const envOrg = { ...env, BACKLOG_OPS_ORGS: org, ...(b.since !== null ? { BACKLOG_OPS_REVISIONS_SINCE_MS: String(b.since) } : {}) };
      const dOrg: BacklogOpsDispatch = { ...d, maxObjects: budgets === null ? null : budgets.get(org)! };
      const r = await run(backlogOpsChildSpawn(envOrg, dOrg, aws, dbUrl, null, b.bound));
      const one = finalReceipt(d, env.GITHUB_RUN_ID!, r.code, r.stdout, orgScopeId(org), [org], ledger, nhpId, home, b.since);
      return { org, exitCode: one.exitCode === 0 && r.code === 0 ? 0 : one.exitCode || r.code || 1, summary: one.summary };
    }, { fallback: "backlog_ops_wave_org_failed" });
    const ran = new Map(settled.map((x) => [x.org, x.ok ? x.value : { org: x.org, exitCode: 1, summary: { refused: x.code } }]));
    const slices = orgs.map((o) => carried.has(o) ? { org: o, exitCode: 0, summary: carried.get(o)!.summary, carriedMs: carried.get(o)!.carriedMs } : ran.get(o)!);
    const checked = waveReceiptSummary(waveSummary(orgs, slices), orgs, (sl) => waveSliceCheck(d, sl));
    const sumC = checked.summary as { ok_count?: number };
    // An execute is complete only when EVERY org completed: an incomplete one exits nonzero (its receipt keeps every
    // slice, so a retry with previous_run runs only the outstanding orgs). A dry run may show an eligible subset.
    const complete = checked.ok && (!isExecute(d.mode) || sumC.ok_count === orgs.length);
    receipt = { ...backlogOpsReceipt(d, env.GITHUB_RUN_ID!, 0, "{}", scope.id, ledger, nhpId, home),
      bindings: waveBindings(d), ...(d.previousRun ? { previous_run: d.previousRun } : {}), summary: checked.summary, exitCode: complete ? 0 : 1 };
    exit = receipt.exitCode;
    // A green wave run may still have refused orgs: say how many completed (counts only, never an org).
    const sum = checked.summary as { ok_count?: number; org_count?: number };
    console.log(`backlog_ops_wave_result ok_count=${sum.ok_count ?? 0} org_count=${sum.org_count ?? orgs.length}` +
      ((sum.ok_count ?? 0) < orgs.length ? " incomplete" : " complete"));
  } else {
    const { code, stdout } = await run(backlogOpsChildSpawn(env, d, aws, dbUrl, session, bound));
    receipt = finalReceipt(d, env.GITHUB_RUN_ID!, code, stdout, scope.id, scope.orgs, ledger, nhpId, home, since);
    exit = receipt.exitCode === 0 && code === 0 ? 0 : 1;
  }
  writePrivate(paths.receipt, receipt);
  console.log("backlog_ops_receipt " + JSON.stringify(receipt));
  return exit;
}

/** The runs a wave receipt's dispatch bound, by kind (a per-org retry must bind exactly the same). */
export const waveBindings = (d: BacklogOpsDispatch) => Object.fromEntries(backlogOpsBindings(d).map((b) => [b.kind, b.run]));
const ledgerIdFor = (env: Record<string, string | undefined>, d: BacklogOpsDispatch) =>
  backlogOpsLedgerId(d, backlogOpsLedgerTarget(env, backlogOpsTarget(env)));

/**
 * A per-org retry's previous run (input previous_run; the gate checked it is this execute at this sha, failed, recent,
 * created after every binding, and wrote previous_created_ms): its receipt must be a wave receipt of exactly this
 * mode, sha, run, scope, ledger and home, with exactly this dispatch's bindings, not complete. Returns the orgs it
 * completed, each with the time it really ran (its own carried_ms, or the previous run's creation).
 */
export function previousSlices(env: Record<string, string | undefined>, d: BacklogOpsDispatch, scope: { orgs: string[] | null; id: string }, ledger: string,
  _since: number | null, read: (p: string) => string = (p) => readFileSync(p, "utf8")): Map<string, { summary: unknown; carriedMs: number }> {
  const raw = (() => { try { return read(backlogOpsPaths(env).bound("previous")); } catch { return refuse("backlog_ops_bound_previous_missing"); } })();
  const createdRaw = env.BACKLOG_OPS_PREVIOUS_CREATED_MS ?? "";
  if (!/^[1-9][0-9]{0,15}$/u.test(createdRaw)) refuse("backlog_ops_previous_created");
  const createdMs = Number(createdRaw);
  let r: Record<string, any>;
  try { r = JSON.parse(raw); } catch { return refuse("backlog_ops_bound_previous_receipt"); }
  const t = backlogOpsTarget(env);
  const home = BACKLOG_OPS_LEDGER_ID_TOOLS.includes(toolOf(d.mode)) ? backlogOpsHomeId(t, backlogOpsLedgerTarget(env, t)) : undefined;
  if (!r || r.contract !== "backlog-ops-receipt/v1" || r.mode !== d.mode || r.sha !== d.sha || r.run !== d.previousRun || r.scope !== scope.id ||
      r.ledger !== ledger || r.home !== home || r.exitCode === 0 || JSON.stringify(r.bindings) !== JSON.stringify(waveBindings(d)))
    refuse("backlog_ops_bound_previous_receipt");
  const checked = waveReceiptSummary(r.summary, scope.orgs, (sl) => waveSliceCheck(d, sl));
  if ((checked.summary as { refused?: unknown }).refused !== undefined && !Array.isArray((checked.summary as { per_org?: unknown }).per_org))
    refuse("backlog_ops_bound_previous_receipt");
  const per = (checked.summary as { per_org: { org_hash: string; exit_code: number; summary: unknown; carried_ms?: number }[] }).per_org;
  const byHash = new Map(per.map((p) => [p.org_hash, p]));
  const out = new Map<string, { summary: unknown; carriedMs: number }>();
  for (const org of scope.orgs ?? []) {
    const p = byHash.get(evidenceHash("org", org));
    if (p && p.exit_code === 0 && p.summary && typeof p.summary === "object" && !("refused" in (p.summary as object)))
      out.set(org, { summary: p.summary, carriedMs: p.carried_ms ?? createdMs });
  }
  return out;
}

/**
 * A wave's per-org share of the drain's max_objects: null when unbounded; otherwise floor(max / n) each, the remainder
 * to the first orgs, so the shares sum to exactly max. Refuses backlog_ops_wave_max_objects when max < n.
 */
export function waveBudgets(max: number | null, orgs: readonly string[]): Map<string, number> | null {
  if (max === null) return null;
  if (!Number.isSafeInteger(max) || max < orgs.length) refuse("backlog_ops_wave_max_objects");
  const base = Math.floor(max / orgs.length), extra = max - base * orgs.length;
  return new Map(orgs.map((o, i) => [o, base + (i < extra ? 1 : 0)]));
}

const refusalCode = (e: unknown) => { const c = (e as { code?: unknown })?.code; return typeof c === "string" && /^backlog_ops_[a-z0-9_]{1,52}$/u.test(c) ? c : "backlog_ops_wave_binding"; };

/** Exact schemas: every authority receipt, and every backfill receipt (all backfill modes share its producer). */
function schemaKindOf(d: BacklogOpsDispatch): AuthorityReceiptKind | null {
  return toolOf(d.mode) === "reshard" ? "reshard" : d.mode === "authority-dry" ? "authority_dry"
    : d.mode === "authority-execute" ? "authority_execute" : isCatchup(d.mode) && d.authorityRun !== null ? "catchup" : toolOf(d.mode) === "backfill" ? "backfill"
    : toolOf(d.mode) === "bridge" ? "bridge" : null;
}

/**
 * The tool's stdout as this run's receipt: counts only, then the mode's exact schema (marker, authority, backfill)
 * before it is persisted or logged; a refusal keeps only a code from the mode's closed map.
 */
export function finalReceipt(d: BacklogOpsDispatch, run: string, code: number, stdout: string, scopeId: string, orgs: string[] | null, ledger: string,
  nhpId: string, home: string | null, since: number | null) {
  const schemaKind = schemaKindOf(d);
  let receipt;
  try {
    receipt = backlogOpsReceipt(d, run, code, stdout, scopeId, ledger, nhpId, home);
    if (isMarker(d.mode)) {
      // Exact marker schema, self-consistent for this scope (and, for marker-execute, the approved wave id).
      const r = markerReceiptSummary(receipt.summary, { mode: isExecute(d.mode) ? "execute" : "dry_run", orgs, waveId: d.waveId ?? null });
      receipt = { ...receipt, summary: r.summary, exitCode: r.ok ? receipt.exitCode : receipt.exitCode || 1 };
    } else if (schemaKind !== null) {
      // The bridge's proofs must match the run's scope and the exact gate-derived cutoff the child was given.
      const r = authorityReceiptSummary(schemaKind, receipt.summary, { orgs, cutoffMs: isBridge(d.mode) ? since : null });
      receipt = { ...receipt, summary: r.summary, exitCode: r.ok ? receipt.exitCode : receipt.exitCode || 1 };
    }
  } catch (e) {
    const c = (e as { code?: string }).code;
    receipt = backlogOpsReceipt(d, run, code || 1, JSON.stringify({ refused: typeof c === "string" && c.startsWith("backlog_ops_") ? c : "receipt" }),
      scopeId, ledger, nhpId, home);
  }
  return receipt;
}

/**
 * A wave slice's summary, re-checked by the wave receipt's validator: counts only under the mode's own rules (lists
 * only where that mode's receipt allows them), then the exact schema where the mode has one; it must come back
 * unchanged. A refusal slice keeps only a backlog_ops_* code or one the schema's closed map keeps.
 */
export function waveSliceCheck(d: BacklogOpsDispatch, s: unknown): unknown {
  const c = countsOnly(s, 0, backlogOpsAuthorityReceipt(d));
  const kind = schemaKindOf(d);
  if (c && typeof c === "object" && Object.keys(c).length === 1 && "refused" in (c as object)) {
    const code = (c as { refused: unknown }).refused;
    if (typeof code === "string" && code.startsWith("backlog_ops_")) return c;
    if (kind !== null && JSON.stringify(authorityReceiptSummary(kind, c).summary) === JSON.stringify(c)) return c;
    if (kind === null) return c;
    return refuse("backlog_ops_receipt_wave");
  }
  if (kind !== null) {
    const r = authorityReceiptSummary(kind, c);
    if (!r.ok || JSON.stringify(r.summary) !== JSON.stringify(c)) refuse("backlog_ops_receipt_wave");
  }
  return c;
}

/**
 * One org's bindings, checked by the unchanged checkBoundReceipts on that org's VIEWS of the bound receipts
 * (backlog-wave.ts waveView: the org's slice of a wave receipt, or a non-wave receipt of exactly the wave's scope
 * re-scoped to the org), as a single-org run of the same dispatch. Any failure refuses that org only.
 */
export function checkOrgBindings(env: Record<string, string | undefined>, d: BacklogOpsDispatch, org: string, since: number | null,
  read: (p: string) => string = (p) => readFileSync(p, "utf8"), now = Date.now()): BoundAuthority {
  return checkOrgBindingsAt(env, d, org, since, read, now).bound;
}
/**
 * checkOrgBindings, also returning the org's own t. A slice CARRIED from an earlier run (carried_ms) is checked
 * against the binding's freshness rule from when that run was created (backlogOpsBindingMaxAgeMs), and a carried FULL
 * slice moves this org's t to its own creation minus the margin: the catch-up must read every revision since the FULL
 * that actually ran for the org, never since a later retry.
 */
export function checkOrgBindingsAt(env: Record<string, string | undefined>, d: BacklogOpsDispatch, org: string, since: number | null,
  read: (p: string) => string = (p) => readFileSync(p, "utf8"), now = Date.now()): { bound: BoundAuthority; since: number | null } {
  const waveScope = backlogOpsScope(env).id;
  const envOrg = { ...env, BACKLOG_OPS_ORGS: org };
  const paths = backlogOpsPaths(env);
  const views = new Map<string, string>();
  const view = (p: string) => { let v = views.get(p); if (v === undefined) { v = waveView(read(p), org, waveScope); views.set(p, v); } return v; };
  let orgSince = since;
  for (const b of backlogOpsBindings(d)) {
    let raw: string;
    try { raw = read(paths.bound(b.kind)); } catch { continue; } // a missing file is checkBoundReceipts' refusal
    // The view refuses any bound receipt not scoped exactly to the wave (backlog_ops_wave_scope), named here, not
    // masked as a missing file by the read inside checkBoundReceipts.
    views.set(paths.bound(b.kind), waveView(raw, org, waveScope));
    const carried: unknown = JSON.parse(views.get(paths.bound(b.kind))!).carried_ms;
    if (carried === undefined) continue;
    if (!Number.isSafeInteger(carried) || (carried as number) < 1 || now - (carried as number) >= backlogOpsBindingMaxAgeMs(d.mode, b.kind) || (carried as number) > now)
      refuse(`backlog_ops_bound_${b.kind}_carried`);
    if (b.kind === "full" && orgSince !== null) orgSince = (carried as number) - BACKLOG_OPS_CATCHUP_MARGIN_MS;
  }
  return { bound: checkBoundReceipts(envOrg, d, view, orgSince, (p) => Buffer.from(view(p), "utf8")), since: orgSince };
}

/**
 * A marker mode whose FULL or catch-up is a per-org wave receipt: the four shared bindings (authority-dry, catch-up,
 * FULL, reshard-dry) per org, as marker-dry binds them; the orgs that pass give one merged baseline and bound record,
 * the others are refused `binding_refused` by the tool. The marker attempt's evidence stays sha256 over the bound
 * catch-up file's EXACT bytes (never a view). marker-execute's DRY is checked once over the whole scope.
 */
export function markerWaveBound(env: Record<string, string | undefined>, d: BacklogOpsDispatch, since: number | null,
  read: (p: string) => string = (p) => readFileSync(p, "utf8"), readBytes: (p: string) => Uint8Array = (p) => readFileSync(p)): BoundAuthority {
  const scope = backlogOpsScope(env), orgs = scope.orgs;
  if (orgs === null) refuse("backlog_ops_scope_required");
  const paths = backlogOpsPaths(env);
  const asDry: BacklogOpsDispatch = { ...d, mode: "marker-dry", dryRun: null, waveId: null };
  const ok: { org: string; b: BoundAuthority }[] = [], refused: string[] = [];
  for (const org of orgs!) {
    try { ok.push({ org, b: checkOrgBindings(env, asDry, org, since, read) }); } catch { refused.push(evidenceHash("org", org)); }
  }
  if (ok.length === 0) refuse("backlog_ops_wave_no_org");
  const rawCatchup = read(paths.bound("backfill")), bytes = readBytes(paths.bound("backfill"));
  if (Buffer.compare(Buffer.from(bytes), Buffer.from(rawCatchup, "utf8")) !== 0) refuse("backlog_ops_bound_backfill_receipt");
  const marker: MarkerBound = {
    sections: { fences: ok.every((x) => x.b.marker!.sections.fences), ownership: ok.every((x) => x.b.marker!.sections.ownership),
      revisions: ok.every((x) => x.b.marker!.sections.revisions) },
    reshardVerified: [...new Set(ok.flatMap((x) => x.b.marker!.reshardVerified))],
    catchupEvidence: receiptSha256(bytes),
    ...(refused.length > 0 ? { refusedOrgs: refused } : {}),
  };
  const out: BoundAuthority = { baseline: mergeBaselines(ok.map((x) => x.b.baseline)), marker };
  if (d.mode === "marker-execute") {
    let raw: string;
    try { raw = read(paths.bound("dry")); } catch { return refuse("backlog_ops_bound_dry_missing"); }
    checkBoundReceipt(raw, { mode: "marker-dry", sha: d.sha, run: d.dryRun!, scope: scope.id, kind: "dry", exact: true });
    let home: unknown;
    try { home = JSON.parse(raw).home; } catch { home = undefined; }
    if (home !== backlogOpsHomeId({ bucket: env.BACKLOG_OPS_BUCKET ?? "", region: env.BACKLOG_OPS_REGION ?? "" },
      backlogOpsLedgerTarget(env, { bucket: env.BACKLOG_OPS_BUCKET ?? "" }))) refuse("backlog_ops_bound_dry_home");
    out.wave = checkMarkerDryReceipt(raw, orgs!, { authority: d.authorityRun!, backfill: d.backfillRun!, full: d.fullBackfillRun!, reshard: d.reshardRun! }, d.waveId!);
  }
  return out;
}

if (import.meta.main)
  backlogOpsRun(process.env)
    .then((c) => { process.exitCode = c; })
    .catch((error) => {
      const code = (error as { code?: unknown })?.code;
      console.error("backlog_ops_refused " + (typeof code === "string" && /^[a-z0-9_]{1,64}$/u.test(code) ? code : "failed"));
      process.exitCode = 1;
    });
