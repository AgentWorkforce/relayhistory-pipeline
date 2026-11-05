/** Existing-owner SSH schema release. Pure validation and injected SQL client; no credentials. */
import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import { fileURLToPath } from "node:url";
import { join } from "node:path";
import {
  generation,
  inspectMigrations,
  migrationProfile,
  plan,
  runMigrations,
  type MigrationClient,
} from "./migrate.ts";
import {
  functionBoundaryEvidence,
  normalizeEventTriggerBody,
  SESSIONS_FUNCTION_EVENT_TRIGGER_PROSRC_SHA256,
} from "./migrate-guards.ts";
import { inputsDigest, APPROVER } from "../../stacks/aws/deploy-guard.ts";
import { ROLE_VALIDATION_NAME, ROLE_VALIDATION_TABLE } from "../shared/db.ts";
export const OWNER_APPLY = {
  project: "pyamkrhoyszgzirqqffq",
  owner: "postgres",
  login: "cli_login_postgres",
  database: "postgres",
  // Window #2 starts from the verified Window #1 ledger (35 rows).
  // Fresh read-only owner proof on 2026-11-05 20:05Z matched this exact
  // digest. Reject earlier baselines rather than allowing migration gaps.
  // This pin is not production apply approval; source, plan, capture and
  // human approval bindings remain mandatory.
  ledger: "3173f2df7f749fd79c340286f3284d91d8f5a8b988b7f1b1a11e01c3a3307641",
  protocol: "owner-schema-apply-v1",
  execution: "ssh-sf-frame-owner-schema-apply",
  maxAgeMs: 3600000,
} as const;
const ROOT = fileURLToPath(new URL("../..", import.meta.url));
export const digest = (text: string) =>
  createHash("sha256").update(text).digest("hex");
const hash = (v: unknown) => inputsDigest(JSON.stringify({ value: v }));
const obj = (v: unknown): v is Record<string, any> =>
  v !== null && typeof v === "object" && !Array.isArray(v);
const hex = (v: unknown, n = 64): v is string =>
  typeof v === "string" && new RegExp("^[0-9a-f]{" + n + "}$", "u").test(v);
export function ownerApplyRefuse(code: string): never {
  throw Object.assign(new Error(code), { code });
}
export interface ConnectedOwnerClient extends MigrationClient {
  connect(): Promise<void>;
  end(): Promise<void>;
}
export interface SourcePin {
  sha: string;
  generation: string;
}
export interface OwnerIdentity {
  owner: string;
  login: string;
  db: string;
  schemaOwner: string;
  role: string;
  pid: number;
  ro: string;
}
export interface WrapperSource {
  sql: string;
  sha256: string;
  prosrcSha256: string;
}
export function ownerProfile() {
  return migrationProfile({
    HOOPDB_DB_MODE: "supabase",
    HOOPDB_SUPABASE_SHIM: "false",
    HOOPDB_ROLE_BOOTSTRAP: "false",
    HOOPDB_ROLE_VERIFY: "true",
    HOOPDB_RUNTIME_ROLE: "bulls96_runtime",
    HOOPDB_RUNTIME_ROLE_LOGIN: "true",
    HOOPDB_FUNCTION_BOUNDARY_MODE: "global-default-revoke",
  });
}
export function freshAt(at: unknown, now = Date.now()) {
  if (
    typeof at !== "string" ||
    !Number.isFinite(Date.parse(at)) ||
    Date.parse(at) > now + 30000 ||
    now - Date.parse(at) >= OWNER_APPLY.maxAgeMs
  )
    ownerApplyRefuse("owner_plan_stale");
}
export function validateReadonlyPlan(
  text: string,
  pin: SourcePin,
  now = Date.now(),
) {
  let p: any;
  try {
    p = JSON.parse(text);
  } catch {
    ownerApplyRefuse("owner_plan_json");
  }
  if (
    !obj(p) ||
    p.version !== 1 ||
    p.status !== "planned_readonly_not_apply_approval" ||
    p.project !== OWNER_APPLY.project ||
    p.sourceSha !== pin.sha ||
    p.generation !== pin.generation ||
    p.ledgerSha256 !== OWNER_APPLY.ledger ||
    p.boundary !== "global-default-revoke" ||
    !Array.isArray(p.inventory) ||
    !p.inventory.length ||
    !obj(p.acl)
  )
    ownerApplyRefuse("owner_plan_binding");
  freshAt(p.at, now);
  return p;
}
export async function wrapperSource(): Promise<WrapperSource> {
  const text = await readFile(
    join(
      ROOT,
      "supabase/bootstrap/hoopdb_sessions_function_boundary_supabase.sql",
    ),
    "utf8",
  );
  const found = [...text.matchAll(/\$boundary\$([\s\S]*?)\$boundary\$/gu)];
  if (
    found.length !== 1 ||
    digest(found[0]![1]!) !== SESSIONS_FUNCTION_EVENT_TRIGGER_PROSRC_SHA256
  )
    ownerApplyRefuse("owner_wrapper_generation");
  const body = found[0]![1]!.replaceAll("__RUNTIME_ROLE__", "bulls96_runtime");
  if (
    body.includes("$owner_apply_boundary$") ||
    normalizeEventTriggerBody(body).sha256 !==
      SESSIONS_FUNCTION_EVENT_TRIGGER_PROSRC_SHA256
  )
    ownerApplyRefuse("owner_wrapper_generation");
  const sql =
    "CREATE OR REPLACE FUNCTION hoopdb_boundary.sessions_function_boundary() RETURNS event_trigger LANGUAGE plpgsql VOLATILE SECURITY INVOKER SET search_path = pg_catalog, pg_temp AS $owner_apply_boundary$" +
    body +
    "$owner_apply_boundary$;";
  return {
    sql,
    sha256: digest(sql),
    prosrcSha256: SESSIONS_FUNCTION_EVENT_TRIGGER_PROSRC_SHA256,
  };
}
/** Preserve quoted contents while erasing comments, including comments in DO bodies.
 * Conservatively reject role-control words in strings too; never guess their execution. */
export function roleAudit(sql: string) {
  let out = "",
    i = 0;
  while (i < sql.length) {
    if (sql.startsWith("--", i)) {
      const end = sql.indexOf("\n", i + 2);
      i = end < 0 ? sql.length : end;
      out += " ";
      continue;
    }
    if (sql.startsWith("/*", i)) {
      let depth = 1;
      i += 2;
      while (i < sql.length && depth) {
        if (sql.startsWith("/*", i)) {
          depth++;
          i += 2;
        } else if (sql.startsWith("*/", i)) {
          depth--;
          i += 2;
        } else i++;
      }
      if (depth) ownerApplyRefuse("owner_sql_comment");
      out += " ";
      continue;
    }
    const dollar = sql.slice(i).match(/^\$(?:[A-Za-z_][A-Za-z_0-9]*)?\$/u)?.[0];
    if (dollar) {
      const end = sql.indexOf(dollar, i + dollar.length);
      if (end < 0) ownerApplyRefuse("owner_sql_quote");
      out += " " + roleAudit(sql.slice(i + dollar.length, end)) + " ";
      i = end + dollar.length;
      continue;
    }
    if (sql[i] === "'" || sql[i] === '"') {
      const quote = sql[i]!,
        escapes =
          quote === "'" && /(?:^|[^A-Za-z0-9_])e$/iu.test(sql.slice(0, i));
      out += quote;
      i++;
      let ended = false;
      while (i < sql.length) {
        const c = sql[i++]!;
        out += c;
        if (escapes && c === "\\" && i < sql.length) {
          out += sql[i++]!;
          continue;
        }
        if (c === quote) {
          if (sql[i] === quote) {
            out += sql[i++]!;
            continue;
          }
          ended = true;
          break;
        }
      }
      if (!ended) ownerApplyRefuse("owner_sql_quote");
      continue;
    }
    out += sql[i++]!;
  }
  const scan = out.replaceAll('"', "");
  if (
    /\b(?:RESET\s+(?:ROLE|ALL)|SET\s+(?:(?:LOCAL|SESSION)\s+)?(?:ROLE|SESSION\s+AUTHORIZATION)|(?:CREATE|ALTER|DROP)\s+ROLE)\b|\bset_config\s*\(\s*'(?:role|session_authorization)'/iu.test(
      scan,
    ) ||
    /\bset_config\s*\(\s*(?:role|session_authorization)\s*,/iu.test(scan)
  )
    ownerApplyRefuse("owner_sql_role_control");
  return out;
}
export async function auditCanonicalOwnerSql() {
  for (const step of await plan(ownerProfile()))
    roleAudit(await readFile(step.path, "utf8"));
}
export async function ownerIdentity(
  client: MigrationClient,
  pid?: number,
  readOnly?: boolean,
): Promise<OwnerIdentity> {
  const row = (
    await client.query<any>(
      "SELECT current_user AS owner,session_user AS login,current_database() AS db,current_setting('role') AS role,pg_backend_pid() AS pid,current_setting('transaction_read_only') AS ro,(SELECT pg_get_userbyid(nspowner) FROM pg_namespace WHERE nspname='sessions') AS \"schemaOwner\"",
    )
  ).rows[0];
  if (
    !row ||
    row.owner !== OWNER_APPLY.owner ||
    row.login !== OWNER_APPLY.login ||
    row.db !== OWNER_APPLY.database ||
    row.schemaOwner !== OWNER_APPLY.owner ||
    row.role !== OWNER_APPLY.owner ||
    !Number.isInteger(row.pid) ||
    (pid !== undefined && row.pid !== pid) ||
    (readOnly !== undefined && row.ro !== (readOnly ? "on" : "off"))
  )
    ownerApplyRefuse("owner_session_identity");
  return row;
}
export async function withOwnerSession<T>(
  client: ConnectedOwnerClient,
  work: (pid: number) => Promise<T>,
): Promise<T> {
  let connected = false;
  try {
    await client.connect();
    connected = true;
    await client.query("SET SESSION ROLE postgres");
    await client.query("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY");
    const identity = await ownerIdentity(client, undefined, true);
    // Supavisor drops startup role options. Probe its real reset baseline in
    // read-only SQL, restoring only the owner role already granted to this login.
    // Canonical migration queries may never issue role control.
    await client.query("RESET ROLE");
    const reset = (await client.query<any>(
      "SELECT current_user AS owner,session_user AS login,current_database() AS db,current_setting('role') AS role,pg_backend_pid() AS pid,current_setting('transaction_read_only') AS ro",
    )).rows[0];
    if (!reset || reset.owner !== OWNER_APPLY.login ||
        reset.login !== OWNER_APPLY.login || reset.db !== OWNER_APPLY.database ||
        reset.role !== "none" || reset.pid !== identity.pid || reset.ro !== "on")
      ownerApplyRefuse("owner_reset_baseline");
    await client.query("SET LOCAL ROLE postgres");
    await ownerIdentity(client, identity.pid, true);
    await client.query("ROLLBACK");
    // Rollback must restore the prior SESSION owner on the SAME backend.
    await ownerIdentity(client, identity.pid, false);
    return await work(identity.pid);
  } finally {
    try {
      if (connected) await client.query("ROLLBACK").catch(() => {});
    } finally {
      await client.end().catch(() => {});
    }
  }
}
export function ownerFencedClient(
  client: MigrationClient,
  pid: number,
): MigrationClient {
  return {
    async query<R>(sql: string, params?: unknown[]) {
      const cleanup =
        /^\s*ROLLBACK\s*;?\s*$/iu.test(sql) ||
        /^SELECT pg_advisory_unlock\(961996, 1\)/u.test(sql);
      if (!cleanup) {
        roleAudit(sql);
        await ownerIdentity(client, pid);
      }
      const result = await client.query<R>(sql, params);
      if (!cleanup) await ownerIdentity(client, pid);
      return result;
    },
  };
}
function requireBoundaryShape(e: any) {
  if (
    !e?.present ||
    e.event !== "ddl_command_end" ||
    !e.tags_exact ||
    !e.enabled_ok ||
    !e.owner_is_migration_owner ||
    !e.function_is_reviewed ||
    !e.function_owner_is_migration_owner ||
    !e.schema_owner_is_migration_owner ||
    !hex(e.prosrc_sha256) ||
    !e.proconfig_exact ||
    e.security_definer !== false ||
    e.volatility !== "v" ||
    e.language !== "plpgsql" ||
    e.returns !== "event_trigger" ||
    e.function_public_execute !== false ||
    !e.runtime_role_setting_matches ||
    !e.schema_private
  )
    ownerApplyRefuse("owner_boundary_identity");
}
export async function inspectOwner(client: MigrationClient, pid: number) {
  await client.query("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY");
  try {
    await ownerIdentity(client, pid, true);
    await client.query("SELECT pg_advisory_xact_lock(961996,1)");
    const report = await inspectMigrations(client, ownerProfile());
    const boundary = await functionBoundaryEvidence(
      client,
      "global-default-revoke",
      "supabase",
      "bulls96_runtime",
    );
    if (!boundary.satisfied) ownerApplyRefuse("owner_boundary_unproven");
    requireBoundaryShape(boundary.event_trigger);
    return { ...report, existingBoundary: boundary.event_trigger };
  } finally {
    await client.query("ROLLBACK");
  }
}
export interface OwnerCapture {
  version: 1;
  kind: string;
  at: string;
  source: SourcePin;
  project: string;
  planSha256: string;
  ledgerSha256: string;
  inventorySha256: string;
  aclSha256: string;
  existingBoundary: any;
  existingBoundarySha256: string;
  wrapperSha256: string;
  wrapperProsrcSha256: string;
  ownerRoleRestorationVerified: true;
  captureGeneration: string;
}
export function ownerCapture(
  planText: string,
  readonlyPlan: any,
  pin: SourcePin,
  inspection: any,
  wrapper: WrapperSource,
  now = new Date(),
): OwnerCapture {
  if (
    inspection.generation !== pin.generation ||
    inspection.ledgerSha256 !== OWNER_APPLY.ledger ||
    hash(inspection.inventory) !== hash(readonlyPlan.inventory) ||
    hash(inspection.acl) !== hash(readonlyPlan.acl)
  )
    ownerApplyRefuse("owner_fresh_plan_drift");
  requireBoundaryShape(inspection.existingBoundary);
  const body = {
    version: 1 as const,
    kind: OWNER_APPLY.protocol,
    at: now.toISOString(),
    source: pin,
    project: OWNER_APPLY.project,
    planSha256: digest(planText),
    ledgerSha256: OWNER_APPLY.ledger,
    inventorySha256: hash(inspection.inventory),
    aclSha256: hash(inspection.acl),
    existingBoundary: inspection.existingBoundary,
    existingBoundarySha256: hash(inspection.existingBoundary),
    wrapperSha256: wrapper.sha256,
    wrapperProsrcSha256: wrapper.prosrcSha256,
    ownerRoleRestorationVerified: true as const,
  };
  return { ...body, captureGeneration: hash(body) };
}
export function validateOwnerApply(
  planText: string,
  captureText: string,
  approvalText: string,
  pin: SourcePin,
  wrapper: WrapperSource,
  now = Date.now(),
) {
  const plan = validateReadonlyPlan(planText, pin, now);
  let capture: any, approval: any;
  try {
    capture = JSON.parse(captureText);
    approval = JSON.parse(approvalText);
  } catch {
    ownerApplyRefuse("owner_apply_json");
  }
  if (
    !obj(capture) ||
    capture.kind !== OWNER_APPLY.protocol ||
    capture.version !== 1 ||
    hash(capture.source) !== hash(pin) ||
    capture.project !== OWNER_APPLY.project ||
    capture.planSha256 !== digest(planText) ||
    capture.ledgerSha256 !== OWNER_APPLY.ledger ||
    capture.wrapperSha256 !== wrapper.sha256 ||
    capture.wrapperProsrcSha256 !== wrapper.prosrcSha256 ||
    capture.ownerRoleRestorationVerified !== true
  )
    ownerApplyRefuse("owner_capture_binding");
  freshAt(capture.at, now);
  requireBoundaryShape(capture.existingBoundary);
  const { captureGeneration, ...body } = capture;
  if (
    captureGeneration !== hash(body) ||
    capture.existingBoundarySha256 !== hash(capture.existingBoundary) ||
    capture.inventorySha256 !== hash(plan.inventory) ||
    capture.aclSha256 !== hash(plan.acl)
  )
    ownerApplyRefuse("owner_capture_hash");
  if (
    !obj(approval) ||
    approval.version !== 1 ||
    approval.decision !== "approved" ||
    approval.by !== APPROVER ||
    approval.execution !== OWNER_APPLY.execution ||
    typeof approval.quote !== "string" ||
    approval.quote.trim().length < 8 ||
    approval.project !== OWNER_APPLY.project ||
    approval.sourceSha !== pin.sha ||
    approval.generation !== pin.generation ||
    approval.ledgerSha256 !== OWNER_APPLY.ledger ||
    approval.planSha256 !== digest(planText) ||
    approval.captureSha256 !== digest(captureText) ||
    approval.captureGeneration !== captureGeneration ||
    approval.wrapperSha256 !== wrapper.sha256
  )
    ownerApplyRefuse("owner_manual_exception_required");
  freshAt(approval.at, now);
  if (Date.parse(approval.at) < Date.parse(capture.at) - 30000)
    ownerApplyRefuse("owner_approval_predates_capture");
  return { plan, capture };
}
export async function refreshOwnerWrapper(
  client: MigrationClient,
  pid: number,
  wrapper: WrapperSource,
) {
  const expected = await wrapperSource();
  if (
    wrapper.sql !== expected.sql ||
    wrapper.sha256 !== expected.sha256 ||
    wrapper.prosrcSha256 !== expected.prosrcSha256
  )
    ownerApplyRefuse("owner_wrapper_generation");
  await ownerIdentity(client, pid, false);
  const existing = await functionBoundaryEvidence(
    client,
    "global-default-revoke",
    "supabase",
    "bulls96_runtime",
  );
  if (!existing.satisfied) ownerApplyRefuse("owner_boundary_unproven");
  requireBoundaryShape(existing.event_trigger);
  await client.query("BEGIN");
  try {
    await client.query(wrapper.sql);
    const boundary = await functionBoundaryEvidence(
      client,
      "event-trigger",
      "supabase",
      "bulls96_runtime",
    );
    if (!boundary.satisfied || !boundary.event_trigger.verified)
      ownerApplyRefuse("owner_wrapper_refresh_unproven");
    await client.query("COMMIT");
  } catch (error) {
    await client.query("ROLLBACK").catch(() => {});
    throw error;
  }
  await ownerIdentity(client, pid, false);
}
export async function applyOwnerRelease(
  client: MigrationClient,
  pid: number,
  args: {
    pin: SourcePin;
    planText: string;
    captureText: string;
    approvalText: string;
    wrapper: WrapperSource;
    recheckSource: () => Promise<void>;
  },
) {
  const expectedWrapper = await wrapperSource();
  if (
    args.wrapper.sql !== expectedWrapper.sql ||
    args.wrapper.sha256 !== expectedWrapper.sha256 ||
    args.wrapper.prosrcSha256 !== expectedWrapper.prosrcSha256
  )
    ownerApplyRefuse("owner_wrapper_generation");
  const approved = validateOwnerApply(
    args.planText,
    args.captureText,
    args.approvalText,
    args.pin,
    args.wrapper,
  );
  const fenced = ownerFencedClient(client, pid);
  await fenced.query("SELECT pg_advisory_lock(961996, 1)");
  try {
    const fresh = await inspectOwner(client, pid);
    const derived = ownerCapture(
      args.planText,
      approved.plan,
      args.pin,
      fresh,
      args.wrapper,
      new Date(approved.capture.at),
    );
    if (hash(derived) !== hash(approved.capture))
      ownerApplyRefuse("owner_catalog_changed");
    await auditCanonicalOwnerSql();
    const result = await runMigrations(fenced, {
      profile: ownerProfile(),
      expectedLedgerSha256: OWNER_APPLY.ledger,
      beforeWrite: async () => {
        validateOwnerApply(
          args.planText,
          args.captureText,
          args.approvalText,
          args.pin,
          args.wrapper,
        );
        await args.recheckSource();
        await ownerIdentity(client, pid, false);
        await refreshOwnerWrapper(client, pid, args.wrapper);
      },
    });
    const marker = (
      await fenced.query<any>(
        "SELECT generation,status FROM " +
          ROLE_VALIDATION_TABLE +
          " WHERE name=$1",
        [ROLE_VALIDATION_NAME],
      )
    ).rows[0];
    const final = await inspectOwner(client, pid);
    if (
      marker?.status !== "pass" ||
      marker.generation !== args.pin.generation ||
      final.inventory.some((r: any) => r.pending) ||
      !final.existingBoundary.verified
    )
      ownerApplyRefuse("owner_apply_verification");
    return {
      status: "owner_apply_verified",
      sourceSha: args.pin.sha,
      generation: args.pin.generation,
      ledgerSha256: final.ledgerSha256,
      ...result,
    };
  } finally {
    await client.query("ROLLBACK").catch(() => {});
    await client.query("SELECT pg_advisory_unlock(961996, 1)").catch(() => {});
  }
}
