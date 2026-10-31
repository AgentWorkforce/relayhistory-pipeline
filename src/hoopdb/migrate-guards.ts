/**
 * The migrator's read-only guards. Each runs under the migrator's advisory lock before
 * any write (before the ledger or marker tables are created, before the pending marker
 * upsert, before any step), and the runtime-role allowlist runs again after the last
 * migration, before phase 2 can mark PASS. Every refusal names each violation and writes
 * nothing.
 */
import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";
import { MIGRATION_SENTINELS } from "./migration-sentinels.ts";

export interface GuardClient {
  query<R = Record<string, unknown>>(
    text: string,
    params?: unknown[],
  ): Promise<{ rows: R[] }>;
}
export interface GuardStep {
  name: string;
  path: string;
  /** A step's own sentinel (test-injected steps); otherwise MIGRATION_SENTINELS[name]. */
  sentinel?: { evidence: string; sql: string };
}

export function guardError(code: string, message: string): Error {
  return Object.assign(new Error(`96bulls: ${message}`), { code });
}

// ---------------------------------------------------------------------------
// Runtime role: an exact least-privilege allowlist
// ---------------------------------------------------------------------------

export interface RuntimeRoleConfig {
  mode: "local" | "selfhosted" | "supabase";
  role: string;
  /** HOOPDB_RUNTIME_ROLE_LOGIN: whether the role logs in itself (true) or is reached through a workload role (false). */
  login?: boolean;
  /** HOOPDB_RUNTIME_WORKLOAD_ROLE: the one role allowed to be a member of the runtime role (no ADMIN OPTION). */
  workload?: string;
}

export interface RuntimeTables {
  ledger: string;
  marker: string;
}

/**
 * Approved for the runtime role, and nothing else:
 *   USAGE on schema sessions; SELECT on the ledger and the readiness marker;
 *   CONNECT on the current database, which is REQUIRED (effective), as it is for the
 *   workload role when one is configured.
 * Refused, among everything else: effective TEMPORARY on the database (PUBLIC has it by
 * default; see the runbook), and EXECUTE on any sessions function, trigger functions
 * included (a trigger fires without its caller holding EXECUTE).
 *
 * Coverage, stated exactly:
 *   current database  effective privileges in every non-system schema: tables, views,
 *                     sequences (any privilege), SECURITY DEFINER functions (EXECUTE),
 *                     every sessions function (EXECUTE), CREATE on any schema, CREATE
 *                     and TEMPORARY on the database; default privileges in sessions.
 *   every database    direct references naming the role (pg_shdepend): ownership,
 *                     ACL entries, default ACLs, policies.
 *   not covered       PUBLIC-derived privileges in other databases (for example
 *                     CONNECT to another database through PUBLIC: keep one database
 *                     per cluster, or revoke PUBLIC CONNECT there), and SECURITY INVOKER
 *                     functions outside sessions, which run with the caller's own
 *                     privileges (extension functions are reviewed with the extension).
 * phase "pre" (before any write) ignores PUBLIC-derived EXECUTE on sessions functions:
 * the migrator revokes that in the same transaction as validation. phase "post" counts
 * every EXECUTE.
 * The services' other runtime table grants are out of scope here (activation dossier
 * #98): a role holding them is refused until this allowlist is extended in a reviewed
 * change.
 */
export async function runtimeRoleViolations(
  client: GuardClient,
  cfg: RuntimeRoleConfig,
  tables: RuntimeTables,
  phase: "pre" | "post" = "post",
): Promise<string[]> {
  const out: string[] = [];
  const role = (
    await client.query<{
      oid: number;
      rolsuper: boolean;
      rolcreatedb: boolean;
      rolcreaterole: boolean;
      rolreplication: boolean;
      rolbypassrls: boolean;
      rolcanlogin: boolean;
    }>(
      "SELECT oid::int AS oid, rolsuper, rolcreatedb, rolcreaterole, rolreplication, rolbypassrls, rolcanlogin FROM pg_roles WHERE rolname = $1",
      [cfg.role],
    )
  ).rows[0];
  if (!role) return [`role ${cfg.role} does not exist`];
  for (const [flag, label] of [
    ["rolsuper", "SUPERUSER"],
    ["rolcreatedb", "CREATEDB"],
    ["rolcreaterole", "CREATEROLE"],
    ["rolreplication", "REPLICATION"],
    ["rolbypassrls", "BYPASSRLS"],
  ] as const)
    if (role[flag]) out.push(label);
  if (cfg.login === undefined)
    out.push(
      "HOOPDB_RUNTIME_ROLE_LOGIN must be set for a dedicated runtime role",
    );
  else if (role.rolcanlogin !== cfg.login)
    out.push(
      role.rolcanlogin
        ? "LOGIN (configured NOLOGIN)"
        : "NOLOGIN (configured LOGIN)",
    );

  for (const m of (
    await client.query<{ rolname: string }>(
      "SELECT r.rolname FROM pg_auth_members m JOIN pg_roles r ON r.oid = m.roleid WHERE m.member = $1::oid ORDER BY 1",
      [role.oid],
    )
  ).rows)
    out.push(`member of ${m.rolname}`);
  for (const m of (
    await client.query<{ rolname: string; admin_option: boolean }>(
      "SELECT r.rolname, m.admin_option FROM pg_auth_members m JOIN pg_roles r ON r.oid = m.member WHERE m.roleid = $1::oid ORDER BY 1",
      [role.oid],
    )
  ).rows) {
    if (m.rolname !== cfg.workload) out.push(`granted to ${m.rolname}`);
    else if (m.admin_option)
      out.push(`granted to ${m.rolname} WITH ADMIN OPTION`);
  }

  const ids = (
    await client.query<{
      db: number;
      sessions: number | null;
      ledger: number | null;
      marker: number | null;
    }>(
      `SELECT (SELECT oid::int FROM pg_database WHERE datname = current_database()) AS db,
              to_regnamespace('sessions')::oid::int AS sessions,
              to_regclass($1)::oid::int AS ledger, to_regclass($2)::oid::int AS marker`,
      [tables.ledger, tables.marker],
    )
  ).rows[0]!;
  // Every owner, ACL, policy and initial-privilege reference naming the role, in every
  // database of the cluster (pg_shdepend). Only the approved ACL entries may remain.
  for (const d of (
    await client.query<{
      dbid: number;
      cls: string;
      objid: number;
      objsubid: number;
      deptype: string;
      descr: string;
    }>(
      `SELECT d.dbid::int AS dbid, d.classid::regclass::text AS cls, d.objid::int AS objid, d.objsubid, d.deptype::text AS deptype,
              CASE WHEN d.dbid IN (0, (SELECT oid FROM pg_database WHERE datname = current_database()))
                   THEN pg_describe_object(d.classid, d.objid, d.objsubid)
                   ELSE format('an object in database %I', db.datname) END AS descr
         FROM pg_shdepend d LEFT JOIN pg_database db ON db.oid = d.dbid
        WHERE d.refclassid = 'pg_authid'::regclass AND d.refobjid = $1::oid
        ORDER BY 6`,
      [role.oid],
    )
  ).rows) {
    const approved =
      d.deptype === "a" &&
      d.objsubid === 0 &&
      ((d.dbid === ids.db &&
        d.cls === "pg_namespace" &&
        d.objid === ids.sessions) ||
        (d.dbid === ids.db &&
          d.cls === "pg_class" &&
          (d.objid === ids.ledger || d.objid === ids.marker)) ||
        (d.dbid === 0 && d.cls === "pg_database" && d.objid === ids.db));
    if (approved) continue;
    const verb =
      d.deptype === "o"
        ? "owns"
        : d.deptype === "a"
          ? "has privileges on"
          : d.deptype === "r"
            ? "is named in policy"
            : "is referenced by";
    out.push(`${verb} ${d.descr}`);
  }
  // Exact privileges on the approved objects (direct aclitems).
  const direct = await client.query<{
    what: string;
    privilege_type: string;
    is_grantable: boolean;
  }>(
    `SELECT 'schema sessions' AS what, a.privilege_type, a.is_grantable FROM pg_namespace n, aclexplode(n.nspacl) a
      WHERE n.nspname = 'sessions' AND a.grantee = $1::oid
     UNION ALL
     SELECT c.oid::regclass::text, a.privilege_type, a.is_grantable FROM pg_class c, aclexplode(c.relacl) a
      WHERE c.oid IN (coalesce(to_regclass($2), 0), coalesce(to_regclass($3), 0)) AND a.grantee = $1::oid
     UNION ALL
     SELECT 'database ' || d.datname, a.privilege_type, a.is_grantable FROM pg_database d, aclexplode(d.datacl) a
      WHERE d.datname = current_database() AND a.grantee = $1::oid`,
    [role.oid, tables.ledger, tables.marker],
  );
  for (const p of direct.rows) {
    const ok =
      p.what === "schema sessions"
        ? p.privilege_type === "USAGE"
        : p.what.startsWith("database ")
          ? p.privilege_type === "CONNECT"
          : p.privilege_type === "SELECT";
    if (!ok || p.is_grantable)
      out.push(
        `${p.what}: ${p.privilege_type}${p.is_grantable ? " WITH GRANT OPTION" : ""} is not approved`,
      );
  }
  // Effective privileges, however obtained (PUBLIC, memberships, defaults), in every
  // non-system schema of the current database.
  const NONSYSTEM = `n.nspname NOT IN ('pg_catalog', 'information_schema') AND n.nspname NOT LIKE 'pg\\_toast%' AND n.nspname NOT LIKE 'pg\\_temp%'`;
  const db = (
    await client.query<{
      create: boolean;
      temp: boolean;
      connect: boolean;
      wl_connect: boolean | null;
    }>(
      `SELECT has_database_privilege($1::oid, current_database(), 'CREATE') AS create,
              has_database_privilege($1::oid, current_database(), 'TEMPORARY') AS temp,
              has_database_privilege($1::oid, current_database(), 'CONNECT') AS connect,
              (SELECT has_database_privilege(r.oid, current_database(), 'CONNECT') FROM pg_roles r WHERE r.rolname = $2) AS wl_connect`,
      [role.oid, cfg.workload ?? null],
    )
  ).rows[0]!;
  if (db.create) out.push("CREATE on the database");
  if (db.temp)
    out.push(
      "TEMPORARY on the database (PUBLIC has it by default: revoke it from PUBLIC, see the runbook)",
    );
  if (!db.connect) out.push("no CONNECT on the database (required)");
  if (cfg.workload && db.wl_connect !== true)
    out.push(
      `workload role ${cfg.workload} has no CONNECT on the database (required)`,
    );
  for (const sc of (
    await client.query<{ nspname: string }>(
      `SELECT n.nspname FROM pg_namespace n WHERE ${NONSYSTEM} AND has_schema_privilege($1::oid, n.oid, 'CREATE') ORDER BY 1`,
      [role.oid],
    )
  ).rows)
    out.push(`CREATE on schema ${sc.nspname}`);
  // Schema USAGE: an exact allowlist. sessions is approved; public is PostgreSQL's own
  // PUBLIC default (non-mutating: it only lets the role resolve names there, and every
  // object in public is checked above and below). Any other non-system schema is refused.
  // Type USAGE through PUBLIC (PostgreSQL's default for every type) is accepted as
  // non-mutating; a type grant naming the role is a pg_shdepend reference, refused above.
  for (const sc of (
    await client.query<{ nspname: string }>(
      `SELECT n.nspname FROM pg_namespace n WHERE ${NONSYSTEM} AND n.nspname NOT IN ('sessions', 'public')
          AND has_schema_privilege($1::oid, n.oid, 'USAGE') ORDER BY 1`,
      [role.oid],
    )
  ).rows)
    out.push(
      `USAGE on schema ${sc.nspname} (only sessions and public are accepted)`,
    );
  // PostgreSQL 17+ MAINTAIN (VACUUM, ANALYZE, REINDEX, REFRESH, CLUSTER, LOCK): refused on
  // any relation. The privilege does not exist before 17, so the check is version-guarded.
  if (
    Number(
      (
        await client.query<{ v: string }>(
          "SELECT current_setting('server_version_num') AS v",
        )
      ).rows[0]?.v,
    ) >= 170000
  )
    for (const m of (
      await client.query<{ rel: string }>(
        `SELECT c.oid::regclass::text AS rel FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
          WHERE ${NONSYSTEM} AND c.relkind IN ('r', 'p', 'm')
            AND CASE WHEN c.relkind IN ('r', 'p', 'm') THEN has_table_privilege($1::oid, c.oid, 'MAINTAIN') ELSE false END ORDER BY 1`,
        [role.oid],
      )
    ).rows)
      out.push(`MAINTAIN on ${m.rel}`);
  for (const t of (
    await client.query<{ rel: string; privs: string }>(
      `SELECT c.oid::regclass::text AS rel,
              concat_ws(',', CASE WHEN has_table_privilege($1::oid, c.oid, 'SELECT') AND c.oid NOT IN (coalesce(to_regclass($2), 0), coalesce(to_regclass($3), 0)) THEN 'SELECT' END,
                CASE WHEN has_table_privilege($1::oid, c.oid, 'INSERT') THEN 'INSERT' END,
                CASE WHEN has_table_privilege($1::oid, c.oid, 'UPDATE') THEN 'UPDATE' END,
                CASE WHEN has_table_privilege($1::oid, c.oid, 'DELETE') THEN 'DELETE' END,
                CASE WHEN has_table_privilege($1::oid, c.oid, 'TRUNCATE') THEN 'TRUNCATE' END,
                CASE WHEN has_table_privilege($1::oid, c.oid, 'REFERENCES') THEN 'REFERENCES' END,
                CASE WHEN has_table_privilege($1::oid, c.oid, 'TRIGGER') THEN 'TRIGGER' END,
                CASE WHEN NOT has_table_privilege($1::oid, c.oid, 'SELECT, INSERT, UPDATE, REFERENCES') AND has_any_column_privilege($1::oid, c.oid, 'SELECT, INSERT, UPDATE, REFERENCES') THEN 'column' END) AS privs
         FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE ${NONSYSTEM} AND c.relkind IN ('r', 'p', 'v', 'm', 'f') ORDER BY 1`,
      [role.oid, tables.ledger, tables.marker],
    )
  ).rows)
    if (t.privs) out.push(`${t.privs} on ${t.rel}`);
  for (const q of (
    await client.query<{ rel: string }>(
      `SELECT c.oid::regclass::text AS rel FROM pg_class c JOIN pg_namespace n ON n.oid = c.relnamespace
        WHERE ${NONSYSTEM} AND c.relkind = 'S'
          -- CASE: AND order is not guaranteed, and the function errors on a non-sequence.
          AND CASE WHEN c.relkind = 'S' THEN has_sequence_privilege($1::oid, c.oid, 'USAGE, SELECT, UPDATE') ELSE false END ORDER BY 1`,
      [role.oid],
    )
  ).rows)
    out.push(`privileges on sequence ${q.rel}`);
  for (const fnr of (
    await client.query<{ fn: string }>(
      `SELECT p.oid::regprocedure::text AS fn FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE ${NONSYSTEM} AND (n.nspname = 'sessions' OR p.prosecdef)
          AND has_function_privilege($1::oid, p.oid, 'EXECUTE')
          -- Before any write, PUBLIC-derived EXECUTE on sessions functions is left to the
          -- revoke that runs in the validation transaction.
          AND NOT ($2 = 'pre' AND n.nspname = 'sessions'
                   AND (p.proacl IS NULL OR EXISTS (SELECT 1 FROM aclexplode(p.proacl) a WHERE a.grantee = 0 AND a.privilege_type = 'EXECUTE')))
        ORDER BY 1`,
      [role.oid, phase],
    )
  ).rows)
    out.push(`EXECUTE on ${fnr.fn}`);
  // Detection only: a per-schema default-privileges GRANT in sessions naming PUBLIC or the
  // role is refused. This is NOT a schema-scoped fix for PostgreSQL's built-in PUBLIC
  // EXECUTE on new functions: per-schema ALTER DEFAULT PRIVILEGES cannot revoke a global
  // or built-in default. That residual is handled by the per-run revoke, the live gate
  // and the activation gate (runbook "Function boundary").
  for (const d of (
    await client.query<{ objtype: string }>(
      `SELECT d.defaclobjtype::text AS objtype FROM pg_default_acl d, aclexplode(d.defaclacl) a
        WHERE d.defaclnamespace = coalesce(to_regnamespace('sessions')::oid, 0) AND a.grantee IN (0, $1::oid) ORDER BY 1`,
      [role.oid],
    )
  ).rows)
    out.push(
      `default privileges in schema sessions grant PUBLIC or the runtime role (${d.objtype})`,
    );
  return out;
}

// ---------------------------------------------------------------------------
// One migration authority: a contiguous ledger prefix and no unledgered footprint
// ---------------------------------------------------------------------------

export interface InventoryObject {
  kind:
    "relation" | "index" | "function" | "type" | "trigger" | "policy" | "role";
  name: string;
  /** The table, for triggers and policies. */
  on?: string;
}

const IDENT = `"?[A-Za-z_][\\w$]*"?(?:\\."?[A-Za-z_][\\w$]*"?)?`;
const norm = (n: string) =>
  n
    .split(".")
    .map((p) => (p.startsWith('"') ? p.slice(1, -1) : p.toLowerCase()))
    .join(".");

/**
 * Removes SQL comments, but never inside a single-quoted string ('' escapes) or a
 * dollar-quoted body ($tag$...$tag$): a literal such as '\.git/*$' must not start a
 * comment. Block comments nest, as in PostgreSQL.
 */
export function stripComments(sql: string): string {
  let out = "";
  let i = 0;
  while (i < sql.length) {
    const c = sql[i]!;
    const next = sql[i + 1];
    if (c === "-" && next === "-") {
      const end = sql.indexOf("\n", i);
      i = end < 0 ? sql.length : end;
      out += " ";
    } else if (c === "/" && next === "*") {
      let depth = 1;
      i += 2;
      while (i < sql.length && depth > 0) {
        if (sql[i] === "/" && sql[i + 1] === "*") (depth++, (i += 2));
        else if (sql[i] === "*" && sql[i + 1] === "/") (depth--, (i += 2));
        else i++;
      }
      out += " ";
    } else if (c === "'") {
      let j = i + 1;
      while (j < sql.length) {
        if (sql[j] === "'" && sql[j + 1] === "'") j += 2;
        else if (sql[j] === "'") break;
        else j++;
      }
      out += sql.slice(i, j + 1);
      i = j + 1;
    } else if (c === "$") {
      const tag = /^\$[A-Za-z_]*\$/.exec(sql.slice(i))?.[0];
      if (tag) {
        const end = sql.indexOf(tag, i + tag.length);
        const stop = end < 0 ? sql.length : end + tag.length;
        out += sql.slice(i, stop);
        i = stop;
      } else ((out += c), i++);
    } else ((out += c), i++);
  }
  return out;
}

/**
 * Every object a migration file creates, read from its CREATE statements (comments
 * stripped; temporary objects, schemas and extensions excluded). A test checks that
 * the union covers every object a full migration leaves in the sessions schema.
 */
export function inventoryOf(sql: string): InventoryObject[] {
  const text = stripComments(sql);
  const out: InventoryObject[] = [];
  const add = (o: InventoryObject) => {
    if (!o.name.includes("%")) out.push(o);
  };
  const main = new RegExp(
    `\\bCREATE\\s+(?:OR\\s+REPLACE\\s+)?(?:UNIQUE\\s+)?(TEMP\\s+|TEMPORARY\\s+|UNLOGGED\\s+)?(MATERIALIZED\\s+VIEW|TABLE|VIEW|SEQUENCE|INDEX|FUNCTION|PROCEDURE|TYPE|ROLE)\\s+(?:CONCURRENTLY\\s+)?(?:IF\\s+NOT\\s+EXISTS\\s+)?(${IDENT})`,
    "gi",
  );
  for (const m of text.matchAll(main)) {
    const temp = (m[1] ?? "").trim().toUpperCase();
    if (temp === "TEMP" || temp === "TEMPORARY") continue;
    const kind = m[2]!.toUpperCase().replace(/\s+/g, " ");
    const name = m[3]!;
    if (kind === "INDEX" && name.toUpperCase() === "ON") continue; // unnamed index
    add({
      kind:
        kind === "INDEX"
          ? "index"
          : kind === "FUNCTION" || kind === "PROCEDURE"
            ? "function"
            : kind === "TYPE"
              ? "type"
              : kind === "ROLE"
                ? "role"
                : "relation",
      name: norm(name),
    });
  }
  for (const m of text.matchAll(
    new RegExp(
      `\\bCREATE\\s+(?:OR\\s+REPLACE\\s+)?(?:CONSTRAINT\\s+)?TRIGGER\\s+(${IDENT})[\\s\\S]*?\\bON\\s+(${IDENT})`,
      "gi",
    ),
  ))
    add({ kind: "trigger", name: norm(m[1]!), on: norm(m[2]!) });
  for (const m of text.matchAll(
    new RegExp(`\\bCREATE\\s+POLICY\\s+(${IDENT})\\s+ON\\s+(${IDENT})`, "gi"),
  ))
    add({ kind: "policy", name: norm(m[1]!), on: norm(m[2]!) });
  return out;
}

const key = (o: InventoryObject) =>
  `${o.kind}:${o.name}${o.on ? `@${o.on}` : ""}`;

async function exists(
  client: GuardClient,
  o: InventoryObject,
): Promise<boolean> {
  const [schema, name] = o.name.includes(".")
    ? o.name.split(".")
    : ["public", o.name];
  const one = async (sql: string, params: unknown[]) =>
    Boolean((await client.query<{ e: boolean }>(sql, params)).rows[0]?.e);
  try {
    switch (o.kind) {
      case "relation":
        return one("SELECT to_regclass($1) IS NOT NULL AS e", [o.name]);
      case "index":
        return o.name.includes(".")
          ? one("SELECT to_regclass($1) IS NOT NULL AS e", [o.name])
          : one(
              "SELECT EXISTS (SELECT 1 FROM pg_class WHERE relname = $1 AND relkind IN ('i', 'I')) AS e",
              [o.name],
            );
      case "function":
        return one(
          "SELECT EXISTS (SELECT 1 FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace WHERE n.nspname = $1 AND p.proname = $2) AS e",
          [schema, name],
        );
      case "type":
        return one(
          "SELECT EXISTS (SELECT 1 FROM pg_type t JOIN pg_namespace n ON n.oid = t.typnamespace WHERE n.nspname = $1 AND t.typname = $2) AS e",
          [schema, name],
        );
      case "trigger":
        return one(
          "SELECT EXISTS (SELECT 1 FROM pg_trigger WHERE tgname = $1 AND tgrelid = to_regclass($2)) AS e",
          [o.name, o.on],
        );
      case "policy":
        return one(
          "SELECT EXISTS (SELECT 1 FROM pg_policy WHERE polname = $1 AND polrelid = to_regclass($2)) AS e",
          [o.name, o.on],
        );
      case "role":
        return one(
          "SELECT EXISTS (SELECT 1 FROM pg_roles WHERE rolname = $1) AS e",
          [o.name],
        );
    }
  } catch {
    return true; // can't look: fail closed, as if the object were there
  }
}

/**
 * Refuses, before any write, unless the database is either empty of this migration
 * set or exactly a ledgered prefix of it:
 *   ledger_unknown_step   the ledger names a schema step this image doesn't have;
 *   ledger_gap            the ledger isn't one contiguous prefix of the ordered steps;
 *   migration_changed     a ledgered step's file changed;
 *   unledgered_schema     an object that an unledgered step creates already exists.
 * Nothing is ever imported into, or marked in, the ledger.
 */
export async function sequencePreflight(
  client: GuardClient,
  schemaSteps: GuardStep[],
  ledger: Map<string, string>,
): Promise<void> {
  const names = schemaSteps.map((s) => s.name);
  const known = new Set(names);
  const isSchema = (n: string) =>
    n.startsWith("upstream:") || n.startsWith("96bulls:");
  const unknown = [...ledger.keys()]
    .filter((n) => isSchema(n) && !known.has(n))
    .sort();
  if (unknown.length)
    throw guardError(
      "ledger_unknown_step",
      `the ledger records ${unknown.join(", ")}, which this image doesn't have; nothing is marked, see the runbook decision point`,
    );
  let k = 0;
  while (k < names.length && ledger.has(names[k]!)) k++;
  const beyond = names.slice(k).filter((n) => ledger.has(n));
  if (beyond.length)
    throw guardError(
      "ledger_gap",
      `${names[k]} is not recorded but ${beyond[0]} is; the ledger must be one contiguous prefix, nothing is marked, see the runbook decision point`,
    );
  const sql = await Promise.all(
    schemaSteps.map((s) => readFile(s.path, "utf8")),
  );
  for (let i = 0; i < k; i++) {
    const sha = createHash("sha256").update(sql[i]!).digest("hex");
    if (ledger.get(names[i]!) !== sha)
      throw guardError(
        "migration_changed",
        `migration ${names[i]} changed after it was applied; add a new migration instead`,
      );
  }
  const seen = new Set<string>();
  for (let i = 0; i < k; i++)
    for (const o of inventoryOf(sql[i]!)) seen.add(key(o));
  const found: string[] = [];
  for (let i = k; i < names.length; i++) {
    for (const o of inventoryOf(sql[i]!)) {
      const id = key(o);
      if (seen.has(id)) continue;
      seen.add(id);
      if (await exists(client, o))
        found.push(
          `${o.kind} ${o.name}${o.on ? ` on ${o.on}` : ""} (${names[i]})`,
        );
    }
  }
  // Per-step catalog sentinels: they see ALTER-only and replacement steps too.
  for (let i = k; i < names.length; i++) {
    const sentinel = schemaSteps[i]!.sentinel ?? MIGRATION_SENTINELS[names[i]!];
    if (!sentinel)
      throw guardError(
        "sentinel_missing",
        `${names[i]} has no catalog sentinel (src/hoopdb/migration-sentinels.ts); refusing`,
      );
    let applied: boolean;
    try {
      applied = Boolean(
        (await client.query<{ applied: boolean }>(sentinel.sql)).rows[0]
          ?.applied,
      );
    } catch {
      applied = true; // can't look: fail closed, as if the step had run
    }
    if (applied) found.push(`sentinel ${sentinel.evidence} (${names[i]})`);
  }
  if (found.length)
    throw guardError(
      "unledgered_schema",
      `objects of unrecorded migrations already exist: ${found.slice(0, 5).join("; ")}${found.length > 5 ? `; and ${found.length - 5} more` : ""}. Nothing is imported or marked, see the runbook decision point`,
    );
}

// ---------------------------------------------------------------------------
// Durable function boundary (activation gate)
// ---------------------------------------------------------------------------

/** The reviewed event trigger an equivalent control must use, if chosen. */
export const SESSIONS_FUNCTION_EVENT_TRIGGER =
  "sessions_function_public_revoke";
export type FunctionBoundaryApproval =
  "global-default-revoke" | "event-trigger";

/** Names and booleans only. */
export interface FunctionBoundaryEvidence {
  migration_owner: string;
  /**
   * (B) is false by design in this repository: the search Worker's RPCs are called
   * through PostgREST with Content-Profile sessions (src/proof/aws/checks.ts, #73), so
   * sessions is an exposed schema, and authenticated and service_role hold USAGE on it.
   */
  sessions_unexposed_and_no_untrusted_usage: false;
  global_function_default_revokes_public: boolean;
  event_trigger_present: boolean;
  mode: FunctionBoundaryApproval | "none";
  satisfied: boolean;
}

/**
 * PostgreSQL gives every new function PUBLIC EXECUTE by built-in default, and a
 * per-schema ALTER DEFAULT PRIVILEGES cannot revoke that. So a function created in
 * sessions after a migrate, outside the migrator, is directly invocable by any role with
 * USAGE on sessions (authenticated, service_role, and through PostgREST RPC) until the
 * next migrate's revoke; the live gate only turns readiness off. Activation therefore
 * needs one durable control, selected by the deployment (HOOPDB_FUNCTION_BOUNDARY_MODE, which
 * is configuration, not a human approval) and proven by the catalog. The gate applies in
 * selfhosted and supabase mode; only explicit local mode is ungated:
 *   (A) "global-default-revoke": ALTER DEFAULT PRIVILEGES FOR ROLE <migration owner>
 *       REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC. On Supabase the owner is postgres, so
 *       this changes every future postgres-created function in every schema.
 *       In selfhosted, the database is dedicated to HoopDb, so the privileged
 *       supabase/bootstrap/hoopdb_function_default_revoke.sql installs it safely.
 *   (E) "event-trigger": NOT IMPLEMENTED, refused (hook kept for a reviewed trigger).
 * (B) cannot be claimed here (see the evidence field). Follow-up: a dedicated sessions
 * object owner, which needs an ownership transfer of live objects (activation dossier #98).
 */
export async function functionBoundaryEvidence(
  client: GuardClient,
  approval: FunctionBoundaryApproval | undefined,
): Promise<FunctionBoundaryEvidence> {
  const r = (
    await client.query<{
      owner: string;
      global_revoke: boolean;
      event_trigger: boolean;
    }>(
      `SELECT current_user AS owner,
              EXISTS (SELECT 1 FROM pg_default_acl d
                       WHERE d.defaclrole = (SELECT oid FROM pg_roles WHERE rolname = current_user)
                         AND d.defaclnamespace = 0 AND d.defaclobjtype = 'f'
                         AND NOT EXISTS (SELECT 1 FROM aclexplode(d.defaclacl) a WHERE a.grantee = 0 AND a.privilege_type = 'EXECUTE')) AS global_revoke,
              EXISTS (SELECT 1 FROM pg_event_trigger WHERE evtname = $1 AND evtenabled <> 'D' AND evtevent = 'ddl_command_end') AS event_trigger`,
      [SESSIONS_FUNCTION_EVENT_TRIGGER],
    )
  ).rows[0]!;
  // Event-trigger mode is not implemented: a trigger's name proves nothing (its enabled
  // state, events and tags, owner, function identity and definition, SECURITY and
  // search_path, and behaviour would all need verifying). Hook kept for that later work.
  const satisfied = approval === "global-default-revoke" && r.global_revoke;
  return {
    migration_owner: r.owner,
    sessions_unexposed_and_no_untrusted_usage: false,
    global_function_default_revokes_public: r.global_revoke,
    event_trigger_present: r.event_trigger,
    mode: approval ?? "none",
    satisfied,
  };
}

/**
 * Every sessions function's ACL after a migrate, normalized for comparison with the
 * committed manifest (src/hoopdb/sessions-function-acl.json): grantee names (PUBLIC for
 * the pseudo-role; <owner> for the function owner), privilege, "*" for GRANT OPTION.
 * A NULL ACL (built-in default: owner plus PUBLIC EXECUTE) is reported as ["<default>"].
 */
export async function sessionsFunctionAcl(
  client: GuardClient,
): Promise<Record<string, string[]>> {
  const rows = (
    await client.query<{ fn: string; entries: string[] | null }>(
      `SELECT p.oid::regprocedure::text AS fn,
              CASE WHEN p.proacl IS NULL THEN NULL ELSE (
                SELECT array_agg(
                         CASE WHEN a.grantee = 0 THEN 'PUBLIC' WHEN a.grantee = p.proowner THEN '<owner>' ELSE (SELECT rolname FROM pg_roles WHERE oid = a.grantee) END
                         || ':' || a.privilege_type || CASE WHEN a.is_grantable THEN '*' ELSE '' END ORDER BY 1)
                  FROM aclexplode(p.proacl) a) END AS entries
         FROM pg_proc p JOIN pg_namespace n ON n.oid = p.pronamespace
        WHERE n.nspname = 'sessions' ORDER BY 1`,
    )
  ).rows;
  return Object.fromEntries(
    rows.map((r) => [r.fn, r.entries ?? ["<default>"]]),
  );
}
