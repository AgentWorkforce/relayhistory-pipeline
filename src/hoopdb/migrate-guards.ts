/**
 * The migrator's read-only guards. Each runs under the migrator's advisory lock before
 * any write (before the ledger or marker tables are created, before the pending marker
 * upsert, before any step), and the runtime-role allowlist runs again after the last
 * migration, before phase 2 can mark PASS. Every refusal names each violation and writes
 * nothing.
 */
import { createHash } from "node:crypto";
import { readFile } from "node:fs/promises";

export interface GuardClient {
  query<R = Record<string, unknown>>(
    text: string,
    params?: unknown[],
  ): Promise<{ rows: R[] }>;
}
export interface GuardStep {
  name: string;
  path: string;
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
 *   CONNECT / TEMPORARY on the current database;
 *   EXECUTE only on sessions trigger functions (not callable directly).
 * The services' other runtime table grants are out of scope here (activation dossier
 * #98): a role holding them is refused until this allowlist is extended in a reviewed
 * change.
 */
export async function runtimeRoleViolations(
  client: GuardClient,
  cfg: RuntimeRoleConfig,
  tables: RuntimeTables,
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
      WHERE d.datname = current_database() AND a.grantee = $1`,
    [role.oid, tables.ledger, tables.marker],
  );
  for (const p of direct.rows) {
    const ok =
      p.what === "schema sessions"
        ? p.privilege_type === "USAGE"
        : p.what.startsWith("database ")
          ? p.privilege_type === "CONNECT" || p.privilege_type === "TEMPORARY"
          : p.privilege_type === "SELECT";
    if (!ok || p.is_grantable)
      out.push(
        `${p.what}: ${p.privilege_type}${p.is_grantable ? " WITH GRANT OPTION" : ""} is not approved`,
      );
  }
  // Effective privileges, however obtained (PUBLIC, memberships, defaults).
  for (const s of (
    await client.query<{ nspname: string }>(
      `SELECT nspname FROM pg_namespace
        WHERE nspname NOT IN ('pg_catalog', 'information_schema') AND nspname NOT LIKE 'pg\\_toast%' AND nspname NOT LIKE 'pg\\_temp%'
          AND has_schema_privilege($1::oid, oid, 'CREATE') ORDER BY 1`,
      [role.oid],
    )
  ).rows)
    out.push(`CREATE on schema ${s.nspname}`);
  if (
    (
      await client.query<{ c: boolean }>(
        "SELECT has_database_privilege($1::oid, current_database(), 'CREATE') AS c",
        [role.oid],
      )
    ).rows[0]?.c
  )
    out.push("CREATE on the database");
  if (ids.sessions !== null) {
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
           FROM pg_class c WHERE c.relnamespace = $4 AND c.relkind IN ('r', 'p', 'v', 'm', 'f') ORDER BY 1`,
        [role.oid, tables.ledger, tables.marker, ids.sessions],
      )
    ).rows)
      if (t.privs) out.push(`${t.privs} on ${t.rel}`);
    for (const q of (
      await client.query<{ rel: string }>(
        `SELECT c.oid::regclass::text AS rel FROM pg_class c
          WHERE c.relnamespace = $2 AND c.relkind = 'S'
            -- CASE: AND order is not guaranteed, and the function errors on a non-sequence.
            AND CASE WHEN c.relkind = 'S' THEN has_sequence_privilege($1::oid, c.oid, 'USAGE, SELECT, UPDATE') ELSE false END ORDER BY 1`,
        [role.oid, ids.sessions],
      )
    ).rows)
      out.push(`privileges on sequence ${q.rel}`);
    for (const f of (
      await client.query<{ fn: string }>(
        `SELECT p.oid::regprocedure::text AS fn FROM pg_proc p
          WHERE p.pronamespace = $2 AND p.prorettype <> 'trigger'::regtype AND has_function_privilege($1::oid, p.oid, 'EXECUTE') ORDER BY 1`,
        [role.oid, ids.sessions],
      )
    ).rows)
      out.push(`EXECUTE on ${f.fn}`);
  }
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
  if (found.length)
    throw guardError(
      "unledgered_schema",
      `objects of unrecorded migrations already exist: ${found.slice(0, 5).join("; ")}${found.length > 5 ? `; and ${found.length - 5} more` : ""}. Nothing is imported or marked, see the runbook decision point`,
    );
}
