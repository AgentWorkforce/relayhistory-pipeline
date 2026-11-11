/**
 * The guarded production release's bootstrap-role stage: the plan section, its typed
 * approval, the exactness predicate for each chain role, plan-vs-stage drift, the
 * in-transaction readback verdict and the stage itself (runBootstrapStage) over an
 * already connected client. Everything here except readBootstrapState,
 * releaseBootstrapFiles and runBootstrapStage is pure; the database job only feeds it
 * catalog rows.
 *
 * Evidence carries role names, attribute flags, validity, membership edges and a
 * settings kind token only: never a password, rolconfig value, URL or host name. A
 * setting row's sha256 digest is compared in memory and never written to evidence.
 */
import { createHash } from "node:crypto";
import { execFileSync } from "node:child_process";
import {
  SESSION_BOOTSTRAP_FILES,
  SESSION_BOOTSTRAP_ROLES,
  type BootstrapRoleContract,
} from "./session-bootstrap-chain.ts";
import { SCHEMA_RELEASE as P, refuse } from "./schema-release-contract.ts";

/**
 * pg_roles columns the stage reads: the name, the attribute flags and the password
 * validity (as text; never rolpassword).
 */
export const BOOTSTRAP_ROLE_COLUMNS = [
  "rolname",
  "rolsuper",
  "rolinherit",
  "rolcreaterole",
  "rolcreatedb",
  "rolcanlogin",
  "rolreplication",
  "rolbypassrls",
  "rolconnlimit",
  "rolvaliduntil",
] as const;
export interface BootstrapRoleRow {
  rolname: string;
  rolsuper: boolean;
  rolinherit: boolean;
  rolcreaterole: boolean;
  rolcreatedb: boolean;
  rolcanlogin: boolean;
  rolreplication: boolean;
  rolbypassrls: boolean;
  rolconnlimit: number;
  /** rolvaliduntil::text, null when unset. */
  rolvaliduntil: string | null;
}
/**
 * One pg_auth_members row of the cluster; inherit/set are null before PG16. A grantor
 * OID without a role (possible before PG16) reads as `oid <n>`, never a superuser.
 */
export interface BootstrapEdge {
  role: string;
  member: string;
  member_inherit: boolean;
  grantor: string;
  grantor_super: boolean;
  admin_option: boolean;
  inherit_option: boolean | null;
  set_option: boolean | null;
}
/**
 * One pg_db_role_setting row of the cluster, as a kind token (never the value). role is
 * "" for a database-wide row (setrole 0); database is the setdatabase OID as text ("0":
 * role-wide). digest is sha256 over setconfig::text, computed by the server, used only
 * for the before/after comparison and never written to evidence.
 */
export interface BootstrapSetting {
  role: string;
  global: boolean;
  kind: "statement_timeout=2s" | "statement_timeout=60s" | "other";
  database: string;
  digest: string;
}
export interface BootstrapState {
  serverVersionNum: number;
  /** Every role of the cluster (names, flags, validity), for the side-effect diff. */
  roles: BootstrapRoleRow[];
  /** Every membership edge of the cluster. */
  edges: BootstrapEdge[];
  /** Every pg_db_role_setting row of the cluster. */
  settings: BootstrapSetting[];
}
export interface BootstrapFile {
  file: string;
  sha256_release: string;
  sha256_target: string | null;
  differs: boolean;
}
export interface BootstrapRole {
  name: string;
  file: string;
  present: boolean;
  exact: boolean;
  will_create: boolean;
}
export interface BootstrapSection {
  db_mode: "supabase";
  files: BootstrapFile[];
  roles: BootstrapRole[];
  required: boolean;
  will_create: string[];
  chain_digest: string;
  next: "bootstrap stage required before apply" | "apply";
}

export const BOOTSTRAP_REQUIRED_NOTE = "bootstrap stage required before apply";
const codeSort = (a: string[]) => a.sort((p, q) => (p < q ? -1 : p > q ? 1 : 0));
const sha256 = (b: Buffer | string) => createHash("sha256").update(b).digest("hex");
const ROLE_NAME = /^[a-z_][a-z0-9_]{0,62}$/;

/** Every chain role in chain order, with its file and contract. */
export function bootstrapChainRoles(
  map: typeof SESSION_BOOTSTRAP_ROLES = SESSION_BOOTSTRAP_ROLES,
) {
  return SESSION_BOOTSTRAP_FILES.flatMap((file) =>
    Object.entries(map[file]).map(([name, contract]) => ({ name, file, contract })),
  );
}

/** sha256 over the ordered `file\n<sha256_release>\n` lines of the whole chain. */
export function bootstrapChainDigest(
  files: readonly Pick<BootstrapFile, "file" | "sha256_release">[],
) {
  return sha256(files.map((f) => `${f.file}\n${f.sha256_release}\n`).join(""));
}

/** The one typed BOOTSTRAP approval string (approval roles: sorted, comma-joined, or none). */
export function bootstrapApproval(x: {
  project: string;
  sha: string;
  target: string;
  targetSha: string;
  chainDigest: string;
  willCreate: readonly string[];
  planRun: string;
}) {
  const roles = x.willCreate.length ? codeSort([...x.willCreate]).join(",") : "none";
  return `BOOTSTRAP ${x.project} ${x.sha} TARGET ${x.target} ${x.targetSha} CHAIN ${x.chainDigest} ROLES ${roles} PLAN ${x.planRun}`;
}

/** The roles of a parsed `ROLES` field (`none` is the empty list). */
export function approvalRoles(field: string): string[] {
  return field === "none" ? [] : field.split(",");
}

/** psql meta lines dropped, as the rehearsal's psql() helper does. */
export function stripPsqlMeta(text: string) {
  return text
    .split("\n")
    .filter((l) => !l.startsWith("\\"))
    .join("\n");
}

const IDENT_START = /[A-Za-z_\u0080-￿]/;
const IDENT_CONT = /[A-Za-z0-9_$\u0080-￿]/;
const DOLLAR_TAG = /\$(?:[A-Za-z_\u0080-￿][A-Za-z0-9_\u0080-￿]*)?\$/y;

/**
 * The top level of a SQL script: every `-- ...` and (nested) `/* ... *\/` comment,
 * '...' / E'...' string, "..." identifier and $tag$ ... $tag$ body replaced by one
 * space, as PostgreSQL's lexer delimits them (standard_conforming_strings on; an
 * identifier may contain `$`, so `x$$` opens no body). Refuses
 * release_bootstrap_chain_unsafe for an unterminated comment, quote or body.
 */
export function chainSqlTopLevel(text: string): string {
  const unsafe = () => refuse("release_bootstrap_chain_unsafe");
  const n = text.length;
  const quoted = (start: number, q: string, backslash: boolean) => {
    let j = start + 1;
    for (;;) {
      if (j >= n) unsafe();
      const c = text[j];
      if (backslash && c === "\\") j += 2;
      else if (c === q) {
        if (text[j + 1] !== q) return j + 1;
        j += 2;
      } else j++;
    }
  };
  let out = "";
  let i = 0;
  while (i < n) {
    const c = text[i];
    if (c === "-" && text[i + 1] === "-") {
      const eol = text.indexOf("\n", i);
      i = eol < 0 ? n : eol;
      out += " ";
    } else if (c === "/" && text[i + 1] === "*") {
      let depth = 1;
      i += 2;
      while (depth) {
        if (i >= n) unsafe();
        if (text[i] === "/" && text[i + 1] === "*") (depth++, (i += 2));
        else if (text[i] === "*" && text[i + 1] === "/") (depth--, (i += 2));
        else i++;
      }
      out += " ";
    } else if (c === "'" || c === '"') {
      i = quoted(i, c, false);
      out += " ";
    } else if (c === "$") {
      DOLLAR_TAG.lastIndex = i;
      const tag = DOLLAR_TAG.exec(text)?.[0];
      if (tag === undefined) {
        out += c;
        i++;
        continue;
      }
      const close = text.indexOf(tag, i + tag.length);
      if (close < 0) unsafe();
      i = close + tag.length;
      out += " ";
    } else if (IDENT_START.test(c)) {
      let j = i + 1;
      while (j < n && IDENT_CONT.test(text[j])) j++;
      const word = text.slice(i, j);
      if ((word === "E" || word === "e") && text[j] === "'") {
        i = quoted(j, "'", true);
        out += " ";
      } else {
        out += word;
        i = j;
      }
    } else {
      out += c;
      i++;
    }
  }
  return out;
}

/**
 * A chain file runs inside the stage's single transaction, so it must not end or split
 * it; else release_bootstrap_chain_unsafe. Anywhere in the text, comments and bodies
 * included (strict on purpose: these never belong in a chain file, even as a comment
 * word): COMMIT, ROLLBACK, ABORT, SAVEPOINT, START/PREPARE TRANSACTION, BEGIN [WORK |
 * TRANSACTION];. At the top level (chainSqlTopLevel: comments, strings, identifiers and
 * dollar bodies removed) additionally RELEASE, and any statement starting with BEGIN or
 * END (so `END;`, `end ;`, `END WORK;`, `END TRANSACTION;`), while a plpgsql `END`
 * inside a DO body stays allowed. A SQL-standard `BEGIN ATOMIC ... END;` function body
 * at the top level is refused too (no chain file uses one). The stage's txid
 * continuity check remains as defence in depth.
 */
export function requireTransactionSafeChainSql(text: string) {
  if (
    /\b(COMMIT|ROLLBACK|ABORT|SAVEPOINT|START\s+TRANSACTION|PREPARE\s+TRANSACTION)\b|\bBEGIN\s*(WORK|TRANSACTION)?\s*;/i.test(
      text,
    )
  )
    refuse("release_bootstrap_chain_unsafe");
  const top = chainSqlTopLevel(text);
  if (
    /\b(COMMIT|ROLLBACK|ABORT|SAVEPOINT|RELEASE|START\s+TRANSACTION|PREPARE\s+TRANSACTION)\b/i.test(top) ||
    top.split(";").some((statement) => /^\s*(BEGIN|END)\b/i.test(statement))
  )
    refuse("release_bootstrap_chain_unsafe");
  return text;
}

const FLAG_FALSE = [
  "rolsuper",
  "rolinherit",
  "rolcreaterole",
  "rolcreatedb",
  "rolreplication",
  "rolbypassrls",
] as const;

/** The exact Supabase PG16+ creator edge (member postgres, grantor superuser supabase_admin). */
export function isSupabaseCreatorEdge(e: BootstrapEdge) {
  return (
    e.member === P.owner &&
    e.grantor === "supabase_admin" &&
    e.grantor_super === true &&
    e.admin_option === true &&
    e.inherit_option === false &&
    e.set_option === false
  );
}

function inboundProblems(
  inbound: BootstrapRoleContract["inbound"],
  edges: BootstrapEdge[],
  pg16: boolean,
  supabaseMode: boolean,
): string[] {
  if (inbound === "unchecked") return [];
  const out: string[] = [];
  for (const e of edges) {
    const creator = pg16 && supabaseMode && isSupabaseCreatorEdge(e);
    let ok = false;
    if (inbound === "none") ok = creator;
    else if (inbound === "worker")
      ok =
        creator ||
        (e.member === "authenticator" &&
          !e.member_inherit &&
          !e.admin_option &&
          (!pg16 || e.inherit_option === false));
    else if (inbound === "verifier")
      ok =
        creator ||
        (e.member === "authenticator" &&
          !e.admin_option &&
          (pg16
            ? e.inherit_option === false && e.set_option === true
            : !e.member_inherit));
    else if (inbound === "consumer")
      ok =
        e.member === P.owner &&
        e.admin_option &&
        (!pg16 || (e.inherit_option === false && e.set_option === false));
    if (!ok) out.push(`member ${e.member}`);
  }
  return out;
}

/**
 * Why an existing chain role does not match its declared contract (empty: exact).
 * PG15 any member refuses unless the contract names it; PG16+ the creator edge is
 * accepted only in supabase db mode; any outbound membership refuses.
 */
export function roleContractProblems(
  name: string,
  contract: BootstrapRoleContract,
  state: Pick<BootstrapState, "serverVersionNum" | "roles" | "edges" | "settings">,
  supabaseMode: boolean,
): string[] {
  const r = state.roles.find((x) => x.rolname === name);
  if (!r) return ["absent"];
  const out: string[] = [];
  for (const f of FLAG_FALSE) if (r[f] !== false) out.push(f);
  if (r.rolconnlimit !== -1) out.push("rolconnlimit");
  if (typeof r.rolcanlogin !== "boolean") out.push("rolcanlogin");
  if (contract.login === "never" && r.rolcanlogin !== false) out.push("rolcanlogin");
  const pg16 = state.serverVersionNum >= 160000;
  if (state.edges.some((e) => e.member === name))
    out.push(
      ...state.edges.filter((e) => e.member === name).map((e) => `member of ${e.role}`),
    );
  out.push(
    ...inboundProblems(
      contract.inbound,
      state.edges.filter((e) => e.role === name),
      pg16,
      supabaseMode,
    ),
  );
  const settings = state.settings.filter((s) => s.role === name);
  const exactly = (kind: BootstrapSetting["kind"]) =>
    settings.length === 1 && settings[0].global && settings[0].kind === kind;
  if (contract.settings === "worker-2s" && !exactly("statement_timeout=2s"))
    out.push("settings");
  if (
    contract.settings === "consumer" &&
    (r.rolcanlogin ? !exactly("statement_timeout=60s") : settings.length !== 0)
  )
    out.push("settings");
  return out;
}

/**
 * Why a role this stage just created is not the bare shape its file creates (empty:
 * exact): NOLOGIN and every flag false, rolconnlimit -1, no VALID UNTIL, member of none;
 * before PG16 no member, PG16+ at most the one Supabase creator edge, in supabase db
 * mode only; no setting except the worker's exact statement_timeout=2s.
 */
export function freshRoleProblems(
  name: string,
  contract: BootstrapRoleContract,
  state: Pick<BootstrapState, "serverVersionNum" | "roles" | "edges" | "settings">,
  supabaseMode: boolean,
): string[] {
  const out = roleContractProblems(
    name,
    { ...contract, login: "never", inbound: "none" },
    state,
    supabaseMode,
  );
  if (state.roles.find((x) => x.rolname === name)?.rolvaliduntil != null) out.push("rolvaliduntil");
  const inbound = state.edges.filter((e) => e.role === name);
  if (inbound.length > 1) out.push("members");
  const settings = state.settings.filter((s) => s.role === name);
  if (contract.settings === "worker-2s") {
    if (
      !(settings.length === 1 && settings[0].global && settings[0].kind === "statement_timeout=2s")
    )
      out.push("settings");
  } else if (settings.length) out.push("settings");
  return [...new Set(out)];
}

/**
 * The plan's bootstrap section from release-sha bytes (and target-sha digests, info
 * only) and a catalog state. Refuses release_bootstrap_bytes_missing when a chain file
 * is absent from the release tree and release_bootstrap_role_drift when a present
 * chain role does not match its contract (the drifted names go to stderr).
 */
export function bootstrapSection(x: {
  files: readonly { file: string; release: Buffer | string | undefined; target: Buffer | string | undefined }[];
  state: BootstrapState;
  supabaseMode: boolean;
  map?: typeof SESSION_BOOTSTRAP_ROLES;
  log?: (line: string) => void;
}): BootstrapSection {
  if (
    x.files.length !== SESSION_BOOTSTRAP_FILES.length ||
    x.files.some((f, i) => f.file !== SESSION_BOOTSTRAP_FILES[i])
  )
    refuse("release_bootstrap_chain");
  const files: BootstrapFile[] = x.files.map((f) => {
    if (f.release === undefined) refuse("release_bootstrap_bytes_missing");
    const sha256_release = sha256(f.release);
    const sha256_target = f.target === undefined ? null : sha256(f.target);
    return { file: f.file, sha256_release, sha256_target, differs: sha256_target !== sha256_release };
  });
  const drift: string[] = [];
  const roles: BootstrapRole[] = bootstrapChainRoles(x.map).map(({ name, file, contract }) => {
    const present = x.state.roles.some((r) => r.rolname === name);
    const problems = present ? roleContractProblems(name, contract, x.state, x.supabaseMode) : [];
    if (problems.length) drift.push(name);
    return { name, file, present, exact: present && !problems.length, will_create: !present };
  });
  if (drift.length) {
    (x.log ?? console.error)("release_bootstrap_role_drift " + codeSort(drift).join(","));
    refuse("release_bootstrap_role_drift");
  }
  const willCreate = codeSort(roles.filter((r) => r.will_create).map((r) => r.name));
  return {
    db_mode: "supabase",
    files,
    roles,
    required: willCreate.length > 0,
    will_create: willCreate,
    chain_digest: bootstrapChainDigest(files),
    next: willCreate.length ? BOOTSTRAP_REQUIRED_NOTE : "apply",
  };
}

/** The chain files this stage runs: those provisioning an absent (will_create) role. */
export function bootstrapFilesToRun(section: Pick<BootstrapSection, "roles">) {
  const files = new Set(section.roles.filter((r) => r.will_create).map((r) => r.file));
  return SESSION_BOOTSTRAP_FILES.filter((f) => files.has(f));
}

const hex64 = /^[0-9a-f]{64}$/;
/** The plan artifact's bootstrap section, shape-checked; refuses release_plan_artifact. */
export function validateBootstrapSection(value: any): BootstrapSection {
  const ok =
    value &&
    value.db_mode === "supabase" &&
    Array.isArray(value.files) &&
    value.files.length === SESSION_BOOTSTRAP_FILES.length &&
    value.files.every(
      (f: any, i: number) =>
        f?.file === SESSION_BOOTSTRAP_FILES[i] &&
        hex64.test(f.sha256_release ?? "") &&
        (f.sha256_target === null || hex64.test(f.sha256_target ?? "")) &&
        f.differs === (f.sha256_target !== f.sha256_release),
    ) &&
    Array.isArray(value.roles) &&
    Array.isArray(value.will_create) &&
    value.will_create.every((n: unknown) => typeof n === "string" && ROLE_NAME.test(n)) &&
    typeof value.required === "boolean" &&
    value.required === value.will_create.length > 0 &&
    hex64.test(value.chain_digest ?? "") &&
    bootstrapChainDigest(value.files) === value.chain_digest &&
    value.next === (value.required ? BOOTSTRAP_REQUIRED_NOTE : "apply");
  const chain = bootstrapChainRoles();
  if (
    !ok ||
    value.roles.length !== chain.length ||
    value.roles.some(
      (r: any, i: number) =>
        r?.name !== chain[i].name ||
        r.file !== chain[i].file ||
        typeof r.present !== "boolean" ||
        r.exact !== r.present ||
        r.will_create !== !r.present,
    ) ||
    JSON.stringify(codeSort(value.roles.filter((r: any) => r.will_create).map((r: any) => r.name))) !==
      JSON.stringify(value.will_create)
  )
    refuse("release_plan_artifact");
  return value as BootstrapSection;
}

/** Refuses release_bootstrap_plan_changed when files/chain_digest/required/will_create differ. */
export function requireSameBootstrapPlan(
  planned: Pick<BootstrapSection, "files" | "chain_digest" | "required" | "will_create">,
  live: Pick<BootstrapSection, "files" | "chain_digest" | "required" | "will_create">,
) {
  if (
    JSON.stringify(planned.files) !== JSON.stringify(live.files) ||
    planned.chain_digest !== live.chain_digest ||
    planned.required !== live.required ||
    JSON.stringify(planned.will_create) !== JSON.stringify(live.will_create)
  )
    refuse("release_bootstrap_plan_changed");
}

/**
 * Apply guard: refuses release_bootstrap_required, listing the absent chain role names
 * on stderr, when any chain role is absent from the catalog.
 */
export function requireBootstrapRoles(
  state: { roles: readonly { rolname: string }[] },
  log: (line: string) => void = console.error,
) {
  const names = new Set(state.roles.map((r) => r.rolname));
  const absent = codeSort(bootstrapChainRoles().map((r) => r.name).filter((n) => !names.has(n)));
  if (absent.length) {
    log("release_bootstrap_required " + absent.join(","));
    refuse("release_bootstrap_required");
  }
}

const flags = (r: BootstrapRoleRow) =>
  JSON.stringify(BOOTSTRAP_ROLE_COLUMNS.map((c) => r[c]));
const edgeKey = (e: BootstrapEdge) =>
  JSON.stringify([e.role, e.member, e.grantor, e.grantor_super, e.admin_option, e.inherit_option, e.set_option]);
const settingKey = (s: BootstrapSetting) =>
  JSON.stringify([s.role, s.database, s.global, s.kind, s.digest]);
/** Multiset equality of two key lists. */
const sameKeys = (a: string[], b: string[]) =>
  JSON.stringify([...a].sort()) === JSON.stringify([...b].sort());

/**
 * The in-transaction readback verdict over the whole cluster: exactly the approved roles
 * appeared, each the fresh bare shape; no other role added or removed and no role's
 * flags, connection limit or validity altered; every pg_auth_members edge (role, member,
 * grantor, admin/inherit/set options) and every pg_db_role_setting row unchanged except
 * those a created role may gain (PG16+ in supabase db mode at most its Supabase creator
 * edge; the worker its one statement_timeout=2s row, checked by freshRoleProblems); every
 * chain role present and matching its contract. Anything else refuses
 * release_bootstrap_readback (the caller rolls back). Returns the evidence. Privileges
 * on objects are out of scope: no chain file grants any.
 */
export function bootstrapReadbackVerdict(x: {
  before: BootstrapState;
  after: BootstrapState;
  approved: readonly string[];
  supabaseMode: boolean;
  map?: typeof SESSION_BOOTSTRAP_ROLES;
}) {
  const chain = bootstrapChainRoles(x.map);
  const before = new Map(x.before.roles.map((r) => [r.rolname, r]));
  const after = new Map(x.after.roles.map((r) => [r.rolname, r]));
  const created = codeSort([...after.keys()].filter((n) => !before.has(n)));
  const removed = codeSort([...before.keys()].filter((n) => !after.has(n)));
  const altered = codeSort(
    [...before.keys()].filter((n) => after.has(n) && flags(before.get(n)!) !== flags(after.get(n)!)),
  );
  const approved = codeSort([...x.approved]);
  const createdSet = new Set(created);
  const pg16 = x.after.serverVersionNum >= 160000;
  // The one edge a created role may gain: its Supabase creator edge (PG16+, supabase
  // db mode); freshRoleProblems refuses more than one inbound edge per created role.
  const permittedEdge = (e: BootstrapEdge) =>
    createdSet.has(e.role) && pg16 && x.supabaseMode && isSupabaseCreatorEdge(e);
  const edgesAfter = x.after.edges.filter((e) => !permittedEdge(e)).map(edgeKey);
  // Settings a created role gained are checked against its contract by freshRoleProblems.
  const setAfter = x.after.settings.filter((s) => !createdSet.has(s.role)).map(settingKey);
  const problems: string[] = [];
  if (JSON.stringify(created) !== JSON.stringify(approved)) problems.push("created");
  if (removed.length) problems.push("removed");
  if (altered.length) problems.push("altered");
  if (x.after.roles.length !== x.before.roles.length + created.length) problems.push("count");
  if (!sameKeys(x.before.edges.map(edgeKey), edgesAfter)) problems.push("edges");
  if (!sameKeys(x.before.settings.map(settingKey), setAfter)) problems.push("settings");
  if (x.after.serverVersionNum !== x.before.serverVersionNum) problems.push("server");
  for (const { name, contract } of chain) {
    const p = createdSet.has(name)
      ? freshRoleProblems(name, contract, x.after, x.supabaseMode)
      : roleContractProblems(name, contract, x.after, x.supabaseMode);
    if (p.length) problems.push("contract " + name);
  }
  const chainNames = new Set(chain.map((r) => r.name));
  const roleView = (rows: Map<string, BootstrapRoleRow>) =>
    [...rows.values()]
      .filter((r) => chainNames.has(r.rolname))
      .map((r) => Object.fromEntries(BOOTSTRAP_ROLE_COLUMNS.map((c) => [c, r[c]])));
  const evidence = {
    skipped: false,
    created,
    approved,
    roles_before: roleView(before),
    roles_after: roleView(after),
    created_edges: x.after.edges
      .filter((e) => createdSet.has(e.role) || createdSet.has(e.member))
      .map((e) => ({
        role: e.role,
        member: e.member,
        grantor: e.grantor,
        admin_option: e.admin_option,
        inherit_option: e.inherit_option,
        set_option: e.set_option,
      })),
    cluster: { roles_before: x.before.roles.length, roles_after: x.after.roles.length, removed, altered },
    problems,
  };
  if (problems.length) {
    console.error("release_bootstrap_readback " + problems.join(";"));
    refuse("release_bootstrap_readback");
  }
  return evidence;
}

/** Each chain file's bytes at a commit, from the Git object database (never the working tree). */
export function releaseBootstrapFiles(repo: string, releaseSha: string, targetSha: string) {
  const blob = (sha: string, file: string) => {
    try {
      return execFileSync("git", ["cat-file", "blob", `${sha}:${file}`], {
        cwd: repo,
        env: { PATH: process.env.PATH },
        stdio: ["ignore", "pipe", "ignore"],
        maxBuffer: 4 * 1024 * 1024,
      });
    } catch {
      return undefined;
    }
  };
  return SESSION_BOOTSTRAP_FILES.map((file) => ({
    file,
    release: blob(releaseSha, file),
    target: blob(targetSha, file),
  }));
}

interface Queryable {
  query(sql: string, params?: unknown[]): Promise<{ rows: any[] }>;
}
const roleSelect = BOOTSTRAP_ROLE_COLUMNS.map((c) =>
  c === "rolvaliduntil" ? "rolvaliduntil::text AS rolvaliduntil" : c,
).join(", ");
/**
 * Catalog rows of the whole cluster (role names, flags and validity, every membership
 * edge, every per-database role setting as a kind token plus a server-side digest);
 * never rolpassword or a rolconfig/setconfig value.
 */
export async function readBootstrapState(client: Queryable): Promise<BootstrapState> {
  const serverVersionNum = Number(
    (await client.query("SELECT current_setting('server_version_num')::int AS v")).rows[0]?.v,
  );
  if (!Number.isSafeInteger(serverVersionNum) || serverVersionNum < 150000)
    refuse("release_bootstrap_server");
  const pg16 = serverVersionNum >= 160000;
  const roles = (
    await client.query(`SELECT ${roleSelect} FROM pg_roles ORDER BY rolname`)
  ).rows as BootstrapRoleRow[];
  const edges = (
    await client.query(
      `SELECT r.rolname AS role, m.rolname AS member, m.rolinherit AS member_inherit,
              COALESCE(g.rolname, 'oid ' || a.grantor::text) AS grantor,
              COALESCE(g.rolsuper, false) AS grantor_super, a.admin_option,
              ${pg16 ? "a.inherit_option, a.set_option" : "NULL::boolean AS inherit_option, NULL::boolean AS set_option"}
         FROM pg_auth_members a
         JOIN pg_roles r ON r.oid = a.roleid
         JOIN pg_roles m ON m.oid = a.member
         LEFT JOIN pg_roles g ON g.oid = a.grantor
        ORDER BY 1, 2, 4`,
    )
  ).rows as BootstrapEdge[];
  const settings = (
    await client.query(
      `SELECT COALESCE(r.rolname, '') AS role, s.setdatabase::text AS database,
              s.setdatabase = 0 AS global,
              CASE WHEN s.setconfig = ARRAY['statement_timeout=2s']::text[] THEN 'statement_timeout=2s'
                   WHEN s.setconfig = ARRAY['statement_timeout=60s']::text[] THEN 'statement_timeout=60s'
                   ELSE 'other' END AS kind,
              encode(sha256(convert_to(COALESCE(s.setconfig::text, ''), 'UTF8')), 'hex') AS digest
         FROM pg_db_role_setting s LEFT JOIN pg_roles r ON r.oid = s.setrole
        ORDER BY 1, 2, 4, 5`,
    )
  ).rows as BootstrapSetting[];
  return { serverVersionNum, roles, edges, settings };
}

/**
 * The chain digest of the release-sha bytes alone (no catalog): refuses
 * release_bootstrap_chain for a wrong file list and release_bootstrap_bytes_missing for
 * an absent file.
 */
export function releaseChainDigest(
  files: readonly { file: string; release: Buffer | string | undefined }[],
) {
  if (
    files.length !== SESSION_BOOTSTRAP_FILES.length ||
    files.some((f, i) => f.file !== SESSION_BOOTSTRAP_FILES[i])
  )
    refuse("release_bootstrap_chain");
  return bootstrapChainDigest(
    files.map((f) => {
      if (f.release === undefined) refuse("release_bootstrap_bytes_missing");
      return { file: f.file, sha256_release: sha256(f.release) };
    }),
  );
}

/**
 * Before connecting: the release-sha chain bytes digest to the plan artifact's
 * chain_digest and to the typed approval's CHAIN; else release_bootstrap_plan_changed.
 * The stage recomputes the whole section under the lock as well.
 */
export function requireReleaseChainDigest(
  files: readonly { file: string; release: Buffer | string | undefined }[],
  planned: Pick<BootstrapSection, "chain_digest">,
  approvalChain: string,
) {
  const digest = releaseChainDigest(files);
  if (digest !== planned.chain_digest || digest !== approvalChain)
    refuse("release_bootstrap_plan_changed");
  return digest;
}

export const BOOTSTRAP_LOCK = "SELECT pg_advisory_lock(961996, 1)";
export const BOOTSTRAP_UNLOCK = "SELECT pg_advisory_unlock(961996, 1)";

/**
 * The bootstrap stage over an already connected client, which it always ends. Under the
 * migrator's session advisory lock: underLock (the caller's GitHub gate and anchor
 * recheck), then ONE transaction with hoopdb.db_mode=supabase bound: recompute the plan's
 * bootstrap section and refuse release_bootstrap_plan_changed on any difference; skip
 * (ROLLBACK, skipped evidence) when nothing is absent; otherwise run each chain file
 * provisioning an absent role (release-sha bytes, psql meta lines stripped, transaction
 * safe), require the same txid, read back and COMMIT only on an exact verdict. Every
 * refusal or error rolls back, unlocks and ends the client, then propagates.
 */
export async function runBootstrapStage(x: {
  client: Queryable & { end(): Promise<void> };
  files: readonly { file: string; release: Buffer | string | undefined; target: Buffer | string | undefined }[];
  planned: BootstrapSection;
  supabaseMode: boolean;
  underLock: () => Promise<void>;
  writeEvidence: (evidence: Record<string, unknown>) => Promise<void>;
  /** Non-secret evidence header (version, shas, plan run, run id). */
  header: Record<string, unknown>;
  log?: (line: string) => void;
}): Promise<"skipped" | "verified"> {
  const { client } = x;
  const log = x.log ?? console.log;
  let locked = false;
  try {
    await client.query(BOOTSTRAP_LOCK);
    locked = true;
    await x.underLock();
    await client.query("BEGIN");
    let ended = false;
    try {
      await client.query(
        "SELECT set_config('hoopdb.db_mode', $1, true), set_config('hoopdb.allow_blocking_index_build', $2, true)",
        ["supabase", "off"],
      );
      const before = await readBootstrapState(client);
      const live = bootstrapSection({ files: x.files, state: before, supabaseMode: x.supabaseMode });
      requireSameBootstrapPlan(x.planned, live);
      if (!live.required) {
        await client.query("ROLLBACK");
        ended = true;
        await x.writeEvidence({
          ...x.header,
          chain_digest: live.chain_digest,
          files: live.files,
          skipped: true,
          created: [],
          roles: live.roles,
        });
        log("release_bootstrap_skipped");
        return "skipped";
      }
      const run = bootstrapFilesToRun(live);
      const sql = run.map((file) => {
        const bytes = x.files.find((f) => f.file === file)?.release;
        if (bytes === undefined) refuse("release_bootstrap_bytes_missing");
        return requireTransactionSafeChainSql(stripPsqlMeta(Buffer.from(bytes).toString("utf8")));
      });
      const xid = async () =>
        String((await client.query("SELECT txid_current()::text AS x")).rows[0]?.x ?? "");
      const xidBefore = await xid();
      for (const text of sql) await client.query(text);
      // Still the same transaction: no file ended or split it (else refuse, roll back).
      if (!xidBefore || (await xid()) !== xidBefore) refuse("release_bootstrap_readback");
      const after = await readBootstrapState(client);
      const evidence = bootstrapReadbackVerdict({
        before,
        after,
        approved: live.will_create,
        supabaseMode: x.supabaseMode,
      });
      await client.query("COMMIT");
      ended = true;
      await x.writeEvidence({
        ...x.header,
        chain_digest: live.chain_digest,
        files: live.files,
        ran: run,
        ...evidence,
      });
      log("release_bootstrap_created " + evidence.created.length);
      log("release_bootstrap_verified");
      return "verified";
    } finally {
      if (!ended) await client.query("ROLLBACK").catch(() => {});
    }
  } finally {
    if (locked) await client.query(BOOTSTRAP_UNLOCK).catch(() => {});
    await client.end().catch(() => {});
  }
}
