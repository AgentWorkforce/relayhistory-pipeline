/**
 * The guarded production release's bootstrap-role stage: the plan section, its typed
 * approval, the exactness predicate for each chain role, plan-vs-stage drift and the
 * in-transaction readback verdict. Everything here except readBootstrapState and
 * releaseBootstrapFiles is pure; the database job only feeds it catalog rows.
 *
 * Evidence carries role names, attribute flags, membership edges and a settings kind
 * token only: never a password, rolconfig value, URL or host name.
 */
import { createHash } from "node:crypto";
import { execFileSync } from "node:child_process";
import {
  SESSION_BOOTSTRAP_FILES,
  SESSION_BOOTSTRAP_ROLES,
  type BootstrapRoleContract,
} from "./session-bootstrap-chain.ts";
import { SCHEMA_RELEASE as P, refuse } from "./schema-release-contract.ts";

/** pg_roles columns the stage reads: the name and the attribute flags only. */
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
}
/** One pg_auth_members row touching a chain role; inherit/set are null before PG16. */
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
/** One pg_db_role_setting row of a chain role, as a kind token (never the value). */
export interface BootstrapSetting {
  role: string;
  global: boolean;
  kind: "statement_timeout=2s" | "statement_timeout=60s" | "other";
}
export interface BootstrapState {
  serverVersionNum: number;
  /** Every role of the cluster (names and flags), for the side-effect diff. */
  roles: BootstrapRoleRow[];
  edges: BootstrapEdge[];
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

/**
 * A chain file runs inside the stage's single transaction, so it must not end or split
 * it: refuse transaction control (COMMIT, ROLLBACK, ABORT, SAVEPOINT, START/PREPARE
 * TRANSACTION, a top-level BEGIN;) with release_bootstrap_chain_unsafe. A bare END; is
 * indistinguishable from a plpgsql block end here; the stage's txid continuity check
 * catches that case.
 */
export function requireTransactionSafeChainSql(text: string) {
  if (
    /\b(COMMIT|ROLLBACK|ABORT|SAVEPOINT|START\s+TRANSACTION|PREPARE\s+TRANSACTION)\b|\bBEGIN\s*(WORK|TRANSACTION)?\s*;/i.test(
      text,
    )
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
 * exact): NOLOGIN and every flag false, rolconnlimit -1, member of none; before PG16 no
 * member, PG16+ at most the one Supabase creator edge, in supabase db mode only; no
 * setting except the worker's exact statement_timeout=2s.
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
const edgeKey = (e: BootstrapEdge) => JSON.stringify([e.role, e.member, e.grantor, e.admin_option, e.inherit_option, e.set_option]);
const settingKey = (s: BootstrapSetting) => JSON.stringify([s.role, s.global, s.kind]);

/**
 * The in-transaction readback verdict: exactly the approved roles appeared, each the
 * fresh bare shape; no other role added, removed or altered (names and flags of the
 * whole cluster); the edges and settings of every pre-existing chain role unchanged;
 * every chain role present and matching its contract. Anything else refuses
 * release_bootstrap_readback (the caller rolls back). Returns the evidence.
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
  const keep = (e: BootstrapEdge) => !createdSet.has(e.role) && !createdSet.has(e.member);
  const edgesBefore = x.before.edges.filter(keep).map(edgeKey).sort();
  const edgesAfter = x.after.edges.filter(keep).map(edgeKey).sort();
  const setBefore = x.before.settings.filter((s) => !createdSet.has(s.role)).map(settingKey).sort();
  const setAfter = x.after.settings.filter((s) => !createdSet.has(s.role)).map(settingKey).sort();
  const problems: string[] = [];
  if (JSON.stringify(created) !== JSON.stringify(approved)) problems.push("created");
  if (removed.length) problems.push("removed");
  if (altered.length) problems.push("altered");
  if (x.after.roles.length !== x.before.roles.length + created.length) problems.push("count");
  if (JSON.stringify(edgesBefore) !== JSON.stringify(edgesAfter)) problems.push("edges");
  if (JSON.stringify(setBefore) !== JSON.stringify(setAfter)) problems.push("settings");
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
/** Catalog rows only (names, flags, edges, setting kinds); never passwords or rolconfig. */
export async function readBootstrapState(client: Queryable): Promise<BootstrapState> {
  const names = bootstrapChainRoles().map((r) => r.name);
  const serverVersionNum = Number(
    (await client.query("SELECT current_setting('server_version_num')::int AS v")).rows[0]?.v,
  );
  if (!Number.isSafeInteger(serverVersionNum) || serverVersionNum < 150000)
    refuse("release_bootstrap_server");
  const pg16 = serverVersionNum >= 160000;
  const roles = (
    await client.query(
      `SELECT ${BOOTSTRAP_ROLE_COLUMNS.join(", ")} FROM pg_roles ORDER BY rolname`,
    )
  ).rows as BootstrapRoleRow[];
  const edges = (
    await client.query(
      `SELECT r.rolname AS role, m.rolname AS member, m.rolinherit AS member_inherit,
              g.rolname AS grantor, g.rolsuper AS grantor_super, a.admin_option,
              ${pg16 ? "a.inherit_option, a.set_option" : "NULL::boolean AS inherit_option, NULL::boolean AS set_option"}
         FROM pg_auth_members a
         JOIN pg_roles r ON r.oid = a.roleid
         JOIN pg_roles m ON m.oid = a.member
         JOIN pg_roles g ON g.oid = a.grantor
        WHERE r.rolname = ANY($1::text[]) OR m.rolname = ANY($1::text[])
        ORDER BY 1, 2, 4`,
      [names],
    )
  ).rows as BootstrapEdge[];
  const settings = (
    await client.query(
      `SELECT r.rolname AS role, s.setdatabase = 0 AS global,
              CASE WHEN s.setconfig = ARRAY['statement_timeout=2s']::text[] THEN 'statement_timeout=2s'
                   WHEN s.setconfig = ARRAY['statement_timeout=60s']::text[] THEN 'statement_timeout=60s'
                   ELSE 'other' END AS kind
         FROM pg_db_role_setting s JOIN pg_roles r ON r.oid = s.setrole
        WHERE r.rolname = ANY($1::text[])
        ORDER BY 1, 2, 3`,
      [names],
    )
  ).rows as BootstrapSetting[];
  return { serverVersionNum, roles, edges, settings };
}
