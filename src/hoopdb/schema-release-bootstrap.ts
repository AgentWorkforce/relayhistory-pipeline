/**
 * The guarded production release's bootstrap-role stage: the plan section, its typed
 * approval, the exactness predicate for each chain role, plan-vs-stage drift, the
 * in-transaction readback verdict and the stage itself (runBootstrapStage) over an
 * already connected client. Everything here except readBootstrapState,
 * releaseBootstrapFiles and runBootstrapStage is pure; the database job only feeds it
 * catalog rows and git bytes.
 *
 * Target-bound: the chain bytes and file list come from the bound anchor (target_sha)
 * tree, and only the roles whose first consumer (session-bootstrap-consumers.ts) sorts
 * at or before the target are required, created or checked. The release sha's bytes
 * are only compared (`differs`); a non-excluded file that differs refuses the stage.
 * The role contract map and the role-to-first-consumer manifest are the release sha's
 * reviewed modules; their domain-separated canonical digest is folded into CHAIN, and
 * when the anchor carries both modules they must be canonically equal (else
 * release_bootstrap_contract_drift). Only an anchor carrying neither (a legacy anchor)
 * falls back to the release's modules (contract_source "release").
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
import {
  SESSION_BOOTSTRAP_CONSUMERS,
  chainFileRoles,
  consumerStep,
} from "./session-bootstrap-consumers.ts";
import { RELEASE_TREE } from "./migrate.ts";
import { SCHEMA_RELEASE as P, RELEASE_TARGET_STEP, refuse } from "./schema-release-contract.ts";

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
/**
 * One chain file of the bound anchor (target_sha) tree. sha256_target digests the
 * anchor's bytes (the bytes the stage runs; null only when the anchor has no such file
 * and none of its roles is required); sha256_release digests the release sha's bytes
 * (null: absent), compared only to report `differs`. scope: "run" (every role the
 * anchor file provisions is target-required: it runs whole), "excluded" (none is: it
 * never runs and is named in evidence), "mixed" (required and above-target roles in one
 * file: the stage refuses release_bootstrap_chain_mixed; DO blocks are never parsed,
 * split or partially run).
 */
export interface BootstrapFile {
  file: string;
  sha256_target: string | null;
  sha256_release: string | null;
  differs: boolean;
  scope: "run" | "excluded" | "mixed";
}
export interface BootstrapRole {
  name: string;
  file: string;
  /** The role's first consumer (SESSION_BOOTSTRAP_CONSUMERS), a supabase/migrations file name. */
  first_consumer: string;
  /** First consumer at or before the bound target. */
  required: boolean;
  present: boolean;
  /** Present and matching its contract (informational for a role that is not required). */
  exact: boolean;
  will_create: boolean;
}
export interface BootstrapSection {
  db_mode: "supabase";
  target: string;
  target_sha: string;
  files: BootstrapFile[];
  roles: BootstrapRole[];
  /** The target-required roles, sorted. */
  required_roles: string[];
  /** Files excluded from the run (every role above the target), by name. */
  excluded: string[];
  /** Files mixing required and above-target roles (the stage refuses), by name. */
  mixed: string[];
  /** Non-excluded files whose release-sha bytes differ from the anchor's (the stage refuses). */
  release_drift: string[];
  /** The stage has work: a required role is absent. */
  required: boolean;
  will_create: string[];
  /**
   * "anchor": target_sha carries both contract modules and they are canonically equal to
   * the release sha's; "release": a legacy anchor carrying neither (grandfathered).
   */
  contract_source: BootstrapContractSource;
  /** sha256 of the canonical contract map serialisation (bootstrapContractDigests). */
  contract_map_digest: string;
  /** sha256 of the canonical manifest serialisation. */
  contract_manifest_digest: string;
  /** The domain-separated composite of both, folded into CHAIN. */
  contract_digest: string;
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

/**
 * The CHAIN digest: sha256 over `target <step>\ntarget_sha <sha>\nrequired <the sorted
 * required roles comma-joined, or none>\ncontract <contract_digest>\n` followed by
 * `<file>\n<sha256_target>\n` for each file that will run (scope "run"), in chain order,
 * from the anchor's bytes. The required set and the contract digest (the role contract
 * map and the manifest, bootstrapContractDigests) are folded in here rather than given
 * their own approval fields, so the typed approval changes whenever the target,
 * target_sha, the anchor bytes of a running file, the required set, a role contract or a
 * manifest entry changes.
 */
export function bootstrapChainDigest(x: {
  target: string;
  target_sha: string;
  required_roles: readonly string[];
  contract_digest: string;
  files: readonly Pick<BootstrapFile, "file" | "sha256_target" | "scope">[];
}) {
  const required = x.required_roles.length ? codeSort([...x.required_roles]).join(",") : "none";
  return sha256(
    `target ${x.target}\ntarget_sha ${x.target_sha}\nrequired ${required}\ncontract ${x.contract_digest}\n` +
      x.files
        .filter((f) => f.scope === "run")
        .map((f) => `${f.file}\n${f.sha256_target}\n`)
        .join(""),
  );
}

export type BootstrapContractSource = "anchor" | "release";
type ContractMap = Readonly<Record<string, Readonly<Record<string, BootstrapRoleContract>>>>;
type ContractPairs = readonly (readonly [string, string])[];
const CONTRACT_LOGIN = ["never", "allowed", "claimed-60s"] as const;
const CONTRACT_INBOUND = ["none", "worker", "verifier", "consumer", "unchecked"] as const;
const CONTRACT_SETTINGS = ["unchecked", "worker-2s", "consumer"] as const;

/**
 * The contract digests, domain-separated and canonical (independent of object key order,
 * file order, role order and manifest order):
 *   map       sha256("bootstrap-contract-map-v1\n" + one JSON line
 *             [file, role, login, inbound, settings] per role, lines sorted)
 *   manifest  sha256("bootstrap-contract-manifest-v1\n" + one JSON line [role, first
 *             consumer] per pair, lines sorted; a duplicate pair stays a line)
 *   composite sha256("bootstrap-contract-v1\nmap <map>\nmanifest <manifest>\n"), the
 *             `contract` line of CHAIN.
 * The contract fields are read in the fixed order login, inbound, settings.
 */
export function bootstrapContractDigests(
  map: ContractMap = SESSION_BOOTSTRAP_ROLES,
  consumers: ContractPairs = SESSION_BOOTSTRAP_CONSUMERS,
) {
  const mapLines = codeSort(
    Object.entries(map).flatMap(([file, roles]) =>
      Object.entries(roles).map(([role, c]) =>
        JSON.stringify([file, role, String(c.login), String(c.inbound), String(c.settings)]),
      ),
    ),
  );
  const manifestLines = codeSort(consumers.map((p) => JSON.stringify([String(p[0]), String(p[1])])));
  const contract_map_digest = sha256(
    "bootstrap-contract-map-v1\n" + mapLines.map((l) => l + "\n").join(""),
  );
  const contract_manifest_digest = sha256(
    "bootstrap-contract-manifest-v1\n" + manifestLines.map((l) => l + "\n").join(""),
  );
  return {
    contract_map_digest,
    contract_manifest_digest,
    contract_digest: sha256(
      `bootstrap-contract-v1\nmap ${contract_map_digest}\nmanifest ${contract_manifest_digest}\n`,
    ),
  };
}
/** The composite contract digest folded into CHAIN (bootstrapContractDigests). */
export function bootstrapContractDigest(map?: ContractMap, consumers?: ContractPairs) {
  return bootstrapContractDigests(map, consumers).contract_digest;
}

/**
 * The anchor's (target_sha) contract modules as read from the Git object database:
 * src/hoopdb/session-bootstrap-chain.ts (always present at an anchor; its
 * SESSION_BOOTSTRAP_ROLES section may be absent) and
 * src/hoopdb/session-bootstrap-consumers.ts (undefined: absent).
 */
export interface BootstrapAnchorContract {
  chainModule: Buffer | string | undefined;
  consumersModule: Buffer | string | undefined;
}

type Tok = { t: "s" | "id" | "p"; v: string };
const contractUnparsable = (): never => refuse("release_bootstrap_contract_drift");
/**
 * A minimal tokenizer for one literal value in a TypeScript module, from `start`:
 * `//` and `/* *\/` comments skipped, "..." / '...' strings without backslashes or line
 * breaks, identifiers and the punctuation { } [ ] ( ) : , ; only. Anything else (a
 * template literal, an operator, a spread, an escape) refuses
 * release_bootstrap_contract_drift: an anchor contract is never evaluated, only read.
 */
function contractTokens(text: string, start: number) {
  let i = start;
  return (): Tok | undefined => {
    for (;;) {
      while (i < text.length && /\s/.test(text[i])) i++;
      if (text.startsWith("//", i)) {
        const eol = text.indexOf("\n", i);
        i = eol < 0 ? text.length : eol;
      } else if (text.startsWith("/*", i)) {
        const end = text.indexOf("*/", i + 2);
        if (end < 0) contractUnparsable();
        i = end + 2;
      } else break;
    }
    if (i >= text.length) return undefined;
    const c = text[i];
    if (c === '"' || c === "'") {
      let j = i + 1;
      while (j < text.length && text[j] !== c) {
        if (text[j] === "\\" || text[j] === "\n") contractUnparsable();
        j++;
      }
      if (j >= text.length) contractUnparsable();
      const v = text.slice(i + 1, j);
      i = j + 1;
      return { t: "s", v };
    }
    const id = /[A-Za-z_][A-Za-z0-9_]*/y;
    id.lastIndex = i;
    const m = id.exec(text);
    if (m) {
      i += m[0].length;
      return { t: "id", v: m[0] };
    }
    if ("{}[]():,;".includes(c)) {
      i++;
      return { t: "p", v: c };
    }
    return contractUnparsable();
  };
}
/** The index just past the `=` and `open` of the single `export const <name>` declaration. */
function contractValueStart(text: string, name: string, open: "{" | "[") {
  const decl = new RegExp(`export const ${name}\\b`, "g");
  const hits = [...text.matchAll(decl)];
  if (hits.length !== 1) contractUnparsable();
  const eq = new RegExp(`=\\s*\\${open}`, "g");
  eq.lastIndex = hits[0].index! + hits[0][0].length;
  const m = eq.exec(text);
  if (!m) contractUnparsable();
  return m!.index + m![0].length;
}
const BARE_HELPER =
  'const bare = ( login: BootstrapRoleContract["login"], inbound: BootstrapRoleContract["inbound"], ): BootstrapRoleContract => ({ login, inbound, settings: "unchecked" });';
const oneOf = <T extends string>(v: string, allowed: readonly T[]): T =>
  (allowed as readonly string[]).includes(v) ? (v as T) : contractUnparsable();

/**
 * The SESSION_BOOTSTRAP_ROLES map a session-bootstrap-chain.ts text declares, parsed as
 * data (never evaluated): undefined when the text never names SESSION_BOOTSTRAP_ROLES (a
 * legacy anchor). Accepted: an object literal of file-path keys, each an object literal
 * of role keys whose values are `{ login, inbound, settings }` object literals (keys in
 * any order, each exactly once) or `bare("<login>", "<inbound>")` calls, the latter only
 * when the text defines bare exactly as this release does (settings "unchecked").
 * Anything else, a duplicate key or an unknown value refuses
 * release_bootstrap_contract_drift.
 */
export function chainModuleContract(text: string): ContractMap | undefined {
  if (!/\bSESSION_BOOTSTRAP_ROLES\b/.test(text)) return undefined;
  const next = contractTokens(text, contractValueStart(text, "SESSION_BOOTSTRAP_ROLES", "{"));
  const want = (v: string) => {
    const k = next();
    if (!k || k.t !== "p" || k.v !== v) contractUnparsable();
  };
  const bareOk =
    (text.match(/\bbare\b\s*=/g) ?? []).length === 1 &&
    !/\bfunction\s+bare\b/.test(text) &&
    text.replace(/\s+/g, " ").includes(BARE_HELPER);
  /** Object literal entries after its `{`: key, then value(parsed by `value`). */
  const entries = <V>(value: (first: Tok) => V) => {
    const out = new Map<string, V>();
    for (;;) {
      const k = next();
      if (!k) return contractUnparsable();
      if (k.t === "p" && k.v === "}") return out;
      if (k.t === "p" || out.has(k.v)) contractUnparsable();
      want(":");
      const first = next();
      if (!first) contractUnparsable();
      out.set(k.v, value(first!));
      const sep = next();
      if (sep?.t === "p" && sep.v === "}") return out;
      if (!(sep?.t === "p" && sep.v === ",")) contractUnparsable();
    }
  };
  const str = () => {
    const s = next();
    return s && s.t === "s" ? s.v : contractUnparsable();
  };
  const contract = (first: Tok): BootstrapRoleContract => {
    if (first.t === "id" && first.v === "bare") {
      if (!bareOk) contractUnparsable();
      want("(");
      const login = oneOf(str(), CONTRACT_LOGIN);
      want(",");
      const inbound = oneOf(str(), CONTRACT_INBOUND);
      const close = next();
      if (close?.t === "p" && close.v === ",") want(")");
      else if (!(close?.t === "p" && close.v === ")")) contractUnparsable();
      return { login, inbound, settings: "unchecked" };
    }
    if (!(first.t === "p" && first.v === "{")) contractUnparsable();
    const f = entries((v) => (v.t === "s" ? v.v : contractUnparsable()));
    if (f.size !== 3 || !f.has("login") || !f.has("inbound") || !f.has("settings")) contractUnparsable();
    return {
      login: oneOf(f.get("login")!, CONTRACT_LOGIN),
      inbound: oneOf(f.get("inbound")!, CONTRACT_INBOUND),
      settings: oneOf(f.get("settings")!, CONTRACT_SETTINGS),
    };
  };
  const files = entries((first) => {
    if (!(first.t === "p" && first.v === "{")) contractUnparsable();
    return entries(contract);
  });
  want(";");
  const map: Record<string, Record<string, BootstrapRoleContract>> = {};
  for (const [file, roles] of files) {
    if (!/^supabase\/bootstrap\/[a-z0-9_]+\.sql$/.test(file)) contractUnparsable();
    map[file] = {};
    for (const [role, c] of roles) {
      if (!ROLE_NAME.test(role) || role === "__proto__") contractUnparsable();
      map[file][role] = c;
    }
  }
  return map;
}

/**
 * The SESSION_BOOTSTRAP_CONSUMERS pairs a session-bootstrap-consumers.ts text declares,
 * parsed as data (never evaluated): an array literal of two-string array literals.
 * Anything else refuses release_bootstrap_contract_drift.
 */
export function consumersModuleContract(text: string): [string, string][] {
  const next = contractTokens(text, contractValueStart(text, "SESSION_BOOTSTRAP_CONSUMERS", "["));
  const is = (k: Tok | undefined, v: string) => k?.t === "p" && k.v === v;
  const out: [string, string][] = [];
  for (;;) {
    const open = next();
    if (is(open, "]")) break;
    if (!is(open, "[")) contractUnparsable();
    const role = next();
    if (!is(next(), ",")) contractUnparsable();
    const file = next();
    let close = next();
    if (is(close, ",")) close = next();
    if (role?.t !== "s" || file?.t !== "s" || !is(close, "]")) contractUnparsable();
    out.push([role!.v, file!.v]);
    const sep = next();
    if (is(sep, "]")) break;
    if (!is(sep, ",")) contractUnparsable();
  }
  if (!is(next(), ";")) contractUnparsable();
  return out;
}

/**
 * Where the role contract and the manifest come from, refusing
 * release_bootstrap_contract_drift (never falling back) when target_sha carries exactly
 * one of the two contract modules (the SESSION_BOOTSTRAP_ROLES section of
 * session-bootstrap-chain.ts, session-bootstrap-consumers.ts), when either present module
 * does not parse, or when the anchor's map or manifest differs canonically from the
 * release's (`map` / `consumers`: key order, file order and pair order are not
 * differences; any value is). Both present and equal: "anchor" (authoritative; the
 * release matches it). Both absent (a legacy anchor such as Release A's): "release",
 * grandfathered: the release's reviewed modules, bound by the composite digest in CHAIN,
 * with coverage proved by bootstrapTargetChain.
 */
export function bootstrapContractSource(
  anchor: BootstrapAnchorContract,
  map: ContractMap = SESSION_BOOTSTRAP_ROLES,
  consumers: ContractPairs = SESSION_BOOTSTRAP_CONSUMERS,
): BootstrapContractSource {
  const text = (b: Buffer | string | undefined) =>
    b === undefined ? undefined : Buffer.from(b).toString("utf8");
  const anchorMap = chainModuleContract(text(anchor.chainModule) ?? "");
  const consumersText = text(anchor.consumersModule);
  if (anchorMap === undefined && consumersText === undefined) return "release";
  if (anchorMap === undefined || consumersText === undefined)
    refuse("release_bootstrap_contract_drift");
  const anchorDigests = bootstrapContractDigests(anchorMap, consumersModuleContract(consumersText!));
  const release = bootstrapContractDigests(map, consumers);
  if (
    anchorDigests.contract_map_digest !== release.contract_map_digest ||
    anchorDigests.contract_manifest_digest !== release.contract_manifest_digest
  )
    refuse("release_bootstrap_contract_drift");
  return "anchor";
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

const FIRST_CONSUMER = /^[0-9]{14}_[a-z0-9_]+\.sql$/;
const laneOf = (step: string) => {
  const i = RELEASE_TREE.lanes.findIndex((l) => step.startsWith(l.prefix));
  if (i < 0 || !RELEASE_TARGET_STEP.test(step)) refuse("release_bootstrap_manifest");
  return i;
};
/**
 * Migrator order of two schema step names, as release-target.ts / schemaSteps order
 * them: lane first (RELEASE_TREE.lanes: every upstream step before every 96bulls step),
 * then the name within the lane (upstream names sort by their 14-digit prefix). Refuses
 * release_bootstrap_manifest for a name that is not a schema step.
 */
export function compareSteps(a: string, b: string) {
  const d = laneOf(a) - laneOf(b);
  return d !== 0 ? d : a < b ? -1 : a > b ? 1 : 0;
}

/**
 * The static role-to-first-consumer manifest, validated: refuses
 * release_bootstrap_manifest for a malformed entry, a duplicate or conflicting role, a
 * role that is not a chain role, or a chain role it does not map.
 */
export function bootstrapConsumers(
  consumers: readonly (readonly [string, string])[] = SESSION_BOOTSTRAP_CONSUMERS,
  map: typeof SESSION_BOOTSTRAP_ROLES = SESSION_BOOTSTRAP_ROLES,
): Map<string, string> {
  const chain = new Set(bootstrapChainRoles(map).map((r) => r.name));
  const out = new Map<string, string>();
  for (const entry of consumers) {
    const [role, file] = Array.isArray(entry) ? entry : [];
    if (
      !Array.isArray(entry) ||
      entry.length !== 2 ||
      typeof role !== "string" ||
      typeof file !== "string" ||
      !FIRST_CONSUMER.test(file) ||
      !chain.has(role) ||
      out.has(role)
    )
      refuse("release_bootstrap_manifest");
    out.set(role, file);
  }
  if (out.size !== chain.size) refuse("release_bootstrap_manifest");
  return out;
}

/**
 * The roles a bound target requires, sorted: those whose first consumer sorts at or
 * before the target (compareSteps). Pure on the target and the static manifest, so the
 * plan artifact's required_roles can be recomputed from its target alone.
 */
export function targetRequiredRoles(
  target: string,
  consumers?: readonly (readonly [string, string])[],
  map?: typeof SESSION_BOOTSTRAP_ROLES,
) {
  const m = bootstrapConsumers(consumers, map);
  laneOf(target);
  return codeSort(
    [...m].filter(([, file]) => compareSteps(consumerStep(file), target) <= 0).map(([role]) => role),
  );
}

/**
 * targetRequiredRoles bound to the anchor: the target must be one of the anchor's
 * ordered schema steps and every required role's first consumer must be an anchor step
 * at or before it; else release_bootstrap_manifest.
 */
export function bootstrapRequiredRoles(x: {
  target: string;
  steps: readonly string[];
  consumers?: readonly (readonly [string, string])[];
  map?: typeof SESSION_BOOTSTRAP_ROLES;
}) {
  const at = x.steps.indexOf(x.target);
  if (at < 0) refuse("release_bootstrap_manifest");
  const upTo = new Set(x.steps.slice(0, at + 1));
  const m = bootstrapConsumers(x.consumers, x.map);
  const required = targetRequiredRoles(x.target, x.consumers, x.map);
  if (required.some((r) => !upTo.has(consumerStep(m.get(r)!)))) refuse("release_bootstrap_manifest");
  return required;
}

/** One chain file's bytes at the anchor (target) and at the release sha; undefined: absent. */
export interface BootstrapChainInput {
  file: string;
  release: Buffer | string | undefined;
  target: Buffer | string | undefined;
}
export type BootstrapTargetChain = Pick<
  BootstrapSection,
  | "target"
  | "target_sha"
  | "files"
  | "required_roles"
  | "excluded"
  | "mixed"
  | "release_drift"
  | "contract_source"
  | "contract_map_digest"
  | "contract_manifest_digest"
  | "contract_digest"
  | "chain_digest"
>;

/**
 * The target-bound chain, from git alone (no catalog). Files in SESSION_BOOTSTRAP_FILES
 * order (else release_bootstrap_chain). For each file the roles its ANCHOR bytes
 * provision (chainFileRoles: literal names only, DO blocks never parsed) must be roles
 * SESSION_BOOTSTRAP_ROLES declares for that file and must include each of its
 * target-required roles (else release_bootstrap_chain); an anchor file absent while one
 * of its roles is required refuses release_bootstrap_bytes_missing. Scope per file: run
 * (all of its roles required), excluded (none), mixed (both). The release sha's bytes
 * only set `differs`; release_drift lists the non-excluded files that differ. Refusing
 * mixed and drifting files is the stage's job (requireRunnableChain); a plan reports them.
 *
 * The contract: bootstrapContractSource over the anchor's contract modules (refuses
 * release_bootstrap_contract_drift for partial, unparsable or canonically different
 * anchor modules), then the coverage proof (bootstrapContractCoverage), then the
 * contract digests, whose composite CHAIN folds in.
 */
export function bootstrapTargetChain(x: {
  target: string;
  targetSha: string;
  steps: readonly string[];
  files: readonly BootstrapChainInput[];
  /** The anchor's contract modules (releaseBootstrapContract). */
  contract: BootstrapAnchorContract;
  consumers?: readonly (readonly [string, string])[];
  map?: typeof SESSION_BOOTSTRAP_ROLES;
}): BootstrapTargetChain {
  if (
    x.files.length !== SESSION_BOOTSTRAP_FILES.length ||
    x.files.some((f, i) => f.file !== SESSION_BOOTSTRAP_FILES[i])
  )
    refuse("release_bootstrap_chain");
  if (!/^[0-9a-f]{40}$/.test(x.targetSha)) refuse("release_bootstrap_chain");
  const map: Readonly<Record<string, Readonly<Record<string, BootstrapRoleContract>>>> =
    x.map ?? SESSION_BOOTSTRAP_ROLES;
  const contract_source = bootstrapContractSource(x.contract, map, x.consumers ?? SESSION_BOOTSTRAP_CONSUMERS);
  const required_roles = bootstrapRequiredRoles(x);
  const req = new Set(required_roles);
  const files: BootstrapFile[] = x.files.map((f) => {
    const declared = new Set(Object.keys(map[f.file]));
    const sha256_release = f.release === undefined ? null : sha256(f.release);
    let sha256_target: string | null = null;
    let scope: BootstrapFile["scope"] = "excluded";
    if (f.target === undefined) {
      if ([...declared].some((r) => req.has(r))) refuse("release_bootstrap_bytes_missing");
    } else {
      sha256_target = sha256(f.target);
      const provisioned = chainFileRoles(Buffer.from(f.target).toString("utf8"));
      if (
        [...provisioned].some((r) => !declared.has(r)) ||
        [...declared].some((r) => req.has(r) && !provisioned.has(r))
      )
        refuse("release_bootstrap_chain");
      const n = [...provisioned].filter((r) => req.has(r)).length;
      scope = n === 0 ? "excluded" : n === provisioned.size ? "run" : "mixed";
    }
    return { file: f.file, sha256_target, sha256_release, differs: sha256_release !== sha256_target, scope };
  });
  bootstrapContractCoverage({ target: x.target, files: x.files, scopes: files, required_roles, map, consumers: x.consumers });
  const named = (p: (f: BootstrapFile) => boolean) => files.filter(p).map((f) => f.file);
  const chain = {
    target: x.target,
    target_sha: x.targetSha,
    files,
    required_roles,
    excluded: named((f) => f.scope === "excluded"),
    mixed: named((f) => f.scope === "mixed"),
    release_drift: named((f) => f.scope !== "excluded" && f.differs),
    contract_source,
    ...bootstrapContractDigests(map, x.consumers ?? SESSION_BOOTSTRAP_CONSUMERS),
  };
  return { ...chain, chain_digest: bootstrapChainDigest(chain) };
}

/**
 * The contract coverage proof (every contract source; the legacy fallback depends on
 * it): every role literal of every anchor chain file that is not excluded has a contract
 * entry for that file and a manifest entry; every required role is provisioned by a
 * non-excluded anchor file of its own contract file (no extra required role); and every
 * required role's first consumer sorts at or before the target. Refuses
 * release_bootstrap_contract_drift. bootstrapTargetChain's per-file checks
 * (release_bootstrap_chain, release_bootstrap_bytes_missing) and bootstrapConsumers
 * (release_bootstrap_manifest) refuse most violations first; this is the explicit proof.
 */
export function bootstrapContractCoverage(x: {
  target: string;
  files: readonly BootstrapChainInput[];
  scopes: readonly Pick<BootstrapFile, "file" | "scope">[];
  required_roles: readonly string[];
  map: ContractMap;
  consumers?: ContractPairs;
}) {
  const manifest = new Map(x.consumers ?? SESSION_BOOTSTRAP_CONSUMERS);
  const provisionedBy = new Map<string, string>();
  for (const f of x.files) {
    const scope = x.scopes.find((s) => s.file === f.file)?.scope;
    if (f.target === undefined || scope === undefined || scope === "excluded") continue;
    for (const role of chainFileRoles(Buffer.from(f.target).toString("utf8"))) {
      if (!x.map[f.file] || !Object.hasOwn(x.map[f.file], role) || !manifest.has(role))
        refuse("release_bootstrap_contract_drift");
      provisionedBy.set(role, f.file);
    }
  }
  for (const role of x.required_roles) {
    const file = provisionedBy.get(role);
    const consumer = manifest.get(role);
    if (
      file === undefined ||
      !Object.hasOwn(x.map[file], role) ||
      consumer === undefined ||
      compareSteps(consumerStep(consumer), x.target) > 0
    )
      refuse("release_bootstrap_contract_drift");
  }
}

/**
 * The stage runs only a clean chain: refuses release_bootstrap_chain_mixed when a file
 * mixes required and above-target roles, then release_bootstrap_release_drift when the
 * release sha's bytes of a non-excluded file differ from the anchor's (names on stderr).
 */
export function requireRunnableChain(
  chain: Pick<BootstrapSection, "mixed" | "release_drift">,
  log: (line: string) => void = console.error,
) {
  if (chain.mixed.length) {
    log("release_bootstrap_chain_mixed " + chain.mixed.join(","));
    refuse("release_bootstrap_chain_mixed");
  }
  if (chain.release_drift.length) {
    log("release_bootstrap_release_drift " + chain.release_drift.join(","));
    refuse("release_bootstrap_release_drift");
  }
}

const CHAIN_KEYS = [
  "target",
  "target_sha",
  "files",
  "required_roles",
  "excluded",
  "mixed",
  "release_drift",
  "contract_source",
  "contract_map_digest",
  "contract_manifest_digest",
  "contract_digest",
  "chain_digest",
] as const;
const chainView = (s: Pick<BootstrapSection, (typeof CHAIN_KEYS)[number]>) =>
  JSON.stringify(CHAIN_KEYS.map((k) => s[k]));

/**
 * The git-side binding, recomputed (before connecting, and again under the lock before
 * BEGIN): the target chain must equal the plan artifact's (target, target_sha, files,
 * required set, excluded/mixed/drift lists, contract source and digests, CHAIN digest)
 * and its digest the typed
 * approval's CHAIN, else release_bootstrap_plan_changed; then requireRunnableChain.
 */
export function requireBootstrapTargetChain(
  chain: BootstrapTargetChain,
  planned: Pick<BootstrapSection, (typeof CHAIN_KEYS)[number]>,
  approvalChain: string,
  log?: (line: string) => void,
) {
  if (chainView(chain) !== chainView(planned) || chain.chain_digest !== approvalChain)
    refuse("release_bootstrap_plan_changed");
  requireRunnableChain(chain, log);
  return chain.chain_digest;
}

/**
 * The plan's bootstrap section: the target-bound chain (bootstrapTargetChain, from the
 * anchor's bytes) plus a catalog state. A role is will_create when it is required and
 * absent. Refuses release_bootstrap_role_drift when a present REQUIRED role does not
 * match its contract (names on stderr); a role above the target is reported (present,
 * exact) and never required, created or refused. Mixed and drifting files are reported,
 * not refused (the stage refuses them).
 */
export function bootstrapSection(x: {
  target: string;
  targetSha: string;
  steps: readonly string[];
  files: readonly BootstrapChainInput[];
  contract: BootstrapAnchorContract;
  state: BootstrapState;
  supabaseMode: boolean;
  consumers?: readonly (readonly [string, string])[];
  map?: typeof SESSION_BOOTSTRAP_ROLES;
  log?: (line: string) => void;
}): BootstrapSection {
  const chain = bootstrapTargetChain(x);
  const consumers = bootstrapConsumers(x.consumers, x.map);
  const req = new Set(chain.required_roles);
  const drift: string[] = [];
  const roles: BootstrapRole[] = bootstrapChainRoles(x.map).map(({ name, file, contract }) => {
    const required = req.has(name);
    const present = x.state.roles.some((r) => r.rolname === name);
    const problems = present ? roleContractProblems(name, contract, x.state, x.supabaseMode) : [];
    if (required && problems.length) drift.push(name);
    return {
      name,
      file,
      first_consumer: consumers.get(name)!,
      required,
      present,
      exact: present && !problems.length,
      will_create: required && !present,
    };
  });
  if (drift.length) {
    (x.log ?? console.error)("release_bootstrap_role_drift " + codeSort(drift).join(","));
    refuse("release_bootstrap_role_drift");
  }
  const willCreate = codeSort(roles.filter((r) => r.will_create).map((r) => r.name));
  return {
    db_mode: "supabase",
    target: chain.target,
    target_sha: chain.target_sha,
    files: chain.files,
    roles,
    required_roles: chain.required_roles,
    excluded: chain.excluded,
    mixed: chain.mixed,
    release_drift: chain.release_drift,
    required: willCreate.length > 0,
    will_create: willCreate,
    contract_source: chain.contract_source,
    contract_map_digest: chain.contract_map_digest,
    contract_manifest_digest: chain.contract_manifest_digest,
    contract_digest: chain.contract_digest,
    chain_digest: chain.chain_digest,
    next: willCreate.length ? BOOTSTRAP_REQUIRED_NOTE : "apply",
  };
}

/** The chain files this stage runs: run-scope files provisioning an absent (will_create) role. */
export function bootstrapFilesToRun(section: Pick<BootstrapSection, "roles" | "files">) {
  const files = new Set(section.roles.filter((r) => r.will_create).map((r) => r.file));
  return section.files.filter((f) => f.scope === "run" && files.has(f.file)).map((f) => f.file);
}

const hex64 = /^[0-9a-f]{64}$/;
const sameList = (a: unknown, b: readonly string[]) => JSON.stringify(a) === JSON.stringify(b);
/**
 * The plan artifact's bootstrap section, shape-checked and self-consistent (required
 * set recomputed from its target and the static manifest, lists from the file scopes,
 * contract digests recomputed from this release's modules, CHAIN recomputed); refuses
 * release_plan_artifact.
 */
export function validateBootstrapSection(value: any): BootstrapSection {
  const shaOrNull = (v: unknown) => v === null || (typeof v === "string" && hex64.test(v));
  let requiredRoles: string[] | undefined;
  try {
    requiredRoles =
      typeof value?.target === "string" && RELEASE_TARGET_STEP.test(value.target)
        ? targetRequiredRoles(value.target)
        : undefined;
  } catch {
    requiredRoles = undefined;
  }
  const files: any[] = Array.isArray(value?.files) ? value.files : [];
  const named = (scope: string) => files.filter((f) => f?.scope === scope).map((f) => f.file);
  const contract = bootstrapContractDigests();
  const ok =
    value &&
    value.db_mode === "supabase" &&
    requiredRoles !== undefined &&
    typeof value.target_sha === "string" &&
    /^[0-9a-f]{40}$/.test(value.target_sha) &&
    Array.isArray(value.files) &&
    files.length === SESSION_BOOTSTRAP_FILES.length &&
    files.every(
      (f: any, i: number) =>
        f?.file === SESSION_BOOTSTRAP_FILES[i] &&
        shaOrNull(f.sha256_target) &&
        shaOrNull(f.sha256_release) &&
        f.differs === (f.sha256_target !== f.sha256_release) &&
        (f.scope === "run" || f.scope === "excluded" || f.scope === "mixed") &&
        (f.scope === "excluded" || f.sha256_target !== null),
    ) &&
    sameList(value.required_roles, requiredRoles) &&
    sameList(value.excluded, named("excluded")) &&
    sameList(value.mixed, named("mixed")) &&
    sameList(
      value.release_drift,
      files.filter((f) => f.scope !== "excluded" && f.differs).map((f) => f.file),
    ) &&
    Array.isArray(value.roles) &&
    Array.isArray(value.will_create) &&
    value.will_create.every((n: unknown) => typeof n === "string" && ROLE_NAME.test(n)) &&
    typeof value.required === "boolean" &&
    value.required === value.will_create.length > 0 &&
    (value.contract_source === "anchor" || value.contract_source === "release") &&
    value.contract_map_digest === contract.contract_map_digest &&
    value.contract_manifest_digest === contract.contract_manifest_digest &&
    value.contract_digest === contract.contract_digest &&
    hex64.test(value.chain_digest ?? "") &&
    bootstrapChainDigest(value) === value.chain_digest &&
    value.next === (value.required ? BOOTSTRAP_REQUIRED_NOTE : "apply");
  const chain = bootstrapChainRoles();
  const consumers = new Map(SESSION_BOOTSTRAP_CONSUMERS);
  const req = new Set(requiredRoles ?? []);
  if (
    !ok ||
    value.roles.length !== chain.length ||
    value.roles.some(
      (r: any, i: number) =>
        r?.name !== chain[i].name ||
        r.file !== chain[i].file ||
        r.first_consumer !== consumers.get(chain[i].name) ||
        r.required !== req.has(chain[i].name) ||
        typeof r.present !== "boolean" ||
        typeof r.exact !== "boolean" ||
        (r.exact && !r.present) ||
        (r.required && r.exact !== r.present) ||
        r.will_create !== (r.required && !r.present),
    ) ||
    JSON.stringify(codeSort(value.roles.filter((r: any) => r.will_create).map((r: any) => r.name))) !==
      JSON.stringify(value.will_create)
  )
    refuse("release_plan_artifact");
  return value as BootstrapSection;
}

/**
 * Refuses release_bootstrap_plan_changed when the target chain (target, target_sha,
 * files, required set, excluded/mixed/drift lists, contract source and digests, CHAIN)
 * or required/will_create differ.
 */
export function requireSameBootstrapPlan(
  planned: Pick<BootstrapSection, (typeof CHAIN_KEYS)[number] | "required" | "will_create">,
  live: Pick<BootstrapSection, (typeof CHAIN_KEYS)[number] | "required" | "will_create">,
) {
  if (
    chainView(planned) !== chainView(live) ||
    planned.required !== live.required ||
    JSON.stringify(planned.will_create) !== JSON.stringify(live.will_create)
  )
    refuse("release_bootstrap_plan_changed");
}

/**
 * Apply guard: refuses release_bootstrap_required, listing the absent names on stderr,
 * when a TARGET-REQUIRED chain role (bootstrapRequiredRoles for the bound target) is
 * absent from the catalog. A role first consumed above the target is not checked.
 */
export function requireBootstrapRoles(
  state: { roles: readonly { rolname: string }[] },
  required: readonly string[],
  log: (line: string) => void = console.error,
) {
  const names = new Set(state.roles.map((r) => r.rolname));
  const absent = codeSort(required.filter((n) => !names.has(n)));
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
 * target-required chain role present and matching its contract. Anything else refuses
 * release_bootstrap_readback (the caller rolls back). Returns the evidence. Privileges
 * on objects are out of scope: no chain file grants any.
 */
export function bootstrapReadbackVerdict(x: {
  before: BootstrapState;
  after: BootstrapState;
  approved: readonly string[];
  /** The target-required roles whose contracts are checked (default: every chain role). */
  required?: readonly string[];
  supabaseMode: boolean;
  map?: typeof SESSION_BOOTSTRAP_ROLES;
}) {
  const chain = bootstrapChainRoles(x.map);
  const checked = new Set(x.required ?? chain.map((r) => r.name));
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
    // A role above the target is not checked against its contract; the cluster-wide
    // diff above still refuses any change to it.
    if (!checked.has(name) && !createdSet.has(name)) continue;
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

const CHAIN_MODULE = "src/hoopdb/session-bootstrap-chain.ts";
const CONSUMERS_MODULE = "src/hoopdb/session-bootstrap-consumers.ts";
/** One blob from the Git object database (never the working tree); undefined: absent. */
function gitBlob(repo: string, sha: string, file: string) {
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
}
/**
 * The SESSION_BOOTSTRAP_FILES list a session-bootstrap-chain.ts text declares: the
 * quoted supabase/bootstrap paths of its literal array, `//` comment lines ignored;
 * undefined when the text has no such list.
 */
export function chainModuleFiles(text: string): string[] | undefined {
  const m = /export const SESSION_BOOTSTRAP_FILES = \[([\s\S]*?)\] as const;/.exec(text);
  if (!m) return undefined;
  const body = m[1]
    .split("\n")
    .filter((l) => !/^\s*\/\//.test(l))
    .join("\n");
  return [...body.matchAll(/"(supabase\/bootstrap\/[a-z0-9_]+\.sql)"/g)].map((x) => x[1]);
}

/**
 * Each chain file's bytes at the anchor (target_sha: the bytes the stage runs) and at the
 * release sha (compared only to report `differs`), from the Git object database (never
 * the working tree; env limited to PATH). The anchor's own chain list
 * (session-bootstrap-chain.ts at target_sha) must be this checkout's list or an ordered
 * subsequence of it (an older anchor that predates a later file), else
 * release_bootstrap_chain; a file the anchor does not list reads as absent at the anchor.
 */
export function releaseBootstrapFiles(
  repo: string,
  releaseSha: string,
  targetSha: string,
): BootstrapChainInput[] {
  const blob = (sha: string, file: string) => gitBlob(repo, sha, file);
  const module = blob(targetSha, CHAIN_MODULE);
  const listed = module === undefined ? undefined : chainModuleFiles(module.toString("utf8"));
  if (!listed || new Set(listed).size !== listed.length) refuse("release_bootstrap_chain");
  let at = 0;
  for (const file of listed) {
    const i = SESSION_BOOTSTRAP_FILES.indexOf(file as (typeof SESSION_BOOTSTRAP_FILES)[number], at);
    if (i < 0) refuse("release_bootstrap_chain");
    at = i + 1;
  }
  const anchorFiles = new Set(listed);
  return SESSION_BOOTSTRAP_FILES.map((file) => ({
    file,
    release: blob(releaseSha, file),
    target: anchorFiles.has(file) ? blob(targetSha, file) : undefined,
  }));
}

/**
 * The anchor's (target_sha) contract modules from the Git object database, read like
 * releaseBootstrapFiles (git cat-file argv, no shell, env limited to PATH). The release
 * side is this checkout's imported modules (the release sha). target_sha must be 40 hex
 * and its session-bootstrap-chain.ts must exist (else release_bootstrap_chain); a
 * consumers module that cannot be read reads as absent, which bootstrapContractSource
 * refuses unless the chain module also carries no SESSION_BOOTSTRAP_ROLES section.
 */
export function releaseBootstrapContract(repo: string, targetSha: string): BootstrapAnchorContract {
  if (!/^[0-9a-f]{40}$/.test(targetSha)) refuse("release_bootstrap_chain");
  const chainModule = gitBlob(repo, targetSha, CHAIN_MODULE);
  if (chainModule === undefined) refuse("release_bootstrap_chain");
  return { chainModule, consumersModule: gitBlob(repo, targetSha, CONSUMERS_MODULE) };
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

export const BOOTSTRAP_LOCK = "SELECT pg_advisory_lock(961996, 1)";
export const BOOTSTRAP_UNLOCK = "SELECT pg_advisory_unlock(961996, 1)";

/**
 * The bootstrap stage over an already connected client, which it always ends. Under the
 * migrator's session advisory lock: underLock (the caller's GitHub gate and anchor
 * recheck) returns the git inputs re-read under the lock (chain bytes at the anchor and
 * the release sha, the anchor's ordered step names); the target chain is recomputed and
 * compared with the plan and the approval's CHAIN, and mixed or release-drifting files
 * refuse, all before BEGIN. Then ONE transaction with hoopdb.db_mode=supabase bound:
 * recompute the whole section from the live catalog and refuse
 * release_bootstrap_plan_changed on any difference; skip (ROLLBACK, skipped evidence)
 * when no required role is absent; otherwise run each run-scope chain file provisioning
 * an absent required role (the ANCHOR's bytes, psql meta lines stripped, transaction
 * safe), require the same txid, read back and COMMIT only on an exact verdict. Every
 * refusal or error rolls back (when begun), unlocks and ends the client, then propagates.
 */
export async function runBootstrapStage(x: {
  client: Queryable & { end(): Promise<void> };
  target: string;
  targetSha: string;
  planned: BootstrapSection;
  /** The typed approval's CHAIN field. */
  approvalChain: string;
  supabaseMode: boolean;
  underLock: () => Promise<{
    files: readonly BootstrapChainInput[];
    steps: readonly string[];
    /** The anchor's contract modules, re-read under the lock (releaseBootstrapContract). */
    contract: BootstrapAnchorContract;
  }>;
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
    const { files, steps, contract } = await x.underLock();
    // Under the lock, before BEGIN: git inputs (chain bytes, steps, contract modules) vs
    // the plan and the approval; contract drift refuses here too.
    requireBootstrapTargetChain(
      bootstrapTargetChain({ target: x.target, targetSha: x.targetSha, steps, files, contract }),
      x.planned,
      x.approvalChain,
    );
    await client.query("BEGIN");
    let ended = false;
    try {
      await client.query(
        "SELECT set_config('hoopdb.db_mode', $1, true), set_config('hoopdb.allow_blocking_index_build', $2, true)",
        ["supabase", "off"],
      );
      const before = await readBootstrapState(client);
      const live = bootstrapSection({
        target: x.target,
        targetSha: x.targetSha,
        steps,
        files,
        contract,
        state: before,
        supabaseMode: x.supabaseMode,
      });
      requireSameBootstrapPlan(x.planned, live);
      requireRunnableChain(live);
      const scope = {
        target: live.target,
        target_sha: live.target_sha,
        chain_digest: live.chain_digest,
        contract_source: live.contract_source,
        contract_map_digest: live.contract_map_digest,
        contract_manifest_digest: live.contract_manifest_digest,
        contract_digest: live.contract_digest,
        required_roles: live.required_roles,
        excluded: live.excluded,
        files: live.files,
      };
      if (!live.required) {
        await client.query("ROLLBACK");
        ended = true;
        await x.writeEvidence({ ...x.header, ...scope, skipped: true, created: [], roles: live.roles });
        log("release_bootstrap_skipped");
        return "skipped";
      }
      const run = bootstrapFilesToRun(live);
      const sql = run.map((file) => {
        const bytes = files.find((f) => f.file === file)?.target;
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
        required: live.required_roles,
        supabaseMode: x.supabaseMode,
      });
      await client.query("COMMIT");
      ended = true;
      await x.writeEvidence({ ...x.header, ...scope, ran: run, ...evidence });
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
