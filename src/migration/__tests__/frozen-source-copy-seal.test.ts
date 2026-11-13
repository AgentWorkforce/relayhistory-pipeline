// Pure parts of the frozen-source sealer (src/migration/frozen-source-copy-seal.ts). No database, no network.
import { test, expect } from "bun:test";
import { readFileSync } from "node:fs";
import { SCHEMA_RELEASE } from "../../hoopdb/schema-release-contract.ts";
import { SUPABASE_PROD_CA_2021 } from "../../session-pipeline/supabase-ca.ts";
import {
  FrozenCopyRefusal, FrozenCopyUnknownCommit, MANIFEST_VERSION, SEAL_VERIFIED, addDigest, buildManifest,
  emptyDigest, extraOrgRefusal, fenceRefusal, frozenCopyCredentials, frozenCopyCredentialKeys, frozenCopyNeonClientConfig, frozenCopyNeonConnection, frozenCopyOwnerClientConfig,
  frozenCopyOwnerConnection, guardedWrite, jobBudget, orgParityRefusal, policyRefusal, privacyParityRefusal,
  privacySide, requireFrozenCopyOwner, resetParityRefusal, sealSnapshot, type Digest, type PreRead,
} from "../frozen-source-copy-seal.ts";
import { FROZEN_COPY_TEST_OWNER_PASSWORD, SYNTHETIC_NEON_URL, syntheticOwnerUrl } from "./fixtures/frozen-copy-owner-url.ts";

const code = (f: () => unknown) => {
  try {
    f();
  } catch (e) {
    return (e as { code?: string }).code ?? (e as Error).message;
  }
  return "no-refusal";
};
const acode = async (f: () => Promise<unknown>) => {
  try {
    await f();
  } catch (e) {
    return (e as { code?: string }).code ?? (e as Error).message;
  }
  return "no-refusal";
};
const dig = (...texts: string[]): Digest => {
  const d = emptyDigest();
  for (const t of texts) addDigest(d, t);
  return d;
};

// ---- credential document and direct-owner adapter ----------------------------------------------------------------

test("credential keys: --plan needs only the Neon URL, every other mode needs the owner URL too", () => {
  expect(frozenCopyCredentialKeys("plan")).toEqual({ required: ["neonOwnerUrl"], allowed: ["neonOwnerUrl", "supabaseOwnerUrl"] });
  for (const m of ["copy", "seal", "repair-head"] as const) expect(frozenCopyCredentialKeys(m).required).toEqual(["neonOwnerUrl", "supabaseOwnerUrl"]);
  expect(frozenCopyCredentials({ neonOwnerUrl: SYNTHETIC_NEON_URL }, "plan").owner).toBeNull();
  expect(code(() => frozenCopyCredentials({ neonOwnerUrl: SYNTHETIC_NEON_URL }, "seal"))).toBe("credentials_missing_key.supabaseOwnerUrl");
  expect(code(() => frozenCopyCredentials({ neonOwnerUrl: 7, supabaseOwnerUrl: syntheticOwnerUrl() }, "seal"))).toBe("credentials_invalid.neonOwnerUrl");
});

test("the owner URL must be exactly the pinned direct owner (host, port, database, user, password)", () => {
  const ok = frozenCopyOwnerConnection(syntheticOwnerUrl());
  expect(ok).toEqual({ host: SCHEMA_RELEASE.host, port: SCHEMA_RELEASE.port, database: "postgres", user: `postgres.${SCHEMA_RELEASE.project}`, password: FROZEN_COPY_TEST_OWNER_PASSWORD });
  expect(frozenCopyOwnerConnection(syntheticOwnerUrl({ query: "?sslmode=verify-full&channel_binding=require" })).user).toBe(ok.user);
  for (const bad of [
    syntheticOwnerUrl({ host: "db.example.invalid" }),
    syntheticOwnerUrl({ port: 6543 }),
    syntheticOwnerUrl({ database: "template1" }),
    syntheticOwnerUrl({ user: "postgres" }),
    syntheticOwnerUrl({ user: `cli_login_postgres.${SCHEMA_RELEASE.project}` }),
    syntheticOwnerUrl({ user: `authenticator.${SCHEMA_RELEASE.project}` }),
    syntheticOwnerUrl({ password: "" }),
    syntheticOwnerUrl() + "#frag",
    "http://example.invalid/",
    "not a url",
    42,
  ])
    expect(code(() => frozenCopyOwnerConnection(bad))).toBe("frozen_copy_owner_url_refused");
  for (const q of ["?sslmode=disable", "?sslmode=prefer", "?sslmode=no-verify", "?options=-c%20role%3Dpostgres", "?application_name=x", "?channel_binding=disable"])
    expect(code(() => frozenCopyOwnerConnection(syntheticOwnerUrl({ query: q })))).toBe("frozen_copy_owner_url_options_refused");
});

test("the owner client verifies TLS against the pinned CA and pooler name, with discrete fields (no URL overrides)", () => {
  const cfg = frozenCopyOwnerClientConfig(frozenCopyOwnerConnection(syntheticOwnerUrl()));
  expect(cfg.ssl).toEqual({ ca: SUPABASE_PROD_CA_2021, rejectUnauthorized: true, servername: SCHEMA_RELEASE.host });
  expect("connectionString" in cfg).toBe(false);
  expect(cfg.options).toBe("-c statement_timeout=60000 -c lock_timeout=5000");
  expect(cfg.options).not.toMatch(/role|session_authorization/i);
});

// The Neon owner URL with one part replaced (the synthetic fixture already carries the pinned endpoint label).
const neonUrl = (f: (u: URL) => void) => {
  const u = new URL(SYNTHETIC_NEON_URL);
  f(u);
  return u.toString();
};
test("Neon owner URL: sessionAffinityUrl rules, then the source pins (user, database, endpoint digest, options)", () => {
  const ok = frozenCopyNeonConnection(SYNTHETIC_NEON_URL);
  expect(ok).toEqual({ host: new URL(SYNTHETIC_NEON_URL).hostname, port: 5432, database: "neondb", user: "neondb_owner", password: "synthetic-not-authorized" });
  expect(frozenCopyNeonConnection(neonUrl((u) => (u.search = "?sslmode=require&channel_binding=require"))).user).toBe("neondb_owner");
  const cases: [string, unknown][] = [
    ["neon_owner_url_password_required", neonUrl((u) => (u.password = ""))],
    ["neon_owner_url_user_required", neonUrl((u) => ((u.username = ""), (u.password = "")))],
    ["neon_owner_url_pooler", neonUrl((u) => (u.hostname = u.hostname.replace(/^([^.]+)\./, "$1-pooler.")))],
    ["neon_owner_url_transaction_pooler", neonUrl((u) => (u.port = "6543"))],
    ["neon_owner_url_transaction_pooler", neonUrl((u) => (u.search = "?pgbouncer=true"))],
    ["neon_owner_url_host_required", neonUrl((u) => (u.search = "?host=/tmp"))],
    ["neon_owner_url_user_refused", neonUrl((u) => (u.username = "neon_superuser"))],
    ["neon_owner_url_endpoint_refused", neonUrl((u) => (u.hostname = "ep-other-endpoint.synthetic.invalid"))],
    ["neon_owner_url_database_refused", neonUrl((u) => (u.pathname = "/postgres"))],
    ["neon_owner_url_refused", neonUrl((u) => (u.port = "5433"))],
    ["neon_owner_url_options_refused", neonUrl((u) => (u.search = "?sslmode=disable"))],
    ["neon_owner_url_options_refused", neonUrl((u) => (u.search = "?options=-c%20role%3Dx"))],
    ["credentials_invalid.neonOwnerUrl", 7],
  ];
  for (const [want, bad] of cases) expect(code(() => frozenCopyNeonConnection(bad))).toBe(want);
  expect(code(() => frozenCopyCredentials({ neonOwnerUrl: neonUrl((u) => (u.password = "")) }, "plan"))).toBe("neon_owner_url_password_required");
});

test("the ~/.pgpass fallback is unreachable: an empty Neon password never reaches a client, and the config is discrete", () => {
  // node-postgres reads ~/.pgpass only when the password is null/empty (client.js); both are refused before a Client.
  let constructed = 0;
  const FakeClient = function (this: unknown, _cfg: unknown) { constructed++; } as unknown as new (cfg: unknown) => unknown;
  const connect = (url: string) => new FakeClient(frozenCopyNeonClientConfig(frozenCopyNeonConnection(url)));
  for (const url of [neonUrl((u) => (u.password = "")), SYNTHETIC_NEON_URL.replace(/:[^:@]+@/, "@"), SYNTHETIC_NEON_URL.replace(/:[^:@]+@/, ":@")])
    expect(code(() => connect(url))).toMatch(/^neon_owner_url_(password|user)_required$/);
  expect(constructed).toBe(0);
  const cfg = frozenCopyNeonClientConfig(frozenCopyNeonConnection(SYNTHETIC_NEON_URL));
  expect("connectionString" in cfg).toBe(false);
  expect(cfg.password.length).toBeGreaterThan(0);
  expect(cfg.ssl).toEqual({ rejectUnauthorized: true });
  const script = readFileSync(new URL("../../../scripts/frozen-source-copy.ts", import.meta.url), "utf8");
  expect(script).not.toMatch(/connectionString|neondb_owner|ep-[a-z]/);
  expect(script).toContain("new pg.Client(frozenCopyNeonClientConfig(");
});

const identityRow = { me: "postgres", session: "postgres", db: "postgres", rolcanlogin: true, rolsuper: false, rolcreaterole: true, rolcreatedb: true, rolbypassrls: true, rolreplication: true, schema_owner: "postgres" };
const identity = (row: Record<string, unknown> | undefined) => ({ query: async () => ({ rows: row ? [row] : [] }) });
test("the connected session must BE the owner: role switching, another login, superuser or another db refuse", async () => {
  expect(await acode(() => requireFrozenCopyOwner(identity(identityRow)))).toBe("no-refusal");
  for (const bad of [
    { session: "cli_login_postgres" }, // a temporary login after SET ROLE postgres
    { me: "authenticator" },
    { me: "postgres", session: "supabase_admin" },
    { rolsuper: true },
    { db: "template1" },
    { schema_owner: "supabase_admin" },
    { rolbypassrls: false },
  ])
    expect(await acode(() => requireFrozenCopyOwner(identity({ ...identityRow, ...bad })))).toBe("frozen_copy_owner_identity");
  expect(await acode(() => requireFrozenCopyOwner(identity(undefined)))).toBe("frozen_copy_owner_identity");
});

test("source text: the CLI login path, role switching and implicit credential sources are gone from the driver", () => {
  const script = readFileSync(new URL("../../../scripts/frozen-source-copy.ts", import.meta.url), "utf8");
  const mod = readFileSync(new URL("../frozen-source-copy-seal.ts", import.meta.url), "utf8");
  for (const forbidden of [/parseCliLogin/, /supabase-cli-login/, /cli_login/, /\bCLI\s*=/, /SUPABASE_CLI/, /\.local\/bin\/supabase/, /execFileSync|child_process/, /SET (SESSION )?ROLE/i, /requiredSupabaseProjectRef|SUPABASE_PROJECT_REF/, /readFileSync\(0/, /\bop read\b.*\$\(|Bun\.spawn|spawnSync/])
    expect(script).not.toMatch(forbidden);
  for (const forbidden of [/supabase-cli-login|cli_login|execFileSync|child_process|process\.env|SET (SESSION )?ROLE/i]) expect(mod).not.toMatch(forbidden);
  expect(script).toContain("readStdinCredentials(");
  expect(script).toContain("refuseCredentialEnv()");
});

// ---- parity ---------------------------------------------------------------------------------------------------------

test("partial copies refuse: org missing, record count mismatch, same count different rows, receipt mismatch, extra org", () => {
  const neon = { records: dig("a", "b"), receipts: dig("r") };
  expect(orgParityRefusal("o", neon, { records: dig("a", "b"), receipts: dig("r") })).toBeNull();
  expect(orgParityRefusal("o", neon, { records: emptyDigest(), receipts: emptyDigest() })?.code).toBe("frozen_copy_org_missing");
  const short = orgParityRefusal("o", neon, { records: dig("a"), receipts: dig("r") });
  expect(short?.code).toBe("frozen_copy_record_mismatch");
  expect(short?.detail).toEqual({ org: "o", neonRows: "2", supabaseRows: "1" });
  expect(orgParityRefusal("o", neon, { records: dig("a", "c"), receipts: dig("r") })?.code).toBe("frozen_copy_record_mismatch");
  expect(orgParityRefusal("o", neon, { records: dig("a", "b"), receipts: emptyDigest() })?.code).toBe("frozen_copy_receipt_mismatch");
  expect(extraOrgRefusal(0)).toBeNull();
  expect(extraOrgRefusal(2)?.code).toBe("frozen_copy_extra_org");
});

const del = (org: string, s = "s1") => ({ org_id: org, workspace_id: "w", user_id: "u", origin_id: "o", source: "src", session_id: s });
const rst = (org: string, req: string, deleted: string | number) => ({ org_id: org, workspace_id: "w", user_id: "u", request_id: req, deleted: String(deleted) });
const head = (org: string, v: string) => ({ org_id: org, workspace_id: "w", user_id: "u", v, digest: "d".repeat(64), pending: false });

test("reset parity refuses a missing, an extra and a CHANGED reset row, each with its own code", () => {
  const n = privacySide([], [rst("o1", "r1", 3), rst("o1", "r2", 0)], []);
  expect(resetParityRefusal(n, privacySide([], [rst("o1", "r2", "0"), rst("o1", "r1", "3")], []))).toBeNull();
  expect(resetParityRefusal(n, privacySide([], [rst("o1", "r1", 3)], []))).toBe("frozen_copy_reset_missing");
  expect(resetParityRefusal(n, privacySide([], [rst("o1", "r1", 3), rst("o1", "r2", 0), rst("o2", "r9", 1)], []))).toBe("frozen_copy_reset_extra");
  expect(resetParityRefusal(n, privacySide([], [rst("o1", "r1", 4), rst("o1", "r2", 0)], []))).toBe("frozen_copy_reset_changed");
  // The same request id under another user is a different witness input (missing + extra, not "equal").
  expect(resetParityRefusal(n, privacySide([], [{ ...rst("o1", "r1", 3), user_id: "u2" }, rst("o1", "r2", 0)], []))).toBe("frozen_copy_reset_missing");
  // Through the full privacy parity too; deletions and heads keep their own codes.
  expect(privacyParityRefusal(n, privacySide([], [rst("o1", "r1", 4), rst("o1", "r2", 0)], []))).toBe("frozen_copy_reset_changed");
  expect(privacyParityRefusal(privacySide([del("o1")], [], []), privacySide([], [], []))).toBe("frozen_copy_deletion_mismatch");
  expect(privacyParityRefusal(privacySide([], [], [head("o1", "2")]), privacySide([], [], [head("o1", "1")]))).toBe("frozen_copy_control_head_mismatch");
});

test("privacy is digested per org for the org seals (deletions + resets)", () => {
  const side = privacySide([del("o1"), del("o1", "s2"), del("o2")], [rst("o1", "r1", 1)], []);
  expect(side.byOrg.get("o1")!.n).toBe(3n);
  expect(side.byOrg.get("o2")!.n).toBe(1n);
  expect(side.deletions.n).toBe(3n);
  expect(side.resets.n).toBe(1n);
});

// ---- policy binding -------------------------------------------------------------------------------------------------

const E = "e".repeat(64), B = "b".repeat(64), I = "c".repeat(64);
const policy = { control_epoch: E, privacy_baseline_sha256: B, source_identity_sha256: I };
const status = { phase: "active", epoch: E, privacyBaselineSha256: B };
test("policy must equal the descriptor and the pre-read: stale epoch, stale baseline, stale source identity", () => {
  const pre = { policy, status };
  expect(policyRefusal(policy, status, pre)).toBeNull();
  expect(policyRefusal(undefined, status, pre)).toBe("frozen_copy_policy_missing");
  expect(policyRefusal({ ...policy, control_epoch: "f".repeat(64) }, status, pre)).toBe("frozen_copy_stale_epoch");
  expect(policyRefusal(policy, status, { policy, status: { ...status, epoch: "1".repeat(64) } })).toBe("frozen_copy_stale_epoch");
  expect(policyRefusal({ ...policy, privacy_baseline_sha256: "9".repeat(64) }, status, pre)).toBe("frozen_copy_stale_privacy_baseline");
  expect(policyRefusal(policy, status, { policy: { ...policy, privacy_baseline_sha256: "9".repeat(64) }, status: { ...status, privacyBaselineSha256: "9".repeat(64) } })).toBe("frozen_copy_stale_privacy_baseline");
  expect(policyRefusal(policy, status, { policy: { ...policy, source_identity_sha256: "0".repeat(64) }, status })).toBe("frozen_copy_stale_source_identity");
});

// ---- convergence decision and manifest ------------------------------------------------------------------------------

test("fence decision: any head difference is mirror_advanced; privacy movement since the pre-read refuses", () => {
  const side = (heads: ReturnType<typeof head>[], resets = [rst("o1", "r1", 1)]) => privacySide([del("o1")], resets, heads);
  const pre: PreRead = { status, policy, neon: side([head("o1", "1")]), supa: side([head("o1", "1")]) };
  expect(fenceRefusal(pre, side([head("o1", "1")]), side([head("o1", "1")]))).toBeNull();
  expect(fenceRefusal(pre, side([head("o1", "1")]), side([head("o1", "2")]))).toBe("frozen_copy_mirror_advanced"); // supabase moved
  expect(fenceRefusal(pre, side([head("o1", "2")]), side([head("o1", "2")]))).toBe("frozen_copy_mirror_advanced"); // both moved, still equal
  expect(fenceRefusal(pre, side([head("o1", "1"), head("o2", "1")]), side([head("o1", "1"), head("o2", "1")]))).toBe("frozen_copy_mirror_advanced"); // new user
  const moved = [rst("o1", "r1", 1), rst("o1", "r2", 1)];
  expect(fenceRefusal(pre, side([head("o1", "1")], moved), side([head("o1", "1")], moved))).toBe("frozen_copy_privacy_advanced");
  expect(fenceRefusal(pre, side([head("o1", "1")], moved), side([head("o1", "1")]))).toBe("frozen_copy_reset_missing");
});

test("manifest v2 names records, receipts, privacy per org, deletions, resets and control heads", () => {
  const side = privacySide([del("o1")], [rst("o1", "r1", 1), rst("o2", "r2", 2)], [head("o1", "1")]);
  const m = buildManifest([{ org: "o1", records: dig("a"), receipts: dig("r") }, { org: "o2", records: dig("b", "c"), receipts: emptyDigest() }], side);
  const lines = m.text.trimEnd().split("\n");
  expect(lines[0]).toBe(MANIFEST_VERSION);
  expect(lines[1]!.startsWith('org\t"o1"\trecords\t1\t')).toBe(true);
  expect(lines[1]).toContain("\tprivacy\t2\t");
  expect(lines[2]).toContain("\tprivacy\t1\t");
  expect(lines.slice(3).map((l) => l.split("\t").slice(0, 2).join(" "))).toEqual(["deletions 1", "resets 2", "control_heads 1"]);
  expect(m.sha256).toMatch(/^[0-9a-f]{64}$/);
  const snap = sealSnapshot(status, m);
  expect(snap.verified).toEqual([...SEAL_VERIFIED]);
  expect(snap).toMatchObject({ resetRows: "2", deletionRows: "1", privacyRows: "3", controlHeads: "1", receiptRows: "1" });
  expect(snap.privacyBaseline).toContain("not recomputed");
  // A changed reset value changes the manifest.
  const changed = buildManifest([{ org: "o1", records: dig("a"), receipts: dig("r") }, { org: "o2", records: dig("b", "c"), receipts: emptyDigest() }], privacySide([del("o1")], [rst("o1", "r1", 9), rst("o2", "r2", 2)], [head("o1", "1")]));
  expect(changed.sha256).not.toBe(m.sha256);
  expect(code(() => buildManifest([{ org: "o1", records: dig("a"), receipts: dig() }, { org: "o1", records: dig("a"), receipts: dig() }], side))).toBe("frozen_copy_duplicate_org");
});

// ---- job envelope ---------------------------------------------------------------------------------------------------

test("budget: refuses at the deadline, rejects invalid budgets", () => {
  let t = 1000;
  const b = jobBudget(50, () => t);
  expect(code(() => b.check())).toBe("no-refusal");
  t = 1049;
  expect(code(() => b.check())).toBe("no-refusal");
  t = 1050;
  expect(code(() => b.check())).toBe("frozen_copy_budget_exhausted");
  expect(code(() => jobBudget(0, () => 5).check())).toBe("frozen_copy_budget_exhausted");
  for (const bad of [-1, 1.5, Number.NaN]) expect(code(() => jobBudget(bad))).toBe("frozen_copy_budget_invalid");
});

test("guardedWrite: a SQLSTATE is the server's refusal; no answer is an in-doubt write carrying its context", async () => {
  const w = { phase: "copy" as const, dataset: null, checkpointSeq: null };
  expect(await acode(() => guardedWrite(w, async () => { throw Object.assign(new Error("dup"), { code: "23505" }); }))).toBe("23505");
  expect(await acode(() => guardedWrite(w, async () => { throw new Error("Connection terminated unexpectedly"); }))).toBe("frozen_copy_unknown_commit");
  expect(await acode(() => guardedWrite(w, async () => { throw undefined; }))).toBe("frozen_copy_unknown_commit");
  const err = await guardedWrite(w, async () => { throw undefined; }, () => 42).catch((e) => e);
  expect(err).toBeInstanceOf(FrozenCopyUnknownCommit);
  expect(err.context).toEqual({ ...w, detectedAt: 42 });
  expect(err.during).toBe("copy");
  expect(await guardedWrite(w, async () => 7)).toBe(7);
});
