/**
 * Fast mocked-session tests for the owner-buffered parity CI wrapper (scripts/owner-buffered-parity-ci.ts):
 * fail-closed partial writes, readback mismatch, generation and census drift between census and write,
 * bulk unreachability, the dormant switch before any statement or credential, and the credential hand-off
 * (stdin only, never argv or the child's environment). Fixture identifiers only; no database, no network.
 */
import { expect, test } from "bun:test";
import { spawnSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  OWNER_PARITY_CHILD_ENV,
  ownerParityCode,
  ownerParityDiagnosticCode,
  ownerParityChildSpawn,
  refuseOwnerParityCredentialEnv,
  runOwnerParity,
  type OwnerParityDependencies,
  type OwnerParityOutcome,
  type OwnerParitySessions,
} from "../../../scripts/owner-buffered-parity-ci.ts";
import { BULK_TARGET_GENERATION_SQL, tenantSha256 } from "../../../scripts/owner-buffered-parity-operator.ts";
import { ParityCensusRefusal, READ_OWNER_BUFFERED_PARITY_SQL, type OwnerBufferedParityProof, type ParityCensusReceipt, type ParityTenant } from "../owner-buffered-parity.ts";
import { OWNER_PARITY_WRITE_ENABLED, ownerParityConfirmation, type OwnerParityCensusArtifact } from "../owner-buffered-parity-ci-contract.ts";

const H = (c: string) => c.repeat(64);
const SHA = "5".repeat(40);
const tenants: ParityTenant[] = Array.from({ length: 9 }, (_, i) => ({ orgId: `fixture-org-${i}`, workspaceId: `fixture-ws-${i}`, userId: `fixture-user-${i}` }));
const roster = { contract: "owner-buffered-parity-roster/v1", tenants: tenants.slice(0, 7).map(tenantSha256) };
const key = (t: ParityTenant) => t.userId;

/** A mock world: the ledger (9 buffered tenants), the live generation, per-tenant source digests and stored rows. */
function world() {
  const w = {
    generation: H("4"),
    deletion: new Map(tenants.map((t) => [key(t), H("a")])),
    stored: new Map<string, Record<string, unknown>>(),
    statements: [] as Array<{ sql: string; params: unknown[] }>,
    writes: [] as string[],
    failWriteAt: -1,
    corruptWriteAt: -1,
    generationAfterTenant: -1,
  };
  const proof = (t: ParityTenant): OwnerBufferedParityProof => ({
    contract: "owner-buffered-parity/v1", tenant: { ...t }, controlEpoch: H("1"), privacyBaselineSha256: H("2"), explicitNoHead: false,
    head: { epoch: H("1"), version: "3", digest: H("3"), kind: "selection" }, deletionSetSha256: w.deletion.get(key(t))!,
    resetSetSha256: H("b"), targetLineageDigest: H("c"), targetGeneration: w.generation,
  });
  const g1 = (t: ParityTenant): ParityCensusReceipt => ({ contract: "owner-buffered-parity-census/v1", tenant: { ...t }, equal: true, mismatches: [], proof: proof(t) });
  const row = (p: OwnerBufferedParityProof) => ({ cutover_epoch: p.controlEpoch, head_epoch: p.head!.epoch, head_version: p.head!.version,
    head_digest: p.head!.digest, head_kind: p.head!.kind, no_head: false, deletion_set_digest: p.deletionSetSha256, reset_set_digest: p.resetSetSha256,
    target_lineage_digest: p.targetLineageDigest, target_generation: p.targetGeneration, privacy_baseline_sha256: p.privacyBaselineSha256 });
  let fetched = false;
  const sessions: OwnerParitySessions = {
    source: { query: async () => { throw new Error("source used directly"); } },
    enumerator: {
      query: async <R>(sql: string, params: unknown[] = []) => {
        w.statements.push({ sql, params });
        if (sql.startsWith("DECLARE")) fetched = false;
        if (sql.startsWith("FETCH")) {
          const rows = fetched ? [] : tenants.map((t) => ({ org_id: t.orgId, workspace_id: t.workspaceId, user_id: t.userId }));
          fetched = true;
          return { rows: rows as R[] };
        }
        return { rows: [] as R[] };
      },
    },
    target: {
      query: async <R>(sql: string, params: unknown[] = []) => {
        w.statements.push({ sql, params });
        if (sql === BULK_TARGET_GENERATION_SQL) return { rows: [{ generation: w.generation }] as R[] };
        if (sql === READ_OWNER_BUFFERED_PARITY_SQL) {
          const stored = w.stored.get(String(params[2]));
          return { rows: (stored ? [stored] : []) as R[] };
        }
        throw new Error("unexpected target sql");
      },
    },
  };
  const deps: Required<OwnerParityDependencies> = {
    readCensus: (async (s: unknown, t: unknown, tenant: ParityTenant, _source: unknown, options?: unknown) => {
      // Census, write and readback keep readParityCensus's own transactions on the source/target pair; only
      // discovery may pass the caller-held snapshot option.
      if (s !== sessions.source || t !== sessions.target || options !== undefined) throw new Error("census read off its own sessions");
      w.statements.push({ sql: "G1", params: [tenant.userId] });
      return g1(tenant);
    }) as never,
    writeProof: (async (_s: unknown, _t: unknown, tenant: ParityTenant, generation: string) => {
      const index = tenants.findIndex((t) => key(t) === key(tenant));
      if (index === w.failWriteAt) throw new ParityCensusRefusal("parity_mismatch:source_moved");
      if (generation !== w.generation) throw new ParityCensusRefusal("target_generation_mismatch");
      const p = proof(tenant);
      const existing = w.stored.get(key(tenant));
      w.stored.set(key(tenant), row(index === w.corruptWriteAt ? { ...p, resetSetSha256: H("0") } : p));
      w.writes.push(key(tenant));
      if (index === w.generationAfterTenant) w.generation = H("9");
      return { census: g1(tenant), outcome: existing ? "unchanged" : "insert", writtenAt: null };
    }) as never,
  };
  return { w, sessions, deps };
}

const base = { OWNER_PARITY_SHA: SHA, OWNER_PARITY_CENSUS_SOURCE: "frozen-supabase" };
const T0 = Date.parse("2026-11-13T00:00:00.000Z");
async function census(): Promise<OwnerParityCensusArtifact> {
  const { sessions, deps } = world();
  const out = await runOwnerParity({ ...base, OWNER_PARITY_MODE: "census" }, sessions, { roster, runId: "101", now: () => T0, deps });
  expect(out.exitCode).toBe(0);
  return out.census!;
}
const writeEnv = (c: OwnerParityCensusArtifact) => ({ ...base, OWNER_PARITY_MODE: "write", OWNER_PARITY_CENSUS_RUN: c.censusRun,
  OWNER_PARITY_CONFIRMATION: ownerParityConfirmation(c) });
const write = (c: OwnerParityCensusArtifact, x: ReturnType<typeof world>) =>
  runOwnerParity(writeEnv(c), x.sessions, { roster, runId: "202", census: c, now: () => T0 + 3600_000, writeEnabled: true, deps: x.deps });
const statuses = (o: OwnerParityOutcome) => o.receipt.tenants.map((t) => t.status);

test("diagnostic codes expose only fixed SQLSTATE/connection classes and never driver content", () => {
  const pg = Object.assign(new Error("password=secret host=private.example query=SELECT tenant_id"), { code: "42501" });
  const connection = Object.assign(new Error("connect ECONNRESET private.example"), { code: "ECONNRESET" });
  const arbitrary = Object.assign(new Error("host=private.example"), { code: "private.example" });
  expect(ownerParityCode(pg)).toBe("owner_parity_pg_42501");
  expect(ownerParityCode(connection)).toBe("owner_parity_connection_econnreset");
  expect(ownerParityCode(arbitrary)).toBe("owner_parity_failed");
  expect(JSON.stringify([ownerParityCode(pg), ownerParityCode(connection), ownerParityCode(arbitrary)]))
    .not.toMatch(/secret|private\.example|SELECT|tenant_id/u);
});

test("temporary phase diagnostic exposes only fixed path and structural shape", () => {
  const arbitrary = Object.assign(new Error("password=secret host=private.example query=SELECT tenant_id"), {
    code: "private.example", detail: "tenant-secret", query: "SELECT private_row",
  });
  const noCode = new TypeError("postgres://owner:secret@private.example/database");
  expect(ownerParityDiagnosticCode(arbitrary, "session_setup"))
    .toBe("owner_parity_diag_session_setup_error_code_other");
  expect(ownerParityDiagnosticCode(noCode, "supabase_url"))
    .toBe("owner_parity_diag_supabase_url_error_no_code");
  expect(ownerParityDiagnosticCode(null, "stdin")).toBe("owner_parity_diag_stdin_null");
  expect(JSON.stringify([
    ownerParityDiagnosticCode(arbitrary, "session_setup"),
    ownerParityDiagnosticCode(noCode, "supabase_url"),
  ])).not.toMatch(/secret|private\.example|SELECT|tenant|database/u);
});

test("census then write: seven rows, roster order, exact readback; unlisted tenants never reach G1 or a write", async () => {
  const c = await census();
  expect(c.tenants.map((t) => t.tenantSha256)).toEqual(roster.tenants);
  const x = world();
  const out = await write(c, x);
  expect(out.exitCode).toBe(0);
  expect(out.receipt.counts).toMatchObject({ roster: 7, found: 7, inserted: 7, verified: 7 });
  expect(x.w.writes).toEqual(tenants.slice(0, 7).map(key));
  for (const t of tenants.slice(7)) expect(x.w.statements.some((s) => s.params.includes(t.userId))).toBe(false);
});

test("partial seven-row write fails closed: first refusal stops the loop, written rows are reported, exit is non-zero", async () => {
  const c = await census();
  const x = world();
  x.w.failWriteAt = 3;
  const out = await write(c, x);
  expect(out.exitCode).toBe(1);
  expect(out.receipt.status).toBe("refused");
  expect(out.receipt.code).toBe("parity_mismatch:source_moved");
  expect(out.receipt.counts.inserted).toBe(3);
  expect(out.receipt.counts.verified).toBe(3);
  expect(statuses(out)).toEqual(["inserted", "inserted", "inserted", "refused", "not_attempted", "not_attempted", "not_attempted"]);
  expect(x.w.writes).toEqual(tenants.slice(0, 3).map(key));
});

test("readback mismatch: a stored row that differs from the fresh proof refuses and stops the write", async () => {
  const c = await census();
  const x = world();
  x.w.corruptWriteAt = 1;
  const out = await write(c, x);
  expect(out.exitCode).toBe(1);
  expect(out.receipt.code).toBe("owner_parity_readback_mismatch");
  expect(out.receipt.counts.verified).toBe(1);
  expect(x.w.writes).toEqual(tenants.slice(0, 2).map(key));
});

test("readback mode: a stale or missing row fails the run", async () => {
  const x = world();
  const c = await census();
  await write(c, x);
  x.w.stored.delete(key(tenants[0]!));
  x.w.deletion.set(key(tenants[1]!), H("e"));
  const out = await runOwnerParity({ ...base, OWNER_PARITY_MODE: "readback" }, x.sessions, { roster, runId: "303", deps: x.deps });
  expect(out.exitCode).toBe(3);
  expect(out.receipt.status).toBe("mismatch");
  expect(statuses(out)).toEqual(["missing", "stale", "equal", "equal", "equal", "equal", "equal"]);
  expect(x.w.writes.length).toBe(7);
});

test("concurrent change between census and write: a moved PASS generation refuses before any write", async () => {
  const c = await census();
  const x = world();
  x.w.generation = H("9");
  const out = await write(c, x);
  expect(out.receipt.code).toBe("owner_parity_generation_mismatch");
  expect(x.w.writes).toEqual([]);
  expect(x.w.statements.some((s) => s.sql === "G1")).toBe(false);
});

test("concurrent change between census and write: a moved source refuses census drift at that tenant", async () => {
  const c = await census();
  const x = world();
  x.w.deletion.set(key(tenants[2]!), H("e"));
  const out = await write(c, x);
  expect(out.receipt.code).toBe("owner_parity_census_drift");
  expect(x.w.writes).toEqual(tenants.slice(0, 2).map(key));
});

test("generation moving mid-write fails the generation postcondition and stops the loop", async () => {
  const c = await census();
  const x = world();
  x.w.generationAfterTenant = 0;
  const out = await write(c, x);
  expect(out.exitCode).toBe(1);
  expect(out.receipt.code).toBe("owner_parity_generation_postcondition");
  expect(x.w.writes).toEqual([key(tenants[0]!)]);
});

test("bulk mode is unreachable and dormant write issues no statement", async () => {
  for (const mode of ["bulk", "BULK", "renew"]) {
    const x = world();
    const out = await runOwnerParity({ ...base, OWNER_PARITY_MODE: mode }, x.sessions, { roster, runId: "1", writeEnabled: true, deps: x.deps });
    expect(out.receipt.code).toBe("owner_parity_inputs");
    expect(x.w.statements).toEqual([]);
  }
  expect(OWNER_PARITY_WRITE_ENABLED).toBe(false);
  const c = await census();
  const x = world();
  const out = await runOwnerParity(writeEnv(c), x.sessions, { roster, runId: "202", census: c, now: () => T0, deps: x.deps });
  expect(out.receipt.code).toBe("owner_parity_write_dormant");
  expect(x.w.statements).toEqual([]);
});

test("credential leakage: the child gets the URL on stdin only, never argv or env; the child refuses a credential env", () => {
  const url = "postgresql://fixture-user:fixture-secret@db.fixture.invalid:5432/postgres";
  const env = { PATH: "/usr/bin", HOME: "/home/runner", RUNNER_TEMP: "/tmp/r", GITHUB_RUN_ID: "202", GITHUB_SHA: SHA,
    GITHUB_TOKEN: "fixture-token", SUPABASE_MIGRATION_OWNER_URL: url, DATABASE_URL: url, ACTIONS_RUNTIME_TOKEN: "fixture-token",
    OWNER_PARITY_MODE: "census", OWNER_PARITY_SHA: SHA, OWNER_PARITY_CENSUS_SOURCE: "frozen-supabase" };
  const spec = ownerParityChildSpawn(env, url, "/x/scripts/owner-buffered-parity-ci.ts");
  expect(spec.args).toEqual(["/x/scripts/owner-buffered-parity-ci.ts", "--operator-stdin"]);
  expect(Object.keys(spec.env).sort()).toEqual(["GITHUB_RUN_ID", "GITHUB_SHA", "HOME", "OWNER_PARITY_CENSUS_SOURCE", "OWNER_PARITY_MODE",
    "OWNER_PARITY_SHA", "PATH", "RUNNER_TEMP"]);
  expect(JSON.stringify([spec.command, spec.args, spec.env])).not.toContain("fixture-secret");
  expect(JSON.stringify([spec.command, spec.args, spec.env])).not.toContain("fixture-token");
  expect(JSON.parse(spec.input)).toEqual({ supabaseUrl: url });
  for (const k of Object.keys(spec.env)) expect((OWNER_PARITY_CHILD_ENV as readonly string[]).includes(k)).toBe(true);
  expect(() => refuseOwnerParityCredentialEnv(spec.env)).not.toThrow();
  for (const extra of [{ SUPABASE_MIGRATION_OWNER_URL: url }, { GITHUB_TOKEN: "t" }, { PGPASSWORD: "p" }])
    expect(() => refuseOwnerParityCredentialEnv({ ...spec.env, ...extra })).toThrow();
});

test("parent process: dormant write and the empty committed roster refuse before the secret is read; nothing secret is printed or written", () => {
  const temp = mkdtempSync(join(tmpdir(), "owner-parity-ci-"));
  try {
    const url = "postgresql://fixture-user:fixture-secret@db.fixture.invalid:5432/postgres";
    const run = (extra: Record<string, string>) => spawnSync(process.execPath, ["scripts/owner-buffered-parity-ci.ts"], {
      cwd: join(import.meta.dir, "../../.."), encoding: "utf8",
      env: { PATH: process.env.PATH ?? "", RUNNER_TEMP: temp, GITHUB_RUN_ID: "202", SUPABASE_MIGRATION_OWNER_URL: url, ...base, ...extra },
    });
    const dormant = run({ OWNER_PARITY_MODE: "write", OWNER_PARITY_CENSUS_RUN: "101", OWNER_PARITY_CONFIRMATION: "WRITE-OWNER-PARITY x" });
    expect(dormant.status).toBe(1);
    expect(dormant.stdout).toContain("\"code\":\"owner_parity_write_dormant\"");
    const empty = run({ OWNER_PARITY_MODE: "census" });
    expect(empty.status).toBe(1);
    expect(empty.stdout).toContain("\"code\":\"roster_empty\"");
    const receipt = readFileSync(join(temp, "owner-parity-out", "owner-buffered-parity.json"), "utf8");
    for (const text of [dormant.stdout, dormant.stderr, empty.stdout, empty.stderr, receipt]) {
      expect(text).not.toContain("fixture-secret");
      expect(text).not.toContain("db.fixture.invalid");
      for (const k of ["orgId", "workspaceId", "userId", "neonUrl", "supabaseUrl"]) expect(text).not.toContain(`"${k}"`);
    }
    expect(JSON.parse(receipt)).toMatchObject({ status: "refused", code: "roster_empty", mode: "census", credentialsPersisted: false });
  } finally {
    rmSync(temp, { recursive: true, force: true });
  }
});
