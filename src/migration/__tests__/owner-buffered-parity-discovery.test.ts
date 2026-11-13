/**
 * Mocked-session tests for the one-time, read-only discovery mode of the owner-buffered parity workflow
 * (scripts/owner-buffered-parity-ci.ts runOwnerParityDiscovery): exactly-seven scope, generation stability
 * before/inside/after the snapshot, the empty-roster rule, the mode dispatch table (write and bulk unreachable
 * from discovery), statement shape (read-only only) and sanitization. Fixture identifiers only; no database.
 */
import { expect, test } from "bun:test";
import { spawnSync } from "node:child_process";
import { mkdtempSync, readFileSync, rmSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  discoveryRunReceipt,
  ownerParityDiscoverySummary,
  ownerParityPreflight,
  runOwnerParity,
  runOwnerParityDiscovery,
  type OwnerParityDiscoveryOutcome,
  type OwnerParitySessions,
} from "../../../scripts/owner-buffered-parity-ci.ts";
import { BULK_BUFFERED_TENANTS_SQL, BULK_TARGET_GENERATION_SQL, tenantSha256 } from "../../../scripts/owner-buffered-parity-operator.ts";
import { READ_OWNER_BUFFERED_PARITY_SQL, type OwnerBufferedParityProof, type ParityCensusReceipt, type ParityTenant } from "../owner-buffered-parity.ts";
import {
  OWNER_PARITY_DISCOVERY_CONTRACT,
  OWNER_PARITY_MODE_POLICY,
  OWNER_PARITY_MODES,
  OWNER_PARITY_WRITE_ENABLED,
  OwnerParityCiRefusal,
  ownerParityDispatch,
  parseOwnerParityDiscoveryReceipt,
  requireEmptyOwnerParityRoster,
} from "../owner-buffered-parity-ci-contract.ts";

const H = (c: string) => c.repeat(64);
const SHA = "5".repeat(40);
// Identifier-shaped fixtures (slug-like org, workspace id, email-like user) that must never be emitted.
const tenants: ParityTenant[] = Array.from({ length: 12 }, (_, i) => ({
  orgId: `fixture-org-slug-${i}`, workspaceId: `fixture-workspace-id-${i}`, userId: `fixture.person${i}@example.invalid`,
}));
const IDENTIFIERS = tenants.flatMap((t) => [t.orgId, t.workspaceId, t.userId]).concat(["example.invalid", "fixture-org-slug"]);
const emptyRoster = { contract: "owner-buffered-parity-roster/v1", comment: "ships empty", tenants: [] as string[] };
const fullRoster = { contract: "owner-buffered-parity-roster/v1", tenants: tenants.slice(0, 7).map(tenantSha256) };
const env = (mode = "discovery", extra: Record<string, string> = {}) =>
  ({ OWNER_PARITY_MODE: mode, OWNER_PARITY_SHA: SHA, OWNER_PARITY_CENSUS_SOURCE: "frozen-supabase", ...extra });
const T0 = Date.parse("2026-11-13T00:00:00.000Z");

type Disposition = "current" | "missing" | "stale";

/**
 * A mock world: `buffered` is the cursor's rows, `state` the per-tenant stored row (current = byte-equal to the
 * fresh proof, stale = differs, missing = none). Generation reads can be scripted per call.
 */
function world(state: Disposition[], opts: { extraRows?: Array<Record<string, unknown>> } = {}) {
  const buffered = tenants.slice(0, state.length);
  const w = {
    generation: H("4"),
    targetGenerations: [] as string[],
    snapshotGeneration: null as string | null,
    proofGeneration: null as string | null,
    statements: [] as Array<{ session: string; sql: string; params: unknown[] }>,
    g1: [] as string[],
  };
  const proof = (t: ParityTenant): OwnerBufferedParityProof => ({
    contract: "owner-buffered-parity/v1", tenant: { ...t }, controlEpoch: H("1"), privacyBaselineSha256: H("2"), explicitNoHead: false,
    head: { epoch: H("1"), version: "3", digest: H("3"), kind: "selection" }, deletionSetSha256: H("a"),
    resetSetSha256: H("b"), targetLineageDigest: H("c"), targetGeneration: w.proofGeneration ?? w.generation,
  });
  const row = (p: OwnerBufferedParityProof, stale: boolean) => ({ cutover_epoch: p.controlEpoch, head_epoch: p.head!.epoch,
    head_version: p.head!.version, head_digest: p.head!.digest, head_kind: p.head!.kind, no_head: false,
    deletion_set_digest: stale ? H("e") : p.deletionSetSha256, reset_set_digest: p.resetSetSha256,
    target_lineage_digest: p.targetLineageDigest, target_generation: p.targetGeneration, privacy_baseline_sha256: p.privacyBaselineSha256 });
  const byUser = new Map(buffered.map((t, i) => [t.userId, { t, d: state[i]! }]));
  let fetched = false;
  const sessions: OwnerParitySessions = {
    source: { query: async () => { throw new Error("source used directly"); } },
    enumerator: {
      query: async <R>(sql: string, params: unknown[] = []) => {
        w.statements.push({ session: "enumerator", sql, params });
        if (sql === BULK_TARGET_GENERATION_SQL) return { rows: [{ generation: w.snapshotGeneration ?? w.generation }] as R[] };
        if (sql.startsWith("DECLARE")) fetched = false;
        if (sql.startsWith("FETCH")) {
          const rows = fetched ? [] : [...buffered.map((t) => ({ org_id: t.orgId, workspace_id: t.workspaceId, user_id: t.userId })),
            ...(opts.extraRows ?? [])];
          fetched = true;
          return { rows: rows as R[] };
        }
        return { rows: [] as R[] };
      },
    },
    target: {
      query: async <R>(sql: string, params: unknown[] = []) => {
        w.statements.push({ session: "target", sql, params });
        if (sql === BULK_TARGET_GENERATION_SQL) return { rows: [{ generation: w.targetGenerations.shift() ?? w.generation }] as R[] };
        if (sql === READ_OWNER_BUFFERED_PARITY_SQL) {
          const hit = byUser.get(String(params[2]));
          if (!hit || hit.d === "missing") return { rows: [] as R[] };
          return { rows: [row(proof(hit.t), hit.d === "stale")] as R[] };
        }
        throw new Error("unexpected target sql");
      },
    },
  };
  const readCensus = (async (_s: unknown, _t: unknown, tenant: ParityTenant): Promise<ParityCensusReceipt> => {
    w.g1.push(tenant.userId);
    return { contract: "owner-buffered-parity-census/v1", tenant: { ...tenant }, equal: true, mismatches: [], proof: proof(tenant) };
  }) as never;
  return { w, sessions, deps: { readCensus } };
}

const seven: Disposition[] = ["current", "missing", "missing", "stale", "missing", "current", "missing", "missing", "missing"];
const discover = (x: ReturnType<typeof world>, e: Record<string, string> = env(), roster: unknown = emptyRoster) =>
  runOwnerParityDiscovery(e, x.sessions, { roster, runId: "505", now: () => T0, deps: x.deps });
const serialized = (o: OwnerParityDiscoveryOutcome) =>
  JSON.stringify(o.receipt) + JSON.stringify(discoveryRunReceipt(o.receipt)) + ownerParityDiscoverySummary(o.receipt);
const expectCountsOnly = (o: OwnerParityDiscoveryOutcome) => {
  expect(o.receipt.owners).toEqual([]);
  for (const t of tenants) expect(serialized(o)).not.toContain(tenantSha256(t));
  expect(serialized(o)).not.toContain("tenantSha256");
};

test("discovery: exactly seven missing/stale owners yields sorted digests, dispositions and counts; nothing is written", async () => {
  const x = world(seven);
  const out = await discover(x);
  expect(out.exitCode).toBe(0);
  expect(out.receipt).toMatchObject({ contract: OWNER_PARITY_DISCOVERY_CONTRACT, status: "ok", runId: "505", sourceSha: SHA,
    generation: H("4"), censusSource: "frozen-supabase", capturedAt: new Date(T0).toISOString(), credentialsPersisted: false,
    counts: { scanned: 9, canonicalCurrent: 2, missing: 6, stale: 1, unparsable: 0 } });
  const expected = tenants.slice(0, 9).map((t, i) => ({ tenantSha256: tenantSha256(t), d: seven[i]! }))
    .filter((o) => o.d !== "current").map((o) => ({ tenantSha256: o.tenantSha256, disposition: o.d }))
    .sort((a, b) => (a.tenantSha256 < b.tenantSha256 ? -1 : 1));
  expect(out.receipt.owners).toEqual(expected);
  expect(parseOwnerParityDiscoveryReceipt(JSON.parse(JSON.stringify(out.receipt)))).toEqual(out.receipt);
  expect(x.w.g1.length).toBe(9);
  for (const s of x.w.statements) expect(s.sql).not.toMatch(/\b(INSERT|UPDATE|DELETE|MERGE|TRUNCATE|LOCK|SET ROLE)\b/iu);
  expect(x.w.statements.filter((s) => s.session === "target").every((s) =>
    s.sql === BULK_TARGET_GENERATION_SQL || s.sql === READ_OWNER_BUFFERED_PARITY_SQL)).toBe(true);
  const summary = ownerParityDiscoverySummary(out.receipt);
  for (const o of expected) expect(summary).toContain(`| \`${o.tenantSha256}\` | ${o.disposition} |`);
  expect(discoveryRunReceipt(out.receipt)).toMatchObject({ mode: "discovery", status: "ok", tenants: [],
    counts: { found: 9, equal: 2, missing: 6, stale: 1, inserted: 0, updated: 0, verified: 0 } });
});

test("discovery reads the generation before, as the snapshot's first read, and after; one read-only repeatable-read cursor", async () => {
  const x = world(seven);
  await discover(x);
  const e = x.w.statements.filter((s) => s.session === "enumerator").map((s) => s.sql);
  expect(e[0]).toBe("BEGIN ISOLATION LEVEL REPEATABLE READ READ ONLY");
  expect(e[1]).toBe("SET LOCAL statement_timeout = '30s'");
  expect(e[2]).toBe(BULK_TARGET_GENERATION_SQL);
  expect(e[3]).toContain(`NO SCROLL CURSOR FOR ${BULK_BUFFERED_TENANTS_SQL}`);
  expect(e.filter((s) => s.startsWith("BEGIN")).length).toBe(1);
  expect(e.at(-1)).toBe("COMMIT");
  const t = x.w.statements.filter((s) => s.session === "target").map((s) => s.sql);
  expect(t[0]).toBe(BULK_TARGET_GENERATION_SQL);
  expect(t.at(-1)).toBe(BULK_TARGET_GENERATION_SQL);
  expect(x.w.statements.findIndex((s) => s.session === "target")).toBeLessThan(
    x.w.statements.findIndex((s) => s.session === "enumerator"));
});

test("discovery: a count other than seven refuses unexpected_scope with counts only, never digests", async () => {
  for (const [state, missing, stale] of [
    [["current", "current", "current", "missing", "missing", "missing", "stale", "stale", "stale"], 3, 3],
    [["missing", "missing", "missing", "missing", "stale", "stale", "stale", "missing", "current"], 5, 3],
    [[], 0, 0],
    [Array.from({ length: 12 }, () => "missing"), 8, 0], // fails fast at the eighth, scanned stays 12
  ] as Array<[Disposition[], number, number]>) {
    const x = world(state);
    const out = await discover(x);
    expect(out.exitCode).toBe(1);
    expect(out.receipt).toMatchObject({ status: "refused", code: "unexpected_scope", counts: { scanned: state.length, missing, stale } });
    expectCountsOnly(out);
    expect(parseOwnerParityDiscoveryReceipt(JSON.parse(JSON.stringify(out.receipt))).status).toBe("refused");
  }
});

test("discovery: stops after the eighth missing/stale owner instead of comparing the rest", async () => {
  const x = world(Array.from({ length: 12 }, () => "missing"));
  const calls: string[] = [];
  const inner = x.deps.readCensus as (...a: unknown[]) => Promise<ParityCensusReceipt>;
  const out = await runOwnerParityDiscovery(env(), x.sessions, { roster: emptyRoster, runId: "505", now: () => T0,
    deps: { readCensus: ((...a: unknown[]) => { calls.push("call"); return inner(...a); }) as never } });
  expect(out.exitCode).toBe(1);
  expect(out.receipt).toMatchObject({ status: "refused", code: "unexpected_scope", counts: { scanned: 12, missing: 8, stale: 0 } });
  expect(out.receipt.owners).toEqual([]);
  expect(calls.length).toBe(8);
  expect(x.w.g1.length).toBe(8);
  expectCountsOnly(out);
});

test("discovery: an unparsable buffered row refuses unexpected_scope even with seven missing/stale owners", async () => {
  const x = world(seven, { extraRows: [{ org_id: null, workspace_id: "w", user_id: "u" }] });
  const out = await discover(x);
  expect(out.receipt).toMatchObject({ status: "refused", code: "unexpected_scope", counts: { scanned: 10, unparsable: 1 } });
  expectCountsOnly(out);
});

test("discovery: generation drift before/inside the snapshot, after it, or in a fresh proof refuses generation_unstable", async () => {
  const inside = world(seven);
  inside.w.snapshotGeneration = H("9");
  const a = await discover(inside);
  expect(a.receipt).toMatchObject({ status: "refused", code: "generation_unstable" });
  expect(inside.w.g1).toEqual([]);
  expect(inside.w.statements.some((s) => s.sql.startsWith("DECLARE"))).toBe(false);
  expect(inside.w.statements.filter((s) => s.session === "enumerator").at(-1)!.sql).toBe("ROLLBACK");

  const after = world(seven);
  after.w.targetGenerations = [H("4"), H("9")];
  const b = await discover(after);
  expect(b.receipt).toMatchObject({ status: "refused", code: "generation_unstable", counts: { scanned: 9, missing: 6, stale: 1 } });
  expect(after.w.g1.length).toBe(9);

  const proof = world(seven);
  proof.w.proofGeneration = H("8");
  const c = await discover(proof);
  expect(c.receipt).toMatchObject({ status: "refused", code: "generation_unstable" });
  for (const out of [a, b, c]) { expect(out.exitCode).toBe(1); expectCountsOnly(out); }
});

test("discovery refuses roster_present for any non-empty roster and roster_invalid for a malformed one, before any statement", async () => {
  for (const [roster, code] of [
    [fullRoster, "roster_present"],
    [{ ...fullRoster, tenants: fullRoster.tenants.slice(0, 1) }, "roster_present"],
    [{ ...fullRoster, tenants: fullRoster.tenants.slice(0, 8).concat([tenantSha256(tenants[8]!)]) }, "roster_present"],
    [{ ...fullRoster, tenants: ["not-a-digest"] }, "roster_invalid"],
    [{ ...emptyRoster, extra: true }, "roster_invalid"],
    [null, "roster_invalid"],
  ] as Array<[unknown, string]>) {
    const x = world(seven);
    const out = await discover(x, env(), roster);
    expect(out.receipt).toMatchObject({ status: "refused", code, owners: [] });
    expect(x.w.statements).toEqual([]);
  }
  expect(() => requireEmptyOwnerParityRoster(emptyRoster)).not.toThrow();
  // The other modes keep every roster refusal: the empty roster still refuses roster_empty.
  for (const mode of ["census", "readback"]) {
    const x = world(seven);
    const out = await runOwnerParity(env(mode), x.sessions, { roster: emptyRoster, runId: "1", deps: x.deps });
    expect(out.receipt.code).toBe("roster_empty");
    expect(x.w.statements).toEqual([]);
  }
});

test("mode dispatch table: write and bulk are unreachable from discovery; only write has a writable session", async () => {
  expect([...OWNER_PARITY_MODES]).toEqual(["census", "write", "readback", "discovery"]);
  expect(Object.keys(OWNER_PARITY_MODE_POLICY).sort()).toEqual([...OWNER_PARITY_MODES].sort());
  expect(OWNER_PARITY_MODES.filter((m) => !OWNER_PARITY_MODE_POLICY[m].readOnly)).toEqual(["write"]);
  expect(OWNER_PARITY_MODE_POLICY.discovery).toEqual({ readOnly: true, roster: "empty" });
  expect(OWNER_PARITY_MODES.filter((m) => OWNER_PARITY_MODE_POLICY[m].roster === "empty")).toEqual(["discovery"]);
  expect(Object.isFrozen(OWNER_PARITY_MODE_POLICY) && Object.isFrozen(OWNER_PARITY_MODE_POLICY.discovery)).toBe(true);
  expect(OWNER_PARITY_WRITE_ENABLED).toBe(false);
  // Discovery takes no census run or confirmation, and passes no census artifact.
  expect(ownerParityDispatch(env(), false)).toMatchObject({ mode: "discovery", sha: SHA, censusSource: "frozen-supabase" });
  const refusal = (fn: () => unknown) => { try { fn(); return null; } catch (e) { return e instanceof OwnerParityCiRefusal ? e.code : "other"; } };
  expect(refusal(() => ownerParityDispatch(env("discovery", { OWNER_PARITY_CENSUS_RUN: "101" }), true))).toBe("owner_parity_census_run");
  expect(refusal(() => ownerParityDispatch(env("discovery", { OWNER_PARITY_CONFIRMATION: "WRITE-OWNER-PARITY" }), true))).toBe("owner_parity_confirmation");
  expect(ownerParityPreflight(env(), emptyRoster, { forged: "census" }, true)).toMatchObject({ roster: [], census: null });
  // The discovery runner refuses every other mode (bulk, write even with the switch on) before any statement.
  for (const [mode, roster, code] of [["bulk", emptyRoster, "owner_parity_inputs"], ["BULK", emptyRoster, "owner_parity_inputs"],
    ["write", fullRoster, "owner_parity_write_dormant"], ["census", fullRoster, "owner_parity_mode_route"],
    ["readback", fullRoster, "owner_parity_mode_route"]] as Array<[string, unknown, string]>) {
    const x = world(seven);
    const out = await discover(x, env(mode), roster);
    expect(out.receipt.code).toBe(code);
    expect(x.w.statements).toEqual([]);
  }
  // And the census/write/readback runner refuses discovery before any statement (it never runs readback logic).
  const x = world(seven);
  const out = await runOwnerParity(env(), x.sessions, { roster: emptyRoster, runId: "1", writeEnabled: true, deps: x.deps });
  expect(out.receipt.code).toBe("owner_parity_mode_route");
  expect(x.w.statements).toEqual([]);
});

test("sanitization: no identifier fixture, private key name or credential reaches the receipt, run receipt or summary", async () => {
  const outcomes = [await discover(world(seven)), await discover(world(["missing", "stale"])),
    await discover(world(seven, { extraRows: [{ org_id: "fixture-org-slug-x", workspace_id: "w", user_id: 7 }] }))];
  for (const out of outcomes) {
    const text = serialized(out);
    for (const id of IDENTIFIERS) expect(text).not.toContain(id);
    for (const k of ["orgId", "workspaceId", "userId", "org_id", "workspace_id", "user_id", "email", "slug", "supabaseUrl", "neonUrl", "postgres"])
      expect(text).not.toContain(k);
    expect(text).not.toContain("SELECT");
  }
});

test("discovery receipt parser: only the contract shape is publishable", async () => {
  const ok = (await discover(world(seven))).receipt;
  const refused = (await discover(world(["missing"]))).receipt;
  const bad = (v: unknown) => { try { parseOwnerParityDiscoveryReceipt(v); return null; } catch (e) { return (e as OwnerParityCiRefusal).code; } };
  const clone = <T>(v: T): any => JSON.parse(JSON.stringify(v));
  for (const mutate of [
    (r: any) => { r.orgId = tenants[0]!.orgId; },
    (r: any) => { r.owners[0].userId = tenants[0]!.userId; },
    (r: any) => { r.owners.pop(); r.counts.missing--; },
    (r: any) => { r.owners.reverse(); },
    (r: any) => { r.owners[0].disposition = "equal"; },
    (r: any) => { r.owners[1].tenantSha256 = r.owners[0].tenantSha256; },
    (r: any) => { r.counts.scanned = 1; },
    (r: any) => { r.counts.extra = 0; },
    (r: any) => { r.credentialsPersisted = true; },
    (r: any) => { r.contract = "owner-buffered-parity-discovery/v2"; },
    (r: any) => { r.generation = null; },
  ]) {
    const r = clone(ok);
    mutate(r);
    expect(bad(r)).toBe("owner_parity_discovery_receipt_invalid");
  }
  const leak = clone(refused);
  leak.owners = clone(ok).owners;
  expect(bad(leak)).toBe("owner_parity_discovery_receipt_invalid");
  expect(bad({ ...clone(refused), code: "postgres://x y" })).toBe("owner_parity_discovery_receipt_invalid");
  expect(bad(clone(ok))).toBeNull();
  expect(bad(clone(refused))).toBeNull();
});

test("parent process: discovery passes the empty committed roster, refuses before the secret, writes a counts-only discovery receipt", () => {
  const temp = mkdtempSync(join(tmpdir(), "owner-parity-discovery-"));
  try {
    const url = "postgresql://fixture-user:fixture-secret@db.fixture.invalid:5432/postgres";
    const root = join(import.meta.dir, "../../..");
    const out = spawnSync(process.execPath, ["scripts/owner-buffered-parity-ci.ts"], {
      cwd: root, encoding: "utf8",
      env: { PATH: process.env.PATH ?? "", RUNNER_TEMP: temp, GITHUB_RUN_ID: "606", SUPABASE_MIGRATION_OWNER_URL: url, ...env() },
    });
    // The committed roster is empty, so discovery passes the roster rule; the fixture sha is not HEAD.
    expect(out.status).toBe(1);
    expect(out.stdout).toContain("\"mode\":\"discovery\"");
    expect(out.stdout).toContain("\"code\":\"checkout_pin_mismatch\"");
    const file = join(temp, "owner-parity-out", "owner-buffered-parity-discovery.json");
    const receipt = readFileSync(file, "utf8");
    expect(JSON.parse(receipt)).toMatchObject({ contract: OWNER_PARITY_DISCOVERY_CONTRACT, status: "refused",
      code: "checkout_pin_mismatch", runId: "606", owners: [] });
    const summaryPath = join(temp, "summary.md");
    writeFileSync(summaryPath, "");
    const summary = spawnSync(process.execPath, ["scripts/owner-buffered-parity-ci.ts", "--discovery-summary"], {
      cwd: root, encoding: "utf8", env: { PATH: process.env.PATH ?? "", RUNNER_TEMP: temp, GITHUB_STEP_SUMMARY: summaryPath },
    });
    expect(summary.status).toBe(0);
    const published = readFileSync(summaryPath, "utf8");
    expect(published).toContain("status: refused (checkout_pin_mismatch)");
    for (const text of [out.stdout, out.stderr, receipt, published, summary.stdout, summary.stderr]) {
      expect(text).not.toContain("fixture-secret");
      expect(text).not.toContain("db.fixture.invalid");
    }
    // A tampered receipt is not published.
    writeFileSync(file, JSON.stringify({ ...JSON.parse(receipt), orgId: "fixture-org-slug-0" }));
    const tampered = spawnSync(process.execPath, ["scripts/owner-buffered-parity-ci.ts", "--discovery-summary"], {
      cwd: root, encoding: "utf8", env: { PATH: process.env.PATH ?? "", RUNNER_TEMP: temp, GITHUB_STEP_SUMMARY: summaryPath },
    });
    expect(tampered.status).toBe(1);
    expect(readFileSync(summaryPath, "utf8")).toBe(published);
    expect(readFileSync(summaryPath, "utf8")).not.toContain("fixture-org-slug");
  } finally {
    rmSync(temp, { recursive: true, force: true });
  }
});
