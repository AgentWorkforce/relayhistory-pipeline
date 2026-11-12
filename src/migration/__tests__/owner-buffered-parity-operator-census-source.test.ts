import { afterAll, beforeAll, describe, expect, test } from "bun:test";
import { execFileSync } from "node:child_process";
import { mkdtempSync, rmSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import type { ParityCensusReceipt, ParityCensusSource, ParityTenant } from "../owner-buffered-parity.ts";
import {
  OwnerBufferedOperatorRefusal,
  operatorCredentialKeys,
  parseOperatorArgs,
  runBulkParity,
  validateCensusSourceInput,
} from "../../../scripts/owner-buffered-parity-operator.ts";
import type { Queryable } from "../relayhistory-export.ts";

// Step-6 edge 5: --census-source neon|frozen-supabase. Fake URLs only; every
// spawned run refuses before any connection could be opened.
const SHA = "a".repeat(40);
const GENERATION = "b".repeat(64);
const SCRIPT = join(import.meta.dir, "../../../scripts/owner-buffered-parity-operator.ts");
const FAKE_URL = "postgres://user:pw@localhost/db";
const TENANT = { orgId: "fixture-org", workspaceId: "fixture-workspace", userId: "fixture-user" };
const census = (...extra: string[]) => ["census", "--expect-source-sha", SHA, "--expect-target-generation", GENERATION, ...extra];
const bulk = (...extra: string[]) => [
  "bulk", "--confirm-write", "owner-buffered-parity-bulk/v1",
  "--expect-source-sha", SHA, "--expect-target-generation", GENERATION, ...extra,
];
const refusalCode = (fn: () => unknown): string => {
  try { fn(); } catch (error) { return (error as { code: string }).code; }
  return "no_refusal";
};

describe("--census-source arguments", () => {
  test("defaults to neon and accepts both sources in every mode", () => {
    expect(parseOperatorArgs(census()).censusSource).toBe("neon");
    expect(parseOperatorArgs(census("--census-source", "neon")).censusSource).toBe("neon");
    expect(parseOperatorArgs(census("--census-source", "frozen-supabase")).censusSource).toBe("frozen-supabase");
    expect(parseOperatorArgs([
      "write", "--confirm-write", "owner-buffered-parity/v1", "--census-source", "frozen-supabase",
      "--expect-source-sha", SHA, "--expect-target-generation", GENERATION,
    ])).toMatchObject({ mode: "write", censusSource: "frozen-supabase" });
    expect(parseOperatorArgs(bulk("--census-source", "frozen-supabase")))
      .toMatchObject({ mode: "bulk", censusSource: "frozen-supabase", batchSize: 25 });
  });

  test("unknown, empty-cased, or duplicate sources refuse", () => {
    for (const value of ["supabase", "Neon", "frozen", "frozen_supabase", "neon "])
      expect(refusalCode(() => parseOperatorArgs(census("--census-source", value)))).toBe("census_source_invalid");
    expect(refusalCode(() => parseOperatorArgs(census("--census-source")))).toBe("usage");
    expect(refusalCode(() => parseOperatorArgs(census("--census-source", "neon", "--census-source", "frozen-supabase"))))
      .toBe("usage");
  });
});

describe("--census-source stdin contract", () => {
  const frozen = parseOperatorArgs(census("--census-source", "frozen-supabase"));
  const frozenBulk = parseOperatorArgs(bulk("--census-source", "frozen-supabase"));
  const neon = parseOperatorArgs(census());

  test("neon keeps the exact pre-edge-5 key contract", () => {
    expect(operatorCredentialKeys(neon)).toEqual({
      required: ["neonUrl", "supabaseUrl", "tenant"], allowed: ["neonUrl", "supabaseUrl", "tenant"],
    });
    expect(operatorCredentialKeys(parseOperatorArgs(bulk())))
      .toEqual({ required: ["neonUrl", "supabaseUrl"], allowed: ["neonUrl", "supabaseUrl"] });
    expect(() => validateCensusSourceInput(neon, { neonUrl: FAKE_URL, supabaseUrl: FAKE_URL, tenant: TENANT })).not.toThrow();
  });

  test("frozen accepts exactly supabaseUrl (+ tenant for single-tenant modes)", () => {
    expect(() => validateCensusSourceInput(frozen, { supabaseUrl: FAKE_URL, tenant: TENANT })).not.toThrow();
    expect(() => validateCensusSourceInput(frozenBulk, { supabaseUrl: FAKE_URL })).not.toThrow();
  });

  test("frozen refuses neonUrl as a source conflict and a missing key by name", () => {
    expect(refusalCode(() => validateCensusSourceInput(frozen, { neonUrl: FAKE_URL, supabaseUrl: FAKE_URL, tenant: TENANT })))
      .toBe("census_source_conflict");
    expect(refusalCode(() => validateCensusSourceInput(frozenBulk, { neonUrl: FAKE_URL, supabaseUrl: FAKE_URL })))
      .toBe("census_source_conflict");
    expect(refusalCode(() => validateCensusSourceInput(frozen, { tenant: TENANT })))
      .toBe("credentials_missing_key.supabaseUrl");
    expect(refusalCode(() => validateCensusSourceInput(frozen, { supabaseUrl: FAKE_URL })))
      .toBe("credentials_missing_key.tenant");
    expect(refusalCode(() => validateCensusSourceInput(frozenBulk, {})))
      .toBe("credentials_missing_key.supabaseUrl");
  });
});

describe("bulk runner forwards the census source", () => {
  test.each(["neon", "frozen-supabase"] as const)("%s reaches G1 and G2", async (source: ParityCensusSource) => {
    const seen: string[] = [];
    const receipt = (tenant: ParityTenant): ParityCensusReceipt => ({
      contract: "owner-buffered-parity-census/v1", tenant, equal: true, mismatches: [],
      proof: {
        contract: "owner-buffered-parity/v1", tenant, controlEpoch: "1".repeat(64), privacyBaselineSha256: "2".repeat(64),
        explicitNoHead: true, head: null, deletionSetSha256: "3".repeat(64), resetSetSha256: "4".repeat(64),
        targetLineageDigest: "5".repeat(64), targetGeneration: GENERATION,
      },
    });
    let pages = [[{ org_id: "o1", workspace_id: "w1", user_id: "u1" }]];
    const enumerator: Queryable = { async query<R>(sql: string) {
      return { rows: (sql.startsWith("FETCH") ? pages.shift() ?? [] : []) as R[] };
    } };
    const target: Queryable = { async query<R>(sql: string) {
      return { rows: (sql.includes("__96bulls_role_validation") ? [{ generation: GENERATION }] : []) as R[] };
    } };
    const args = source === "neon" ? [] : [source];
    const counts = await runBulkParity({ query: async <R>() => ({ rows: [] as R[] }) }, enumerator, target, GENERATION, 1, {
      readCensus: async (_s, _t, tenant, censusSource) => { seen.push(`read:${censusSource}`); return receipt(tenant); },
      writeProof: async (_s, _t, tenant, _g, censusSource) => {
        seen.push(`write:${censusSource}`);
        return { census: receipt(tenant), outcome: "insert", writtenAt: "now" };
      },
    }, ...(args as [ParityCensusSource?]));
    expect(counts).toMatchObject({ inserted: 1 });
    expect(seen).toEqual([`read:${source}`, `write:${source}`]);
    pages = [];
  });
});

describe("operator CLI (spawned, refuses before any connection)", () => {
  let repo = "";
  let home = "";
  let head = "";

  beforeAll(() => {
    // A throwaway clean checkout so the pinned-source check passes without touching this repo.
    repo = mkdtempSync(join(tmpdir(), "census-source-cli-"));
    // Separate HOME: bun writes its cache under HOME, which must not dirty the pinned checkout.
    home = mkdtempSync(join(tmpdir(), "census-source-home-"));
    const git = (...args: string[]) => execFileSync("git", args, { cwd: repo, encoding: "utf8", env: { PATH: process.env.PATH, HOME: home } });
    git("init", "-q");
    git("-c", "user.name=fixture", "-c", "user.email=fixture@example.invalid", "commit", "-q", "--allow-empty", "-m", "fixture");
    head = git("rev-parse", "HEAD").trim();
  });

  afterAll(() => {
    for (const dir of [repo, home]) if (dir) rmSync(dir, { recursive: true, force: true });
  });

  async function run(argv: string[], stdin: unknown): Promise<{ code: number; stdout: string; stderr: string; body: Record<string, unknown> }> {
    const proc = Bun.spawn(["bun", SCRIPT, ...argv.map((a) => a === SHA ? head : a)], {
      cwd: repo, stdin: "pipe", stdout: "pipe", stderr: "pipe", env: { PATH: process.env.PATH ?? "", HOME: home },
    });
    proc.stdin.write(typeof stdin === "string" ? stdin : JSON.stringify(stdin));
    await proc.stdin.end();
    const [stdout, stderr, code] = await Promise.all([
      new Response(proc.stdout).text(), new Response(proc.stderr).text(), proc.exited,
    ]);
    const lines = stdout.trim().split("\n");
    expect(lines).toHaveLength(1);
    const { at, ...body } = JSON.parse(lines[0]!) as Record<string, unknown>;
    expect(typeof at).toBe("string");
    return { code, stdout, stderr, body };
  }

  const noUrl = (r: { stdout: string; stderr: string }) => {
    for (const text of [r.stdout, r.stderr])
      for (const needle of ["postgres://", "localhost", "user:pw", "pw@", "fixture-org", "fixture-user"])
        expect(text).not.toContain(needle);
  };

  const neonInput = { neonUrl: FAKE_URL, supabaseUrl: FAKE_URL, tenant: TENANT };

  test("default is byte-identical to an explicit --census-source neon", async () => {
    for (const input of [neonInput, { supabaseUrl: FAKE_URL, tenant: TENANT }, { ...neonInput, extra: 1 }, "not json"]) {
      const implicit = await run(census(), input);
      const explicit = await run(census("--census-source", "neon"), input);
      expect(explicit.code).toBe(implicit.code);
      expect(JSON.stringify(explicit.body)).toBe(JSON.stringify(implicit.body));
      expect(explicit.stderr).toBe(implicit.stderr);
      expect(implicit.body).not.toHaveProperty("censusSource");
      noUrl(implicit);
    }
    const neon = await run(census(), neonInput);
    expect(neon).toMatchObject({ code: 1, body: { tool: "owner-buffered-parity-operator", status: "refused", code: "neon_url_sslmode_required" } });
  });

  test("an unknown source refuses with exit 64 before reading stdin", async () => {
    const r = await run(census("--census-source", "neon-replica"), neonInput);
    expect(r).toMatchObject({ code: 64, body: { status: "refused", code: "census_source_invalid" } });
    noUrl(r);
  });

  test("frozen mode refuses a neonUrl key as census_source_conflict", async () => {
    for (const argv of [census("--census-source", "frozen-supabase"), bulk("--census-source", "frozen-supabase")]) {
      const r = await run(argv, argv[0] === "bulk" ? { neonUrl: FAKE_URL, supabaseUrl: FAKE_URL } : neonInput);
      expect(r).toMatchObject({ code: 1, body: { status: "refused", code: "census_source_conflict", censusSource: "frozen-supabase" } });
      noUrl(r);
    }
  });

  test("frozen mode refuses a missing supabaseUrl", async () => {
    const r = await run(census("--census-source", "frozen-supabase"), { tenant: TENANT });
    expect(r).toMatchObject({ code: 1, body: { status: "refused", code: "credentials_missing_key.supabaseUrl" } });
    noUrl(r);
  });

  test("frozen mode validates the Supabase URL without printing it", async () => {
    const r = await run(census("--census-source", "frozen-supabase"), { supabaseUrl: FAKE_URL, tenant: TENANT });
    expect(r).toMatchObject({ code: 1, body: { status: "refused", code: "supabase_url_sslmode_required", censusSource: "frozen-supabase" } });
    noUrl(r);
    const unknown = await run(census("--census-source", "frozen-supabase"), { supabaseUrl: FAKE_URL, tenant: TENANT, other: FAKE_URL });
    expect(unknown.body).toMatchObject({ code: "credentials_unknown_key.other" });
    noUrl(unknown);
  });
});
