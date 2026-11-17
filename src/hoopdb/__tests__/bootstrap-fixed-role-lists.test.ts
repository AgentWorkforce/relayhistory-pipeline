/**
 * Guards hand-maintained copies of the fixed role / bootstrap file lists against drift from
 * their sources of truth (the pg-proof workflow checks the same properties against real servers).
 */
import { describe, expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { join } from "node:path";
import { SESSION_BOOTSTRAP_FILES } from "../session-bootstrap-chain.ts";

const root = join(import.meta.dir, "../../..");
const read = (rel: string) => readFileSync(join(root, rel), "utf8");

const seededRoles = (): string[] => {
  const seed = read("supabase/bootstrap/rds_migration_roles.sql");
  const arrayStart = seed.indexOf("FOREACH v_role IN ARRAY ARRAY[");
  expect(arrayStart).toBeGreaterThan(-1);
  const arrayEnd = seed.indexOf("]", arrayStart + "FOREACH v_role IN ARRAY ARRAY[".length);
  const seeded = [...seed.slice(arrayStart, arrayEnd).matchAll(/'([^']+)'::name/g)].map((m) => m[1]!);
  expect(seeded.length).toBeGreaterThan(5);
  return seeded;
};

describe("bootstrap fixed-role lists", () => {
  test("rehearse-relayhistory-t0.sh provisions exactly the single-sourced bootstrap chain", () => {
    const setup = read("scripts/rehearse-relayhistory-t0.sh").match(
      /cat "\$WORK\/bulls\/supabase\/tests\/supabase_shim\.sql"[\s\S]*?\| psql_in dst/,
    )?.[0];
    expect(setup).toBeDefined();
    const files = [
      ...setup!.matchAll(/"\$WORK\/bulls(?:\/([^"\n]+))?"(\/supabase\/migrations\/\*\.sql)?/g),
    ]
      .filter((m) => !m[2])
      .map((m) => m[1]);
    expect(files).toEqual(["supabase/tests/supabase_shim.sql", ...SESSION_BOOTSTRAP_FILES]);
  });

  test("hoopdb_runtime_role_rds.sql refuses every fixed role rds_migration_roles.sql seeds", () => {
    const seeded = seededRoles();

    const runtime = read("supabase/bootstrap/hoopdb_runtime_role_rds.sql");
    const inStart = runtime.indexOf("v_role IN (");
    expect(inStart).toBeGreaterThan(-1);
    const inEnd = runtime.indexOf(")", inStart);
    const refused = new Set([...runtime.slice(inStart, inEnd).matchAll(/'([^']+)'/g)].map((m) => m[1]!));

    const missing = seeded.filter((name) => !refused.has(name));
    expect(missing, `runtime refusal list is missing: ${missing.join(", ")}`).toEqual([]);
  });

  test("rds-profile-proof.ts FIXED covers every role rds_migration_roles.sql seeds", () => {
    const seeded = seededRoles();
    const proof = read("scripts/pg-proof/rds-profile-proof.ts");
    const start = proof.indexOf("const FIXED = [");
    expect(start).toBeGreaterThan(-1);
    const end = proof.indexOf("];", start);
    expect(end).toBeGreaterThan(start);
    const fixed = [...proof.slice(start, end).matchAll(/"([^"]+)"/g)].map((m) => m[1]!);
    expect(fixed.length).toBeGreaterThan(5);

    const missing = seeded.filter((name) => !fixed.includes(name));
    expect(missing, `pg-proof FIXED is missing: ${missing.join(", ")}`).toEqual([]);
  });
});
