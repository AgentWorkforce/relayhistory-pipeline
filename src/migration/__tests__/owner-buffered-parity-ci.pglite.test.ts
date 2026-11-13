/**
 * Synthetic PGlite proof for the owner-buffered parity CI wrapper over every migration (the same proof the
 * workflow's proof job runs with `bun scripts/owner-buffered-parity-ci.ts --proof`): census, write (enabled
 * only by test injection) and readback for exactly the seven roster tenants of nine buffered ones, and every
 * refusal named in docs/owner-buffered-parity-ci.md, plus the read-only discovery mode. Fixture identifiers only.
 */
import { expect, test } from "bun:test";
import { ownerParityProof } from "../../../scripts/owner-buffered-parity-ci.ts";

test("PGlite proof: census, dormant/roster/confirmation/bulk/stale/sha/generation refusals, write, readback, discovery, eighth tenant untouched", async () => {
  const { cases, failed } = await ownerParityProof();
  expect(cases.filter((c) => !c.pass)).toEqual([]);
  expect(failed).toBe(0);
  const names = cases.map((c) => c.name).join("\n");
  for (const required of ["census:", "dormant", "roster_empty", "roster_incomplete", "bulk is unreachable", "wrong confirmation",
    "stale census", "another sha", "generation moved", "eighth and ninth tenants never touched", "readback: equal",
    "missing", "no private field names", "discovery: refuses roster_present", "discovery: four missing/stale owners refuses unexpected_scope",
    "discovery: read-only snapshot on a fresh image finds exactly seven"])
    expect(names).toContain(required);
}, 300000);
