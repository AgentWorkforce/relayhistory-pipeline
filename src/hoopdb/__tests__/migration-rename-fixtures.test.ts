/**
 * The original bytes of the two renamed migrations, as shipped in services image
 * 070fe6fd (source f1c25726), and that image's installed ledger, pin
 * MIGRATION_RENAME_ALIASES. Fixtures ported from #396.
 */
import { expect, test } from "bun:test";
import { createHash } from "node:crypto";
import { existsSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { MIGRATION_RENAME_ALIASES, resolveRenamedLedger } from "../migrate-guards.ts";

const root = join(import.meta.dir, "../../..");
const fixtures = join(import.meta.dir, "fixtures/released-migration-history");
const ledger = JSON.parse(readFileSync(join(fixtures, "ledger.json"), "utf8")) as { name: string; sha256: string }[];
/** The planned file's sha256; undefined for a name this tree has no file for (the migrator's plan lookup). */
const fileSha = (name: string) => {
  const path = join(root, "supabase/migrations", name.slice("upstream:".length));
  return existsSync(path) ? createHash("sha256").update(readFileSync(path)).digest("hex") : undefined;
};

test("each ledger_sha is the sha256 of the released original file bytes", () => {
  for (const a of MIGRATION_RENAME_ALIASES) {
    const bytes = readFileSync(join(fixtures, a.ledger_name.slice("upstream:".length)));
    expect(createHash("sha256").update(bytes).digest("hex")).toBe(a.ledger_sha);
    // The migrator hashes the utf8 text; for these files it is the same digest.
    expect(createHash("sha256").update(bytes.toString("utf8")).digest("hex")).toBe(a.ledger_sha);
  }
});

test("the released ledger records both original names with exactly those shas, and no release name", () => {
  expect(new Set(ledger.map((r) => r.name)).size).toBe(ledger.length);
  for (const a of MIGRATION_RENAME_ALIASES) {
    expect(ledger.filter((r) => r.name === a.ledger_name)).toEqual([{ name: a.ledger_name, sha256: a.ledger_sha }]);
    expect(ledger.some((r) => r.name === a.migration)).toBe(false);
  }
});

test("the released ledger adopts both rows against this tree's files, in memory only", () => {
  const physical = new Map(ledger.map((r) => [r.name, r.sha256]));
  const { effective: map, adopted, refusals } = resolveRenamedLedger(physical, fileSha);
  expect(adopted).toEqual([...MIGRATION_RENAME_ALIASES]);
  expect(refusals).toEqual([]);
  expect(physical.size).toBe(ledger.length);
  for (const a of MIGRATION_RENAME_ALIASES) {
    expect(map.has(a.ledger_name)).toBe(false);
    expect(map.get(a.migration)).toBe(a.accepted_as_alias_of);
  }
  expect(map.size).toBe(ledger.length);
});
