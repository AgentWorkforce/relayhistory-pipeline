import { expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { ORG_ADMISSION_HOLD_CLASSES, ORG_ADMISSION_HOLD_OWNER_CHECKED } from "../org-admission-hold-classes.ts";

const ACL = JSON.parse(readFileSync("src/hoopdb/sessions-function-acl.json", "utf8")).functions as Record<string, string[]>;
const RUNTIME = ["bulls96_session_ingest", "bulls96_session_ingest_seeder"];
const MIGRATION = readFileSync("supabase/migrations/20261105062320_ingest_org_admission_fences.sql", "utf8");
const name = (sig: string) => sig.slice(0, sig.indexOf("("));

/** The class each replaced body in the migration checks, from its marked line. */
function checkedBodies(): Map<string, string> {
  const out = new Map<string, string>();
  for (const chunk of MIGRATION.split(/^(?=CREATE OR REPLACE FUNCTION )/mu).slice(1)) {
    const fn = /^CREATE OR REPLACE FUNCTION (sessions\.[a-z_0-9]+)\(/u.exec(chunk)![1]!;
    const marks = [...chunk.matchAll(/'([a-z]+)'\);(?: END IF;)? -- 062320 org admission hold$/gmu)];
    expect(marks.length).toBe(1);
    out.set(fn, marks[0]![1]!);
  }
  return out;
}

test("every runtime-granted function is classified, and nothing else", () => {
  const granted = Object.entries(ACL).filter(([, g]) => g.some((x) => RUNTIME.includes(x.split(":")[0]!))).map(([s]) => s).sort();
  expect(Object.keys(ORG_ADMISSION_HOLD_CLASSES).sort()).toEqual(granted);
  expect(granted.length).toBeGreaterThan(80);
});

test("every checked class carries exactly its check in the migration, once", () => {
  const bodies = checkedBodies();
  const want = new Map<string, string>();
  for (const [sig, c] of Object.entries(ORG_ADMISSION_HOLD_CLASSES)) {
    if (c.class === "unaffected") continue;
    want.set(c.via ?? name(sig), c.class);
  }
  for (const [fn, c] of Object.entries(ORG_ADMISSION_HOLD_OWNER_CHECKED)) want.set(fn, c);
  expect(Object.fromEntries([...bodies].sort())).toEqual(Object.fromEntries([...want].sort()));
  expect((MIGRATION.match(/-- 062320 org admission hold\n/gu) ?? []).length).toBe(bodies.size);
});

test("a delegating entry's checked body is itself classified or owner-checked", () => {
  for (const c of Object.values(ORG_ADMISSION_HOLD_CLASSES)) {
    if (!c.via) continue;
    const direct = Object.keys(ORG_ADMISSION_HOLD_CLASSES).some((s) => name(s) === c.via);
    expect(direct || c.via in ORG_ADMISSION_HOLD_OWNER_CHECKED).toBe(true);
  }
});
