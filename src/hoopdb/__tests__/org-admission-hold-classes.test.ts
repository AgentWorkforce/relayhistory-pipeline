import { expect, test } from "bun:test";
import { readFileSync } from "node:fs";
import { ORG_ADMISSION_HOLD_CLASSES, ORG_ADMISSION_HOLD_OWNER_CHECKED } from "../org-admission-hold-classes.ts";
import { HOLD_HISTORY_SHAPE } from "../../../scripts/sessions-function-boundary/lanes/ingest-org-admission-hold.ts";

const ACL = JSON.parse(readFileSync("src/hoopdb/sessions-function-acl.json", "utf8")).functions as Record<string, string[]>;
// Every role that can write admission content: the ingest roles and the continuous-publication roles (no grants today;
// a future grant to any of them must be classified here, layered-sessions-pipeline 251996174404665344).
const RUNTIME = ["bulls96_session_ingest", "bulls96_session_ingest_seeder", "bulls96_publication_scoped", "bulls96_publication_recovery"];
const MIGRATION = readFileSync("supabase/migrations/20261105062320_ingest_org_admission_fences.sql", "utf8");
// Later slots that add the same marked check to further entries (each one function, one mark).
const LATER: Readonly<Record<string, string>> = {
  "062390": readFileSync("supabase/migrations/20261105062390_ingest_seed_capture_admission.sql", "utf8"),
};
const name = (sig: string) => sig.slice(0, sig.indexOf("("));

/** The class each replaced body in the migration checks, from its marked line. */
function checkedBodies(): Map<string, string> {
  const out = new Map<string, string>();
  for (const chunk of MIGRATION.split(/^(?=CREATE OR REPLACE FUNCTION )/mu).slice(1)) {
    const fn = /^CREATE OR REPLACE FUNCTION (sessions\.[a-z_0-9]+)\(/u.exec(chunk)![1]!;
    if (fn.startsWith("sessions.ingest_org_admission_hold_")) continue; // the slot's own functions, not replaced bodies
    const marks = [...chunk.matchAll(/'([a-z]+)'\);(?: END IF;)? -- 062320 org admission hold$/gmu)];
    expect(marks.length).toBe(1);
    out.set(fn, marks[0]![1]!);
  }
  for (const [slot, sql] of Object.entries(LATER)) {
    for (const chunk of sql.split(/^(?=CREATE OR REPLACE FUNCTION )/mu).slice(1)) {
      const fn = /^CREATE OR REPLACE FUNCTION (sessions\.[a-z_0-9]+)\(/u.exec(chunk)![1]!;
      const marks = [...chunk.matchAll(new RegExp(`'([a-z]+)'\\);(?: END IF;)? -- ${slot} org admission hold$`, "gmu"))];
      expect(marks.length).toBe(1);
      expect(out.has(fn)).toBe(false); // a function is checked by exactly one slot
      out.set(fn, marks[0]![1]!);
    }
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
  const later = Object.entries(LATER).reduce((n, [slot, sql]) => n + (sql.match(new RegExp(`-- ${slot} org admission hold\\n`, "gu")) ?? []).length, 0);
  expect((MIGRATION.match(/-- 062320 org admission hold\n/gu) ?? []).length + later).toBe(bodies.size);
});

test("a delegating entry's checked body is itself classified or owner-checked", () => {
  for (const c of Object.values(ORG_ADMISSION_HOLD_CLASSES)) {
    if (!c.via) continue;
    const direct = Object.keys(ORG_ADMISSION_HOLD_CLASSES).some((s) => name(s) === c.via);
    expect(direct || c.via in ORG_ADMISSION_HOLD_OWNER_CHECKED).toBe(true);
  }
});

test("the retained-history shape is one predicate, verbatim in the up and in the generated down", () => {
  const down = readFileSync("supabase/rollbacks/20261105062320_ingest_org_admission_fences.down.sql", "utf8");
  expect(MIGRATION).toContain(HOLD_HISTORY_SHAPE);
  expect(down).toContain(HOLD_HISTORY_SHAPE);
  expect(HOLD_HISTORY_SHAPE).toContain("FOREIGN KEY (epoch) REFERENCES sessions.ingest_org_admission_holds(epoch)");
});
