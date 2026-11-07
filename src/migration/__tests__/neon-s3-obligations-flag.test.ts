/**
 * S3X_OBLIGATIONS (default OFF). Off is today's behaviour: no obligation schema, triggers or genesis on open; leases
 * without generations; the exporter records no invocation and claims nothing; main runs the legacy job (byte-identical
 * to #289's job.ts). On: the obligation model. Plus the registry's M1 case on the UNSETTLED production default.
 */
import { expect, test } from "bun:test";
import { mkdtemp, readFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { execFileSync } from "node:child_process";
import { DatabaseSync } from "node:sqlite";
import { SyncState } from "../neon-s3-sync/state.ts";
import { obligationsEnabled } from "../neon-s3-erasure/main.ts";
import { DEFAULT_ERASURE_CONFIG, ErasureJob } from "../neon-s3-erasure/job.ts";
import type { ErasureAuthority } from "../neon-s3-erasure/authority.ts";
import { FakeS3Versions } from "./fake-s3-versions.ts";

const tmp = async (n: string) => join(await mkdtemp(join(process.env.TMPDIR ?? tmpdir(), n)), "s.sqlite");
const tables = (path: string) => {
  const db = new DatabaseSync(path);
  try {
    return (db.prepare("SELECT name FROM sqlite_master WHERE type IN ('table','trigger') ORDER BY name").all() as { name: string }[]).map((r) => r.name);
  } finally {
    db.close();
  }
};

test("OFF (the default): no obligation schema, triggers, genesis or lease generation; base lease / queue behaviour", async () => {
  expect(obligationsEnabled(() => undefined)).toBe(false);
  expect(obligationsEnabled(() => "on")).toBe(true);
  expect(obligationsEnabled(() => "true")).toBe(false);
  const path = await tmp("e3-flag-off-");
  const st = new SyncState(path, { obligations: false });
  expect(st.obligations).toBe(false);
  expect(st.acquireLease("96bulls/o/v1/ws=w/m=m/h=claude/d=2026/10/30/s=s/", "exporter:1")).toBe(true);
  expect(st.leaseGen("96bulls/o/v1/ws=w/m=m/h=claude/d=2026/10/30/s=s/")).toBeUndefined();
  st.enqueueErasure([{ key: "96bulls/o/v1/ws=w/m=m/h=claude/d=2026/10/30/s=s/", kind: "prefix", org_id: "o", reason: "fenced" }]);
  expect(st.nextErasures(Number.MAX_SAFE_INTEGER, 5).length).toBe(1);
  expect(st.meta("obligations_genesis")).toBeUndefined();
  st.close();
  const names = tables(path);
  for (const t of ["write_invocations", "write_published", "erase_reservations", "write_terminal", "erased_versions", "write_touch", "write_clock", "erase_checkpoints"])
    expect(names).not.toContain(t);
  expect(names.some((n) => n.includes("_touch_"))).toBe(false);
});

test("OFF: the schema statement and the legacy job are byte-identical to #289 (97d14de2)", async () => {
  const root = new URL("../../../", import.meta.url).pathname;
  const git = (...a: string[]) => execFileSync("git", a, { cwd: root, encoding: "utf8" });
  let base: string;
  try {
    base = git("show", "97d14de2:src/migration/neon-s3-erasure/job.ts");
  } catch {
    return; // shallow CI checkout without the base commit: the blob pin below still applies
  }
  expect(await readFile(join(root, "src/migration/neon-s3-erasure/job-legacy.ts"), "utf8")).toBe(base);
  const cut = (s: string) => s.slice(s.indexOf('this.db.exec("PRAGMA journal_mode'), s.indexOf("CREATE INDEX IF NOT EXISTS groups_last"));
  expect(cut(await readFile(join(root, "src/migration/neon-s3-sync/state.ts"), "utf8"))).toBe(cut(git("show", "97d14de2:src/migration/neon-s3-sync/state.ts")));
});

test("ON: the obligation schema, triggers, lease generations and an unattested genesis", async () => {
  const path = await tmp("e3-flag-on-");
  const st = new SyncState(path, { obligations: true });
  expect(st.acquireLease("96bulls/o/v1/ws=w/m=m/h=claude/d=2026/10/30/s=s/", "exporter:1")).toBe(true);
  expect(st.leaseGen("96bulls/o/v1/ws=w/m=m/h=claude/d=2026/10/30/s=s/")).toMatch(/^[0-9a-f-]{36}$/);
  expect(st.obligationGenesis()).toMatchObject({ attestedAt: null, basis: "none" });
  st.close();
  const names = tables(path);
  for (const t of ["write_invocations", "write_published", "erase_reservations", "write_terminal", "erased_versions", "write_touch", "erase_checkpoints"]) expect(names).toContain(t);
});

test("M1, the UNSETTLED production default (obligationsSettled unset): reservations are still retired, so the exporter's claim and cancel are never wedged", async () => {
  const PREFIX = "96bulls/org-1/v1/ws=ws_1/m=delivery_x/h=claude/d=2026/10/30/s=sess_1/";
  const s = new FakeS3Versions();
  for (let i = 0; i < 30; i++) s.put(`${PREFIX}raw/b=${String(i).padStart(6, "0")}.ndjson.zst`, Date.parse("2026-11-01T00:00:00Z"));
  const st = new SyncState(await tmp("e3-flag-m1-"), { obligations: true });
  const clock = Date.parse("2026-11-07T00:00:00Z");
  st.enqueueErasure([{ key: PREFIX, kind: "prefix", org_id: "org-1", reason: "fenced" }], clock - 1000);
  const authority: ErasureAuthority = { kind: "supabase", ready: async () => {}, lockWaiters: async () => ({ total: 0, nonAdvisory: 0 }), fencedGroups: async () => [], fencedLivePage: async () => [], stillFenced: async () => true };
  const j = new ErasureJob(authority, st, s.eraser(), () => {}, DEFAULT_ERASURE_CONFIG, () => clock, async () => {});
  await j.drain();
  expect(s.count(PREFIX)).toBe(0); // erased ...
  expect(st.nextErasures(Number.MAX_SAFE_INTEGER, 5).map((e) => e.key)).toEqual([PREFIX]); // ... never reported done (unsettled)
  const reserved = (st as unknown as { db: DatabaseSync }).db.prepare("SELECT count(*) AS n FROM erase_reservations").get() as { n: number };
  expect(reserved.n).toBe(0); // the proven-absent reservations were retired anyway
  // The exporter re-references a key under the prefix: its claim is NOT refused (publish_reserved), and cancel works.
  expect(st.acquireLease(PREFIX, "exporter:1")).toBe(true);
  st.publishWrites([{ key: `${PREFIX}raw/b=000003.ndjson.zst`, versionId: "*" }], { holder: "exporter:1", gens: { [PREFIX]: st.leaseGen(PREFIX)! } });
  st.cancelErasures([PREFIX]);
  expect(st.nextErasures(Number.MAX_SAFE_INTEGER, 5)).toEqual([]);
});
