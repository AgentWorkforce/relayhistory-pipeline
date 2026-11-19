/**
 * The trace import's scrubbing and guard rails (docs/session-intel-import.md, "Privacy rules"): path hashing
 * from a closed clear set, home directories in every spelling, tenant hashing, outlier timestamps, and every
 * input bound. Synthetic data only.
 */
import { describe, expect, test } from "bun:test";
import { spawnSync } from "node:child_process";
import { mkdirSync, readFileSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { readCodex, readGeneric, SessionSet } from "../import/adapters.ts";
import { buildRows, keepWindow, main, readSaltFile } from "../import/cli.ts";
import { listInputFiles, readJsonl } from "../import/io.ts";
import { emptyStats, Hasher, IMPORT_BOUNDS, isHomeRoot, repoRelativePath, scrubPath, SessionBuilder } from "../import/normalize.ts";
import { loadSnapshot } from "../replay/snapshot.ts";
import { tmp } from "./helpers.ts";

const SALT = "fixture-salt-0123456789";
const h = new Hasher(SALT);
const seg = (s: string) => h.segment(s);
const NOW = new Date("2026-11-18T00:00:00.000Z");
const DELETE_BY = "2026-12-01T00:00:00.000Z";
const quiet = () => { const lines: string[] = []; return { lines, io: { out: (l: string) => lines.push(l), err: (l: string) => lines.push(l), now: NOW } }; };
function saltFile(): string {
  const p = join(tmp("si-imp-salt-"), "salt");
  writeFileSync(p, SALT, { mode: 0o600 });
  return p;
}
const jsonl = (rows: unknown[]) => `${rows.map((r) => JSON.stringify(r)).join("\n")}\n`;
async function importGeneric(rows: unknown[]) {
  const dir = tmp("si-imp-gen-");
  writeFileSync(join(dir, "events.jsonl"), jsonl(rows));
  const out = join(tmp("si-imp-"), "snap");
  const q = quiet();
  expect(await main(["--format", "generic", "--in", dir, "--out", out, "--salt-file", saltFile(), "--delete-by", DELETE_BY], q.io)).toBe(0);
  return { out, lines: q.lines, text: readFileSync(join(out, "sessions.jsonl"), "utf8") };
}

describe("F1: path segments are hashed; only a closed set stays in clear", () => {
  test.each([
    ["src/billing/invoice.ts", () => `src/${seg("billing")}/${seg("invoice")}.ts`],
    ["supabase/migrations/0042_add_index.sql", () => `supabase/migrations/0042_${seg("add_index")}.sql`],
    ["supabase/migrations/20261105062150_add_events.sql", () => `supabase/migrations/20261105062150_${seg("add_events")}.sql`],
    ["db/migrate/0042-create-users.rb", () => `db/migrate/0042-${seg("create-users")}.rb`],
    ["supabase/migrations/0042.sql", () => "supabase/migrations/0042.sql"],
    // A non-digit "prefix" is hashed with the rest of the name.
    ["supabase/migrations/v0042_add.sql", () => `supabase/migrations/${seg("v0042_add")}.sql`],
    ["supabase/migrations/12_short.sql", () => `supabase/migrations/${seg("12_short")}.sql`],
    // The number prefix is kept only inside a migration directory.
    ["src/0042_add_index.sql", () => `src/${seg("0042_add_index")}.sql`],
    // A sentence after the last dot is not an extension: the whole name is hashed.
    ["docs/notes.this is a secret sentence", () => `docs/${seg("notes.this is a secret sentence")}`],
    ["docs/notes.abcdefghijk", () => `docs/${seg("notes.abcdefghijk")}`],
    ["docs/notes.tar-gz", () => `docs/${seg("notes.tar-gz")}`],
    ["docs/notes.ABCDEFGHIJ", () => `docs/${seg("notes")}.abcdefghij`],
    ["src/AKIAFIXTUREKEYSHAPE0/config.json", () => `src/${seg("akiafixturekeyshape0")}/${seg("config")}.json`],
    ["src/my private plan for acquisition.md", () => `src/${seg("my private plan for acquisition")}.md`],
    ["node_modules/@scope/pkg/index.js", () => `${seg("node_modules")}/${seg("@scope")}/${seg("pkg")}/${seg("index")}.js`],
    ["bun.lock", () => "bun.lock"],
    ["packages/x/package.json", () => `packages/${seg("x")}/package.json`],
    [".github/workflows/ci.yml", () => `${seg(".github")}/${seg("workflows")}/${seg("ci")}.yml`],
    ["Makefile", () => seg("makefile")],
  ])("%s", (path, want) => {
    expect(scrubPath(path, h)).toBe(want());
  });

  test("equal paths still match (case-insensitively, as the detectors compare them); another salt hashes differently", () => {
    expect(scrubPath("src/Billing/Invoice.ts", h)).toBe(scrubPath("src/billing/invoice.ts", h));
    expect(scrubPath("src/billing/invoice.ts", new Hasher("another-salt-0123456789"))).not.toBe(scrubPath("src/billing/invoice.ts", h));
    expect(scrubPath("src/a/b/c/d.ts", h).split("/")).toHaveLength(5);
  });

  test("a URL, with or without credentials, is not a path; whitespace and `@` names are hashed, never kept", () => {
    expect(repoRelativePath("https://user:hunter2@example.invalid/x.ts", null)).toBeNull();
    expect(repoRelativePath("file:///etc/passwd", null)).toBeNull();
    for (const raw of ["src/a name with spaces.ts", "src/user@example.invalid.ts"]) {
      const p = repoRelativePath(raw, null)!;
      expect(scrubPath(p, h)).not.toMatch(/\s|@|name|example/u);
    }
  });

  test("a Codex patch header holding a sentence leaves no clear text in the snapshot", async () => {
    const dir = tmp("si-imp-codex-sentence-");
    writeFileSync(join(dir, "rollout-2026-11-01T09-00-00-00000000-0000-4000-8000-0000000005e1.jsonl"), jsonl([
      { timestamp: "2026-11-01T09:00:00.000Z", type: "session_meta", payload: { id: "fx-sentence", cwd: "/work/acme" } },
      { timestamp: "2026-11-01T09:01:00.000Z", type: "response_item", payload: { type: "custom_tool_call", name: "apply_patch", call_id: "c", input: "*** Begin Patch\n*** Add File: docs/the merger with fixture corp closes friday.md\n+x\n*** End Patch\n" } },
    ]));
    const out = join(tmp("si-imp-"), "snap");
    expect(await main(["--format", "codex", "--in", dir, "--out", out, "--salt-file", saltFile(), "--delete-by", DELETE_BY], quiet().io)).toBe(0);
    const text = readFileSync(join(out, "sessions.jsonl"), "utf8");
    for (const word of ["merger", "fixture corp", "friday"]) expect(text).not.toContain(word);
    const [s] = (await loadSnapshot(out, { now: NOW })).sessions;
    expect(s!.writes.map((w) => w.path)).toEqual([`docs/${seg("the merger with fixture corp closes friday")}.md`]);
    const mapping = JSON.parse(readFileSync(`${out}.mapping.json`, "utf8"));
    expect(mapping.sessions[0].paths).toEqual({ [s!.writes[0]!.path]: "docs/the merger with fixture corp closes friday.md" });
  });
});

describe("F2 and F3: no home directory or user name survives, in any spelling", () => {
  test.each([
    "/Users/fixture-user", "/home/fixture-user", "/var/home/fixture-user", "/export/home/fixture-user",
    "/mnt/c/Users/fixture-user", "C:\\Users\\fixture-user", "c:/users/fixture-user/", "/root", "/root/",
  ])("%s is a home root", (dir) => {
    expect(isHomeRoot(dir)).toBe(true);
  });
  test.each(["/Users/fixture-user/work/acme", "/home", "/srv/acme", "/rooted", null])("%s is not a home root", (dir) => {
    expect(isHomeRoot(dir)).toBe(false);
  });

  test.each([
    // A cwd at a home root: home-relative paths are dropped unless anchored at a source root.
    ["Documents/plan.txt", "/Users/fixture-user", null],
    [".ssh/config", "/home/fixture-user", null],
    ["/Users/fixture-user/Desktop/notes.txt", "/Users/fixture-user", null],
    ["/Users/fixture-user/.config/tool/settings.json", "/Users/fixture-user", null],
    ["/Users/fixture-user/acme/src/a.ts", "/Users/fixture-user", "src/a.ts"],
    ["acme/src/a.ts", "/Users/fixture-user", "src/a.ts"],
    [".bashrc", "/root", null],
    // Each home spelling: anchored, or dropped when the user name is still a segment.
    ["/var/home/fixture-user/work/acme/src/a.ts", "/var/home/fixture-user/work/acme", "src/a.ts"],
    ["/var/home/fixture-user/x/src/fixture-user/a.ts", null, null],
    ["/export/home/fixture-user/repo/lib/a.ts", null, "lib/a.ts"],
    ["/export/home/fixture-user/repo/lib/fixture-user/a.ts", null, null],
    ["/mnt/c/Users/fixture-user/repo/src/a.ts", null, "src/a.ts"],
    ["/mnt/c/Users/fixture-user/repo/src/fixture-user/a.ts", null, null],
    ["C:\\Users\\fixture-user\\repo\\src\\fixture-user\\a.ts", null, null],
    ["C:\\Users\\fixture-user\\repo\\src\\a.ts", "C:\\Users\\fixture-user\\repo", "src/a.ts"],
    ["/root/.ssh/id_rsa", null, null],
    ["/root/proj/src/a.ts", null, "src/a.ts"],
    ["fixture-user/src/a.ts", "/home/fixture-user/repo", null],
  ])("%s (cwd %s)", (raw, cwd, want) => {
    expect(repoRelativePath(raw, cwd)).toBe(want);
  });
});

describe("F4: tenant ids", () => {
  test("org and workspace are hashed with the salt, domain-separated, and differ per salt", () => {
    expect(h.org("acme")).toMatch(/^o_[0-9a-f]{24}$/u);
    expect(h.workspace("acme")).toMatch(/^w_[0-9a-f]{24}$/u);
    expect(h.org("acme").slice(2)).not.toBe(h.workspace("acme").slice(2));
    expect(new Hasher("another-salt-0123456789").org("acme")).not.toBe(h.org("acme"));
  });
});

describe("adapter notes", () => {
  test("a Codex session naming itself as its parent has no parent", async () => {
    const dir = tmp("si-imp-codex-self-");
    writeFileSync(join(dir, "rollout.jsonl"), jsonl([{ timestamp: "2026-11-01T09:00:00.000Z", type: "session_meta",
      payload: { id: "fx-self", cwd: "/work/acme", source: { subagent: { thread_spawn: { parent_thread_id: "fx-self" } } } } }]));
    const set = new SessionSet(emptyStats());
    await readCodex(join(dir, "rollout.jsonl"), 0, set);
    expect([...set.byKey.get("codex\u0000fx-self")!.parents]).toEqual([]);
  });

  test("the generic kind label is never copied, even when it is not `write`", async () => {
    const { text } = await importGeneric([{ sessionId: "k", ts: "2026-11-01T09:00:00Z", kind: "fixture_secret_label", files: ["src/a.ts"] }]);
    expect(text).not.toContain("fixture_secret_label");
    expect(JSON.parse(text.trim()).writes[0].kind).toBe("write");
  });
});

describe("outlier timestamps: dropped and counted, never an abort", () => {
  const ev = (sessionId: string, ts: string, files = ["src/a.ts"]) => ({ sessionId, ts, kind: "write", files, projectId: "p", repo: "r" });
  test("a session far outside the window, a stray early record, and a future record", async () => {
    const { out, lines } = await importGeneric([
      ev("a", "2026-11-01T09:00:00Z"), ev("a", "2026-11-01T10:00:00Z"),
      ev("b", "2026-11-02T09:00:00Z"),
      // One stray record in b, eight months earlier: b keeps its later facts.
      ev("b", "2026-03-01T09:00:00Z", ["src/stray.ts"]),
      // A whole session long before: dropped.
      ev("old", "2025-01-01T09:00:00Z"),
      // A record after the clock: dropped at read.
      ev("a", "2027-01-01T09:00:00Z", ["src/future.ts"]),
    ]);
    const snap = await loadSnapshot(out, { now: NOW });
    expect(snap.sessions).toHaveLength(2);
    expect(snap.sessions.flatMap((s) => s.writes.map((w) => w.ts)).sort()).toEqual(["2026-11-01T09:00:00.000Z", "2026-11-01T10:00:00.000Z", "2026-11-02T09:00:00.000Z"]);
    expect(Date.parse(snap.manifest.window.until) - Date.parse(snap.manifest.window.since)).toBeLessThanOrEqual(IMPORT_BOUNDS.windowMs);
    const stats = JSON.parse(lines.find((l) => l.startsWith("import: {"))!.slice("import: ".length));
    expect(stats).toMatchObject({ outOfWindowSessions: 1, outOfWindowFacts: 1, futureTimestamps: 1 });
    expect(lines.some((l) => l.includes("warning: kept a 120-day window"))).toBe(true);
  });

  test("keepWindow keeps the 120 days in which the most sessions end, and leaves a short import alone", () => {
    const stats = emptyStats();
    const mk = (id: string, first: string, last: string) => {
      const b = new SessionBuilder("generic", id, stats);
      b.touch(first, { file: 0, line: 1 }); b.touch(last, { file: 0, line: 2 });
      return b;
    };
    const short = [mk("a", "2026-11-01T00:00:00Z", "2026-11-02T00:00:00Z"), mk("b", "2026-08-01T00:00:00Z", "2026-08-02T00:00:00Z")];
    expect(keepWindow(short, stats)).toHaveLength(2);
    const late = [mk("x", "2026-01-01T00:00:00Z", "2026-01-02T00:00:00Z"), mk("y", "2026-01-03T00:00:00Z", "2026-01-04T00:00:00Z"), mk("z", "2026-11-01T00:00:00Z", "2026-11-02T00:00:00Z")];
    expect(keepWindow(late, stats).map((b) => b.sourceId)).toEqual(["x", "y"]);
    expect(stats.outOfWindowSessions).toBe(1);
  });
});

describe("collision abort", () => {
  test("two source sessions that hash to one id stop the import", () => {
    const stats = emptyStats();
    const same = Object.assign(new Hasher(SALT), { session: () => "s_collide" });
    const bs = ["a", "b"].map((id) => { const b = new SessionBuilder("generic", id, stats); b.touch("2026-11-01T09:00:00.000Z", { file: 0, line: 1 }); return b; });
    expect(() => buildRows(bs, same, { orgId: "o", workspaceId: "w" }, stats)).toThrow("hash to the same id");
  });
});

describe("input bounds", () => {
  const LIMITS = { walkDepth: 3, inputFiles: 2, inputFileBytes: 64, inputLineBytes: 16 };
  test("walk depth and file count", async () => {
    const deep = tmp("si-imp-deep-");
    let d = deep;
    for (let i = 0; i < 5; i++) { d = join(d, `d${i}`); mkdirSync(d); }
    writeFileSync(join(d, "x.jsonl"), "{}\n");
    await expect(listInputFiles(deep, LIMITS)).rejects.toThrow("nested deeper than 3");
    const many = tmp("si-imp-many-");
    for (const n of ["a", "b", "c"]) writeFileSync(join(many, `${n}.jsonl`), "{}\n");
    await expect(listInputFiles(many, LIMITS)).rejects.toThrow("more than 2 .jsonl files");
    expect(await listInputFiles(many, { ...LIMITS, inputFiles: 3 })).toHaveLength(3);
  });

  test("per-file and per-line limits", async () => {
    const dir = tmp("si-imp-size-");
    writeFileSync(join(dir, "big.jsonl"), `${"{}\n".repeat(30)}`);
    await expect(readJsonl(join(dir, "big.jsonl"), emptyStats(), () => {}, LIMITS)).rejects.toThrow("exceeds 64 bytes");
    writeFileSync(join(dir, "lines.jsonl"), `{"a":1}\n{"b":"${"x".repeat(20)}"}\n{"c":3}\n`);
    const stats = emptyStats();
    const got: unknown[] = [];
    await readJsonl(join(dir, "lines.jsonl"), stats, (v) => got.push(v), LIMITS);
    expect(got).toEqual([{ a: 1 }, { c: 3 }]);
    expect(stats.oversizeLines).toBe(1);
  });

  test("a FIFO is skipped by the walk and refused at once (never blocks) as a trace or a salt file", async () => {
    const dir = tmp("si-imp-fifo-");
    const fifo = join(dir, "pipe.jsonl");
    expect(spawnSync("mkfifo", [fifo]).status).toBe(0);
    expect(await listInputFiles(dir)).toEqual([]);
    await expect(readJsonl(fifo, emptyStats(), () => {})).rejects.toThrow("not a regular file");
    await expect(readSaltFile(fifo)).rejects.toThrow("regular file");
  });

  test("the generic adapter skips a session beyond the clock without touching the others", async () => {
    const set = new SessionSet(emptyStats(), "2026-11-19T00:00:00.000Z");
    const dir = tmp("si-imp-future-");
    writeFileSync(join(dir, "e.jsonl"), jsonl([{ sessionId: "f", ts: "2030-01-01T00:00:00Z", kind: "write", files: ["src/a.ts"] }, { sessionId: "g", ts: "2026-11-01T00:00:00Z", kind: "write" }]));
    await readGeneric(join(dir, "e.jsonl"), 0, set);
    expect([...set.byKey.keys()]).toEqual(["generic\u0000g"]);
    expect(set.stats.futureTimestamps).toBe(1);
  });
});
