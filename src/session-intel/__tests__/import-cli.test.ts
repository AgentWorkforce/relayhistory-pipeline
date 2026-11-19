/**
 * The trace import CLI end to end (docs/session-intel-import.md): privacy of the output, the deleteBy and
 * output-directory refusals, and import followed by the unchanged replay CLI. Synthetic fixtures only.
 */
import { describe, expect, test } from "bun:test";
import { chmodSync, existsSync, mkdirSync, readdirSync, readFileSync, statSync, symlinkSync, writeFileSync } from "node:fs";
import { join, resolve, sep } from "node:path";
import { Readable } from "node:stream";
import { main, readSaltFile, readSaltStdin } from "../import/cli.ts";
import { isInside } from "../replay/cli.ts";
import { Hasher } from "../import/normalize.ts";
import { mappingPath } from "../import/writer.ts";
import { main as replayMain } from "../replay/cli.ts";
import { replaySnapshot } from "../replay/replay.ts";
import { advisorDetector } from "../replay/detector.ts";
import { loadSnapshot } from "../replay/snapshot.ts";
import { tmp, writeSnapshot } from "./helpers.ts";

const FIX = join(import.meta.dir, "fixtures", "import");
const REPO = resolve(import.meta.dir, "..", "..", "..");
const NOW = new Date("2026-11-18T00:00:00.000Z");
const SALT = "fixture-salt-0123456789";
/** A salt file outside the repository, owner-only unless `mode` says otherwise. */
function saltFile(content: string, mode = 0o600): string {
  const path = join(tmp("si-imp-salt-"), "salt");
  writeFileSync(path, content, { mode });
  chmodSync(path, mode);
  return path;
}
const SALT_FILE = saltFile(`${SALT}\n`);
const DELETE_BY = "2026-12-01T00:00:00.000Z";
const quiet = () => { const lines: string[] = []; return { lines, io: { out: (l: string) => lines.push(l), err: (l: string) => lines.push(l), now: NOW } }; };
const args = (format: string, dir: string, out: string, extra: string[] = []) =>
  ["--format", format, "--in", join(FIX, dir), "--out", out, "--salt-file", SALT_FILE, "--delete-by", DELETE_BY, ...extra];

async function runImport(format: string, dir: string, extra: string[] = []) {
  const out = join(tmp("si-imp-"), "snap");
  const q = quiet();
  expect(await main(args(format, dir, out, extra), q.io)).toBe(0);
  return { out, stdout: q.lines.join("\n") };
}
const allText = (dir: string) => readdirSync(dir).map((f) => readFileSync(join(dir, f), "utf8")).join("\n");
const fixtureText = (dir: string): string[] => readdirSync(join(FIX, dir), { recursive: true, withFileTypes: true })
  .filter((e) => e.isFile()).map((e) => readFileSync(join(e.parentPath, e.name), "utf8"));

describe("import cli", () => {
  test.each([["claude-code", "claude", 2], ["codex", "codex", 1], ["generic", "generic", 2]] as const)("%s: the M0 loader accepts the output", async (format, dir, n) => {
    const { out } = await runImport(format, dir, ["--org", "org_fx", "--workspace", "ws_fx"]);
    const snap = await loadSnapshot(out, { now: NOW });
    expect(snap.sessions).toHaveLength(n);
    expect(snap.manifest.deleteBy).toBe(DELETE_BY);
    expect(snap.manifest.createdAt).toBe(NOW.toISOString());
    for (const s of snap.sessions) {
      expect(s).toMatchObject({ orgId: "org_fx", workspaceId: "ws_fx", summaryTimeline: [], claims: [], title: null, userId: null, taskRef: null });
      expect(s.sessionId).toMatch(/^s_[0-9a-f]{32}$/u);
      expect(snap.manifest.window.since <= s.createdAt && s.lastActivityAt < snap.manifest.window.until).toBe(true);
    }
    expect(readdirSync(out).sort()).toEqual(["manifest.json", "sessions.jsonl"]);
    expect(statSync(join(out, "sessions.jsonl")).mode & 0o077).toBe(0);
  });

  test("no message text, raw id, home path, user name, project name or salt reaches the snapshot or stdout", async () => {
    for (const [format, dir] of [["claude-code", "claude"], ["codex", "codex"], ["generic", "generic"], ["claude-code", "e2e"]] as const) {
      const { out, stdout } = await runImport(format, dir);
      const text = `${allText(out)}\n${stdout.replace(/-> \S+/gu, "")}`;
      // Every marker the fixture plants in message, tool input, tool output, summary and reasoning text.
      const secrets = new Set(fixtureText(dir).flatMap((t) => t.match(/FIXTURE_SECRET_[A-Za-z0-9_]+/gu) ?? []));
      expect(secrets.size).toBeGreaterThan(0);
      for (const s of secrets) expect(text).not.toContain(s);
      for (const raw of ["00000000-0000-4000-8000", "gen-1", "fx-claude-main", "fixture-user", "/Users/", "/home/", "acme-api", "fixture-co", "widgets", "fixture-proj", "feat/", SALT, "please add an index"])
        expect(text).not.toContain(raw);
      const mapping = readFileSync(mappingPath(out), "utf8");
      for (const s of secrets) expect(mapping).not.toContain(s);
      expect(mapping).not.toContain(SALT);
    }
  });

  test("the private mapping sits beside the snapshot and maps hashed ids back to the source", async () => {
    const { out } = await runImport("claude-code", "claude");
    const m = JSON.parse(readFileSync(mappingPath(out), "utf8"));
    expect(mappingPath(out).startsWith(`${out}.`)).toBe(true);
    expect(statSync(mappingPath(out)).mode & 0o077).toBe(0);
    const h = new Hasher(SALT);
    const main = "00000000-0000-4000-8000-0000000c1a0d";
    expect(m.sessions.map((s: { sessionId: string; sourceSessionId: string }) => [s.sessionId, s.sourceSessionId]).sort()).toEqual([
      [h.session("claude", main), main], [h.session("claude", `${main}#agent-fxsub1`), `${main}#agent-fxsub1`],
    ].sort());
    expect(m.projects).toEqual({ [h.project("acme-api")]: "acme-api" });
    expect(m.inputFiles).toEqual([join("fx-claude-main", "subagents", "agent-fxsub1.jsonl"), "fx-claude-main.jsonl"]);
  });

  test("idempotent: the same traces and salt give the same sessions.jsonl; another salt gives other ids", async () => {
    const a = await runImport("claude-code", "claude"), b = await runImport("claude-code", "claude");
    expect(readFileSync(join(b.out, "sessions.jsonl"), "utf8")).toBe(readFileSync(join(a.out, "sessions.jsonl"), "utf8"));
    const out = join(tmp("si-imp-"), "snap");
    expect(await main(["--format", "claude-code", "--in", join(FIX, "claude"), "--out", out, "--salt-file", saltFile("another-salt-0123456789"), "--delete-by", DELETE_BY], quiet().io)).toBe(0);
    expect(readFileSync(join(out, "sessions.jsonl"), "utf8")).not.toBe(readFileSync(join(a.out, "sessions.jsonl"), "utf8"));
  });

  test("deleteBy: required, ISO with a zone, in the future, at most 30 days out", async () => {
    const run = (deleteBy: string | null) => main(["--format", "generic", "--in", join(FIX, "generic"), "--out", join(tmp("si-imp-"), "snap"), "--salt-file", SALT_FILE,
      ...(deleteBy === null ? [] : ["--delete-by", deleteBy])], quiet().io);
    await expect(run(null)).rejects.toThrow("--delete-by is required");
    await expect(run("2026-12-01")).rejects.toThrow("ISO-8601");
    await expect(run("2026-11-17T00:00:00Z")).rejects.toThrow("in the future");
    await expect(run("2026-12-18T00:00:00.001Z")).rejects.toThrow("at most 30 days");
    expect(await run("2026-12-18T00:00:00Z")).toBe(0);
  });

  test("refuses a missing or short salt, a bad format, bad tenant ids and unknown flags", async () => {
    const out = join(tmp("si-imp-"), "snap");
    const base = ["--in", join(FIX, "generic"), "--out", out, "--delete-by", DELETE_BY];
    await expect(main(["--format", "generic", ...base], quiet().io)).rejects.toThrow("exactly one of --salt-stdin");
    // The salt is never taken from argv.
    await expect(main(["--format", "generic", ...base, "--salt", SALT], quiet().io)).rejects.toThrow("Unknown option '--salt'");
    await expect(main(["--format", "otel", ...base, "--salt-file", SALT_FILE], quiet().io)).rejects.toThrow("--format");
    await expect(main(["--format", "generic", ...base, "--salt-file", SALT_FILE, "--org", "../x"], quiet().io)).rejects.toThrow("--org");
    await expect(main(["--format", "generic", ...base, "--salt-file", SALT_FILE, "--token", "x"], quiet().io)).rejects.toThrow();
    expect(existsSync(out)).toBe(false);
  });

  test("refuses --out inside the repository, including through a symlink and a `..`-prefixed directory name", async () => {
    const q = quiet();
    await expect(main(args("generic", "generic", join(FIX, "out")), q.io)).rejects.toThrow("inside the repository");
    // A directory named "..cache" inside the repository is still inside it.
    const dotdot = join(REPO, "..si-import-test-out", "snap");
    await expect(main(args("generic", "generic", dotdot), q.io)).rejects.toThrow("inside the repository");
    expect(existsSync(join(REPO, "..si-import-test-out"))).toBe(false);
    const link = join(tmp("si-imp-link-"), "into-repo");
    symlinkSync(join(REPO, "src", "session-intel", "__tests__", "fixtures"), link);
    await expect(main(args("generic", "generic", join(link, "snap")), q.io)).rejects.toThrow("inside the repository");
    await expect(main(args("generic", "generic", link), q.io)).rejects.toThrow("inside the repository");
    expect(existsSync(join(FIX, "..", "snap"))).toBe(false);
  });

  test("the shared M0 isInside counts only '..', '..' + sep or an absolute relative path as outside", () => {
    const r = tmp("si-imp-root-");
    expect(isInside(r, join(r, "..cache", "x"))).toBe(true);
    expect(isInside(r, r)).toBe(true);
    expect(isInside(r, join(r, "a"))).toBe(true);
    expect(isInside(r, resolve(r, ".."))).toBe(false);
    expect(isInside(r, `${r}x`)).toBe(false);
    expect(isInside(r, `${r}${sep}..${sep}s`)).toBe(false);
  });

  test("refuses a non-empty --out, --out inside --in, and an --in with no traces; never follows symlinks under --in", async () => {
    const root = tmp("si-imp-");
    const busy = join(root, "busy");
    mkdirSync(busy);
    writeFileSync(join(busy, "keep.txt"), "x");
    await expect(main(args("generic", "generic", busy), quiet().io)).rejects.toThrow("not empty");
    expect(readdirSync(busy)).toEqual(["keep.txt"]);
    const input = join(root, "in");
    mkdirSync(input);
    await expect(main(["--format", "generic", "--in", input, "--out", join(input, "snap"), "--salt-file", SALT_FILE, "--delete-by", DELETE_BY], quiet().io)).rejects.toThrow("contain each other");
    // The only trace is a symlink: it is not read, so there is nothing to import.
    symlinkSync(join(FIX, "generic", "events.jsonl"), join(input, "linked.jsonl"));
    await expect(main(["--format", "generic", "--in", input, "--out", join(root, "snap"), "--salt-file", SALT_FILE, "--delete-by", DELETE_BY], quiet().io)).rejects.toThrow("no .jsonl files");
    // An empty existing --out is fine: the snapshot replaces it.
    const empty = join(root, "empty");
    mkdirSync(empty);
    expect(await main(args("generic", "generic", empty), quiet().io)).toBe(0);
    expect(readdirSync(empty).sort()).toEqual(["manifest.json", "sessions.jsonl"]);
    expect(readdirSync(root).filter((f) => f.includes(".import-"))).toEqual([]);
  });
});

describe("salt from stdin", () => {
  const CLI = join(REPO, "src", "session-intel", "import", "cli.ts");
  const genericArgs = (out: string, salt: string[]) => ["--format", "generic", "--in", join(FIX, "generic"), "--out", out, ...salt, "--delete-by", DELETE_BY];
  const piped = (...chunks: (string | Buffer)[]) => Readable.from(chunks);

  test("a piped salt gives the same ids as the same salt from a file", async () => {
    const a = await runImport("generic", "generic");
    const out = join(tmp("si-imp-"), "snap");
    const q = quiet();
    expect(await main(genericArgs(out, ["--salt-stdin"]), { ...q.io, stdin: piped("fixture-salt-", Buffer.from("0123456789\n")) })).toBe(0);
    expect(readFileSync(join(out, "sessions.jsonl"), "utf8")).toBe(readFileSync(join(a.out, "sessions.jsonl"), "utf8"));
    expect(q.lines.join("\n")).not.toContain(SALT);
  });

  test("a real pipe into the CLI process (the `op read ... |` path)", async () => {
    const out = join(tmp("si-imp-"), "snap");
    // --delete-by is relative to the real clock here, not the test's NOW.
    const deleteBy = new Date(Date.now() + 7 * 24 * 60 * 60 * 1_000).toISOString();
    const proc = Bun.spawn([process.execPath, "run", CLI, "--format", "generic", "--in", join(FIX, "generic"), "--out", out, "--salt-stdin", "--delete-by", deleteBy], {
      stdin: new Blob([`${SALT}\n`]), stdout: "pipe", stderr: "pipe", env: { PATH: process.env.PATH ?? "" },
    });
    const [code, stdout, stderr] = await Promise.all([proc.exited, new Response(proc.stdout).text(), new Response(proc.stderr).text()]);
    expect(stderr).toBe("");
    expect(code).toBe(0);
    expect(`${stdout}${stderr}`).not.toContain(SALT);
    const h = new Hasher(SALT);
    expect(readFileSync(join(out, "sessions.jsonl"), "utf8")).toContain(h.session("generic", "gen-1"));
  });

  test("exactly one salt source: both flags, or neither, is refused before any output", async () => {
    const out = join(tmp("si-imp-"), "snap");
    await expect(main(genericArgs(out, ["--salt-stdin", "--salt-file", SALT_FILE]), { ...quiet().io, stdin: piped(SALT) })).rejects.toThrow("exactly one of --salt-stdin");
    await expect(main(genericArgs(out, []), { ...quiet().io, stdin: piped(SALT) })).rejects.toThrow("exactly one of --salt-stdin");
    expect(existsSync(out)).toBe(false);
  });

  test("refuses a terminal, over 4 KiB, and too short or too long once trimmed; never echoes the salt", async () => {
    const tty = Object.assign(piped(SALT), { isTTY: true });
    await expect(readSaltStdin(tty)).rejects.toThrow("not a terminal");
    await expect(main(genericArgs(join(tmp("si-imp-"), "snap"), ["--salt-stdin"]), { ...quiet().io, stdin: tty })).rejects.toThrow("not a terminal");
    await expect(readSaltStdin(piped("x".repeat(4_000), "y".repeat(200)))).rejects.toThrow("exceeds 4096 bytes");
    await expect(readSaltStdin(piped("short-salt\n"))).rejects.toThrow("16 to 1024");
    await expect(readSaltStdin(piped(" \n\t"))).rejects.toThrow("16 to 1024");
    await expect(readSaltStdin(piped("z".repeat(1_025)))).rejects.toThrow("16 to 1024");
    expect(await readSaltStdin(piped(`\n  ${SALT}  \r\n`))).toBe(SALT);
    const secret = "never-echo-stdin-salt-0123";
    const err = await readSaltStdin(piped(secret.repeat(200))).catch((e: Error) => e);
    expect((err as Error).message).not.toContain(secret.slice(0, 10));
  });
});

describe("salt file", () => {
  test("a trimmed 0600 file outside the repository gives the same ids as its content", async () => {
    expect(await readSaltFile(saltFile(`  ${SALT}\n\n`))).toBe(SALT);
    const a = await runImport("generic", "generic");
    const out = join(tmp("si-imp-"), "snap");
    expect(await main(["--format", "generic", "--in", join(FIX, "generic"), "--out", out, "--salt-file", saltFile(`\t${SALT}  \n`), "--delete-by", DELETE_BY], quiet().io)).toBe(0);
    expect(readFileSync(join(out, "sessions.jsonl"), "utf8")).toBe(readFileSync(join(a.out, "sessions.jsonl"), "utf8"));
  });

  test("refuses group- or world-accessible modes, symlinks, non-files, bad lengths and files in the repository", async () => {
    for (const mode of [0o640, 0o604, 0o660, 0o602, 0o644]) await expect(readSaltFile(saltFile(SALT, mode))).rejects.toThrow("group or others");
    const link = join(tmp("si-imp-salt-link-"), "salt");
    symlinkSync(SALT_FILE, link);
    await expect(readSaltFile(link)).rejects.toThrow("must not be a symlink");
    await expect(readSaltFile(tmp("si-imp-salt-dir-"))).rejects.toThrow("regular file");
    await expect(readSaltFile(join(tmp("si-imp-"), "missing"))).rejects.toThrow("could not be opened");
    await expect(readSaltFile(saltFile("short-salt \n"))).rejects.toThrow("16 to 1024");
    await expect(readSaltFile(saltFile("   \n"))).rejects.toThrow("16 to 1024");
    await expect(readSaltFile(saltFile("x".repeat(1_025)))).rejects.toThrow("16 to 1024");
    await expect(readSaltFile(join(FIX, "salt"))).rejects.toThrow("inside the repository");
    expect(existsSync(join(FIX, "salt"))).toBe(false);
  });

  test("the salt never reaches an error, stdout or stderr", async () => {
    const secret = "never-echo-this-salt-0123456789";
    const seen: string[] = [];
    const io = { out: (l: string) => seen.push(l), err: (l: string) => seen.push(l), now: NOW };
    for (const path of [saltFile(secret, 0o644), saltFile(`${secret}${"y".repeat(1_100)}`)]) {
      const err = await main(["--format", "generic", "--in", join(FIX, "generic"), "--out", join(tmp("si-imp-"), "snap"), "--salt-file", path, "--delete-by", DELETE_BY], io).catch((e: Error) => e);
      expect(err).toBeInstanceOf(Error);
      seen.push((err as Error).message);
    }
    const out = join(tmp("si-imp-"), "snap");
    expect(await main(["--format", "generic", "--in", join(FIX, "generic"), "--out", out, "--salt-file", saltFile(secret), "--delete-by", DELETE_BY], io)).toBe(0);
    for (const line of seen) expect(line).not.toContain(secret.slice(0, 12));
    expect(`${allText(out)}${readFileSync(mappingPath(out), "utf8")}`).not.toContain(secret);
  });
});

describe("import, then the unchanged replay", () => {
  test("two imported sessions writing the same migration give a HIGH duplicate_work through replay/cli.ts", async () => {
    const { out } = await runImport("claude-code", "e2e");
    const evalDir = tmp("si-imp-eval-");
    const q = quiet();
    expect(await replayMain(["replay", "--snapshot", out, "--out", join(evalDir, "replay")], q.io)).toBe(0);
    const r = JSON.parse(readFileSync(join(evalDir, "replay", "alerts.json"), "utf8"));
    const h = new Hasher(SALT);
    const a = h.session("claude", "00000000-0000-4000-8000-00000000e2ea"), b = h.session("claude", "00000000-0000-4000-8000-00000000e2eb");
    expect(r.alerts.map(({ urgency, kind, artifact, pair, channel, alertAt, sentAt }: Record<string, unknown>) => ({ urgency, kind, artifact, pair, channel, alertAt, sentAt }))).toEqual([{
      urgency: "high", kind: "duplicate_work", artifact: "number:0042", pair: [`claude:${a}`, `claude:${b}`].sort(), channel: "dm",
      alertAt: "2026-11-01T09:31:00.000Z", sentAt: "2026-11-01T09:33:00.000Z",
    }]);
    expect(r.format).toBe("session-intel-replay/2");
    expect(r.detectors[0].diagnostics).toMatchObject({ failed: 0, rejected: 0 });
  });

  test("a Claude Code and a Codex import of the same repository line up on project and repo", async () => {
    const claude = await runImport("claude-code", "claude"), codex = await runImport("codex", "codex");
    const lines = [claude, codex].flatMap(({ out }) => readFileSync(join(out, "sessions.jsonl"), "utf8").trim().split("\n").map((l) => JSON.parse(l)));
    const dir = writeSnapshot(tmp("si-imp-merged-"), lines, { window: { since: "2026-11-01T09:00:00.000Z", until: "2026-11-01T10:00:00.000Z" } });
    const r = await replaySnapshot(await loadSnapshot(dir, { now: NOW }), { detectors: [advisorDetector()] });
    const h = new Hasher(SALT);
    const dup = r.alerts.filter((x) => x.kind === "duplicate_work");
    expect(dup.map((x) => [x.urgency, x.artifact, x.pair])).toEqual([["high", "number:0042",
      [`claude:${h.session("claude", "00000000-0000-4000-8000-0000000c1a0d")}`, `codex:${h.session("codex", "00000000-0000-4000-8000-00000000c0de")}`].sort()]]);
  });
});
