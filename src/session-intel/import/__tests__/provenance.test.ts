/** Synthetic action scope evidence; no shell, Git, model or network execution. */
import { afterAll, expect, test } from "bun:test";
import { chmodSync, mkdtempSync, readFileSync, readdirSync, realpathSync, rmSync, statSync, symlinkSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import ts from "typescript";
import { readClaudeCode, readCodex, readGeneric, SessionSet } from "../adapters.ts";
import { main } from "../cli.ts";
import { codeModeShellCalls, codeModeShells } from "../code-mode.ts";
import { emptyStats, Hasher, SessionBuilder, toSnapshotSession } from "../normalize.ts";
import { actionArtifacts, actionContext, directoryPath, gitDirectory, parseRepositoryMap, PROVENANCE_BOUNDS, readRepositoryMap, repositoryIdentity } from "../provenance.ts";
import { loadSnapshot } from "../../replay/snapshot.ts";
import { sha256Hex } from "../../replay/util.ts";

const dirs: string[] = [];
const temp = () => { const d = mkdtempSync(join(tmpdir(), "si-action-synthetic-")); dirs.push(d); return d; };
afterAll(() => { for (const d of dirs) rmSync(d, { recursive: true, force: true }); });
const at = (minute: string) => `2026-11-01T09:${minute}:00.000Z`;
const now = new Date("2026-11-02T00:00:00Z"), deleteBy = "2026-11-10T00:00:00.000Z";
const h = new Hasher("synthetic-action-salt-keep-private");
const tenant = { orgId: "fixture-org", workspaceId: "fixture-ws" };
const roots = [{ directory: "/work/other", repository: "git.example/acme/other", observedAt: at("30") }];
const row = (type: string, payload: unknown, minute: string) => ({ type, timestamp: at(minute), payload });
function writeRows(rows: unknown[]) { const file = join(temp(), "fixture.jsonl"); writeFileSync(file, rows.map((r) => JSON.stringify(r)).join("\n")); return file; }
const meta = row("session_meta", { id: "fixture-session", cwd: "/work/home", git: { repository_url: "https://git.example/acme/home.git" } }, "00");
const output = (id: string, minute: string, sha = "abcdef12") => row("response_item", { type: "function_call_output", call_id: id, output: `[fixture ${sha}] synthetic` }, minute);
const call = (id: string, command: string, minute: string, extra = {}) => row("response_item", { type: "function_call", call_id: id, name: "exec_command", arguments: JSON.stringify({ cmd: command, ...extra }) }, minute);
async function codex(rows: unknown[]) { const set = new SessionSet(emptyStats(), null, true); await readCodex(writeRows(rows), 0, set); return [...set.byKey.values()][0]!; }

test("path and repository claim normalization rejects credentials and preserves case-sensitive paths", () => {
  expect(directoryPath("../other", "/work/home")).toBe("/work/other");
  expect(directoryPath("C:\\work\\repo")).toBe("C:/work/repo");
  expect(directoryPath("C:/")).toBe("C:/");
  for (const raw of ["C:/../work", "D:/../work", "C:/../../work", "C:/../D:/work", "C:/../C:/work", "C:relative"]) expect(directoryPath(raw)).toBeNull();
  expect(directoryPath("../../work", "C:/repo")).toBeNull();
  expect(directoryPath("../../C:/work", "C:/repo")).toBeNull();
  expect(directoryPath("../other", "C:/repo")).toBe("C:/other");
  expect(directoryPath("/work/Case")).not.toBe(directoryPath("/work/case"));
  for (const raw of ["relative", "~/repo", "//server/share", "/work\\repo", "/work\nrepo"]) expect(directoryPath(raw)).toBeNull();
  expect(repositoryIdentity("git@GIT.EXAMPLE:acme/repo.git")).toBe("git.example/acme/repo");
  for (const raw of ["repo", "https://user:private@git.example/acme/repo", "https://token@git.example/acme/repo", "https://git.example/acme/repo?token=private", "git.example/../repo"])
    expect(repositoryIdentity(raw)).toBeNull();
  expect(h.location("directory", "/work/Case")).not.toBe(h.location("directory", "/work/case"));
});

test("location uses have distinct HMAC domains even for identical hostile preimages", () => {
  const value = JSON.stringify(["git.example/acme/api", "src/A.ts"]);
  const domains = ["record", "directory", "target", "repository-target", "basename-hint"] as const;
  expect(new Set(domains.map((domain) => h.location(domain, value))).size).toBe(domains.length);
  expect(h.location("directory", value)).not.toBe(h.location("repository-target", value));
});

for (const [command, base, expected] of [
  ["git commit -m fixture", "/work/home", "/work/home"],
  ["cd '../other repo' && git add src/a.ts && git commit -m 'fixture message'", "/work/home", "/work/other repo"],
  ["git -C ../other -C nested commit -m fixture", "/work/home", "/work/other/nested"],
  ["git -C/work/other commit", null, "/work/other"],
  ["git add src/a.ts; git commit -m fixture", "/work/home", "/work/home"],
  ["cd /work/other; git commit", "/work/home", null],
  ["git -C $TARGET commit", "/work/home", null],
  ["git -C /work/* commit", "/work/home", null],
  ["git --git-dir /work/other/.git commit", "/work/home", null],
  ["git commit && git -C /work/other commit", "/work/home", null],
  ["source setup.sh && git commit", "/work/home", null],
  ["git commit | cat", "/work/home", null],
  ["git commit -m 'unfinished", "/work/home", null],
] as const) test(`bounded Git directory evidence: ${command}`, () => {
  expect(gitDirectory(command, base, "commit")).toBe(expected);
});

test("structured code-mode calls preserve their own workdir without changing the old command-only API", () => {
  const code = "const d='/work/other'; tools.exec_command({cmd:'git commit',workdir:d}); tools.exec_command({cmd:'git status',workdir:dynamic});";
  expect(codeModeShellCalls(code)).toEqual({ calls: [
    { command: "git commit", workdir: { kind: "literal", value: "/work/other" } },
    { command: "git status", workdir: { kind: "unknown", value: null } },
  ], incomplete: false });
  expect(codeModeShells(code)).toEqual({ commands: ["git commit", "git status"], incomplete: false });
  expect(codeModeShellCalls("tools.exec_command({cmd:'git commit',cwd:'/work/other'})").calls[0]!.workdir.kind).toBe("unknown");
});

test("a cross-repo code-mode commit retains call location and output availability, never the session repo", async () => {
  const b = await codex([meta,
    row("response_item", { type: "custom_tool_call", name: "exec", call_id: "nested", input: "tools.exec_command({cmd:'git commit -m fixture',workdir:'/work/other'})" }, "01"),
    output("nested", "02"), call("home", "git commit -m home", "03"), output("home", "04", "12345678"),
  ]);
  const plain = toSnapshotSession(b, h, tenant)!, mapped = toSnapshotSession(b, h, tenant, roots)!;
  expect(mapped.line).toEqual(plain.line); // The legacy snapshot scope/schema is untouched.
  expect(mapped.session.repo).toBe(h.repo("home"));
  const [other, home] = mapped.provenance!.records;
  expect(other).toMatchObject({ eventId: "c1", kind: "commit", repository: h.repo("git.example/acme/other"), repositoryBasis: "operator_declared", source: "tool_workdir", sourceAt: at("01"), factAt: at("02"), availableAt: at("30") });
  expect(mapped.provenance!.locations[other!.location!]!.source).toEqual({ file: 0, line: 2 });
  expect(b.commits[0]!.at.line).toBe(3); // SHA evidence is the matching output, not the call.
  expect(home).toMatchObject({ repository: h.repo("git.example/acme/home"), repositoryBasis: "trace_declared", availableAt: at("04") });
  expect(plain.provenance!.records[0]).toMatchObject({ repository: null, repositoryBasis: "unattributed" });
});

test("changed/dynamic/missing directory scopes do not inherit old or later session declarations", async () => {
  const b = await codex([meta, row("turn_context", { cwd: "/work/other" }, "05"), call("changed", "git commit", "06"), output("changed", "07"),
    call("dynamic", "git commit", "08", { workdir: { unsupported: true } }), output("dynamic", "09", "12345678"),
    row("session_meta", { id: "fixture-session", cwd: "/work/other", git: { repository_url: "https://git.example/acme/later.git" } }, "10"),
  ]);
  const records = toSnapshotSession(b, h, tenant)!.provenance!.records;
  expect(records.every((r) => r.repository === null)).toBe(true);
  expect(records[1]).toMatchObject({ effectiveLocation: null, startingLocation: null, repositoryBasis: "unattributed" });
  const legacy = new SessionBuilder("codex", "legacy", emptyStats(), true);
  legacy.cwd = "/work/other"; legacy.remote = "https://git.example/acme/other.git";
  legacy.commit("abcdef12", at("01"), { file: 0, line: 1 });
  expect(toSnapshotSession(legacy, h, tenant, roots)!.provenance!.records[0]).toMatchObject({ repository: null, reason: "missing_directory", source: "unknown" });
});

test("Git directory changes bind each action; unsupported flow retains only starting-directory evidence", async () => {
  const b = await codex([meta, call("cd", "cd /work/other && git commit", "01"), output("cd", "02"),
    call("unsafe", "cd /work/other; git commit", "03"), output("unsafe", "04", "12345678")]);
  const data = toSnapshotSession(b, h, tenant, roots)!.provenance!;
  expect(data.records[0]!.repository).toBe(h.repo("git.example/acme/other"));
  expect(data.records[1]).toMatchObject({ repository: null, reason: "unsupported_shell", startingLocation: h.location("directory", "/work/home") });
  expect(data.locations[data.records[1]!.location!]!.directory).toBeNull();
});

test("absolute write paths choose their own root; generic per-event declarations never smear later events", async () => {
  const b = await codex([meta, row("response_item", { type: "custom_tool_call", name: "apply_patch", input: "*** Begin Patch\n*** Add File: /work/other/src/a.ts\n+x\n*** End Patch" }, "01")]);
  expect(toSnapshotSession(b, h, tenant, roots)!.provenance!.records[0]!.repository).toBe(h.repo("git.example/acme/other"));
  expect(toSnapshotSession(b, h, tenant)!.provenance!.records[0]!.repository).toBeNull();
  const set = new SessionSet(emptyStats(), null, true);
  await readGeneric(writeRows([
    { sessionId: "g", ts: at("01"), kind: "commit", commit: "abcdef12", repo: "https://git.example/acme/other.git" },
    { sessionId: "g", ts: at("02"), kind: "commit", commit: "12345678" },
  ]), 0, set);
  const records = toSnapshotSession([...set.byKey.values()][0]!, h, tenant)!.provenance!.records;
  expect(records[0]).toMatchObject({ repository: h.repo("git.example/acme/other"), repositoryBasis: "trace_declared" });
  expect(records[1]!.repository).toBeNull();
});

test("clamping and absorbing retain action references without reassigning scope", () => {
  const a = new SessionBuilder("codex", "one", emptyStats(), true), b = new SessionBuilder("codex", "one", emptyStats(), true);
  b.commit("abcdef12", at("01"), { file: 1, line: 4 }, actionContext("/work/other", "tool_workdir", { file: 1, line: 3 }, at("00")));
  a.absorb(b);
  expect(toSnapshotSession(a, h, tenant, roots)!.provenance!.locations).toEqual(toSnapshotSession(b, h, tenant, roots)!.provenance!.locations);
  a.clampFrom(at("01"));
  expect(toSnapshotSession(a, h, tenant, roots)!.provenance!.records).toHaveLength(1);
});

test("operator maps are bounded private declarations with deterministic longest-root precedence", async () => {
  const text = JSON.stringify({ format: "session-intel-repository-map/1", roots: [...roots, { directory: "/work", repository: "git.example/acme/parent", observedAt: at("00") }] });
  const map = parseRepositoryMap(text, now);
  expect(map.roots[0]!.directory).toBe("/work/other"); expect(map.sha256).toBe(sha256Hex(text));
  const path = join(temp(), "map.json"); writeFileSync(path, text, { mode: 0o600 });
  expect(await readRepositoryMap(path, now)).toEqual(map);
  chmodSync(path, 0o644); await expect(readRepositoryMap(path, now)).rejects.toThrow("private");
  chmodSync(path, 0o600); const link = join(temp(), "alias"); symlinkSync(path, link);
  await expect(readRepositoryMap(link, now)).rejects.toThrow();
  for (const bad of [text.replace('"roots":[', '"roots":[{} ,'), text.replace(at("30"), "2100-01-01T00:00:00Z"), " ".repeat(PROVENANCE_BOUNDS.mapBytes + 1)])
    expect(() => parseRepositoryMap(bad, now)).toThrow("values withheld");
});

test("opt-in CLI sidecar is manifest-pinned, private, capture-only and leaves legacy session bytes unchanged", async () => {
  const input = writeRows([{ sessionId: "g", ts: at("01"), kind: "write", files: ["src/private-name.ts"], cwd: "/work/other", commit: "abcdef12" }]);
  const root = temp(), out = join(root, "with"), old = join(root, "without"), map = join(root, "roots.json");
  writeFileSync(map, JSON.stringify({ format: "session-intel-repository-map/1", roots }), { mode: 0o600 });
  const args = ["--format", "generic", "--in", input, "--salt-stdin", "--delete-by", deleteBy];
  const io = () => ({ now, out: (_: string) => {}, err: (_: string) => {}, stdin: (async function* () { yield "synthetic-action-salt-keep-private"; })() });
  expect(await main([...args, "--out", old], io())).toBe(0);
  expect(await main([...args, "--out", out, "--repository-map", map], io())).toBe(0);
  expect(readFileSync(join(out, "sessions.jsonl"), "utf8")).toBe(readFileSync(join(old, "sessions.jsonl"), "utf8"));
  expect(readdirSync(old).sort()).toEqual(["manifest.json", "sessions.jsonl"]);
  const loaded = await loadSnapshot(out, { now }), body = loaded.extraFiles!["action-provenance.json"]!, parsed = JSON.parse(body);
  expect(loaded.manifest.files["action-provenance.json"]).toBe(sha256Hex(body));
  expect(parsed).toMatchObject({ status: "capture_only", consumedByReplay: false, legacySessionScopeUnchanged: true, deleteBy });
  expect(parsed.records).toHaveLength(2);
  expect(parsed.sessionsSha256).toBe(loaded.manifest.files["sessions.jsonl"]);
  expect(parsed.implementationSha256).toMatch(/^[a-f0-9]{64}$/u);
  expect(parsed.counts).toMatchObject({ records: 2, operatorDeclared: 2, availableAfterFact: 2 });
  const mapSha256 = sha256Hex(readFileSync(map));
  expect(parsed.repositoryMapFingerprint).toBe(h.repositoryMap(mapSha256));
  expect(h.repositoryMap(mapSha256)).not.toBe(new Hasher("another-synthetic-salt").repositoryMap(mapSha256));
  expect(body).not.toContain(mapSha256);
  expect(parsed).not.toHaveProperty("repositoryMapSha256");
  for (const secret of ["/work/other", "private-name", "synthetic-action-salt"]) expect(body).not.toContain(secret);
  expect(statSync(join(out, "action-provenance.json")).mode & 0o077).toBe(0);
  const privateMap = JSON.parse(readFileSync(`${out}.mapping.json`, "utf8"));
  expect(JSON.stringify(privateMap.actionProvenance)).toContain("/work/other");
  expect(privateMap.deleteBy).toBe(deleteBy);
  expect(privateMap.actionProvenance.repositoryMapSha256).toBe(mapSha256);
});

test("an empty repository-map option is refused instead of silently enabling capture", async () => {
  const input = writeRows([{ sessionId: "g", ts: at("01"), kind: "commit", commit: "abcdef12" }]);
  const out = join(temp(), "out");
  for (const value of ["", " "]) await expect(main(["--format", "generic", "--in", input, "--out", out,
    "--salt-stdin", "--delete-by", deleteBy, "--repository-map", value], {
    now, out: () => {}, err: () => {}, stdin: (async function* () { yield "synthetic-action-salt-keep-private"; })(),
  })).rejects.toThrow("--repository-map must name a private file");
  expect(() => statSync(out)).toThrow();
});

test("sidecar and capture budgets fail before writing an apparently complete artifact", () => {
  const b = new SessionBuilder("generic", "b", emptyStats(), true, { sizes: new WeakMap(), count: PROVENANCE_BOUNDS.actions, bytes: 0 });
  expect(() => b.commit("abcdef12", at("01"), { file: 0, line: 1 }, actionContext("/work/other", "generic_event", { file: 0, line: 1 }, at("01")))).toThrow("capture exceeds");
  expect(() => actionArtifacts([{ records: [], locations: { fixture: { directory: "x".repeat(PROVENANCE_BOUNDS.exportBytes), startingDirectory: null, target: null, repository: null, source: { file: 0, line: 1 }, output: null, claims: [] } } }], deleteBy, null, { sessionsSha256: sha256Hex("fixture"), implementationSha256: sha256Hex("code") })).toThrow("private action provenance map exceeds");
});

test("captured context bytes overflow before the action-count bound", () => {
  const budget = { sizes: new WeakMap(), count: 0, bytes: 0 };
  const b = new SessionBuilder("generic", "byte-bound", emptyStats(), true, budget);
  const context = actionContext(`/${"x".repeat(1000)}`, "generic_event", { file: 0, line: 1 }, at("01"));
  expect(() => { for (let i = 0; i < 10_000; i++) b.commit("abcdef12", at("01"), { file: 0, line: i + 1 }, context); }).toThrow("capture exceeds");
  expect(budget.count).toBeLessThan(PROVENANCE_BOUNDS.actions);
  expect(budget.bytes).toBeLessThanOrEqual(PROVENANCE_BOUNDS.captureBytes);
});

test("per-session export bytes overflow while captured context still fits", () => {
  const b = new SessionBuilder("generic", "export-byte-bound", emptyStats(), true);
  const context = actionContext(`/${"x".repeat(600)}`, "generic_event", { file: 0, line: 1 }, at("01"));
  for (let i = 0; i < 10_000; i++) b.commit("abcdef12", at("01"), { file: 0, line: i + 1 }, context);
  expect(b.commits).toHaveLength(10_000);
  expect(() => toSnapshotSession(b, h, tenant)).toThrow("export exceeds");
});

test("rest object defaults are hidden while an ordinary RHS object spread remains visible", () => {
  expect(codeModeShells("let a; ({...[a = tools.exec_command({cmd:'git commit'})]} = {})")).toEqual({ commands: [], incomplete: true });
  expect(codeModeShells("let a,b; a = {...[b = tools.exec_command({cmd:'git commit'})]}")).toEqual({ commands: ["git commit"], incomplete: false });
});

test("default-off CLI matches the pre-provenance landed golden byte digests", async () => {
  // Captured from 2da9e9c9 using this synthetic input/clock/salt. No reverse-map body is committed.
  const input = writeRows([{ sessionId: "golden", ts: at("01"), kind: "write", files: ["src/a.ts"], commit: "abcdef12", cwd: "/work/api", repo: "https://git.example/acme/api.git" }]);
  const out = join(temp(), "out"), stdout: string[] = [], stderr: string[] = [];
  expect(await main(["--format", "generic", "--in", input, "--out", out, "--salt-stdin", "--delete-by", deleteBy, "--org", "fixture-org", "--workspace", "fixture-ws"], {
    now, out: (s) => stdout.push(s), err: (s) => stderr.push(s), stdin: (async function* () { yield "synthetic-action-salt-keep-private"; })(),
  })).toBe(0);
  expect({ sessions: sha256Hex(readFileSync(join(out, "sessions.jsonl"))), manifest: sha256Hex(readFileSync(join(out, "manifest.json"))),
    mapping: sha256Hex(readFileSync(`${out}.mapping.json`)), stdout: sha256Hex(stdout.join("\n").split(realpathSync(`${out}.mapping.json`)).join("<mapping>").split(realpathSync(out)).join("<out>")), stderr }).toEqual({
    sessions: "68a524d7f35caa018c5a36ac387507fd55c3f261419476b4fe5bde555c9bec1a",
    manifest: "4a302a2f2e2e54cd724a9cfd3f055d569c50501bd5a5419a76dec464e78b820d",
    mapping: "474635fc9ee3ec223e6c23a9183b970d1ae94c23ac93751e01ccbacd10a258c5",
    stdout: "e576cd517ceda27952ba0caeb04d727bb9bed33facd74a1428af5b7905eae8de", stderr: [],
  });
});

test("provenance production modules have no process or Bun shell imports", () => {
  const forbiddenImports = (source: string) => {
    const file = ts.createSourceFile("fixture.ts", source, ts.ScriptTarget.Latest, true);
    const found: string[] = [];
    const check = (value: ts.Expression | undefined) => {
      if (value && ts.isStringLiteralLike(value) && ["bun", "child_process", "node:child_process"].includes(value.text)) found.push(value.text);
    };
    const visit = (node: ts.Node) => {
      if (ts.isImportDeclaration(node) || ts.isExportDeclaration(node)) check(node.moduleSpecifier);
      if (ts.isImportEqualsDeclaration(node) && ts.isExternalModuleReference(node.moduleReference)) check(node.moduleReference.expression);
      if (ts.isCallExpression(node) && (node.expression.kind === ts.SyntaxKind.ImportKeyword || (ts.isIdentifier(node.expression) && node.expression.text === "require"))) check(node.arguments[0]);
      ts.forEachChild(node, visit);
    };
    visit(file); return found;
  };
  expect(forbiddenImports('import {exec} from "child_process"; require("node:child_process"); import {$} from "bun";')).toHaveLength(3);
  for (const file of ["provenance.ts", "adapters.ts", "normalize.ts"])
    expect(forbiddenImports(readFileSync(new URL(`../${file}`, import.meta.url), "utf8"))).toEqual([]);
});

test("capture never executes Git or probes a directory controlled by a trace", () => {
  // Isolate module mocks in a child test process; no mocks leak into the other import suites.
  const root = temp(), input = join(root, "fixture.jsonl"), map = join(root, "map.json"), out = join(root, "out"), trace = join(root, "must-not-probe");
  writeFileSync(input, JSON.stringify({ sessionId: "g", ts: at("01"), kind: "commit", commit: "abcdef12", cwd: trace }) + "\n");
  const native = join(root, "codex.jsonl"), claude = join(root, "claude.jsonl");
  writeFileSync(native, [
    row("session_meta", { id: "guard-codex", cwd: trace }, "00"),
    row("response_item", { type: "custom_tool_call", name: "exec", call_id: "nested", input: `tools.exec_command({cmd:'git commit',workdir:${JSON.stringify(`${trace}/nested`)}})` }, "01"), output("nested", "02"),
    call("plain", "git commit", "03", { workdir: `${trace}/plain` }), output("plain", "04", "12345678"),
  ].map((r) => JSON.stringify(r)).join("\n"));
  writeFileSync(claude, [
    { type: "assistant", sessionId: "guard-claude", timestamp: at("01"), cwd: `${trace}/claude`, message: { content: [{ type: "tool_use", id: "commit", name: "Bash", input: { command: "git commit" } }] } },
    { type: "user", sessionId: "guard-claude", timestamp: at("02"), message: { content: [{ type: "tool_result", tool_use_id: "commit", content: "[fixture abcdef12] synthetic" }] } },
  ].map((r) => JSON.stringify(r)).join("\n"));
  writeFileSync(map, JSON.stringify({ format: "session-intel-repository-map/1", roots: [{ directory: trace, repository: "git.example/acme/api", observedAt: at("00") }] }), { mode: 0o600 });
  const child = join(root, "guard.test.ts"), cli = new URL("../cli.ts", import.meta.url).pathname;
  writeFileSync(child, `import {expect,test,mock} from "bun:test";
import * as fs from "node:fs"; import * as fsp from "node:fs/promises"; import * as cp from "node:child_process";
const trace=${JSON.stringify(trace)}, forbidden=[];
function isTrace(v){const s=v instanceof URL?v.pathname:typeof v==="string"?v:null;return s!==null&&(s===trace||s.startsWith(trace+"/"));}
function guarded(original,kind,keys){const result={...original};for(const key of keys){const fn=original[key];if(typeof fn!=="function")continue;result[key]=(...args)=>{if(kind==="process"||isTrace(args[0])){forbidden.push(kind+":"+key);throw new Error("forbidden synthetic probe");}return Reflect.apply(fn,original,args);};}return {...result,default:result};}
const nativeFs={...fs}, nativePromises={...fsp}, nativeProcesses={...cp};
const promiseGuards=guarded(nativePromises,"fs",["stat","lstat","access","realpath","readlink","readdir","opendir","open","readFile","watch"]);
mock.module("node:fs",()=>{const g=guarded(nativeFs,"fs",["stat","statSync","lstat","lstatSync","access","accessSync","exists","existsSync","realpath","realpathSync","readlink","readlinkSync","readdir","readdirSync","opendir","opendirSync","open","openSync","readFile","readFileSync","createReadStream","watch","watchFile"]);g.promises=promiseGuards;g.default.promises=promiseGuards;return g;});
mock.module("node:fs/promises",()=>promiseGuards);
mock.module("node:child_process",()=>guarded(nativeProcesses,"process",["exec","execFile","execSync","execFileSync","spawn","spawnSync","fork"]));
Bun.$=()=>{forbidden.push("process:Bun.$");throw new Error("forbidden synthetic probe");};
for(const key of ["spawn","spawnSync"]){Bun[key]=()=>{forbidden.push("process:Bun."+key);throw new Error("forbidden synthetic probe");};}
for(const key of ["file","write"]){const fn=Bun[key];Bun[key]=(...args)=>{if(isTrace(args[0])){forbidden.push("fs:Bun."+key);throw new Error("forbidden synthetic probe");}return Reflect.apply(fn,Bun,args);};}
test("only explicitly selected input/map files are read",async()=>{
 const {main}=await import(${JSON.stringify(cli)});
 for (const [format,input,out] of ${JSON.stringify([["generic", input, out], ["codex", native, `${out}-codex`], ["claude-code", claude, `${out}-claude`]])}) {
  const code=await main(["--format",format,"--in",input,"--out",out,"--repository-map",${JSON.stringify(map)},"--salt-stdin","--delete-by",${JSON.stringify(deleteBy)}],{now:new Date(${JSON.stringify(now.toISOString())}),out:()=>{},err:()=>{},stdin:(async function*(){yield "synthetic-action-salt-keep-private";})()});
  expect(code).toBe(0);
 }
 expect(forbidden).toEqual([]);
 // Prove the guards are live instead of passing vacuously.
 const guardedFs=await import("node:fs/promises"),guardedCp=await import("node:child_process");
 await expect(Promise.resolve().then(()=>guardedFs.stat(trace))).rejects.toThrow("forbidden synthetic probe");
 expect(()=>guardedCp.execFileSync("git",["-C",trace,"remote","get-url","origin"])).toThrow("forbidden synthetic probe");
 expect(()=>Bun.spawn(["git","status"])).toThrow("forbidden synthetic probe");
 const guardedFsModule=await import("node:fs");
 await expect(Promise.resolve().then(()=>guardedFsModule.promises.stat(trace))).rejects.toThrow("forbidden synthetic probe");
 expect(()=>guardedFsModule.createReadStream(trace)).toThrow("forbidden synthetic probe");
 expect(()=>guardedFsModule.exists(trace,()=>{})).toThrow("forbidden synthetic probe");
 expect(()=>guardedFsModule.watch(trace,()=>{})).toThrow("forbidden synthetic probe");
 expect(()=>Bun.$(["git status"])).toThrow("forbidden synthetic probe");
 expect(forbidden.length).toBe(8);
});`);
  const result = Bun.spawnSync({ cmd: [process.execPath, "test", child], env: { PATH: process.env.PATH ?? "" }, stdout: "pipe", stderr: "pipe" });
  if (result.exitCode !== 0) throw new Error(`synthetic probe guard failed: ${result.stderr.toString()}`);
  expect(result.exitCode).toBe(0);
});


test("out-of-order trace context cannot make repository evidence available before it was recorded", async () => {
  const b = await codex([
    row("session_meta", { id: "fixture-session", cwd: "/work/home", git: { repository_url: "https://git.example/acme/home.git" } }, "10"),
    call("earlier", "git commit", "01"), output("earlier", "02"),
  ]);
  expect(toSnapshotSession(b, h, tenant)!.provenance!.records[0]).toMatchObject({ factAt: at("02"), availableAt: at("10"), sourceAt: at("10") });
});


test("Claude SHA scope availability waits for output without changing its legacy snapshot time", async () => {
  const file = writeRows([
    { type: "assistant", sessionId: "claude-fixture", timestamp: at("01"), cwd: "/work/other", message: { content: [{ type: "tool_use", id: "commit", name: "Bash", input: { command: "git commit -m fixture" } }] } },
    { type: "user", sessionId: "claude-fixture", timestamp: at("05"), cwd: "/work/other", message: { content: [{ type: "tool_result", tool_use_id: "commit", content: "[fixture abcdef12] synthetic" }] } },
  ]);
  const set = new SessionSet(emptyStats(), null, true); await readClaudeCode(file, 0, set);
  const b = [...set.byKey.values()][0]!, data = toSnapshotSession(b, h, tenant, [{ ...roots[0]!, observedAt: at("00") }])!;
  expect(data.session.commits[0]!.ts).toBe(at("01"));
  const r = data.provenance!.records[0]!;
  expect(r).toMatchObject({ sourceAt: at("01"), factAt: at("01"), availableAt: at("05") });
  expect(data.provenance!.locations[r.location!]!).toMatchObject({ source: { file: 0, line: 1 }, output: { file: 0, line: 2 } });
  const old = new SessionSet(emptyStats()); await readClaudeCode(file, 0, old);
  expect(data.line).toEqual(toSnapshotSession([...old.byKey.values()][0]!, h, tenant)!.line);
});


test("repository target identity is stable across declared checkout roots and preserves case", async () => {
  const set = new SessionSet(emptyStats(), null, true);
  await readGeneric(writeRows([
    { sessionId: "one", ts: at("01"), kind: "write", cwd: "/work/one", files: ["src/A.ts"] },
    { sessionId: "two", ts: at("01"), kind: "write", cwd: "/work/two", files: ["src/A.ts"] },
    { sessionId: "three", ts: at("01"), kind: "write", cwd: "/work/two", files: ["src/a.ts"] },
  ]), 0, set);
  const declared = ["/work/one", "/work/two"].map((directory) => ({ directory, repository: "git.example/acme/api", observedAt: at("00") }));
  const [a,b,c] = [...set.byKey.values()].map((s) => toSnapshotSession(s, h, tenant, declared)!.provenance!.records[0]!);
  expect(a!.repositoryTarget).toBe(b!.repositoryTarget);
  expect(a!.repositoryTarget).not.toBe(c!.repositoryTarget);
  expect(a!.target).not.toBe(b!.target);
  expect(a!.repository).toBe(b!.repository);
});


test("repository resolution is longest-root even when a library caller supplies parent first", async () => {
  const b = await codex([meta, call("other", "git commit", "01", { workdir: "/work/other" }), output("other", "02")]);
  const data = toSnapshotSession(b, h, tenant, [{ directory: "/work", repository: "git.example/acme/parent", observedAt: at("00") }, ...roots])!;
  expect(data.provenance!.records[0]!.repository).toBe(h.repo("git.example/acme/other"));
});


test("two commit calls in one Claude source line keep their distinct output references", async () => {
  const file = writeRows([
    { type: "assistant", sessionId: "claude-fixture", timestamp: at("01"), cwd: "/work/other", message: { content: ["first", "second"].map((id) => ({ type: "tool_use", id, name: "Bash", input: { command: "git commit -m fixture" } })) } },
    { type: "user", sessionId: "claude-fixture", timestamp: at("03"), message: { content: [{ type: "tool_result", tool_use_id: "first", content: "[fixture abcdef12] synthetic" }] } },
    { type: "user", sessionId: "claude-fixture", timestamp: at("04"), message: { content: [{ type: "tool_result", tool_use_id: "second", content: "[fixture 12345678] synthetic" }] } },
  ]);
  const set = new SessionSet(emptyStats(), null, true); await readClaudeCode(file, 0, set);
  const data = toSnapshotSession([...set.byKey.values()][0]!, h, tenant)!.provenance!, [a,b] = data.records;
  expect(a!.location).not.toBe(b!.location);
  expect(data.locations[a!.location!]!.output).toEqual({ file: 0, line: 2 });
  expect(data.locations[b!.location!]!.output).toEqual({ file: 0, line: 3 });
});


test("invalid path encoding cannot collapse identities and repository-map digest pins raw UTF-8 bytes", async () => {
  expect(directoryPath("/work/\ud800")).toBeNull();
  expect(directoryPath("/work/\ud801")).toBeNull();
  const path = join(temp(), "invalid.json");
  writeFileSync(path, Buffer.concat([Buffer.from('{"format":"session-intel-repository-map/1","roots":[],"unused":"'), Buffer.from([255]), Buffer.from('"}')]), { mode: 0o600 });
  await expect(readRepositoryMap(path, now)).rejects.toThrow("valid UTF-8");
});


test("invalid turn context and metadata missing cwd cannot reuse an earlier repository declaration", async () => {
  const b = await codex([meta,
    row("turn_context", { cwd: { dynamic: true } }, "01"), call("missing-dir", "git commit", "02"), output("missing-dir", "03"),
    row("session_meta", { id: "fixture-session", git: { repository_url: "https://git.example/acme/other.git" } }, "04"),
    call("explicit-dir", "git commit", "05", { workdir: "/work/home" }), output("explicit-dir", "06", "12345678"),
  ]);
  const records = toSnapshotSession(b, h, tenant)!.provenance!.records;
  expect(records.every((r) => r.repository === null)).toBe(true);
  expect(records[0]!.effectiveLocation).toBeNull();
  expect(records[1]!.effectiveLocation).toBe(h.location("directory", "/work/home"));
});


test("contradictory repository declarations remain explicit and cannot silently override one another", async () => {
  const b = await codex([meta, call("one", "git commit", "01"), output("one", "02")]);
  const data = toSnapshotSession(b, h, tenant, [{ directory: "/work/home", repository: "git.example/acme/conflicting", observedAt: at("30") }])!.provenance!, r = data.records[0]!;
  expect(r).toMatchObject({ repository: null, repositoryTarget: null, repositoryBasis: "conflicting_claims", availableAt: at("30") });
  expect(r.repositoryClaims.map((c) => c.repository)).toEqual([h.repo("git.example/acme/conflicting"), h.repo("git.example/acme/home")]);
  expect(JSON.stringify(r)).not.toContain("git.example");
  expect(data.locations[r.location!]!.claims).toHaveLength(2);
});


test("flat code-mode and shell-patch writes retain starting evidence without inventing effective paths", async () => {
  const patch = "*** Begin Patch\n*** Add File: src/a.ts\n+x\n*** End Patch";
  const b = await codex([meta,
    row("response_item", { type: "custom_tool_call", name: "exec", call_id: "patch", input: `tools.apply_patch(${JSON.stringify(patch)})` }, "01"),
    call("shell-patch", `cd /work/other && apply_patch <<'PATCH'\n${patch}\nPATCH`, "02"),
  ]);
  const data = toSnapshotSession(b, h, tenant, [{ directory: "/work/home", repository: "git.example/acme/home", observedAt: at("00") }])!.provenance!;
  expect(data.records).toHaveLength(2);
  for (const r of data.records) expect(r).toMatchObject({ repository: null, repositoryTarget: null, effectiveLocation: null, target: null, startingLocation: h.location("directory", "/work/home") });
});
