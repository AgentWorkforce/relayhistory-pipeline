/** Synthetic capture-only repository paths. Reuse M4 normalization; never introduce another scrub vocabulary. */
import { afterAll, describe, expect, test } from "bun:test";
import { mkdtempSync, readFileSync, rmSync, statSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { outcomeFacts, type LaneFacts } from "../../../../packages/coordination-detect/src/index.ts";
import { main } from "../cli.ts";
import { emptyStats, Hasher, IMPORT_BOUNDS, repoRelativePath, scrubPath, SessionBuilder, toSnapshotSession } from "../normalize.ts";
import { actionArtifacts, actionContext, PROVENANCE_FORMAT, PROVENANCE_PATH_FORMAT, REPOSITORY_PATH_ENCODING, type ActionContext, type ActionRecord } from "../provenance.ts";
import { readActionScope } from "../../patterns/action-scope.ts";
import { loadSnapshot } from "../../replay/snapshot.ts";
import { sha256Hex } from "../../replay/util.ts";

const dirs: string[] = [];
const temp = () => { const dir = mkdtempSync(join(tmpdir(), "si-paths-fixture-")); dirs.push(dir); return dir; };
afterAll(() => { for (const dir of dirs) rmSync(dir, { recursive: true, force: true }); });
const at = (m: string) => `2026-11-01T09:${m}:00.000Z`, now = new Date("2026-11-02T00:00:00.000Z"), deleteBy = "2026-11-10T00:00:00.000Z";
const salt = "synthetic-repository-paths-salt", h = new Hasher(salt), tenant = { orgId: "fixture", workspaceId: "fixture" };
const root = { directory: "/work/api", repository: "git.example/acme/api", observedAt: at("10") };
const line = { file: 0, line: 1 };
function builder(path: string, context: ActionContext | null = actionContext(root.directory, "tool_workdir", line, at("01"))) {
  const b = new SessionBuilder("codex", "fixture", emptyStats(), true);
  b.cwd = root.directory;
  b.write(path, at("01"), line, root.directory, null, "write", context);
  return b;
}
const capture = (b: SessionBuilder, roots = [root], hasher = h) => toSnapshotSession(b, hasher, tenant, roots, { repositoryPaths: true })!;
const pins = { sessionsSha256: sha256Hex("synthetic sessions"), implementationSha256: sha256Hex("synthetic implementation") };

describe("versioned repository-path capture", () => {
  test("exact existing M4 path vocabulary, canonical target and availability are preserved", () => {
    for (const path of ["supabase/migrations/0042_add_index.sql", "db/migrate/20261105062150_add.rb", "migrations/0042.sql", "migration/007-Name.sql", "src/PrivateFolder/name.privateext", "generated/private_manifest.json", "Cargo.lock", "package.json"]) {
      const b = builder(path), plain = toSnapshotSession(b, h, tenant, [root])!, out = capture(b), r = out.provenance!.records[0]!;
      expect(out.line).toEqual(plain.line);
      expect(out.mapped.paths[r.repositoryPath!]).toBe(repoRelativePath(path, root.directory)!);
      expect(r.repositoryPath).toBe(scrubPath(repoRelativePath(path, root.directory)!, h));
      expect(r.repositoryTarget).toBe(plain.provenance!.records[0]!.repositoryTarget);
      expect(r.availableAt).toBe(at("10"));
      expect(plain.provenance!.records[0]).not.toHaveProperty("repositoryPath");
    }
    const r = capture(builder("supabase/migrations/0042_add_index.sql")).provenance!.records[0]!;
    expect(r.repositoryPath).toMatch(/^supabase\/migrations\/0042_[a-f0-9]{12}\.sql$/u);
    expect(r.repositoryPath).not.toContain("add_index");
  });
  test("migration number survives into the real T1 detector without adding a replay adapter", () => {
    const make = (id: string, name: string): LaneFacts => {
      const out = capture(builder(`supabase/migrations/${name}`)), r = out.provenance!.records[0]!;
      return { source: "codex", sessionId: id, userId: null, projectId: r.repository, repo: r.repository, branch: null, taskRef: null, title: null, parentSessionId: null,
        startedAt: at("00"), lastActionAt: at("01"), writes: [{ path: r.repositoryPath!, ts: r.factAt, eventId: r.eventId, kind: "write" }], commits: [], outcomes: [], usage: [] };
    };
    const result = outcomeFacts.detect({ workspaceId: "fixture", window: { since: at("00"), until: at("20") }, lanes: [make("a", "0042_add_index.sql"), make("b", "0042_other_name.sql")] });
    expect(result.findings.some((f) => f.kind === "duplicate_work" && f.confidence === "high")).toBe(true);
    expect(result.findings[0]!.artifacts[0]!.value).toContain("0042");
  });
  test("the matching action root wins over session cwd; checkout roots and salts behave as in M4", () => {
    const b = builder("/work/other/PrivatePrefix/src/SecretName.ts"), other = { ...root, directory: "/work/other" };
    const out = capture(b, [other]), record = out.provenance!.records[0]!;
    expect(record.repositoryPath).toBe(scrubPath("PrivatePrefix/src/SecretName.ts", h));
    expect(record.repositoryPath).not.toBe(out.session.writes[0]!.path);
    expect(out.mapped.paths[record.repositoryPath!]).toBe("PrivatePrefix/src/SecretName.ts");
    expect(record.repositoryPath).not.toContain("SecretName");
    const same = capture(builder("PrivatePrefix/src/SecretName.ts")).provenance!.records[0]!;
    expect(record.repositoryPath).toBe(same.repositoryPath); expect(record.repositoryTarget).toBe(same.repositoryTarget);
    const changed = capture(b, [other], new Hasher("other-synthetic-path-salt")).provenance!.records[0]!;
    expect(changed.repositoryPath).not.toBe(record.repositoryPath);
    expect(changed.repositoryTarget).not.toBe(record.repositoryTarget);
    const drive = capture(builder("C:/work/other/PrivatePrefix/src/SecretName.ts"), [{ ...root, directory: "C:/work/other" }]).provenance!.records[0]!;
    expect(drive.repositoryPath).toBe(record.repositoryPath); expect(drive.repositoryTarget).toBe(record.repositoryTarget);
  });
  test("no inferred path for missing/conflicting/projection scope, trace-only declarations or non-write facts", () => {
    const ctx = actionContext(root.directory, "tool_workdir", line, at("01"), { repository: "git.example/acme/other", root: root.directory, observedAt: at("01") });
    expect(capture(builder("src/a.ts", ctx)).provenance!.records[0]!.repositoryPath).toBeNull();
    expect(capture(builder("src/a.ts", null)).provenance!.records[0]!.repositoryPath).toBeNull();
    const projection = actionContext(root.directory, "code_mode_projection", line, at("01")); projection.reason = "projection_scope_unavailable";
    expect(capture(builder("src/a.ts", projection)).provenance!.records[0]!.repositoryPath).toBeNull();
    const trace = actionContext(root.directory, "generic_event", line, at("01"), { repository: root.repository, root: root.directory, observedAt: at("01") });
    expect(capture(builder("src/a.ts", trace), []).provenance!.records[0]!.repositoryPath).toBeNull();
    const b = builder("src/a.ts");
    b.commit("abcdef12", at("02"), line, actionContext(root.directory, "tool_workdir", line, at("02")));
    b.outcome("abcdef12", true, at("03"), line, actionContext(root.directory, "tool_workdir", line, at("03")));
    expect(capture(b).provenance!.records.slice(1).map((r) => r.repositoryPath)).toEqual([null, null]);
  });
  test("unsafe home-user paths and encoded path growth stay null instead of bypassing M4 guards", () => {
    const homeRoot = { ...root, directory: "/Users/fixture-person/api" }, b = new SessionBuilder("codex", "fixture", emptyStats(), true);
    // The action root's user appears in the target: M4 refuses it even with explicit scope evidence.
    b.write("src/file.ts", at("01"), line);
    b.capture(b.writes[0]!, { ...actionContext(homeRoot.directory, "tool_workdir", line, at("01")), target: `${homeRoot.directory}/src/fixture-person/file.ts` });
    expect(capture(b, [homeRoot]).provenance!.records[0]!.repositoryPath).toBeNull();
    const deep = builder("src/a.ts");
    deep.capture(deep.writes[0]!, { ...actionContext(root.directory, "tool_workdir", line, at("01")), target: `${root.directory}/${"a/".repeat(400)}file.ts` });
    expect(capture(deep).provenance!.records[0]!.repositoryPath).toBeNull();
  });
  test("artifact versions cannot silently mix path fields; format2 stays capture-only", () => {
    const out = capture(builder("src/a.ts")), old = toSnapshotSession(builder("src/a.ts"), h, tenant, [root])!;
    expect(() => actionArtifacts([out.provenance!], deleteBy, null, pins)).toThrow("requested path format");
    expect(() => actionArtifacts([old.provenance!], deleteBy, null, pins, { repositoryPaths: true })).toThrow("requested path format");
    const artifact = actionArtifacts([out.provenance!], deleteBy, null, pins, { repositoryPaths: true });
    expect(JSON.parse(artifact.body)).toMatchObject({ format: PROVENANCE_PATH_FORMAT, repositoryPathEncoding: REPOSITORY_PATH_ENCODING, status: "capture_only", consumedByReplay: false });
    const oldBody = actionArtifacts([old.provenance!], deleteBy, null, pins).body;
    expect(JSON.parse(oldBody).format).toBe(PROVENANCE_FORMAT);
    // Generated with the /1 artifact writer from main13455daa, before this slice.
    expect(sha256Hex(oldBody)).toBe("04cf01cf672b5f6a16f9a75da8a69cb58110cce32ec3396db9bc8591c879214b");
    for (const value of [undefined, 3, "", "x".repeat(IMPORT_BOUNDS.path + 1)]) {
      const bad = { ...out.provenance!.records[0]!, repositoryPath: value } as unknown as ActionRecord;
      expect(() => actionArtifacts([{ records: [bad], locations: {} }], deleteBy, null, pins, { repositoryPaths: true })).toThrow("invalid type");
    }
  });
});

test("CLI uses the same salt, private map and receipt, with default and /1 session bytes unchanged", async () => {
  const base = temp(), input = join(base, "fixture.jsonl"), map = join(base, "roots.json"), old = join(base, "old"), v1 = join(base, "v1"), v2 = join(base, "v2");
  writeFileSync(input, JSON.stringify({ sessionId: "fixture", ts: at("01"), kind: "write", cwd: root.directory, files: ["supabase/migrations/0042_add_index.sql"] }));
  writeFileSync(map, JSON.stringify({ format: "session-intel-repository-map/1", roots: [root] }), { mode: 0o600 });
  const args = ["--format", "generic", "--in", input, "--salt-stdin", "--delete-by", deleteBy];
  const io = () => ({ now, out: () => {}, err: () => {}, stdin: (async function* () { yield salt; })() });
  expect(await main([...args, "--out", old], io())).toBe(0);
  expect(await main([...args, "--out", v1, "--repository-map", map], io())).toBe(0);
  expect(await main([...args, "--out", v2, "--repository-map", map, "--action-repository-paths"], io())).toBe(0);
  for (const out of [v1, v2]) expect(readFileSync(join(out, "sessions.jsonl"), "utf8")).toBe(readFileSync(join(old, "sessions.jsonl"), "utf8"));
  const loaded = await loadSnapshot(v2, { now }), body = loaded.extraFiles!["action-provenance.json"]!, record = JSON.parse(body).records[0];
  expect(loaded.manifest.files["action-provenance.json"]).toBe(sha256Hex(body));
  expect(JSON.parse(body).sessionsSha256).toBe(loaded.manifest.files["sessions.jsonl"]);
  expect(record.repositoryPath).toBe(scrubPath("supabase/migrations/0042_add_index.sql", h));
  expect(statSync(join(v2, "action-provenance.json")).mode & 0o777).toBe(0o600);
  const privateMap = JSON.parse(readFileSync(`${v2}.mapping.json`, "utf8"));
  expect(privateMap.manifestSha256).toBe(loaded.manifestSha256);
  expect(privateMap.actionProvenance.repositoryMapSha256).toBe(sha256Hex(readFileSync(map)));
  expect(privateMap.sessions[0].paths[record.repositoryPath]).toBe("supabase/migrations/0042_add_index.sql");
  for (const value of [salt, root.directory, "add_index"]) expect(body).not.toContain(value);
  expect(() => readActionScope(loaded, now)).toThrow("values withheld"); // /1 consumer refuses /2 until its separately reviewed adapter.
  expect(readActionScope(await loadSnapshot(v1, { now }), now).size).toBe(1);
  expect(privateMap.deleteBy).toBe(deleteBy);
  const noMap = join(base, "without-map");
  expect(await main([...args, "--out", noMap, "--action-repository-paths"], io())).toBe(0);
  const unknown = JSON.parse((await loadSnapshot(noMap, { now })).extraFiles!["action-provenance.json"]!);
  expect(unknown.format).toBe(PROVENANCE_PATH_FORMAT); expect(unknown.records[0].repositoryPath).toBeNull();
});
