import { expect, test } from "bun:test";
import { Hasher, SessionBuilder, emptyStats, toSnapshotSession, scrubPath } from "../import/normalize.ts";
import { actionArtifacts, actionContext, type ActionRecord } from "../import/provenance.ts";
import { readActionScope } from "../patterns/action-scope.ts";
import type { Snapshot } from "../replay/snapshot.ts";
import { sha256Hex } from "../replay/util.ts";

const now = new Date("2026-11-02T00:00:00.000Z"), ts = "2026-11-01T09:01:00.000Z", deleteBy = "2026-11-10T00:00:00.000Z", h = new Hasher("synthetic-reader-v2-test-salt");
const root = { directory: "/fixture/repo", repository: "example.test/team/repo", observedAt: ts };
function fixture(paths = ["supabase/migrations/0042_add.sql"]) {
  const b = new SessionBuilder("codex", "fixture", emptyStats(), true);
  for (const path of paths) b.write(path, ts, { file: 0, line: 1 }, root.directory, null, "write", actionContext(root.directory, "tool_workdir", { file: 0, line: 1 }, ts));
  return toSnapshotSession(b, h, { orgId: "fixture", workspaceId: "fixture" }, [root], { repositoryPaths: true })!;
}
function snapshot(r: ReturnType<typeof fixture>, records = r.provenance!.records): Snapshot {
  const sessionHash = sha256Hex(JSON.stringify(r.line) + "\n");
  const body = actionArtifacts([{ records, locations: {} }], deleteBy, null, { sessionsSha256: sessionHash, implementationSha256: "a".repeat(64) }, { repositoryPaths: true }).body;
  return { dir: "/unused", manifestSha256: "b".repeat(64), sessions: [r.session], extraFiles: { "action-provenance.json": body },
    manifest: { format: "session-intel-snapshot/1", createdAt: now.toISOString(), deleteBy, window: { since: "2026-11-01T09:00:00.000Z", until: "2026-11-01T10:00:00.000Z" }, files: { "sessions.jsonl": sessionHash, "action-provenance.json": sha256Hex(body) } } };
}
test("v2 is explicit; M4-generated migration, hot-path, extension and hashed vocabularies load", () => {
  const r = fixture(["supabase/migrations/0042_add.sql", "db/migrate/20261105062150_add.rb", "migrations/0042.sql", "generated/private-manifest.json", "src/Private/name.privateext", "src/a.ts", "package.json", "Cargo.lock"]);
  const s = snapshot(r);
  expect(() => readActionScope(s, now)).toThrow("values withheld");
  const records = readActionScope(s, now, { repositoryPaths: true });
  expect(records.size).toBe(8);
  expect([...records.values()].map((r) => r.repositoryPath)).toEqual(r.provenance!.records.map((r) => r.repositoryPath));
  expect(r.provenance!.records[0]!.repositoryPath).toBe(scrubPath("supabase/migrations/0042_add.sql", h));
});
test("v2 refuses raw/absolute/noncanonical path spellings and unsupported encoding descriptors", () => {
  const r = fixture(), good = r.provenance!.records[0]!;
  for (const path of ["supabase/migrations/0042_private_name.sql", "/home/private/src/a.ts", "src/../a.ts", "src/./" + "a".repeat(12) + ".ts", "SRC/" + "a".repeat(12) + ".ts", "private/" + "a".repeat(12) + ".ts", "src/secret.privateext"]) {
    expect(() => readActionScope(snapshot(r, [{ ...good, repositoryPath: path }]), now, { repositoryPaths: true })).toThrow("values withheld");
  }
  const s = snapshot(r), raw = JSON.parse(s.extraFiles!["action-provenance.json"]!);
  raw.repositoryPathEncoding = "unknown";
  const body = JSON.stringify(raw); s.extraFiles!["action-provenance.json"] = body; s.manifest.files["action-provenance.json"] = sha256Hex(body);
  expect(() => readActionScope(s, now, { repositoryPaths: true })).toThrow("values withheld");
});
test("a semantic path requires attributed mapped write scope and an exact target; null is allowed", () => {
  const r = fixture(), good = r.provenance!.records[0]!;
  const edits: ActionRecord[] = [
    { ...good, repositoryTarget: null },
    { ...good, repository: null, repositoryTarget: null, repositoryBasis: "unattributed", repositoryClaims: [] },
    { ...good, repositoryBasis: "trace_declared", repositoryClaims: good.repositoryClaims.map((c) => ({ ...c, basis: "trace_declared" })) },
    { ...good, reason: "unsupported_shell" },
  ];
  for (const bad of edits) expect(() => readActionScope(snapshot(r, [bad]), now, { repositoryPaths: true })).toThrow("values withheld");
  expect(readActionScope(snapshot(r, [{ ...good, repositoryPath: null }]), now, { repositoryPaths: true }).size).toBe(1);
});
