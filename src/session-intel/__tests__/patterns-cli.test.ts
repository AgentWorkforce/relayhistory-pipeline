import { afterAll, describe, expect, test } from "bun:test";
import { mkdirSync, readFileSync, statSync, symlinkSync, existsSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { main } from "../patterns/cli.ts";
import { sha256Hex } from "../replay/util.ts";
import { actionArtifacts } from "../import/provenance.ts";
import { main as importMain } from "../import/cli.ts";
import { session, tmp as makeTmp, writeSnapshot } from "./helpers.ts";

const ownDirs: string[] = [];
const tmp = () => { const dir = makeTmp(); ownDirs.push(dir); return dir; };
afterAll(() => { for (const dir of ownDirs) rmSync(dir, { recursive: true, force: true }); });

const now = new Date("2026-11-02T00:00:00Z");
const io = () => ({ now, out: (_: string) => {} });
function source() { const dir = tmp(); writeSnapshot(dir, [session("fixture-a")]); return dir; }

describe("offline pattern CLI", () => {
  test("synthetic raw events import into the strict action consumer without reading the reverse map", async () => {
    const base = tmp(), input = join(base, "events.jsonl"), map = join(base, "repo-map.json"), snapshot = join(base, "snapshot"), out = join(base, "analysis");
    const roots = ["a", "b"].map((repo) => ({ directory: `/fixture/${repo}`, repository: `example.test/team/${repo}`, observedAt: "2026-11-01T09:00:00.000Z" }));
    writeFileSync(map, JSON.stringify({ format: "session-intel-repository-map/1", roots }), { mode: 0o600 });
    writeFileSync(input, [["one", "a"], ["one", "b"], ["two", "b"]].map(([sessionId, repo]) => JSON.stringify({
      sessionId, kind: "write", ts: "2026-11-01T09:01:00.000Z", cwd: `/fixture/${repo}`, files: ["src/shared.ts"],
    })).join("\n"));
    const stdin = (async function* () { yield "synthetic-import-consumer-salt"; })();
    expect(await importMain(["--format", "generic", "--in", input, "--out", snapshot, "--salt-stdin", "--repository-map", map,
      "--delete-by", "2026-11-10T00:00:00Z"], { ...io(), err: () => {}, stdin })).toBe(0);
    // The consumer needs only the snapshot directory. The map and private mapping are not inputs.
    rmSync(map);
    expect(await main(["--snapshot", snapshot, "--out", out, "--action-scope"], io())).toBe(0);
    const report = JSON.parse(readFileSync(join(out, "report.json"), "utf8"));
    expect(report.profile.all).toMatchObject({ sessions: 2, writes: 3 });
    expect(report.profile.byRepository).toHaveLength(2);
    expect(report.candidates).toHaveLength(1);
    expect(report.coverage.actionScope).toMatchObject({ records: 3, missing: 0, unattributed: 0 });
    expect(JSON.stringify(report)).not.toContain("/fixture/");
  });
  test("action scope is explicit, strict and manifest-bound; failures create no output", async () => {
    const dir = source(), missingOut = join(tmp(), "missing");
    await expect(main(["--snapshot", dir, "--out", missingOut, "--action-scope"], io())).rejects.toThrow("manifest-bound");
    expect(existsSync(missingOut)).toBe(false);
    const manifestPath = join(dir, "manifest.json"), manifest = JSON.parse(readFileSync(manifestPath, "utf8"));
    const body = actionArtifacts([], manifest.deleteBy, null, { sessionsSha256: manifest.files["sessions.jsonl"], implementationSha256: "b".repeat(64) }).body;
    writeFileSync(join(dir, "action-provenance.json"), body); manifest.files["action-provenance.json"] = sha256Hex(body);
    writeFileSync(manifestPath, JSON.stringify(manifest));
    const out = join(tmp(), "scoped");
    expect(await main(["--snapshot", dir, "--out", out, "--action-scope"], io())).toBe(0);
    const report = JSON.parse(readFileSync(join(out, "report.json"), "utf8"));
    expect(report.format).toBe("session-intel-patterns/2");
    expect(report.coverage.actionScope.mode).toBe("retrospective_action_claims");
    expect(readFileSync(join(out, "report.md"), "utf8")).toContain("not early warnings");
    expect(JSON.stringify(report.input)).not.toContain("actionScope");
    const legacy = join(tmp(), "legacy");
    expect(await main(["--snapshot", dir, "--out", legacy], io())).toBe(0);
    expect(JSON.parse(readFileSync(join(legacy, "report.json"), "utf8")).format).toBe("session-intel-patterns/1");
  });
  test("private outputs have pinned bytes/code, inherited retention and no score; existing outputs are refused", async () => {
    const dir = source(), out = join(tmp(), "analysis");
    expect(await main(["--snapshot", dir, "--out", out, "--delete-by", "2026-11-05T00:00:00Z"], io())).toBe(0);
    const manifest = JSON.parse(readFileSync(join(out, "manifest.json"), "utf8"));
    expect(manifest.deleteBy).toBe("2026-11-05T00:00:00.000Z"); expect(manifest.implementationSha256).toMatch(/^[a-f0-9]{64}$/u);
    expect(statSync(out).mode & 0o777).toBe(0o700);
    for (const [file, hash] of Object.entries(manifest.files)) {
      const path = join(out, file); expect(statSync(path).mode & 0o777).toBe(0o600); expect(sha256Hex(readFileSync(path))).toBe(hash);
    }
    const report = JSON.parse(readFileSync(join(out, "report.json"), "utf8")); expect(report.coverage.quality).toBe("not_evaluated");
    await expect(main(["--snapshot", dir, "--out", out], io())).rejects.toThrow();
    expect(JSON.parse(readFileSync(join(out, "manifest.json"), "utf8"))).toEqual(manifest);
  });
  test("outputs cannot escape into another checkout through a symlink", async () => {
    const base = tmp(), repo = join(base, "checkout"), alias = join(base, "alias");
    mkdirSync(repo); mkdirSync(join(repo, ".git")); symlinkSync(repo, alias);
    await expect(main(["--snapshot", source(), "--out", join(alias, "report")], io())).rejects.toThrow("outside every repository");
    expect(existsSync(join(repo, "report"))).toBe(false);
  });
  test("multiple sources need the explicit namespace assertion, expired data and extended retention are refused", async () => {
    const one = source(), two = source(), out = join(tmp(), "analysis");
    await expect(main(["--snapshot", one, "--snapshot", two, "--out", out], io())).rejects.toThrow("identity-space");
    await expect(main(["--snapshot", one, "--out", out, "--delete-by", "2100-01-01T00:00:00Z"], io())).rejects.toThrow("shorten");
    await expect(main(["--snapshot", one, "--out", out], { now: new Date("2100-01-01"), out: () => {} })).rejects.toThrow("deleteBy");
    expect(existsSync(out)).toBe(false);
  });
  test("invalid argv does not echo potentially private values", async () => {
    const privateValue = "fixture-private-value";
    try { await main(["--unknown", privateValue], io()); throw new Error("expected refusal"); }
    catch (e) { expect((e as Error).message).not.toContain(privateValue); expect((e as Error).message).toContain("never echoed"); }
  });
  test("budget censoring writes the receipt but returns the distinct incomplete exit code", async () => {
    const dir = tmp(), out = join(tmp(), "censored");
    writeSnapshot(dir, ["a", "b", "c"].map((id) => session(id, { writes: [{ path: "src/shared.ts", ts: "2026-11-01T09:10:00.000Z", eventId: id, kind: "write", branch: null }] })));
    expect(await main(["--snapshot", dir, "--out", out, "--max-comparisons", "1"], io())).toBe(3);
    expect(JSON.parse(readFileSync(join(out, "report.json"), "utf8")).coverage).toMatchObject({ scanComplete: false, reasons: ["comparison_budget"] });
  });
});

test("bare repositories, malformed retention and tampered input hashes fail before output", async () => {
  const base = tmp(), bare = join(base, "fixture.git"), out = join(bare, "report");
  mkdirSync(bare); mkdirSync(join(bare, "objects")); mkdirSync(join(bare, "refs")); writeFileSync(join(bare, "HEAD"), "ref: refs/heads/main\n");
  await expect(main(["--snapshot", source(), "--out", out], io())).rejects.toThrow("outside every repository");
  const dir = source(), target = join(tmp(), "report");
  await expect(main(["--snapshot", dir, "--out", target, "--delete-by", "November 5, 2026"], io())).rejects.toThrow("ISO timestamp");
  writeFileSync(join(dir, "sessions.jsonl"), "tampered fixture");
  await expect(main(["--snapshot", dir, "--out", target], io())).rejects.toThrow();
  expect(existsSync(target)).toBe(false);
});
