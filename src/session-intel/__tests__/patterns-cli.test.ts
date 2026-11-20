import { afterAll, describe, expect, test } from "bun:test";
import { mkdirSync, readFileSync, statSync, symlinkSync, existsSync, rmSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { main } from "../patterns/cli.ts";
import { sha256Hex } from "../replay/util.ts";
import { session, tmp as makeTmp, writeSnapshot } from "./helpers.ts";

const ownDirs: string[] = [];
const tmp = () => { const dir = makeTmp(); ownDirs.push(dir); return dir; };
afterAll(() => { for (const dir of ownDirs) rmSync(dir, { recursive: true, force: true }); });

const now = new Date("2026-11-02T00:00:00Z");
const io = () => ({ now, out: (_: string) => {} });
function source() { const dir = tmp(); writeSnapshot(dir, [session("fixture-a")]); return dir; }

describe("offline pattern CLI", () => {
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
