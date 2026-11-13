import assert from "node:assert/strict";
import { readFileSync } from "node:fs";
import test from "node:test";

import { checkBunLock, checkPackageLock } from "./check-lockfiles.mjs";

test("this repository's lockfiles resolve from the registry with integrity", () => {
  assert.deepEqual(checkBunLock(readFileSync(new URL("../../bun.lock", import.meta.url), "utf8")), []);
  assert.deepEqual(checkPackageLock(readFileSync(new URL("../package-lock.json", import.meta.url), "utf8"), "flows/package-lock.json"), []);
});

test("bun.lock: another host or a missing hash is refused; workspaces are skipped", () => {
  const ok = `    "a": ["a@1.0.0", "", {}, "sha512-AAAA=="],`;
  const other = `    "b": ["b@1.0.0", "https://evil.example/b-1.0.0.tgz", {}, "sha512-AAAA=="],`;
  const nohash = `    "c": ["c@1.0.0", "", {}],`;
  const ws = `    "@x/y": ["@x/y@workspace:packages/y"],`;
  assert.deepEqual(checkBunLock([ok, ws].join("\n")), []);
  assert.match(checkBunLock(other)[0], /b resolves from https:\/\/evil\.example/);
  assert.match(checkBunLock(nohash)[0], /c has no sha512 integrity/);
});

test("bun.lock: github:, git+ and tarball specs are refused, not skipped", () => {
  for (const line of [
    `    "d": ["d@github:owner/repo#0123456789abcdef0123456789abcdef01234567", {}, "owner-repo-0123456"],`,
    `    "e": ["e@git+https://github.com/owner/repo.git#0123456789abcdef0123456789abcdef01234567", {}, "owner-repo-0123456"],`,
    `    "f": ["f@https://example.com/f-1.0.0.tgz", {}],`,
  ]) {
    const problems = checkBunLock(line);
    assert.equal(problems.length, 1, line);
    assert.match(problems[0], /is not a registry package/);
  }
});

test("package-lock: another host or a missing hash is refused; the organisation's own scope may use GitHub Packages", () => {
  const lock = (packages) => JSON.stringify({ lockfileVersion: 3, packages: { "": {}, ...packages } });
  const npm = { resolved: "https://registry.npmjs.org/a/-/a-1.0.0.tgz", integrity: "sha512-AAAA==" };
  assert.deepEqual(checkPackageLock(lock({ "node_modules/a": npm })), []);
  assert.deepEqual(checkPackageLock(lock({ "node_modules/@agentworkforce/x": { resolved: "https://npm.pkg.github.com/download/@agentworkforce/x/1/abc", integrity: "sha512-AAAA==" } })), []);
  assert.match(checkPackageLock(lock({ "node_modules/@other/x": { resolved: "https://npm.pkg.github.com/download/@other/x/1/abc", integrity: "sha512-AAAA==" } }))[0], /not the npm registry/);
  assert.match(checkPackageLock(lock({ "node_modules/a": { ...npm, integrity: "sha1-AAAA" } }))[0], /no sha512 integrity/);
});
