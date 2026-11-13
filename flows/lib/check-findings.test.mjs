import assert from "node:assert/strict";
import { execFileSync, spawnSync } from "node:child_process";
import { mkdtempSync, readFileSync, writeFileSync } from "node:fs";
import { tmpdir } from "node:os";
import { join } from "node:path";
import test from "node:test";
import { fileURLToPath } from "node:url";

const CHECK = fileURLToPath(new URL("./check-findings.mjs", import.meta.url));
const finding = (file) => ({ severity: "major", file, line: 1, title: "t", failure_scenario: "s" });

function check(report) {
  const dir = mkdtempSync(join(tmpdir(), "findings-test-"));
  const path = join(dir, "findings.json");
  writeFileSync(path, typeof report === "string" ? report : JSON.stringify(report));
  writeFileSync(join(dir, "files.txt"), "flows/ci.flow.ts\nAGENTS.md\n");
  const r = spawnSync(process.execPath, [CHECK, path, join(dir, "files.txt")], { encoding: "utf8" });
  return { status: r.status, stderr: r.stderr, after: r.status === 0 ? JSON.parse(readFileSync(path, "utf8")) : undefined };
}

test("findings on changed files pass untouched; no findings is a valid answer", () => {
  assert.equal(check({ reviewer: "x", findings: [finding("flows/ci.flow.ts")] }).after.findings.length, 1);
  assert.equal(check({ reviewer: "x", findings: [] }).status, 0);
});

const minor = (file) => ({ ...finding(file), severity: "minor" });

test("a minor finding on an unchanged file is dropped with its reason, not failed", () => {
  const { status, after } = check({ reviewer: "x", findings: [finding("AGENTS.md"), minor("probe/src/main.rs")] });
  assert.equal(status, 0);
  assert.deepEqual(after.findings.map((f) => f.file), ["AGENTS.md"]);
  assert.equal(after.dropped[0].file, "probe/src/main.rs");
  assert.match(after.dropped[0].dropped, /does not touch/);
});

test("a blocker or major on an unchanged file fails the gate rather than vanishing", () => {
  const { status, stderr } = check({ reviewer: "x", findings: [finding("probe/src/main.rs")] });
  assert.equal(status, 1);
  assert.match(stderr, /probe\/src\/main\.rs/);
});

test("absolute and ./ paths to a changed file count as that file", () => {
  const root = execFileSync("git", ["rev-parse", "--show-toplevel"], { encoding: "utf8" }).trim();
  const { status, after } = check({ reviewer: "x", findings: [finding(`${root}/flows/ci.flow.ts`), finding("./AGENTS.md")] });
  assert.equal(status, 0);
  assert.deepEqual(after.findings.map((f) => f.file), ["flows/ci.flow.ts", "AGENTS.md"]);
});

test("a reviewer that did not write the shape fails", () => {
  assert.equal(check("I looked and it seems fine").status, 1);
  assert.equal(check({ findings: [] }).status, 1);
  assert.equal(check({ reviewer: "x", findings: [{ ...finding("AGENTS.md"), severity: "huge" }] }).status, 1);
  assert.equal(check({ reviewer: "x", findings: [{ severity: "nit", file: "AGENTS.md" }] }).status, 1);
  assert.equal(check({ reviewer: "x", findings: [{ ...finding("AGENTS.md"), line: "near the top" }] }).status, 1);
});
