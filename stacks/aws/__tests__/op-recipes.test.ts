import { describe, expect, it } from "bun:test";
import { execFileSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

/**
 * `op item create|edit` assignments are `[section.]field[type]=value`: an
 * unescaped `.` in a field name splits it into section + field, and later
 * `.label == "a.b"` lookups find nothing. Every documented assignment must
 * escape dots in the field name (`a\.b[password]=...`).
 */
const ROOT = fileURLToPath(new URL("../../..", import.meta.url));
const docs = execFileSync("git", ["ls-files", "*.md"], { cwd: ROOT, encoding: "utf8" }).split("\n").filter((f) => f && !f.startsWith("vendor/"));

/** Field names of op assignments on lines that run `op item create|edit` (or continue one). */
export function opAssignmentFields(markdown: string): string[] {
  const out: string[] = [];
  let inOp = false;
  for (const line of markdown.split("\n")) {
    if (/\bop item (create|edit)\b/.test(line)) inOp = true;
    if (inOp) for (const m of line.matchAll(/["']([^"'\s=[\]]+)\[[a-z]+\]=/g)) out.push(m[1]!);
    if (!line.trimEnd().endsWith("\\")) inOp = false;
  }
  return out;
}

describe("1Password recipes in the docs", () => {
  it("the parser sees the RelayAuth recipe's fields", () => {
    expect(opAssignmentFields(readFileSync(`${ROOT}/docs/aws-stage.md`, "utf8"))).toEqual(["internal-secret", "bootstrap-api-key", "signing-key\\.pem", "summary-master-keys", "internal-secret", "workspace-bootstrap-secret"]);
    expect(opAssignmentFields('op item create \\\n  "signing-key.pem[password]=$k"')).toEqual(["signing-key.pem"]);
  });

  it("escape every dot in an op field name", () => {
    const bad = docs.flatMap((f) => opAssignmentFields(readFileSync(`${ROOT}/${f}`, "utf8")).filter((n) => /(^|[^\\])\./.test(n)).map((n) => `${f}: ${n}`));
    expect(bad).toEqual([]);
  });
});
