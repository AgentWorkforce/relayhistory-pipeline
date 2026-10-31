import { describe, expect, it } from "bun:test";
import { execFileSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";

/**
 * Khaliq, 2026-10-30 (docs/decisions/0001-production-data-plane.md): hosted
 * production HoopDb is Supabase and production AWS is S3 only. The in-cluster
 * CloudNativePG HoopDb exists for the isolated AWS proof stage. A stale
 * "production HoopDb is Aurora / never Supabase" statement must not come back.
 */
const ROOT = fileURLToPath(new URL("../../..", import.meta.url));
const tracked = execFileSync("git", ["ls-files", "*.md", "stacks/**/*.ts", "alchemy.run.ts"], { cwd: ROOT, encoding: "utf8" })
  .split("\n")
  .filter((f) => f && !f.startsWith("vendor/") && !f.endsWith("data-plane-docs.test.ts"));

const FORBIDDEN: { why: string; re: RegExp }[] = [
  { why: "production HoopDb named as Aurora/RDS", re: /production\W+(?:\w+\W+){0,2}hoopdb[^.\n]{0,120}\b(?:aurora|rds)\b/i },
  { why: "production database named as Aurora/RDS", re: /production\s+(?:database|db|data plane)[^.\n]{0,60}\b(?:is|=|:)\s*(?:a\s+)?(?:dedicated\s+)?(?:aurora|rds)\b/i },
  { why: "Supabase excluded from production", re: /never\s+supabase\b/i },
];

describe("production data plane statements", () => {
  it("scans a real file set", () => {
    expect(tracked).toContain("docs/aws-stage.md");
    expect(tracked).toContain("docs/decisions/0001-production-data-plane.md");
  });

  for (const { why, re } of FORBIDDEN)
    it(`no tracked doc or stack file says: ${why}`, () => {
      const hits = tracked.flatMap((f) =>
        readFileSync(`${ROOT}/${f}`, "utf8").split("\n").flatMap((line, n) => (re.test(line) ? [`${f}:${n + 1}: ${line.trim()}`] : [])),
      );
      expect(hits).toEqual([]);
    });

  it("docs/aws-stage.md states production = Supabase + S3 and proof = in-cluster CNPG", () => {
    const doc = readFileSync(`${ROOT}/docs/aws-stage.md`, "utf8");
    expect(doc).toMatch(/\*\*Production HoopDb:\*\* Supabase/);
    expect(doc).toMatch(/\*\*Proof-stage HoopDb:\*\* in-cluster CloudNativePG/);
  });
});
