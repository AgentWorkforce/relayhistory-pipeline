/** CI guard: production deployment identifiers must not appear in engine source.
 *
 * Engine paths (src/, packages/, scripts/) ship in the open-source repository, so
 * the production AWS account, Supabase project ref and pinned EC2 instance must
 * come from operator configuration (see src/shared/deploy-target.ts). The guard
 * stores only SHA-256 digests, so this file never discloses the values it blocks.
 *
 *   bun scripts/check-engine-identifiers.ts      exit 1 and list file:line on any hit
 */
import { createHash } from "node:crypto";
import { execFileSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { resolve, dirname } from "node:path";
import { fileURLToPath } from "node:url";

/** sha256 of each blocked production identifier, with the token shape that finds it. */
export const BLOCKED: readonly { kind: "aws-account" | "supabase-ref" | "ec2-instance"; sha256: string }[] = [
  { kind: "aws-account", sha256: "8cb9947f94ae75353426bcba152dd91d958ba90a38ddb677912b527c42a7093a" },
  { kind: "supabase-ref", sha256: "963aa096d6944d19073ec6e37b2dd5162ed1f014d1743c2d55b508f066f6977c" },
  { kind: "ec2-instance", sha256: "591fb1d977361fd1ca1451ef0950fbe8fac30cd5fc096b30ef963bd214bd5542" },
];
export const ENGINE_ROOTS = ["src", "packages", "scripts"] as const;
/** Hosted-ops paths that still carry production identifiers. Each is owned by a
 * separate lane and must leave engine paths (moved to the private ops repository)
 * or be parameterized there; do not add engine code to this list.
 *  - workflow-bound: drive .github/workflows/apply-session-schema.yml, whose
 *    typed approval and target change together with a GitHub environment input.
 *  - private-ops: production runbook tooling excluded from the public tree. */
export const PENDING_MOVE: readonly { path: string; lane: "workflow-bound" | "private-ops" }[] = [
  { path: "src/hoopdb/schema-release-contract.ts", lane: "workflow-bound" },
  { path: "scripts/schema-release-owner-operator-plan.ts", lane: "workflow-bound" },
  { path: "src/hoopdb/__tests__/schema-release-owner-operator-plan.test.ts", lane: "workflow-bound" },
  { path: "src/hoopdb/owner-operator-apply.ts", lane: "private-ops" },
  { path: "scripts/sessions-prod-bucket-notification.sh", lane: "private-ops" },
  { path: "scripts/ops/", lane: "private-ops" },
  { path: "packages/session-read-worker/", lane: "private-ops" },
];
const sha = (s: string) => createHash("sha256").update(s).digest("hex");
const blocked = new Map(BLOCKED.map((b) => [b.sha256, b.kind]));
/** Every candidate token: each 12-digit window of a digit run, each 20-char window
 * of a lowercase-alphanumeric run, and each EC2 instance-id shape. */
function* candidates(line: string): Generator<string> {
  for (const m of line.matchAll(/[0-9]{12,}/g)) for (let i = 0; i + 12 <= m[0].length; i++) yield m[0].slice(i, i + 12);
  for (const m of line.matchAll(/[a-z0-9]{20,}/g)) for (let i = 0; i + 20 <= m[0].length; i++) yield m[0].slice(i, i + 20);
  for (const m of line.matchAll(/i-[0-9a-f]{17}/g)) yield m[0];
}
export interface Hit { path: string; line: number; kind: string }
export function scanText(path: string, text: string, digests: Map<string, string> = blocked): Hit[] {
  const hits: Hit[] = [];
  text.split("\n").forEach((line, i) => {
    const seen = new Set<string>();
    for (const token of candidates(line)) {
      if (seen.has(token)) continue;
      seen.add(token);
      const kind = digests.get(sha(token));
      if (kind) hits.push({ path, line: i + 1, kind });
    }
  });
  return hits;
}
export const pendingMove = (path: string) => PENDING_MOVE.some((p) => (p.path.endsWith("/") ? path.startsWith(p.path) : path === p.path));
export function scanRepository(root: string): Hit[] {
  const files = execFileSync("git", ["ls-files", "-z", "--", ...ENGINE_ROOTS], { cwd: root, encoding: "utf8", maxBuffer: 64 << 20 }).split("\0").filter(Boolean);
  const hits: Hit[] = [];
  for (const path of files) {
    if (pendingMove(path)) continue;
    let text: string;
    try { text = readFileSync(resolve(root, path), "utf8"); } catch { continue; } // deleted in the working tree, or a submodule
    if (text.includes("\0")) continue; // binary
    hits.push(...scanText(path, text));
  }
  return hits;
}
if (import.meta.main) {
  const root = resolve(dirname(fileURLToPath(import.meta.url)), "..");
  const hits = scanRepository(root);
  for (const h of hits) console.error(`${h.path}:${h.line}: production ${h.kind} identifier in engine source; supply it as operator configuration (src/shared/deploy-target.ts)`);
  if (hits.length) process.exit(1);
  console.log(JSON.stringify({ engineIdentifierGuard: "clean", roots: ENGINE_ROOTS, pendingMove: PENDING_MOVE.length }));
}
