// The lockfiles the review fleets do not read (review-state.mjs NOT_LOCKFILES keeps them out of the patch
// for size), checked deterministically instead: every package resolves from the public npm registry and
// carries a sha512 integrity, so a change cannot quietly point a dependency at another host or drop its hash.
//
//   node flows/lib/check-lockfiles.mjs <repo root>      exit 1 and one line per problem
//
// bun.lock: each package entry is ["<name>@<version>", "<registry url or empty: the default>", {...}, "sha512-..."];
// workspace and link entries have no registry field and are skipped. flows/package-lock.json: every
// node_modules/* entry has `resolved` on registry.npmjs.org and a sha512 `integrity`.
import { existsSync, readFileSync } from "node:fs";
import { join } from "node:path";
import { fileURLToPath } from "node:url";

const REGISTRY = "https://registry.npmjs.org/";
const OWN_REGISTRY = "https://npm.pkg.github.com/download/@agentworkforce/";

/** Problems in bun.lock text. */
export function checkBunLock(text) {
  const problems = [];
  for (const [i, line] of text.split("\n").entries()) {
    // Every package entry: ["<spec>", ...]. One that does not have a registry entry's shape (a github:, git+
    // or tarball URL spec, which bun writes without the registry field) is refused, never skipped.
    const entry = /^\s*"([^"]+)": \["([^"]+)"/.exec(line);
    if (!entry) continue;
    if (/@(workspace|link|file):/.test(entry[2])) continue;
    const m = /^\s*"([^"]+)": \["([^"]+)", "([^"]*)",/.exec(line);
    if (!m || /@(github:|git\+|git:|https?:)/.test(entry[2])) {
      problems.push(`bun.lock:${i + 1}: ${entry[1]} (${entry[2]}) is not a registry package; only npm registry packages with a sha512 integrity are accepted`);
      continue;
    }
    const [, key, , url] = m;
    if (url !== "" && !url.startsWith(REGISTRY)) problems.push(`bun.lock:${i + 1}: ${key} resolves from ${url.split("/").slice(0, 3).join("/")}, not the npm registry`);
    if (!/"sha512-[A-Za-z0-9+/=]+"\],?\s*$/.test(line)) problems.push(`bun.lock:${i + 1}: ${key} has no sha512 integrity`);
  }
  return problems;
}

/** Problems in a package-lock.json (lockfileVersion 2 or 3). */
export function checkPackageLock(text, name = "package-lock.json") {
  const problems = [];
  const lock = JSON.parse(text);
  for (const [path, entry] of Object.entries(lock.packages ?? {})) {
    if (!path.startsWith("node_modules/") && !path.includes("/node_modules/")) continue;
    if (entry.link) continue;
    // The organisation's own scope is published to GitHub Packages (agent/ installs the layered agent from there).
    const own = path.includes("node_modules/@agentworkforce/") && String(entry.resolved).startsWith(OWN_REGISTRY);
    if (!own && (typeof entry.resolved !== "string" || !entry.resolved.startsWith(REGISTRY))) problems.push(`${name}: ${path} resolves from ${String(entry.resolved ?? "nowhere")}, not the npm registry`);
    if (typeof entry.integrity !== "string" || !entry.integrity.startsWith("sha512-")) problems.push(`${name}: ${path} has no sha512 integrity`);
  }
  return problems;
}

if (process.argv[1] === fileURLToPath(import.meta.url)) {
  const root = process.argv[2] ?? ".";
  const problems = [];
  if (existsSync(join(root, "bun.lock"))) problems.push(...checkBunLock(readFileSync(join(root, "bun.lock"), "utf8")));
  for (const lock of ["flows/package-lock.json", "agent/package-lock.json"]) {
    if (existsSync(join(root, lock))) problems.push(...checkPackageLock(readFileSync(join(root, lock), "utf8"), lock));
  }
  for (const p of problems) console.error(p);
  if (problems.length) process.exitCode = 1;
  else console.log(JSON.stringify({ lockfiles: "registry and integrity clean" }));
}
