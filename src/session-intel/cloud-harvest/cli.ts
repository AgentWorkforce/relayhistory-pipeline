/**
 * Cloud harvest CLI (docs/session-intel-cloud-harvest.md).
 *
 *   CLOUD_HARVEST_TOKEN='op://<vault>/<item>/<field>' op run -- \
 *     bun run src/session-intel/cloud-harvest/cli.ts --cloud-api https://<host>/cloud/api/v1 --workspace <uuid> \
 *       --since <iso> --until <iso> --out <dir outside the repo> --delete-by <iso, at most 14 days out> \
 *       [--concurrency 3] [--max-requests 20000] [--via cloud|reader-mcp]
 *
 * `--via reader-mcp` reads sessions stored since the 2026-11-05 move to the 96Bulls Reader, through the hosted
 * shared-sessions MCP (`--cloud-api` is the same base; the MCP lives at `<base>/mcp/shared-sessions`).
 *
 * Reads session data with a credential: it needs the workspace owner's OK for the stated scope each time.
 */
import { existsSync, mkdirSync, readFileSync, realpathSync, writeFileSync } from "node:fs";
import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { parseArgs } from "node:util";
import { CloudClient } from "./client.ts";
import { runHarvest } from "./harvest.ts";

const REPO_ROOT = realpathSync(resolve(fileURLToPath(new URL("../../..", import.meta.url))));
const DAY = 86_400_000;

async function main(argv: string[]) {
  const { values } = parseArgs({ args: argv, options: {
    "cloud-api": { type: "string" }, workspace: { type: "string" }, since: { type: "string" }, until: { type: "string" },
    out: { type: "string" }, "delete-by": { type: "string" }, concurrency: { type: "string" }, "max-requests": { type: "string" },
    "frame-slices": { type: "string" }, "priority-projects": { type: "string" }, "skip-projects": { type: "string" }, via: { type: "string" },
  } });
  const via = values.via ?? "cloud";
  if (via !== "cloud" && via !== "reader-mcp") throw new Error("--via must be cloud or reader-mcp");
  const t = (v: string | undefined, n: string) => { if (!v || !Number.isFinite(Date.parse(v))) throw new Error(`--${n} must be an ISO time`); return new Date(v).toISOString(); };
  const since = t(values.since, "since"), until = t(values.until, "until"), deleteBy = t(values["delete-by"], "delete-by");
  if (Date.parse(until) <= Date.parse(since) || Date.parse(until) - Date.parse(since) > 120 * DAY) throw new Error("the window must be positive and at most 120 days");
  if (Date.parse(deleteBy) <= Date.now() || Date.parse(deleteBy) - Date.now() > 14 * DAY) throw new Error("--delete-by must be in the next 14 days");
  if (!values.out) throw new Error("--out is required");
  const out = resolve(values.out);
  const inRepo = (p: string) => (p + "/").startsWith(REPO_ROOT + "/");
  // Checked before creating anything (the lexical path), and again after (the real path, through symlinks).
  if (inRepo(out)) throw new Error("--out must be outside this repository");
  mkdirSync(out, { recursive: true, mode: 0o700 });
  if (inRepo(realpathSync(out))) throw new Error("--out must be outside this repository");
  const manifest = join(out, "manifest.json");
  const params = { workspace: values.workspace, since, until, deleteBy, ...(via === "cloud" ? {} : { via }) };
  if (existsSync(manifest)) {
    const prev = JSON.parse(readFileSync(manifest, "utf8"));
    if (JSON.stringify(prev.params) !== JSON.stringify(params)) throw new Error("--out holds a harvest with other parameters");
  } else writeFileSync(manifest, JSON.stringify({ format: "session-intel-cloud-harvest/1", params, createdAt: new Date().toISOString() }, null, 2), { mode: 0o600 });
  const client = CloudClient.fromEnv({ base: values["cloud-api"] ?? "", workspaceId: values.workspace ?? "", maxRequests: Number(values["max-requests"] ?? 20000) });
  const concurrency = Math.max(1, Math.min(6, Number(values.concurrency ?? 3)));
  try {
    const frameSlices = Math.max(1, Math.min(8, Number(values["frame-slices"] ?? 1)));
    const cov = await runHarvest(client, { out, window: { since, until }, concurrency, frameSlices, via,
      priorityProjects: values["priority-projects"]?.split(",").filter(Boolean), skipProjects: values["skip-projects"]?.split(",").filter(Boolean),
      log: (m) => console.error(client.redact(m)) });
    console.log(JSON.stringify({ complete: cov.complete, capped: cov.capped, sessions: cov.frame.sessions, gaps: cov.frame.gaps.length, requests: cov.requests.total }));
    process.exit(cov.capped ? 3 : 0);
  } catch (e) {
    console.error(`cloud-harvest: ${client.redact((e as Error).message)}`);
    process.exit(1);
  }
}

if (import.meta.main) main(process.argv.slice(2)).catch((e) => { console.error(`cloud-harvest: ${(e as Error).message}`); process.exit(1); });
