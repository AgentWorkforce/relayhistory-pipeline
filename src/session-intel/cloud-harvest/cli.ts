/**
 * Cloud harvest CLI (docs/session-intel-cloud-harvest.md).
 *
 *   CLOUD_HARVEST_TOKEN='op://<vault>/<item>/<field>' op run -- \
 *     bun run src/session-intel/cloud-harvest/cli.ts --cloud-api https://<host>/cloud/api/v1 --workspace <uuid> \
 *       --since <iso> --until <iso> --out <dir outside the repo> --delete-by <iso, at most 14 days out> \
 *       [--concurrency 3] [--max-requests 20000] [--max-rows 25000] [--via cloud|reader-mcp] [--accept-client-side-window]
 *
 * `--max-requests`, `--max-rows` and `--concurrency` (at most 5) are fixed when the harvest is created; the request
 * ledger counts every request, retries included, across every resume, and one process holds the harvest at a time. Neither reader bounds event times on the server, so each is refused unless
 * `--accept-client-side-window` records that the approved scope allows reading and dropping out-of-window events.
 *
 * `--via reader-mcp` reads sessions stored since the 2026-11-05 move to the 96Bulls Reader, through the hosted
 * shared-sessions MCP (`--cloud-api` is the same base; the MCP lives at `<base>/mcp/shared-sessions`).
 *
 * Reads session data with a credential: it needs the workspace owner's OK for the stated scope each time.
 */
import { existsSync, realpathSync } from "node:fs";
import { join, resolve } from "node:path";
import { fileURLToPath } from "node:url";
import { parseArgs } from "node:util";
import { CloudClient } from "./client.ts";
import { READERS, type RowBudget, RowCeiling, runHarvest } from "./harvest.ts";
import { lockDir, outsideRoot, readPrivate, writePrivate } from "./private-fs.ts";

const REPO_ROOT = realpathSync(resolve(fileURLToPath(new URL("../../..", import.meta.url))));
const DAY = 86_400_000;

/** The read scope's hard limits. Fixed when the harvest is created; a resume can neither raise nor reset them. */
export interface Limits { maxRequests: number; maxRows: number; concurrency: number }
export const MAX_CONCURRENCY = 5;
export interface Manifest { format: string; params: unknown; createdAt: string; limits: Limits; requestsUsed: number; rowsReceived: number }

const posInt = (v: string | undefined, name: string, dflt: number, max = Number.MAX_SAFE_INTEGER) => {
  const n = Number(v ?? dflt);
  if (!Number.isSafeInteger(n) || n <= 0 || n > max) throw new Error(`--${name} must be a positive integer${max < Number.MAX_SAFE_INTEGER ? ` of at most ${max}` : ""}`);
  return n;
};

/**
 * The harvest's request ledger and limits, in its manifest. The caller must hold the directory lock
 * (private-fs.ts `lockDir`), so one process spends the ledger at a time. Every request, retries included, is
 * recorded (temporary file, then rename) before it is sent, across every resume: an interrupted run can over-count
 * by its requests in flight but never under-count. A manifest from before the ledger is refused, so a spent
 * budget cannot be reset by starting the ledger fresh.
 */
export function openLedger(manifest: string, params: unknown, args: { maxRequests?: string; maxRows?: string; concurrency?: string }) {
  let m: Manifest;
  if (existsSync(manifest)) {
    m = JSON.parse(readPrivate(manifest)) as Manifest;
    if (JSON.stringify(m.params) !== JSON.stringify(params)) throw new Error("--out holds a harvest with other parameters");
    if (m.format !== "session-intel-cloud-harvest/4") throw new Error("--out holds a harvest without a request and row ledger; it cannot be resumed");
    // Restored counters and limits are validated before any request: spent amounts are non-negative integers no
    // larger than their limits, and every limit is within what a new harvest may set.
    const l = m.limits ?? ({} as Limits), int = (n: unknown) => Number.isSafeInteger(n) && (n as number) >= 0;
    if (!int(m.requestsUsed) || !int(m.rowsReceived) || !(int(l.maxRequests) && l.maxRequests > 0) || !(int(l.maxRows) && l.maxRows > 0)
      || !(int(l.concurrency) && l.concurrency >= 1 && l.concurrency <= MAX_CONCURRENCY) || m.requestsUsed > l.maxRequests || m.rowsReceived > l.maxRows) {
      throw new Error("--out holds a request and row ledger that does not validate; it cannot be resumed");
    }
    for (const [k, a] of [["maxRequests", args.maxRequests], ["maxRows", args.maxRows], ["concurrency", args.concurrency]] as const) {
      if (a !== undefined && Number(a) !== m.limits[k]) throw new Error(`--${k.replace(/[A-Z]/g, (c) => `-${c.toLowerCase()}`)} is fixed when the harvest is created`);
    }
  } else {
    const limits = { maxRequests: posInt(args.maxRequests, "max-requests", 20000), maxRows: posInt(args.maxRows, "max-rows", 25000), concurrency: posInt(args.concurrency, "concurrency", 3, MAX_CONCURRENCY) };
    m = { format: "session-intel-cloud-harvest/4", params, createdAt: new Date().toISOString(), limits, requestsUsed: 0, rowsReceived: 0 };
  }
  const write = () => writePrivate(manifest, JSON.stringify(m, null, 2));
  write();
  return {
    manifest: m, remaining: () => m.limits.maxRequests - m.requestsUsed, record: () => { m.requestsUsed++; write(); },
    /** Rows reserved before a request are spent at once (persisted), so an interruption over-counts, never under. */
    rows: {
      reserve: (max: number) => {
        const granted = Math.min(max, m.limits.maxRows - m.rowsReceived);
        if (granted <= 0) throw new RowCeiling(`the approved ceiling of ${m.limits.maxRows} directory rows is reached`);
        m.rowsReceived += granted; write();
        return granted;
      },
      settle: (granted: number, received: number) => {
        if (received > granted) throw new RowCeiling(`a directory page brought ${received} rows for a grant of ${granted}`);
        m.rowsReceived -= granted - received; write();
      },
    } satisfies RowBudget,
  };
}

async function main(argv: string[]) {
  const { values } = parseArgs({ args: argv, options: {
    "cloud-api": { type: "string" }, workspace: { type: "string" }, since: { type: "string" }, until: { type: "string" },
    out: { type: "string" }, "delete-by": { type: "string" }, concurrency: { type: "string" }, "max-requests": { type: "string" },
    "frame-slices": { type: "string" }, "max-rows": { type: "string" }, "priority-projects": { type: "string" }, "skip-projects": { type: "string" }, via: { type: "string" }, "accept-client-side-window": { type: "boolean" },
  } });
  const via = values.via ?? "cloud";
  if (via !== "cloud" && via !== "reader-mcp") throw new Error("--via must be cloud or reader-mcp");
  // A reader that cannot bound event times on the server reads whole session histories over the wire, even
  // though out-of-window events are dropped before derivation. It is refused unless that is explicitly accepted.
  // Every harvest runs under a row ceiling; a reader whose list pages cannot be bounded cannot honour one.
  if (!READERS[via].boundedListPages) throw new Error(`--via ${via} cannot bound its directory pages, so the approved row ceiling cannot be enforced; it is refused`);
  if (!READERS[via].serverSideEventWindow && !values["accept-client-side-window"]) {
    throw new Error(`--via ${via} cannot enforce the event window on the server: it reads each session's whole history and drops events outside the window before derivation. Pass --accept-client-side-window only if the approved read scope allows that.`);
  }
  const t = (v: string | undefined, n: string) => { if (!v || !Number.isFinite(Date.parse(v))) throw new Error(`--${n} must be an ISO time`); return new Date(v).toISOString(); };
  const since = t(values.since, "since"), until = t(values.until, "until"), deleteBy = t(values["delete-by"], "delete-by");
  if (Date.parse(until) <= Date.parse(since) || Date.parse(until) - Date.parse(since) > 120 * DAY) throw new Error("the window must be positive and at most 120 days");
  if (Date.parse(deleteBy) <= Date.now() || Date.parse(deleteBy) - Date.now() > 14 * DAY) throw new Error("--delete-by must be in the next 14 days");
  if (!values.out) throw new Error("--out is required");
  const out = outsideRoot(values.out, REPO_ROOT);
  const release = lockDir(out);
  const manifest = join(out, "manifest.json");
  const params = { workspace: values.workspace, since, until, deleteBy, ...(via === "cloud" ? {} : { via }), ...(values["accept-client-side-window"] ? { clientSideWindow: true } : {}) };
  const ledger = openLedger(manifest, params, { maxRequests: values["max-requests"], maxRows: values["max-rows"], concurrency: values.concurrency });
  const m = ledger.manifest;
  const remaining = ledger.remaining();
  if (remaining <= 0) { console.log(JSON.stringify({ complete: false, capped: true, requestsUsed: m.requestsUsed })); release(); process.exit(3); }
  const client = CloudClient.fromEnv({
    base: values["cloud-api"] ?? "", workspaceId: values.workspace ?? "", maxRequests: remaining, onRequest: ledger.record, concurrency: m.limits.concurrency,
  });
  const concurrency = m.limits.concurrency;
  try {
    const frameSlices = Math.max(1, Math.min(8, Number(values["frame-slices"] ?? 1)));
    const cov = await runHarvest(client, { out, window: { since, until }, concurrency, frameSlices, via, rowBudget: ledger.rows,
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
