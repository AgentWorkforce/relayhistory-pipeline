/**
 * The replay harness CLI (docs/session-intel-replay.md). Offline only: no network, no database and no
 * credentials. The harness reads no environment itself; src/shared/log.ts (loaded with the advisor scan) reads
 * OTEL_SERVICE_NAME, DEPLOYMENT and DEPLOYMENT_ID, and a scan warning prints org and workspace ids to stderr.
 * Outputs carry session ids, so --out must be outside this repository (a synthetic run may pass --allow-repo-out).
 *
 *   bun run src/session-intel/replay/cli.ts replay   --snapshot <dir> --out <dir> [--interval-ms N]
 *   bun run src/session-intel/replay/cli.ts score    --alerts <alerts.json> --labels <labels.json> --out <dir> [--split dev|heldout] [--force [--reason <text>]] [--budget N]
 *   bun run src/session-intel/replay/cli.ts packet   --alerts <alerts.json> [--alerts ...] --seed <s> --out <dir> [--budget N]
 *   bun run src/session-intel/replay/cli.ts verdicts --key <key.json> --verdicts <verdicts.jsonl|json> --out <dir>
 */
import { existsSync, readFileSync, realpathSync } from "node:fs";
import { basename, dirname, isAbsolute, join, relative, resolve, sep } from "node:path";
import { fileURLToPath } from "node:url";
import { parseArgs } from "node:util";
import { buildPacket, parsePacketKey, parseVerdicts, verdictMarkdown, verdictReport } from "./adjudicate.ts";
import { advisorDetector } from "./detector.ts";
import { parseLabels, type Split } from "./labels.ts";
import { DEFAULT_INTERVAL_MS, parseReplayResult, replaySnapshot, type ReplayResult } from "./replay.ts";
import { labelsDigest, recordHeldoutScoring, scoreMarkdown, scoreReplay } from "./score.ts";
import { loadSnapshot } from "./snapshot.ts";
import { writeAtomic } from "./util.ts";

const REPO_ROOT = resolve(fileURLToPath(new URL("../../..", import.meta.url)));
const USAGE = `usage: cli.ts <replay|score|packet|verdicts> [options]; see docs/session-intel-replay.md`;

export interface CliIo { out(line: string): void; err(line: string): void; now?: Date }
const defaultIo: CliIo = { out: (l) => console.log(l), err: (l) => console.error(l) };

/** The path with symlinks resolved: realpath of its nearest existing ancestor, then the parts not yet created. */
export function realPathOf(path: string): string {
  let head = resolve(path);
  const tail: string[] = [];
  while (!existsSync(head)) {
    const up = dirname(head);
    if (up === head) break;
    tail.unshift(basename(head));
    head = up;
  }
  return join(realpathSync(head), ...tail);
}

/** Inside `root` unless the relative path climbs out (exactly "..", or ".." then a separator) or is absolute. */
export function isInside(root: string, path: string): boolean {
  const rel = relative(realPathOf(root), realPathOf(path));
  return !(rel === ".." || rel.startsWith(`..${sep}`) || isAbsolute(rel));
}

export function outDir(value: string | undefined, allowRepo: boolean, repoRoot = REPO_ROOT): string {
  if (!value) throw new Error("--out is required");
  const dir = resolve(value);
  // Symlinks are resolved first, so a link outside the repository that points into it is still inside.
  if (!allowRepo && isInside(repoRoot, dir))
    throw new Error("--out is inside the repository; replay outputs carry session ids and belong outside it (--allow-repo-out for synthetic runs)");
  return dir;
}

const readJson = (path: string): unknown => JSON.parse(readFileSync(path, "utf8"));
const readAlerts = (path: string): ReplayResult => parseReplayResult(readJson(path));
const json = (v: unknown) => `${JSON.stringify(v, null, 2)}\n`;

function replayMarkdown(r: ReplayResult): string {
  const incomplete = r.incomplete;
  const lines = [
    "# Replay", "",
    `Snapshot manifest ${r.snapshot.manifestSha256.slice(0, 16)}, ${r.snapshot.sessions} sessions, window ${r.snapshot.window.since} to ${r.snapshot.window.until}, ${r.ticks} ticks every ${r.intervalMs / 1_000}s.`,
    ...(incomplete.length ? ["", `INCOMPLETE: ${incomplete.join("; ")}. Do not score this replay.`] : []), "",
  ];
  for (const d of r.detectors) {
    const mine = r.alerts.filter((a) => a.detector === d.name);
    const rows = new Map<string, number>();
    for (const a of mine) rows.set(`${a.urgency} | ${a.kind}`, (rows.get(`${a.urgency} | ${a.kind}`) ?? 0) + 1);
    lines.push(`## ${d.name} ${d.version} (${d.hash.slice(0, 12)})`, "", "| Urgency | Kind | Alerts |", "|---|---|---|",
      ...[...rows].sort().map(([k, n]) => `| ${k} | ${n} |`), "",
      `Diagnostics: ${Object.entries(d.diagnostics).map(([k, v]) => `${k}=${v}`).join(" ") || "none"}`, "");
  }
  return `${lines.join("\n")}\n`;
}

export async function main(argv: string[], io: CliIo = defaultIo): Promise<number> {
  const [command, ...rest] = argv;
  const { values } = parseArgs({
    args: rest, strict: true, allowPositionals: false,
    options: {
      snapshot: { type: "string" }, out: { type: "string" }, "interval-ms": { type: "string" },
      alerts: { type: "string", multiple: true }, labels: { type: "string" }, split: { type: "string" }, force: { type: "boolean" }, reason: { type: "string" },
      budget: { type: "string" }, seed: { type: "string" }, key: { type: "string" }, verdicts: { type: "string" },
      "allow-repo-out": { type: "boolean" },
    },
  });
  const allowRepo = values["allow-repo-out"] === true;
  const budget = values.budget === undefined ? undefined : Number(values.budget);
  if (budget !== undefined && !Number.isFinite(budget)) throw new Error("--budget must be a number");

  if (command === "replay") {
    if (!values.snapshot) throw new Error("--snapshot is required");
    const out = outDir(values.out, allowRepo);
    const intervalMs = values["interval-ms"] === undefined ? DEFAULT_INTERVAL_MS : Number(values["interval-ms"]);
    const snapshot = await loadSnapshot(resolve(values.snapshot), { now: io.now });
    const result = await replaySnapshot(snapshot, { detectors: [advisorDetector()], intervalMs });
    // A workspace scan that failed or a row the store refused means alerts may be missing; alerts.json says so.
    const incomplete = result.incomplete;
    await writeAtomic(join(out, "alerts.json"), json(result));
    await writeAtomic(join(out, "report.json"), json({ ...result, alerts: undefined, alertCount: result.alerts.length, incomplete }));
    await writeAtomic(join(out, "report.md"), replayMarkdown(result));
    io.out(`replay: ${result.alerts.length} alerts over ${result.ticks} ticks -> ${out}`);
    if (incomplete.length) { io.err(`replay incomplete: ${incomplete.join("; ")}`); return 1; }
    return 0;
  }

  if (command === "score") {
    if (!values.alerts?.length || values.alerts.length !== 1) throw new Error("score takes exactly one --alerts");
    if (!values.labels) throw new Error("--labels is required");
    const out = outDir(values.out, allowRepo);
    const split = (values.split ?? "dev") as Split;
    if (split !== "dev" && split !== "heldout") throw new Error("--split must be dev or heldout");
    const result = readAlerts(resolve(values.alerts[0]!));
    const labelBytes = readFileSync(resolve(values.labels));
    const labels = parseLabels(JSON.parse(labelBytes.toString("utf8")));
    const labelsSha256 = labelsDigest(labelBytes);
    const report = scoreReplay(result, labels, { split, labelsSha256, budgetPer100: budget });
    // The ledger is checked and written before the held-out numbers are written or shown anywhere.
    if (values.reason !== undefined && values.force !== true) throw new Error("--reason is only for a --force held-out re-score");
    if (split === "heldout") await recordHeldoutScoring(out, result, labelsSha256, { force: values.force === true, reason: values.reason, now: io.now });
    // Split-named reports: a dev run never overwrites the held-out report, which cannot be recomputed.
    await writeAtomic(join(out, `report.${split}.json`), json(report));
    await writeAtomic(join(out, `report.${split}.md`), scoreMarkdown(report));
    if (report.incomplete.length) io.err(`score: the replay is incomplete (${report.incomplete.join("; ")}); alerts may be missing`);
    io.out(scoreMarkdown(report));
    return 0;
  }

  if (command === "packet") {
    if (!values.alerts?.length) throw new Error("--alerts is required");
    if (!values.seed) throw new Error("--seed is required");
    const out = outDir(values.out, allowRepo);
    const paths = values.alerts.map((p) => realPathOf(p));
    if (new Set(paths).size !== paths.length) throw new Error("packet: the same --alerts file is given twice");
    const { packet, key } = buildPacket(paths.map(readAlerts), { seed: values.seed, budgetPer100: budget });
    await writeAtomic(join(out, "packet.jsonl"), packet.map((p) => JSON.stringify(p)).join("\n") + (packet.length ? "\n" : ""));
    await writeAtomic(join(out, "key.json"), json(key));
    const summary = { pooled: packet.length, sources: key.sources };
    await writeAtomic(join(out, "report.json"), json(summary));
    await writeAtomic(join(out, "report.md"), `# Adjudication packet\n\n${packet.length} pooled alerts. Send packet.jsonl to the adjudicators; keep key.json private.\n`);
    io.out(`packet: ${packet.length} pooled alerts -> ${out}`);
    return 0;
  }

  if (command === "verdicts") {
    if (!values.key || !values.verdicts) throw new Error("--key and --verdicts are required");
    const out = outDir(values.out, allowRepo);
    const key = parsePacketKey(readJson(resolve(values.key)));
    const text = readFileSync(resolve(values.verdicts), "utf8");
    const raw = text.trimStart().startsWith("[") ? JSON.parse(text) as unknown[] : text.split("\n").filter((l) => l.trim()).map((l) => JSON.parse(l) as unknown);
    const report = verdictReport(key, parseVerdicts(raw, key));
    await writeAtomic(join(out, "report.json"), json(report));
    await writeAtomic(join(out, "report.md"), verdictMarkdown(report));
    io.out(verdictMarkdown(report));
    return 0;
  }

  io.err(USAGE);
  return 2;
}

if (import.meta.main) {
  try {
    process.exitCode = await main(process.argv.slice(2));
  } catch (err) {
    console.error(`session-intel replay: ${(err as Error).message}`);
    process.exitCode = 1;
  }
}
