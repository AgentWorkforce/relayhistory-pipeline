import { parseDataset, sessionKey } from "./contracts.ts";
import { collectAlerts } from "./collector.ts";
import { fileBaseline } from "./judgment.ts";
import { modelJudge } from "./model.ts";
import { renderAlerts } from "./report.ts";
import { evaluate, parseCases } from "./evaluation.ts";
import { eraseRun, purgeExpiredRuns, readJson, requireUnexpired, writeRun } from "./artifacts.ts";
import { importEvidence } from "./source.ts";

const HELP = `Usage:
  bun run alerts:collect --input dataset.json --out NEW_DIRECTORY --as-of ISO [--replay] [--semantic] [--max-pairs 20]
  bun run alerts:eval --input dataset.json --cases cases.json --out NEW_DIRECTORY [--semantic]
  bun run alerts:import --input authorized-evidence.json --out NEW_DIRECTORY
  bun run alerts:erase --run RUN_DIRECTORY
  bun run alerts:purge --root PRIVATE_ARTIFACT_ROOT

No command sends alerts. Semantic mode requires OVERLAP_MODEL_URL (full Responses endpoint),
OVERLAP_MODEL and OVERLAP_MODEL_KEY. Import accepts normalized authorized evidence only;
there is no direct production API, database or object-store reader.
Production inputs and outputs are private derived copies: use .overlap-eval/, never git.
Import keeps seven days; erase the full run when any source session is erased or access is revoked.`;
export async function main(args: string[]) {
  if (!args.length || args.includes("--help")) { console.log(HELP); return; }
  const command = args.shift();
  const values = new Map<string, string>(), flags = new Set<string>();
  const allowed = new Set(["--input", "--out", "--as-of", "--cases", "--run", "--root", "--max-pairs"]);
  while (args.length) {
    const arg = args.shift()!;
    if (["--semantic", "--replay"].includes(arg)) { flags.add(arg); continue; }
    const value = args.shift();
    if (!allowed.has(arg) || !value || value.startsWith("--") || values.has(arg)) throw new Error("invalid_arguments");
    values.set(arg, value);
  }
  const required = (name: string) => { const value = values.get(name); if (!value) throw new Error(`missing_${name.slice(2)}`); return value; };
  if (command === "erase") { await eraseRun(required("--run")); console.log("Erased derived run."); return; }
  if (command === "purge") { console.log(JSON.stringify({ erased: await purgeExpiredRuns(required("--root")) })); return; }
  const out = required("--out");
  if (command === "import") {
    const data = importEvidence(await readJson(required("--input")));
    await writeRun(out, { "dataset.json": data }, { expiresAt: data.provenance.expiresAt, sessionKeys: data.sessions.map(sessionKey) });
    console.log(JSON.stringify({ sessions: data.sessions.length, complete: data.provenance.complete })); return;
  }
  if (!["collect", "eval"].includes(command ?? "")) throw new Error("unknown_command");
  const input = parseDataset(await readJson(required("--input")));
  const dataset = input.provenance.kind === "production" ? importEvidence(input) : input;
  requireUnexpired(dataset);
  const judge = flags.has("--semantic") ? modelJudge({ url: process.env.OVERLAP_MODEL_URL ?? "", model: process.env.OVERLAP_MODEL ?? "", apiKey: process.env.OVERLAP_MODEL_KEY ?? "" }) : fileBaseline;
  const metadata = { expiresAt: dataset.provenance.expiresAt, sessionKeys: dataset.sessions.map(sessionKey) };
  if (command === "collect") {
    const result = await collectAlerts(dataset, judge, { asOf: required("--as-of"), mode: flags.has("--replay") ? "replay" : "retrospective", lookbackHours: 168, maxPairs: Number(values.get("--max-pairs") ?? "20") });
    await writeRun(out, { "alerts.json": result, "alerts.md": renderAlerts(result, dataset) }, metadata);
    console.log(JSON.stringify({ alerts: result.alerts.length, coverage: result.coverage }));
  } else {
    const cases = parseCases(await readJson(required("--cases")), dataset);
    const result = await evaluate(dataset, cases, judge);
    await writeRun(out, { "eval.json": result }, metadata);
    console.log(JSON.stringify({ source: result.source, development: result.development, heldOut: result.heldOut }));
  }
}
if (import.meta.main) main(process.argv.slice(2)).catch(() => { console.error("overlap_command_failed: check input schema, access, limits and destination configuration; no raw errors are logged."); process.exitCode = 1; });
