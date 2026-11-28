/**
 * bun run e2e/cutover/run.ts [--size ci|representative] [--users N --sessions N --batches N --records N --payload N]
 *                            [--simulate-pending] [--out DIR]
 *
 * The synthetic per-org cutover qualification (harness.ts). Local only: PGlite with every migration on this checkout
 * and a filesystem object store under $TMPDIR; no network, no credentials, synthetic ids. Writes report.json and
 * report.md to --out (default $RELAYFLOW_STATE_DIR/synthetic-cutover or .relayflow/synthetic-cutover). Exits 0 only
 * when the verdict is QUALIFIED; until the release-path heads land it exits 1 (fail closed).
 */
import { mkdirSync, writeFileSync } from "node:fs";
import { join } from "node:path";
import { SIZE_CI, SIZE_REPRESENTATIVE, type FixtureSize } from "./fixture.ts";
import { reportMarkdown, runCutoverQualification } from "./harness.ts";

const argv = process.argv.slice(2);
const flag = (n: string) => argv.includes(n);
const val = (n: string) => { const i = argv.indexOf(n); return i >= 0 ? argv[i + 1] : undefined; };
const int = (n: string, d: number) => { const v = val(n); if (v === undefined) return d; const x = Number(v); if (!Number.isSafeInteger(x) || x < 1) throw new Error(`${n} must be a positive integer`); return x; };
const preset = val("--size") ?? "ci";
if (preset !== "ci" && preset !== "representative") throw new Error("--size is ci or representative");
const base = preset === "ci" ? SIZE_CI : SIZE_REPRESENTATIVE;
const size: FixtureSize = { users: int("--users", base.users), sessionsPerUser: int("--sessions", base.sessionsPerUser), batchesPerSession: int("--batches", base.batchesPerSession),
  recordsPerBatch: int("--records", base.recordsPerBatch), payloadBytes: int("--payload", base.payloadBytes) };
if ((process.env.TMPDIR ?? "/tmp").startsWith("/tmp")) throw new Error("set TMPDIR to a disk path (the fixture's object store lives there)");

const report = await runCutoverQualification({ size, simulatePending: flag("--simulate-pending"), log: (l) => console.error(l) });
const out = val("--out") ?? join(process.env.RELAYFLOW_STATE_DIR ?? ".relayflow", "synthetic-cutover");
mkdirSync(out, { recursive: true });
writeFileSync(join(out, "report.json"), JSON.stringify(report, null, 2) + "\n");
writeFileSync(join(out, "report.md"), reportMarkdown(report));
console.log(reportMarkdown(report));
process.exit(report.verdict === "QUALIFIED" ? 0 : 1);
