#!/usr/bin/env node
/** Offline-only evaluator for a reviewed canonical frontier capture. */
import { open } from "node:fs/promises";
import { resolve } from "node:path";
import { evaluateCanonicalFrontier, frontierCanonicalJson, type CanonicalFrontierCapture, type FrontierExportBinding } from "../src/session-ingest/frontier-plan.ts";

const MAX_EVIDENCE_BYTES = 64 * 1024 * 1024;
const UTC = /^\d{4}-\d{2}-\d{2}T\d{2}:\d{2}:\d{2}\.\d{3,6}Z$/;

function usage(): never {
  throw Object.assign(new Error("usage: canonical-frontier-plan --evidence <local.json> --now <RFC3339>"), { code: "frontier_plan_usage" });
}

function argumentsFor(argv: string[]): { evidence: string; now: Date } {
  let evidence: string | undefined;
  let now: Date | undefined;
  for (let index = 0; index < argv.length; index++) {
    const flag = argv[index];
    if (flag === "--evidence") { if (evidence !== undefined) usage(); evidence = argv[++index]; }
    else if (flag === "--now") {
      if (now !== undefined) usage();
      const value = argv[++index];
      if (!value || !UTC.test(value) || !Number.isFinite(Date.parse(value))) usage();
      now = new Date(value);
    } else usage();
  }
  if (!evidence || !now || evidence.includes("\0") || /^[a-z][a-z0-9+.-]*:/i.test(evidence)) usage();
  return { evidence: resolve(evidence), now };
}

async function main(): Promise<void> {
  const args = argumentsFor(process.argv.slice(2));
  const handle = await open(args.evidence, "r");
  let bytes: Buffer;
  try {
    const metadata = await handle.stat();
    if (!metadata.isFile() || metadata.size > MAX_EVIDENCE_BYTES) throw Object.assign(new Error("frontier evidence exceeds 64 MiB or is not a regular file"), { code: "frontier_plan_evidence_bound" });
    bytes = await handle.readFile();
    if (bytes.byteLength > MAX_EVIDENCE_BYTES) throw Object.assign(new Error("frontier evidence exceeds 64 MiB"), { code: "frontier_plan_evidence_bound" });
  } finally { await handle.close(); }
  let evidence: { capture: CanonicalFrontierCapture; exportBindings: FrontierExportBinding[] };
  try { evidence = JSON.parse(new TextDecoder("utf-8", { fatal: true }).decode(bytes)) as typeof evidence; }
  catch { throw Object.assign(new Error("frontier evidence is not strict UTF-8 JSON"), { code: "frontier_plan_evidence_invalid" }); }
  if (!evidence || typeof evidence !== "object" || !evidence.capture || !Array.isArray(evidence.exportBindings))
    throw Object.assign(new Error("frontier evidence bundle is incomplete"), { code: "frontier_plan_evidence_invalid" });
  const result = evaluateCanonicalFrontier(evidence.capture, { now: args.now, exportBindings: evidence.exportBindings });
  process.stdout.write(frontierCanonicalJson(result) + "\n");
  if (result.status === "hold") process.exitCode = 2;
}

await main().catch((error: unknown) => {
  const value = error as { code?: unknown; message?: unknown };
  process.stderr.write(JSON.stringify({ status: "error", code: typeof value.code === "string" ? value.code : "frontier_plan_error", message: typeof value.message === "string" ? value.message : "frontier plan failed" }) + "\n");
  process.exitCode = 1;
});
