#!/usr/bin/env node
/** Offline-only evaluator for a reviewed canonical frontier capture. */
import { resolve } from "node:path";
import { evaluateCanonicalFrontier, frontierCanonicalJson } from "../src/session-ingest/frontier-plan.ts";
import { readFrontierBundle } from "../src/session-ingest/frontier-plan-input.ts";

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
  const evidence = await readFrontierBundle(args.evidence);
  const result = evaluateCanonicalFrontier(evidence.capture, { now: args.now, exportBindings: evidence.exportBindings });
  process.stdout.write(frontierCanonicalJson({ ...result, qualification: "OFFLINE_ONLY" }) + "\n");
  if (result.status === "hold") process.exitCode = 2;
}

await main().catch((error: unknown) => {
  const code = (error as { code?: unknown })?.code;
  const allowed = ["frontier_plan_usage", "frontier_plan_evidence_invalid"];
  process.stderr.write(JSON.stringify({ status: "error", code: typeof code === "string" && allowed.includes(code) ? code : "frontier_plan_error" }) + "\n");
  process.exitCode = 1;
});
