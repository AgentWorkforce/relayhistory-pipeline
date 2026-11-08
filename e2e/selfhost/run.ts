#!/usr/bin/env bun
/**
 * One command: `bun run e2e:selfhost`.
 * Builds the services image, brings up the self-contained stack (Postgres,
 * MinIO, migrations, role bootstrap, the Control introspection stub, ingest,
 * consumer, read API), runs the acceptance checks, and tears everything down.
 * Set E2E_KEEP=1 to leave the stack up for debugging. Exit non-zero on any FAIL.
 */
import { buildImage, createStack } from "./stack.ts";
import { runAcceptance, type Check } from "./acceptance.ts";

const line = (c: Check) => console.log(`${c.result.padEnd(7)} ${c.name}  ${c.detail}`);
let failed = false;
if (!process.env.E2E_SKIP_BUILD) {
  console.log("building 96bulls/services:e2e ...");
  buildImage();
}
const stack = createStack();
const down = () => {
  if (process.env.E2E_KEEP === "1") {
    console.log("E2E_KEEP=1: stack left up (project rh-e2e)");
    return;
  }
  stack.compose(["down", "-v", "--remove-orphans"], { quiet: true, allowFail: true });
  stack.cleanup();
};
for (const signal of ["SIGINT", "SIGTERM"] as const) process.on(signal, () => { down(); process.exit(130); });
try {
  console.log("starting stack ...");
  // A crashed earlier run must not leak its volumes into this one.
  stack.compose(["down", "-v", "--remove-orphans"], { quiet: true, allowFail: true });
  stack.compose(["up", "-d"], { quiet: true });
  stack.resolveUrls();
  const ready = async (url: string) => {
    for (let i = 0; i < 90; i++) {
      try {
        if ((await fetch(url, { signal: AbortSignal.timeout(3000) })).ok) return;
      } catch {}
      await new Promise((r) => setTimeout(r, 2000));
    }
    throw new Error(`service did not become ready: ${new URL(url).pathname}`);
  };
  await ready(`${stack.ingestUrl}/readyz`);
  await ready(`${stack.readUrl}/healthz`);
  const checks = await runAcceptance(stack, line);
  const counts = { PASS: 0, FAIL: 0, BLOCKED: 0 };
  for (const c of checks) counts[c.result]++;
  console.log(`\nresult: ${counts.PASS} passed, ${counts.FAIL} failed, ${counts.BLOCKED} blocked`);
  failed = counts.FAIL > 0;
} catch (error) {
  failed = true;
  console.error(`e2e aborted: ${(error as Error).message}`);
  for (const svc of ["session-ingest", "session-read-api", "session-pipeline-consumer"]) {
    const logs = stack.compose(["logs", "--no-color", "--tail", "20", svc], { quiet: true, allowFail: true });
    console.error(`--- ${svc}\n${logs.stdout}`);
  }
} finally {
  down();
}
process.exit(failed ? 1 : 0);
