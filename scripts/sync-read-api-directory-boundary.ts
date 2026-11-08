/**
 * Thin wrapper kept for existing docs/tests: the P3b read-api lane is now one manifest
 * (scripts/sessions-function-boundary/lanes/read-api.ts) of the unified generator, which
 * regenerates every lane's shared artifacts, not just this one.
 *   bun scripts/sync-read-api-directory-boundary.ts [--check]
 */
import { BoundaryError } from "./sessions-function-boundary/generate.ts";
import { run } from "./sync-sessions-function-boundary.ts";

const check = process.argv.includes("--check");
try {
  const { drift, generated } = await run({ check });
  if (check && drift.length) {
    for (const p of drift) console.error("read api directory boundary drift: " + p);
    process.exitCode = 1;
  } else {
    const lane = generated.summary.lanes["read-api"]!;
    console.log(JSON.stringify({ functions: lane.functions, grants: lane.grants, boundarySHA: generated.summary.boundarySHA, check }));
  }
} catch (e) {
  if (!(e instanceof BoundaryError)) throw e;
  console.error("read api directory boundary: " + e.message);
  process.exitCode = 1;
}
