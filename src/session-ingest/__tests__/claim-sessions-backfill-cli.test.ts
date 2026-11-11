import { describe, expect, test } from "bun:test";
import { fileURLToPath } from "node:url";

const script = fileURLToPath(new URL("../../../scripts/backfill-ingest-claim-sessions.ts", import.meta.url));
async function refused(args: string[], stdin = "{}") {
  const child = Bun.spawn([process.execPath, script, ...args], { stdin: "pipe", stdout: "pipe", stderr: "pipe" });
  // The CLI refuses before reading stdin, so it may already have exited; EPIPE on write/end is the expected outcome.
  const ignoreEpipe = (error: unknown) => {
    if ((error as { code?: unknown }).code !== "EPIPE") throw error;
  };
  try {
    const written = child.stdin.write(stdin) as unknown;
    if (written instanceof Promise) await written.catch(ignoreEpipe);
    const ended = child.stdin.end() as unknown;
    if (ended instanceof Promise) await ended.catch(ignoreEpipe);
  } catch (error) {
    ignoreEpipe(error);
  }
  const [code, stdout, stderr] = await Promise.all([child.exited, new Response(child.stdout).text(), new Response(child.stderr).text()]);
  expect(code).toBe(1);
  expect(stderr).toBe("");
  return JSON.parse(stdout);
}

describe("claim-session backfill CLI refuses an omitted operator bound before stdin or connection", () => {
  for (const flag of ["--batch-size", "--statement-timeout-ms", "--max-batches"]) {
    test(`${flag} requires a value, including before the next flag`, async () => {
      // Invalid JSON proves argument admission completes before stdin parsing, and thus before pg.Client.
      expect(await refused([flag], "not-json")).toEqual({ refused: "flag_value_required" });
      expect(await refused([flag, "--dry-run"], "not-json")).toEqual({ refused: "flag_value_required" });
    });
  }
  test("invalid values refuse; valid values reach the missing-owner check without a database client", async () => {
    for (const [flag, code] of [["--batch-size", "batch_size_invalid"], ["--statement-timeout-ms", "statement_timeout_invalid"], ["--max-batches", "max_batches_invalid"]]) {
      expect(await refused([flag!, "nope"], "not-json")).toEqual({ refused: code });
    }
    expect(await refused(["--batch-size", "1", "--statement-timeout-ms", "100", "--max-batches", "1", "--dry-run"])).toEqual({ refused: "owner_url_required" });
    expect(await refused([])).toEqual({ refused: "owner_url_required" });
  });
});
