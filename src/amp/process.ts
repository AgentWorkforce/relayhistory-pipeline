import { spawn } from "node:child_process";
import { mkdtemp, rm } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import {
  AMP_CLI_BRIDGE,
  AMP_CLI_VERSION,
  type AmpOperation,
} from "./provider.ts";

export type AmpResult = { v: 1; cliVersion: string } & (
  | { ok: true; value: unknown }
  | { ok: false; outcome: "rejected" | "unknown"; code: string }
);
export function failure(
  outcome: "rejected" | "unknown",
  code: string,
): AmpResult {
  return { v: 1, cliVersion: AMP_CLI_VERSION, ok: false, outcome, code };
}
const MAX_OUTPUT = 17 * 1024 * 1024;
// Fixed RelayHistory-owned commands only. Each short-lived process gets its own
// scratch directory and credential environment, never the ingest service's secrets.
export function createAmpProcessRunner(
  spawnProcess: typeof spawn = spawn,
  cliPath = "/opt/relay-amp/node_modules/.bin/amp",
) {
  return async (key: string, operation: AmpOperation): Promise<AmpResult> => {
    let home: string;
    try {
      home = await mkdtemp(join(tmpdir(), "relay-amp-"));
    } catch {
      return failure("rejected", "amp_process_unavailable");
    }
    try {
      return await executeBridge(key, operation, home, spawnProcess, cliPath);
    } catch {
      return failure(
        operation.kind === "inject" ? "unknown" : "rejected",
        "amp_process_unavailable",
      );
    } finally {
      await rm(home, { recursive: true, force: true }).catch(() => {
        console.error("[amp] temporary home cleanup failed");
      });
    }
  };
}
export const runAmpProcess = createAmpProcessRunner();
async function executeBridge(
  key: string,
  operation: AmpOperation,
  home: string,
  spawnProcess: typeof spawn,
  cliPath: string,
): Promise<AmpResult> {
  const uncertain = operation.kind === "inject" ? "unknown" : "rejected";
  return new Promise((resolve) => {
    const child = spawnProcess(process.execPath, ["-e", AMP_CLI_BRIDGE], {
      cwd: home,
      detached: true,
      stdio: ["ignore", "pipe", "ignore"],
      env: {
        PATH: process.env.PATH,
        HOME: home,
        AMP_HOME: home,
        XDG_CONFIG_HOME: join(home, "config"),
        XDG_CACHE_HOME: join(home, "cache"),
        TMPDIR: home,
        RELAY_AMP_CLI_PATH: cliPath,
        AMP_API_KEY: key,
        RELAY_AMP_OPERATION: JSON.stringify(operation),
      },
    });
    const chunks: Buffer[] = [];
    let size = 0;
    let settled = false;
    const kill = () => {
      if (child.pid) {
        try {
          process.kill(-child.pid, "SIGKILL");
        } catch {
          /* exited */
        }
      }
    };
    const finish = (result: AmpResult) => {
      if (settled) return;
      settled = true;
      clearTimeout(timer);
      kill();
      resolve(result);
    };
    const timer = setTimeout(
      () => finish(failure(uncertain, "amp_process_timeout")),
      50_000,
    );
    child.stdout.on("data", (chunk: Buffer) => {
      size += chunk.byteLength;
      if (size > MAX_OUTPUT) finish(failure(uncertain, "amp_output_too_large"));
      else chunks.push(chunk);
    });
    child.on("error", () =>
      finish(failure("rejected", "amp_process_unavailable")),
    );
    child.on("close", (code) => {
      if (settled) return;
      if (code !== 0) return finish(failure(uncertain, "amp_command_failed"));
      try {
        const result = JSON.parse(Buffer.concat(chunks).toString("utf8"));
        if (result.ok === true && Object.hasOwn(result, "value"))
          return finish({
            v: 1,
            cliVersion: AMP_CLI_VERSION,
            ok: true,
            value: result.value,
          });
        if (
          result.ok === false &&
          ["unknown", "rejected"].includes(result.outcome)
        )
          return finish(failure(result.outcome, "amp_command_failed"));
      } catch {
        /* Never return process output or exception text. */
      }
      finish(failure(uncertain, "amp_invalid_response"));
    });
  });
}
