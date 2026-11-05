import { describe, expect, test } from "bun:test";
import { spawn } from "node:child_process";
import { existsSync } from "node:fs";
import { createAmpProcessRunner } from "../process.ts";
import { AMP_CLI_BRIDGE } from "../provider.ts";
const key = "sgamp_fixture_only_not_a_real_token";
const operation = {
  kind: "inject",
  threadId: "T-fixture-thread",
  text: "hello",
} as const;
describe("Amp process isolation", () => {
  test("runs each operation with a separate temporary home and only that request's credential", async () => {
    const seen: Array<{ args: unknown; env: NodeJS.ProcessEnv; cwd: string }> =
      [];
    const original = process.env.AMP_PROCESS_ISOLATION_TEST;
    process.env.AMP_PROCESS_ISOLATION_TEST = "service-secret-fixture";
    try {
      const run = createAmpProcessRunner(((
        executable: string,
        args: string[],
        options: any,
      ) => {
        seen.push({ args, env: options.env, cwd: options.cwd });
        return spawn(
          executable,
          [
            "-e",
            "process.stdout.write(JSON.stringify({ok:true,value:{home:process.env.HOME,key:process.env.AMP_API_KEY,serviceSecret:process.env.AMP_PROCESS_ISOLATION_TEST}}))",
          ],
          options,
        );
      }) as typeof spawn);
      const results = await Promise.all([
        run(key, operation),
        run(`${key}_second`, { kind: "discover" }),
      ]);
      expect(results.every((r) => r.ok)).toBe(true);
      expect(seen).toHaveLength(2);
      expect(seen[0]!.args).toEqual(["-e", AMP_CLI_BRIDGE]);
      expect(seen.map((call) => call.env.AMP_API_KEY).sort()).toEqual(
        [key, `${key}_second`].sort(),
      );
      expect(
        results.map((result) => (result.ok ? (result.value as any).key : null)),
      ).toEqual([key, `${key}_second`]);
      expect(seen[0]!.cwd).not.toBe(seen[1]!.cwd);
      for (const invocation of seen) {
        expect(invocation.env.AMP_PROCESS_ISOLATION_TEST).toBeUndefined();
        expect(invocation.env.NODE_OPTIONS).toBeUndefined();
        expect(existsSync(invocation.cwd)).toBe(false);
      }
      for (const result of results)
        if (result.ok)
          expect((result.value as any).serviceSecret).toBeUndefined();
    } finally {
      if (original === undefined) delete process.env.AMP_PROCESS_ISOLATION_TEST;
      else process.env.AMP_PROCESS_ISOLATION_TEST = original;
    }
  });
  test("cleans up failures and never returns stderr, credentials, or ambiguous acceptance as a rejection", async () => {
    let home = "";
    const run = createAmpProcessRunner(((
      executable: string,
      _args: string[],
      options: any,
    ) => {
      home = options.cwd;
      return spawn(
        executable,
        [
          "-e",
          "process.stderr.write(process.env.AMP_API_KEY);process.stdout.write(process.env.AMP_API_KEY);process.exit(1)",
        ],
        options,
      );
    }) as typeof spawn);
    const result = await run(key, operation);
    expect(result).toMatchObject({
      ok: false,
      outcome: "unknown",
      code: "amp_command_failed",
    });
    expect(JSON.stringify(result)).not.toContain(key);
    expect(existsSync(home)).toBe(false);
    expect(
      await run(key, { kind: "export", threadId: operation.threadId }),
    ).toMatchObject({ ok: false, outcome: "rejected" });
  });
});
