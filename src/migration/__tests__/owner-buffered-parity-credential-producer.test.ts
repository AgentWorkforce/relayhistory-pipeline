import { describe, expect, test } from "bun:test";
import {
  OWNER_PARITY_CREDENTIAL_TARGET as TARGET,
  ownerParityCredentialDocument,
  parseProducerArgs,
  pinnedNeonReadonlyUrl,
  pinnedSupabaseOwnerUrl,
  producerErrorCode,
  requireRamOnlyProducerOutput,
  type ProducerCommand,
} from "../../../scripts/owner-buffered-parity-credential-producer.ts";

const PIPE = {
  isCharacterDevice: () => false,
  isFile: () => false,
  isFIFO: () => true,
  isSocket: () => false,
};
const outputStat = (kind: "terminal" | "file" | "fifo" | "socket" | "directory" | "block" | "unknown") => ({
  isCharacterDevice: () => kind === "terminal",
  isFile: () => kind === "file",
  isFIFO: () => kind === "fifo",
  isSocket: () => kind === "socket",
});
const SOURCE = "a".repeat(40);
const neonUrl = (overrides: Partial<{ role: string; host: string; database: string; query: string; password: string }> = {}) => {
  const role = overrides.role ?? TARGET.neonRole;
  const host = overrides.host ?? TARGET.neonHost;
  const database = overrides.database ?? TARGET.neonDatabase;
  const query = overrides.query ?? "sslmode=require&channel_binding=require";
  const url = new URL(`postgresql://${host}:5432/${database}`);
  url.username = role;
  url.password = overrides.password ?? "synthetic-neon-password";
  url.search = query;
  return url.toString();
};
const cliLogin = (password = "synthetic-supabase-password") => [
  `PGHOST=db.${TARGET.supabaseProject}.supabase.co`,
  "PGPORT=5432",
  "PGUSER=cli_login_postgres",
  `PGPASSWORD=${password}`,
  "PGDATABASE=postgres",
].join("\n");

describe("owner-buffered parity credential producer", () => {
  test("emits exactly the two pinned verify-full URLs after fixed CLI commands", () => {
    const calls: ProducerCommand[] = [];
    const runner = (command: ProducerCommand) => {
      calls.push(command);
      if (command.executable === TARGET.git)
        return command.args[0] === "rev-parse" ? SOURCE : "";
      if (command.args[0] === "--version")
        return command.executable === TARGET.neonctl ? TARGET.neonctlVersion : TARGET.supabaseCliVersion;
      return command.executable === TARGET.neonctl ? JSON.stringify(neonUrl()) : cliLogin();
    };
    const document = JSON.parse(ownerParityCredentialDocument(SOURCE, runner, { HOME: "/synthetic", PATH: "/bin" }, PIPE));
    expect(Object.keys(document).sort()).toEqual(["neonUrl", "supabaseUrl"]);
    const neon = new URL(document.neonUrl);
    expect({ user: decodeURIComponent(neon.username), host: neon.hostname, port: neon.port, db: neon.pathname, mode: neon.searchParams.get("sslmode") })
      .toEqual({ user: TARGET.neonRole, host: TARGET.neonHost, port: "5432", db: `/${TARGET.neonDatabase}`, mode: "verify-full" });
    const supabase = new URL(document.supabaseUrl);
    expect({ user: decodeURIComponent(supabase.username), host: supabase.hostname, port: supabase.port, db: supabase.pathname, mode: supabase.searchParams.get("sslmode") })
      .toEqual({ user: `cli_login_postgres.${TARGET.supabaseProject}`, host: TARGET.supabaseHost, port: "5432", db: "/postgres", mode: "verify-full" });
    expect(calls.map(({ executable, args }) => [executable, ...args])).toEqual([
      [TARGET.git, "rev-parse", "HEAD"],
      [TARGET.git, "status", "--porcelain"],
      [TARGET.neonctl, "--version"],
      [TARGET.supabaseCli, "--version"],
      [TARGET.neonctl, "connection-string", TARGET.neonBranch, "--project-id", TARGET.neonProject,
        "--role-name", TARGET.neonRole, "--database-name", TARGET.neonDatabase, "--output", "json", "--no-color"],
      [TARGET.supabaseCli, "db", "dump", "--linked", "--project-ref", TARGET.supabaseProject, "--dry-run"],
    ]);
    expect(calls.every((call) => Object.keys(call.env).sort().join() === "HOME,PATH")).toBeTrue();
  });

  test.each([
    [neonUrl({ role: "neondb_owner" }), "neon_target_pin"],
    [neonUrl({ host: "other.neon.tech" }), "neon_target_pin"],
    [neonUrl({ database: "other" }), "neon_target_pin"],
    [neonUrl({ password: "" }), "neon_target_pin"],
    [neonUrl({ query: "sslmode=require&options=role%3Dpostgres" }), "neon_startup_parameter_refused"],
    [neonUrl() + "#hidden", "neon_target_pin"],
  ])("refuses a Neon route outside the exact restricted identity", (input, code) => {
    expect(() => pinnedNeonReadonlyUrl(input)).toThrow(code);
  });

  test("requires one exact source pin and a clean matching checkout before either credential command", () => {
    expect(parseProducerArgs(["--expect-source-sha", SOURCE])).toEqual({ expectedSourceSha: SOURCE });
    expect(() => parseProducerArgs([])).toThrow("producer_usage");
    expect(() => parseProducerArgs(["--expect-source-sha", "main"])).toThrow("producer_usage");
    const calls: string[] = [];
    const runner = (command: ProducerCommand) => {
      calls.push(command.args.join(" "));
      if (command.args[0] === "rev-parse") return "b".repeat(40);
      return "";
    };
    expect(() => ownerParityCredentialDocument(SOURCE, runner, {}, PIPE))
      .toThrow("producer_checkout_pin_mismatch");
    expect(calls).toEqual(["rev-parse HEAD", "status --porcelain"]);
  });

  test("refuses a Supabase CLI reply outside the exact direct-login tuple", () => {
    expect(() => pinnedSupabaseOwnerUrl(cliLogin().replace("PGPORT=5432", "PGPORT=6543")))
      .toThrow("supabase_cli_reply_refused");
  });

  test.each([
    [outputStat("terminal"), "producer_stdout_is_a_terminal"],
    [outputStat("file"), "producer_stdout_is_a_file"],
    [outputStat("directory"), "producer_stdout_not_pipe_or_socket"],
    [outputStat("block"), "producer_stdout_not_pipe_or_socket"],
    [outputStat("unknown"), "producer_stdout_not_pipe_or_socket"],
  ])("refuses a persistent or visible output before any credential command", (output, code) => {
    let ran = false;
    expect(() => ownerParityCredentialDocument(SOURCE, () => { ran = true; return ""; }, {}, output)).toThrow(code);
    expect(ran).toBeFalse();
  });

  test("refuses credential-bearing environment state before any command", () => {
    let ran = false;
    expect(() => ownerParityCredentialDocument(SOURCE, () => { ran = true; return ""; }, { NEON_API_KEY: "not-logged" }, PIPE))
      .toThrow("credential_env_refused.NEON_API_KEY");
    expect(ran).toBeFalse();
  });

  test("version drift and private command failures expose only bounded refusal codes", () => {
    let command = 0;
    expect(() => ownerParityCredentialDocument(SOURCE, () => {
      command++;
      if (command === 1) return SOURCE;
      if (command === 2) return "";
      return "4.17.0";
    }, {}, PIPE)).toThrow("neonctl_version");
    expect(producerErrorCode(new Error("private child stderr"))).toBe("credential_producer_failed");
  });

  test("refuses a combined document larger than the operator stdin bound", () => {
    let command = 0;
    expect(() => ownerParityCredentialDocument(SOURCE, () => {
      command++;
      if (command === 1) return SOURCE;
      if (command === 2) return "";
      if (command === 3) return TARGET.neonctlVersion;
      if (command === 4) return TARGET.supabaseCliVersion;
      if (command === 5) return JSON.stringify(neonUrl({ password: "n".repeat(40_000) }));
      return cliLogin("s".repeat(30_000));
    }, {}, PIPE)).toThrow("credential_document_limit");
  });

  test("the standalone sink guard accepts only the intended pipe/socket shape", () => {
    expect(() => requireRamOnlyProducerOutput(PIPE)).not.toThrow();
    expect(() => requireRamOnlyProducerOutput(outputStat("socket"))).not.toThrow();
  });
});
