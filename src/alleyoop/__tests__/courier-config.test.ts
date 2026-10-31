import { afterEach, beforeEach, describe, expect, test } from "bun:test";
import { mkdtemp, rm, writeFile } from "node:fs/promises";
import { tmpdir } from "node:os";
import { join } from "node:path";
import { configFromEnv, runCourierOnce } from "../courier.ts";

const savedEnv = { ...process.env };
const tempDirs: string[] = [];

beforeEach(() => {
  for (const key of Object.keys(process.env)) delete process.env[key];
  Object.assign(process.env, savedEnv, {
    CONCOURSE_DB: "/does/not/exist/history.db",
    TENANT_ORG_ID: "org-proof",
    WORKSPACE_ID: "workspace-proof",
    USER_ID: "user-proof",
    MACHINE_ID: "machine-current",
    INGEST_URL: "https://history.example.test",
    RELAYAUTH_URL: "https://auth.example.test",
    RELAYAUTH_HISTORY_ORIGIN: "https://history.example.test",
    RELAYAUTH_API_KEY_COMMAND_JSON: '["false"]',
    COURIER_START_MODE: "all",
  });
  delete process.env.CLOUD_URL;
  delete process.env.JWT_PRIVATE_KEY_PEM;
  delete process.env.ALLEYOOP_TOKEN;
});

afterEach(async () => {
  for (const key of Object.keys(process.env))
    if (!(key in savedEnv)) delete process.env[key];
  Object.assign(process.env, savedEnv);
  await Promise.all(tempDirs.splice(0).map((dir) => rm(dir, { recursive: true })));
});

describe("RelayAuth courier machine binding", () => {
  test("a matching RelayAuth configuration is machine-bound", () => {
    expect(configFromEnv()).toMatchObject({
      machineId: "machine-current",
      machineBound: true,
    });
  });

  test("a persisted mismatch fails before mint or upload", async () => {
    const dir = await mkdtemp(join(tmpdir(), "96bulls-courier-config-"));
    tempDirs.push(dir);
    const statePath = join(dir, "courier-state.json");
    await writeFile(
      statePath,
      JSON.stringify({ machine_id: "machine-old", sessions: {} }),
    );
    process.env.COURIER_STATE = statePath;

    const cfg = configFromEnv();
    let tokenReads = 0;
    cfg.token = {
      async get() {
        tokenReads += 1;
        throw new Error("token must not be read");
      },
      invalidate() {
        return true;
      },
    };

    await expect(runCourierOnce(cfg)).rejects.toMatchObject({
      code: "machine_id_mismatch",
      retryable: false,
    });
    expect(tokenReads).toBe(0);
  });
});
