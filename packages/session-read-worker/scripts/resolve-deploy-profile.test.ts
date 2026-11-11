import { readFileSync } from "node:fs";
import { describe, expect, test } from "vitest";
import ts from "typescript";
import { checkPreservedVersion, preservedProfile, profileInputs, resolveProfile } from "./resolve-deploy-profile.ts";
import { renderDeployConfig } from "./render-deploy-config.ts";

const template = readFileSync(new URL("../wrangler.jsonc", import.meta.url), "utf8");
const env = { HYPERDRIVE_ID: "1234567890abcdef1234567890abcdef", OWNER_BUFFERED: "preserve" };
const id = "458d33cf-8952-40d4-a12f-13729b92fd7c";
const values = {
  RELAYHISTORY_READ_OWNER_BUFFERED: "true", READ_API_OWNER_BUFFERED_WORKSPACES: "*",
  READ_API_DIRECTORY_WORKSPACES: "*", READ_API_OWNER_BUFFERED_EVENTS: "on",
  INGEST_S3_BUCKET: "96bulls-aws-sessions-prod-transcripts-941433935742", INGEST_S3_REGION: "us-east-1",
  READ_API_OWNER_BUFFERED_STATEMENT_TIMEOUT_MS: "6000",
};
const control = { name: "HISTORY_CONTROL_HEAD", type: "service", service: "relayhistory-api", entrypoint: "SessionReadPrivacyControls" };
const version = (v: Record<string, string> = values, binding = control) => ({ id, resources: { bindings: [
  ...Object.entries(v).map(([name, text]) => ({ name, type: "plain_text", text })),
  ...(binding ? [binding] : []),
  { name: "PIPELINE_RAW_S3_SECRET_ACCESS_KEY", type: "secret_text", text: "MUST_NOT_BE_COPIED" },
] } });
const get = (v: unknown = version()) => async (path: string) => path === "/deployments"
  ? { deployments: [{ versions: [{ version_id: id, percentage: 100 }] }] } : v;

describe("preserve the deployed Reader profile", () => {
  test("no-input redeploy retains the production buffered/events/allowlist/bucket/6000 profile", async () => {
    const resolved = await resolveProfile(env, template, get());
    const config = ts.parseConfigFileTextToJson("config.jsonc", renderDeployConfig(template, profileInputs({ ...env, ...resolved }))).config;
    expect(config.vars).toMatchObject({ ...values, READ_API_DIRECTORY_WORKSPACES: "" });
    // Directory is a separate wrangler --var, so it must be exported to the deploy step too.
    expect(resolved.DIRECTORY_WORKSPACES).toBe("*");
    expect(config.services).toEqual([{ binding: "HISTORY_CONTROL_HEAD", service: "relayhistory-api", entrypoint: "SessionReadPrivacyControls" }]);
    expect(JSON.stringify(resolved)).not.toContain("MUST_NOT_BE_COPIED");
    expect(resolved).not.toHaveProperty("CLOUDFLARE_API_TOKEN");
  });
  test("an empty mode also preserves, rather than silently disabling", async () => {
    expect((await resolveProfile({ ...env, OWNER_BUFFERED: "" }, template, get())).OWNER_BUFFERED).toBe("on");
  });
  test("a currently disabled reader stays disabled", async () => {
    const live = { id, resources: { bindings: [{ name: "READ_API_DIRECTORY_WORKSPACES", type: "plain_text", text: "" }] } };
    const resolved = await resolveProfile(env, template, get(live));
    expect(resolved.OWNER_BUFFERED).toBe("off");
    expect(ts.parseConfigFileTextToJson("config.jsonc", renderDeployConfig(template, profileInputs({ ...env, ...resolved }))).config.services).toBeUndefined();
  });
  test("explicit off disables without fetching live metadata", async () => {
    const resolved = await resolveProfile({ ...env, OWNER_BUFFERED: "off" }, template, async () => { throw Error("must not call"); });
    expect(resolved).toEqual({ OWNER_BUFFERED: "off" });
  });
  test.each(["OWNER_BUFFERED_EVENTS", "OWNER_BUFFERED_PHASE_LOG", "OWNER_BUFFERED_INDEX_VERIFY", "BUFFERED_SEARCH_GATE", "OWNER_BUFFERED_WORKSPACES"])("preserve refuses a conflicting %s override", async key => {
    await expect(resolveProfile({ ...env, [key]: "on" }, template, get())).rejects.toThrow(/preserve/);
  });
  test("metadata failure and mixed live versions block deployment", async () => {
    await expect(resolveProfile(env, template, async () => { throw Error("unavailable"); })).rejects.toThrow();
    await expect(resolveProfile(env, template, async () => ({ deployments: [{ versions: [{ version_id: id, percentage: 50 }] }] }))).rejects.toThrow(/exactly one live version at 100/);
  });
  test("mismatched version identity, duplicate bindings and wrong privacy authority refuse", async () => {
    await expect(resolveProfile(env, template, get({ ...version(), id: "wrong" }))).rejects.toThrow(/identity/);
    const duplicate = version(); duplicate.resources.bindings.push(duplicate.resources.bindings[0]!);
    expect(() => preservedProfile(duplicate)).toThrow(/duplicate/);
    await expect(resolveProfile(env, template, get(version(values, { ...control, service: "untrusted" })))).rejects.toThrow(/privacy authority/);
  });
  test("missing bucket, invalid flag, secret-typed feature, injected newline and invalid timeout refuse", async () => {
    for (const v of [{ ...values, INGEST_S3_BUCKET: "" }, { ...values, READ_API_OWNER_BUFFERED_EVENTS: "maybe" }, { ...values, READ_API_DIRECTORY_WORKSPACES: "*\nEVIL=1" }, { ...values, READ_API_OWNER_BUFFERED_STATEMENT_TIMEOUT_MS: "6001" }]) {
      await expect(resolveProfile(env, template, get(version(v)))).rejects.toThrow();
    }
    const secret = version(); secret.resources.bindings[0]!.type = "secret_text";
    await expect(resolveProfile(env, template, get(secret))).rejects.toThrow(/plain text/);
  });
  test("preserves the search privacy gate independently when owner-buffered is off", async () => {
    const resolved = await resolveProfile(env, template, get(version({ READ_API_BUFFERED_SEARCH_GATE: "on" })));
    expect(resolved.OWNER_BUFFERED).toBe("off"); expect(resolved.BUFFERED_SEARCH_GATE).toBe("on");
    expect(ts.parseConfigFileTextToJson("config.jsonc", renderDeployConfig(template, profileInputs({ ...env, ...resolved }))).config.services).toHaveLength(1);
  });
  test("an unfamiliar owner-buffered setting or a different control-service environment refuses preservation", async () => {
    await expect(resolveProfile(env, template, get(version({ ...values, READ_API_OWNER_BUFFERED_FUTURE_GUARD: "on" })))).rejects.toThrow(/Unknown live/);
    const other = version(); Object.assign(other.resources.bindings.find(b => b.name === control.name)!, { environment: "preview" });
    await expect(resolveProfile(env, template, get(other))).rejects.toThrow(/privacy authority/);
  });
  test("the predeploy check refuses a changed live version; explicit mode does not inherit a stale pin", async () => {
    await expect(checkPreservedVersion({ PRESERVED_READER_VERSION: id }, get())).resolves.toBeUndefined();
    await expect(checkPreservedVersion({ PRESERVED_READER_VERSION: "7d801289-2443-4978-a3da-b5f07d0a9f1e" }, get())).rejects.toThrow(/changed after/);
    await expect(checkPreservedVersion({}, async () => { throw Error("must not call"); })).resolves.toBeUndefined();
  });
  test("the workflow resolves before render and checks S3 secrets for the effective profile", () => {
    const workflow = readFileSync(new URL("../../../.github/workflows/deploy-session-read-worker.yml", import.meta.url), "utf8");
    expect(workflow).toContain('default: "preserve"');
    expect(workflow.indexOf("run: bun run scripts/resolve-deploy-profile.ts")).toBeLessThan(workflow.indexOf("run: |\n          grep -q"));
    expect(workflow).toContain("if: steps.resolve-profile.outputs.owner_buffered == 'on'");
    expect(workflow).toContain("OWNER_BUFFERED_STATEMENT_TIMEOUT_MS: ${{ inputs.owner_buffered_statement_timeout_ms }}");
  });
});
