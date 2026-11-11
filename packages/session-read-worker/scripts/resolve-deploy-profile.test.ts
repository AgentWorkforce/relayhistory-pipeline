import { readFileSync } from "node:fs";
import { describe, expect, test } from "vitest";
import ts from "typescript";
import { apiRoot, checkPreservedVersion, parseCommittedProfile, preservedProfile, profileInputs, resolveProfile, workflowEnvironment } from "./resolve-deploy-profile.ts";
import { renderDeployConfig } from "./render-deploy-config.ts";

const template = readFileSync(new URL("../wrangler.jsonc", import.meta.url), "utf8");
const env = { HYPERDRIVE_ID: "1234567890abcdef1234567890abcdef", OWNER_BUFFERED: "preserve" };
const id = "11111111-1111-4111-8111-111111111111";
const values = {
  RELAYHISTORY_READ_OWNER_BUFFERED: "true", READ_API_OWNER_BUFFERED_WORKSPACES: "*",
  READ_API_DIRECTORY_WORKSPACES: "*", READ_API_OWNER_BUFFERED_EVENTS: "on",
  INGEST_S3_BUCKET: "fixture-buffered-history-bucket", INGEST_S3_REGION: "us-east-1",
  READ_API_OWNER_BUFFERED_STATEMENT_TIMEOUT_MS: "6000",
};
const control = { name: "HISTORY_CONTROL_HEAD", type: "service", service: "relayhistory-api", entrypoint: "SessionReadPrivacyControls" };
const version = (v: Record<string, string> = values, binding = control) => ({ id, resources: { bindings: [
  ...Object.entries(v).map(([name, text]) => ({ name, type: "plain_text", text })),
  ...(binding ? [{ ...binding }] : []),
  { name: "PIPELINE_RAW_S3_SECRET_ACCESS_KEY", type: "secret_text", text: "MUST_NOT_BE_COPIED" },
] } });
const get = (v: unknown = version()) => async (path: string) => path === "/deployments"
  ? { deployments: [{ versions: [{ version_id: id, percentage: 100 }] }] } : v;

describe("preserve the deployed Reader profile", () => {
  test("provider reads are bound to the reviewed template and the deploy account", () => {
    const synthetic = JSON.stringify({ account_id: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", name: "fixture-reader" });
    expect(apiRoot(synthetic, "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")).toBe("https://api.cloudflare.com/client/v4/accounts/aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa/workers/scripts/fixture-reader");
    expect(() => apiRoot(synthetic, "bbbbbbbbbbbbbbbbbbbbbbbbbbbbbbbb")).toThrow(/account differ/);
    expect(() => apiRoot(JSON.stringify({ account_id: "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa", name: "../other" }), "aaaaaaaaaaaaaaaaaaaaaaaaaaaaaaaa")).toThrow(/script name/);
  });
  test("no-input redeploy retains the production buffered/events/allowlist/bucket/6000 profile", async () => {
    const resolved = await resolveProfile(env, template, get());
    const config = ts.parseConfigFileTextToJson("config.jsonc", renderDeployConfig(template, profileInputs({ ...env, ...resolved }))).config;
    expect(config.vars).toMatchObject({ ...values, READ_API_DIRECTORY_WORKSPACES: "" });
    // Directory is a separate wrangler --var, so it must be exported to the deploy step too.
    expect(resolved.DIRECTORY_WORKSPACES).toBe("*");
    expect(config.services).toEqual([{ binding: "HISTORY_CONTROL_HEAD", service: "relayhistory-api", entrypoint: "SessionReadPrivacyControls" }]);
    expect(JSON.stringify(resolved)).not.toContain("MUST_NOT_BE_COPIED");
    expect(resolved).not.toHaveProperty("CLOUDFLARE_API_TOKEN");
    const exported = workflowEnvironment(resolved);
    expect(exported).toContain("OWNER_BUFFERED_S3_BUCKET=fixture-buffered-history-bucket\n");
    expect(exported).toContain("OWNER_BUFFERED_S3_REGION=us-east-1\n");
    expect(exported).not.toContain("MUST_NOT_BE_COPIED");
  });
  test("an empty mode also preserves, rather than silently disabling", async () => {
    expect((await resolveProfile({ ...env, OWNER_BUFFERED: "" }, template, get())).OWNER_BUFFERED).toBe("on");
  });
  test("the real workflow export accepts all preserved keys but refuses credential/unknown keys and injected lines", async () => {
    const resolved = await resolveProfile(env, template, get());
    expect(() => workflowEnvironment(resolved)).not.toThrow();
    expect(() => workflowEnvironment({ ...resolved, CLOUDFLARE_API_TOKEN: "must-not-export" })).toThrow(/Unsafe/);
    expect(() => workflowEnvironment({ OWNER_BUFFERED: "on\nOTHER=1" })).toThrow(/Unsafe/);
  });
  test("a currently disabled reader stays disabled", async () => {
    const live = { id, resources: { bindings: [{ name: "READ_API_DIRECTORY_WORKSPACES", type: "plain_text", text: "" }] } };
    const resolved = await resolveProfile(env, template, get(live));
    expect(resolved.OWNER_BUFFERED).toBe("off");
    expect(ts.parseConfigFileTextToJson("config.jsonc", renderDeployConfig(template, profileInputs({ ...env, ...resolved }))).config.services).toBeUndefined();
  });
  test("explicit off disables without fetching live metadata", async () => {
    const resolved = await resolveProfile({ ...env, OWNER_BUFFERED: "off", DIRECTORY_WORKSPACES_INPUT: "off" }, template, async () => { throw Error("must not call"); });
    expect(resolved).toEqual({ OWNER_BUFFERED: "off", OWNER_BUFFERED_WORKSPACES: "", OWNER_BUFFERED_EVENTS: "off", OWNER_BUFFERED_STATEMENT_TIMEOUT_MS: "off", DIRECTORY_WORKSPACES: "" });
  });
  test("explicit buffered on/off cannot erase the directory just because its environment variable is unset", async () => {
    const off = await resolveProfile({ ...env, OWNER_BUFFERED: "off" }, template, get());
    expect(off.DIRECTORY_WORKSPACES).toBe("*");
    const on = await resolveProfile({ ...env, OWNER_BUFFERED: "on", OWNER_BUFFERED_WORKSPACES: "*", OWNER_BUFFERED_S3_BUCKET: values.INGEST_S3_BUCKET, OWNER_BUFFERED_S3_REGION: values.INGEST_S3_REGION }, template, get());
    expect(on.DIRECTORY_WORKSPACES).toBe("*");
    expect(on.PRESERVED_READER_VERSION).toBe(id);
  });
  test("preserve refuses directory drift instead of silently ignoring operator intent", async () => {
    await expect(resolveProfile({ ...env, DIRECTORY_WORKSPACES: "fixture-workspace" }, template, get())).rejects.toThrow(/differs from live/);
    await expect(resolveProfile({ ...env, DIRECTORY_WORKSPACES_INPUT: "off" }, template, get())).rejects.toThrow(/differs from live/);
  });
  test("an explicit configuration change can narrow the directory and owner lists", async () => {
    const narrowed = await resolveProfile({ ...env, OWNER_BUFFERED: "on", DIRECTORY_WORKSPACES_INPUT: "fixture-workspace", OWNER_BUFFERED_WORKSPACES: "fixture-workspace", OWNER_BUFFERED_S3_BUCKET: values.INGEST_S3_BUCKET, OWNER_BUFFERED_S3_REGION: values.INGEST_S3_REGION }, template, async () => { throw Error("must not call"); });
    expect(narrowed.DIRECTORY_WORKSPACES).toBe("fixture-workspace");
  });
  test.each(["READ_API_REPEATABLE_READ", "READ_API_OUTPUT_ENABLED", "READ_API_DIGEST_ENABLED", "RELAYCONTROL_ORIGIN"])("default deploy refuses dropping unrelated live setting %s", async name => {
    await expect(resolveProfile(env, template, get(version({ ...values, [name]: "enabled-fixture" })))).rejects.toThrow(/change or drop live setting/);
  });
  test("default deploy refuses dropping an unrelated service or Hyperdrive binding", async () => {
    const digest = version(); digest.resources.bindings.push({ ...control, name: "DIGEST_NARRATIVE", service: "fixture-digest" });
    await expect(resolveProfile(env, template, get(digest))).rejects.toThrow(/drop live service binding/);
    // Push a new binding: overwriting bindings[0] would remove the owner-buffered flag and refuse earlier.
    const withBinding = (extra: Record<string, string>) => { const v = version(); (v.resources.bindings as Array<Record<string, string>>).push(extra); return v; };
    await expect(resolveProfile(env, template, get(withBinding({ name: "OTHER_DATABASE", type: "hyperdrive", id: "cccccccccccccccccccccccccccccccc" })))).rejects.toThrow(/drop live Hyperdrive binding OTHER_DATABASE/);
    await expect(resolveProfile(env, template, get(withBinding({ name: "SUPABASE", type: "hyperdrive", id: "cccccccccccccccccccccccccccccccc" })))).rejects.toThrow(/drop live Hyperdrive binding SUPABASE/);
    // Control: the rendered Hyperdrive binding with the deploy id is not a change.
    await expect(resolveProfile(env, template, get(withBinding({ name: "SUPABASE", type: "hyperdrive", id: env.HYPERDRIVE_ID })))).resolves.toHaveProperty("OWNER_BUFFERED", "on");
  });
  test("default deploy refuses an unknown live binding type and a secret shadowed by a rendered variable", async () => {
    const withBinding = (extra: Record<string, string>) => { const v = version(); (v.resources.bindings as Array<Record<string, string>>).push(extra); return v; };
    await expect(resolveProfile(env, template, get(withBinding({ name: "FIXTURE_QUEUE", type: "queue" })))).rejects.toThrow(/cannot compare live binding FIXTURE_QUEUE/);
    await expect(resolveProfile(env, template, get(withBinding({ name: "CLICKHOUSE_DATABASE", type: "secret_text" })))).rejects.toThrow(/replacing secret binding CLICKHOUSE_DATABASE/);
  });
  test("default deploy accepts unchanged unrelated variables and checks the real CLI overrides", async () => {
    const base = ts.parseConfigFileTextToJson("base.jsonc", template).config;
    await expect(resolveProfile(env, template, get(version({ ...values, CLICKHOUSE_DATABASE: base.vars.CLICKHOUSE_DATABASE })))).resolves.toHaveProperty("OWNER_BUFFERED", "on");
    await expect(resolveProfile({ ...env, BRIEF_TITLES_WORKSPACES: "fixture-workspace" }, template, get(version({ ...values, READ_API_BRIEF_TITLES_WORKSPACES: "fixture-workspace" })))).resolves.toHaveProperty("OWNER_BUFFERED", "on");
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
    await expect(checkPreservedVersion({ PRESERVED_READER_VERSION: "22222222-2222-4222-8222-222222222222" }, get())).rejects.toThrow(/changed after/);
    await expect(checkPreservedVersion({}, async () => { throw Error("must not call"); })).resolves.toBeUndefined();
  });
  test("the workflow resolves before render and checks S3 secrets for the effective profile", () => {
    const workflow = readFileSync(new URL("../../../.github/workflows/deploy-session-read-worker.yml", import.meta.url), "utf8");
    expect(workflow).toContain('default: "profile"');
    expect(workflow).toMatch(/^ {6}DEPLOY_TARGET: production$/m);
    expect(workflow.indexOf("run: bun run scripts/resolve-deploy-profile.ts")).toBeLessThan(workflow.indexOf("run: |\n          grep -q"));
    expect(workflow).toContain("if: steps.resolve-profile.outputs.owner_buffered == 'on'");
    expect(workflow).toContain("OWNER_BUFFERED_STATEMENT_TIMEOUT_MS: ${{ inputs.owner_buffered_statement_timeout_ms }}");
  });
});

describe("committed production profile (DEPLOY_TARGET=production)", () => {
  const prod = { ...env, OWNER_BUFFERED: "profile", DEPLOY_TARGET: "production", OWNER_BUFFERED_EVENTS: "profile", OWNER_BUFFERED_STATEMENT_TIMEOUT_MS: "profile" };
  const committed = parseCommittedProfile(readFileSync(new URL("../deploy/production-profile.json", import.meta.url), "utf8"));
  const render = (resolved: Record<string, string>, base: Record<string, string> = prod) => ts.parseConfigFileTextToJson("config.jsonc", renderDeployConfig(template, profileInputs({ ...base, ...resolved }))).config;
  const withoutTimeout = Object.fromEntries(Object.entries(values).filter(([name]) => name !== "READ_API_OWNER_BUFFERED_STATEMENT_TIMEOUT_MS"));

  test("the committed file is exactly the known production profile, with no workspace id", () => {
    expect(committed).toEqual({ OWNER_BUFFERED: "on", OWNER_BUFFERED_WORKSPACES: "*", OWNER_BUFFERED_EVENTS: "on", OWNER_BUFFERED_STATEMENT_TIMEOUT_MS: "6000" });
  });
  test("a default dispatch (workflow defaults, no inputs) renders the committed profile", async () => {
    for (const base of [prod, { ...env, OWNER_BUFFERED: "", DEPLOY_TARGET: "production" }]) {
      const resolved = await resolveProfile(base, template, get());
      expect(resolved).toMatchObject({ OWNER_BUFFERED: "on", OWNER_BUFFERED_WORKSPACES: "*", OWNER_BUFFERED_EVENTS: "on", OWNER_BUFFERED_STATEMENT_TIMEOUT_MS: "6000", DIRECTORY_WORKSPACES: "*", PRESERVED_READER_VERSION: id });
      expect(render(resolved, base).vars).toMatchObject({ ...values, READ_API_DIRECTORY_WORKSPACES: "" });
      // The render step must never see the "profile" token.
      expect(workflowEnvironment(resolved)).not.toContain("profile");
    }
  });
  test("the restore case: live equal to the profile minus the timeout deploys with 6000", async () => {
    const resolved = await resolveProfile(prod, template, get(version(withoutTimeout)));
    expect(resolved.OWNER_BUFFERED_STATEMENT_TIMEOUT_MS).toBe("6000");
    expect(render(resolved).vars.READ_API_OWNER_BUFFERED_STATEMENT_TIMEOUT_MS).toBe("6000");
    // #511's preserve would carry the loss forward; that remains the explicit opt-out.
    expect((await resolveProfile({ ...prod, OWNER_BUFFERED: "preserve" }, template, get(version(withoutTimeout)))).OWNER_BUFFERED_STATEMENT_TIMEOUT_MS).toBe("off");
  });
  test("settings the profile does not own (search gate, index verify, S3) are preserved from live", async () => {
    const resolved = await resolveProfile(prod, template, get(version({ ...values, READ_API_BUFFERED_SEARCH_GATE: "on", READ_API_OWNER_BUFFERED_INDEX_VERIFY: "on" })));
    expect(resolved).toMatchObject({ BUFFERED_SEARCH_GATE: "on", OWNER_BUFFERED_INDEX_VERIFY: "on", OWNER_BUFFERED_S3_BUCKET: values.INGEST_S3_BUCKET });
  });
  test("live having more than the profile refuses (collateral drop or change)", async () => {
    await expect(resolveProfile(prod, template, get(), { ...committed, OWNER_BUFFERED_EVENTS: "off" })).rejects.toThrow(/would change or drop the live owner_buffered_events/);
    await expect(resolveProfile(prod, template, get(), { ...committed, OWNER_BUFFERED_STATEMENT_TIMEOUT_MS: "off" })).rejects.toThrow(/owner_buffered_statement_timeout_ms/);
    await expect(resolveProfile(prod, template, get(version({ ...values, READ_API_OWNER_BUFFERED_STATEMENT_TIMEOUT_MS: "5000" })))).rejects.toThrow(/change or drop/);
    await expect(resolveProfile(prod, template, get(version({ ...values, READ_API_OWNER_BUFFERED_WORKSPACES: "fixture-workspace", READ_API_DIRECTORY_WORKSPACES: "fixture-workspace" })))).rejects.toThrow(/change or drop/);
    await expect(resolveProfile(prod, template, get(), { OWNER_BUFFERED: "off", OWNER_BUFFERED_WORKSPACES: "", OWNER_BUFFERED_EVENTS: "off", OWNER_BUFFERED_STATEMENT_TIMEOUT_MS: "off" })).rejects.toThrow(/drop live owner-buffered/);
    // The unmanaged-setting guard still applies in profile mode.
    await expect(resolveProfile(prod, template, get(version({ ...values, READ_API_OUTPUT_ENABLED: "enabled-fixture" })))).rejects.toThrow(/change or drop live setting/);
  });
  test("a default dispatch never turns owner-buffered reads on from off", async () => {
    const live = { id, resources: { bindings: [{ name: "READ_API_DIRECTORY_WORKSPACES", type: "plain_text", text: "*" }] } };
    await expect(resolveProfile(prod, template, get(live))).rejects.toThrow(/does not turn them on/);
  });
  test("profile mode refuses child overrides and directory drift instead of half-applying them", async () => {
    for (const [key, value] of [["OWNER_BUFFERED_EVENTS", "off"], ["OWNER_BUFFERED_STATEMENT_TIMEOUT_MS", "5000"], ["OWNER_BUFFERED_PHASE_LOG", "on"], ["OWNER_BUFFERED_WORKSPACES", "*"]]) {
      await expect(resolveProfile({ ...prod, [key!]: value! }, template, get())).rejects.toThrow(/profile cannot/);
    }
    await expect(resolveProfile({ ...prod, DIRECTORY_WORKSPACES: "fixture-workspace" }, template, get())).rejects.toThrow(/differs from live/);
  });
  test("explicit inputs override the profile; unset children under explicit on fall back to it", async () => {
    const on = { ...prod, OWNER_BUFFERED: "on", OWNER_BUFFERED_WORKSPACES: "", OWNER_BUFFERED_S3_BUCKET: values.INGEST_S3_BUCKET, OWNER_BUFFERED_S3_REGION: values.INGEST_S3_REGION };
    expect(await resolveProfile(on, template, get())).toMatchObject({ OWNER_BUFFERED_WORKSPACES: "*", OWNER_BUFFERED_EVENTS: "on", OWNER_BUFFERED_STATEMENT_TIMEOUT_MS: "6000" });
    const overridden = await resolveProfile({ ...on, OWNER_BUFFERED_EVENTS: "off", OWNER_BUFFERED_STATEMENT_TIMEOUT_MS: "4000" }, template, get());
    expect(overridden).toMatchObject({ OWNER_BUFFERED_EVENTS: "off", OWNER_BUFFERED_STATEMENT_TIMEOUT_MS: "4000" });
    expect(render(overridden, on).vars.READ_API_OWNER_BUFFERED_EVENTS).toBeUndefined();
    expect((await resolveProfile({ ...on, OWNER_BUFFERED_STATEMENT_TIMEOUT_MS: "off" }, template, get())).OWNER_BUFFERED_STATEMENT_TIMEOUT_MS).toBe("off");
    // Explicit off ignores the profile entirely.
    expect(await resolveProfile({ ...prod, OWNER_BUFFERED: "off", DIRECTORY_WORKSPACES_INPUT: "off" }, template, async () => { throw Error("must not call"); }))
      .toEqual({ OWNER_BUFFERED: "off", OWNER_BUFFERED_WORKSPACES: "", OWNER_BUFFERED_EVENTS: "off", OWNER_BUFFERED_STATEMENT_TIMEOUT_MS: "off", DIRECTORY_WORKSPACES: "" });
  });
  test("a non-production target ignores the profile: profile means preserve, explicit on renders only its inputs", async () => {
    for (const target of [undefined, "dev", "Production"]) {
      const other = { ...prod, DEPLOY_TARGET: target };
      const resolved = await resolveProfile(other, template, get(version(withoutTimeout)), committed);
      expect(resolved.OWNER_BUFFERED_STATEMENT_TIMEOUT_MS).toBe("off");
      const on = await resolveProfile({ ...other, OWNER_BUFFERED: "on", OWNER_BUFFERED_WORKSPACES: "*", OWNER_BUFFERED_S3_BUCKET: values.INGEST_S3_BUCKET, OWNER_BUFFERED_S3_REGION: values.INGEST_S3_REGION }, template, get(), committed);
      expect(on).toMatchObject({ OWNER_BUFFERED_EVENTS: "off", OWNER_BUFFERED_STATEMENT_TIMEOUT_MS: "off" });
      await expect(resolveProfile({ ...other, OWNER_BUFFERED: "on" }, template, get(), committed)).rejects.toThrow(/needs owner_buffered_workspaces/);
    }
  });
  test("a malformed committed profile refuses, including a workspace id in the repo", () => {
    expect(() => parseCommittedProfile("not json")).toThrow(/valid JSON/);
    expect(() => parseCommittedProfile(JSON.stringify({ owner_buffered: "on" }))).toThrow(/exactly/);
    expect(() => parseCommittedProfile(JSON.stringify({ owner_buffered: "on", owner_buffered_workspaces: "fixture-workspace", owner_buffered_events: "on", owner_buffered_statement_timeout_ms: "6000" }))).toThrow(/no workspace ids/);
    expect(() => parseCommittedProfile(JSON.stringify({ owner_buffered: "on", owner_buffered_workspaces: "*", owner_buffered_events: "on", owner_buffered_statement_timeout_ms: "6000", owner_buffered_phase_log: "on" }))).toThrow(/exactly/);
    expect(() => parseCommittedProfile(JSON.stringify({ owner_buffered: "maybe", owner_buffered_workspaces: "*", owner_buffered_events: "on", owner_buffered_statement_timeout_ms: "6000" }))).toThrow(/on or off/);
  });
});


describe("050030 exact-session deployment selection", () => {
  test("preserve and production profile keep the live selection; neither silently enables it", async () => {
    for (const mode of ["preserve", "profile"]) {
      for (const enabled of [false, true]) {
        const live = version({ ...values, ...(enabled ? { READ_API_OWNER_BUFFERED_EXACT_SESSION: "on" } : {}) });
        const base = { ...env, DEPLOY_TARGET: "production", OWNER_BUFFERED: mode, OWNER_BUFFERED_EXACT_SESSION: "off" };
        const resolved = await resolveProfile(base, template, get(live));
        expect(resolved.OWNER_BUFFERED_EXACT_SESSION).toBe(enabled ? "on" : "off");
        const rendered = ts.parseConfigFileTextToJson("config.jsonc", renderDeployConfig(template, profileInputs({ ...base, ...resolved }))).config;
        expect(rendered.vars.READ_API_OWNER_BUFFERED_EXACT_SESSION).toBe(enabled ? "on" : undefined);
        await expect(resolveProfile({ ...base, OWNER_BUFFERED_EXACT_SESSION: "on" }, template, get(live))).rejects.toThrow(/feature overrides/);
      }
    }
  });
  test("explicit on selects the new RPC without changing the committed profile", async () => {
    const base = { ...env, DEPLOY_TARGET: "production", OWNER_BUFFERED: "on", OWNER_BUFFERED_EXACT_SESSION: "on", OWNER_BUFFERED_S3_BUCKET: values.INGEST_S3_BUCKET, OWNER_BUFFERED_S3_REGION: values.INGEST_S3_REGION };
    const resolved = await resolveProfile(base, template, get());
    const rendered = ts.parseConfigFileTextToJson("config.jsonc", renderDeployConfig(template, profileInputs({ ...base, ...resolved }))).config;
    expect(rendered.vars.READ_API_OWNER_BUFFERED_EXACT_SESSION).toBe("on");
  });
});
