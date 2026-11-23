import { readFileSync } from "node:fs";
import { describe, expect, test } from "vitest";
import ts from "typescript";
import { apiRoot, checkPreservedVersion, parseCommittedProfile, preservedProfile, profileInputs, resolveProfile, workflowEnvironment } from "./resolve-deploy-profile.ts";
import { CANONICAL_EVENTS_HOSTED_VAR, CANONICAL_EVENTS_WORKSPACES_VAR, RAW_STORE_VARS, renderDeployConfig } from "./render-deploy-config.ts";
import { TENANT_KEY_BASE } from "../../../stacks/aws/tenants.ts";

const template = readFileSync(new URL("../wrangler.jsonc", import.meta.url), "utf8");
// RAW_S3_*: the workflow always passes them (the override, else the ingest variables); the render reads them only with summary_fallback=on or analysis=on.
const env = { HYPERDRIVE_ID: "1234567890abcdef1234567890abcdef", OWNER_BUFFERED: "preserve", RAW_S3_BUCKET: "example-transcripts-bucket", RAW_S3_REGION: "us-east-1" };
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
  test("explicit off disables without preserving live settings (live is read only for the analysis guard, no pin)", async () => {
    const resolved = await resolveProfile({ ...env, OWNER_BUFFERED: "off", DIRECTORY_WORKSPACES_INPUT: "off" }, template, get());
    expect(resolved).toEqual({ OWNER_BUFFERED: "off", OWNER_BUFFERED_WORKSPACES: "", OWNER_BUFFERED_EVENTS: "off", OWNER_BUFFERED_STATEMENT_TIMEOUT_MS: "off", DIRECTORY_WORKSPACES: "" });
  });
  test("a template that predeclares a hosted canonical events var is refused by preserve and explicit dispatches", async () => {
    for (const name of [CANONICAL_EVENTS_HOSTED_VAR, CANONICAL_EVENTS_WORKSPACES_VAR]) {
      const flagged = template.replace(/"vars"\s*:\s*\{/, `"vars": { "${name}": "true",`);
      expect(flagged).not.toBe(template);
      for (const mode of [{}, { OWNER_BUFFERED: "off" }, { OWNER_BUFFERED: "off", DIRECTORY_WORKSPACES_INPUT: "off" }])
        await expect(resolveProfile({ ...env, ...mode }, flagged, get())).rejects.toThrow(new RegExp(`must leave ${name} unset`));
    }
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
    const narrowed = await resolveProfile({ ...env, OWNER_BUFFERED: "on", DIRECTORY_WORKSPACES_INPUT: "fixture-workspace", OWNER_BUFFERED_WORKSPACES: "fixture-workspace", OWNER_BUFFERED_S3_BUCKET: values.INGEST_S3_BUCKET, OWNER_BUFFERED_S3_REGION: values.INGEST_S3_REGION }, template, get());
    expect(narrowed.DIRECTORY_WORKSPACES).toBe("fixture-workspace");
    expect(narrowed.PRESERVED_READER_VERSION).toBeUndefined();
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
  test("brief_summary / list_short_summary: off renders no var, on is the exact \"true\" --var, and a dispatch never silently drops a live one", async () => {
    for (const [key, name] of [["BRIEF_SUMMARY", "READ_API_BRIEF_SUMMARY_ENABLED"], ["LIST_SHORT_SUMMARY", "READ_API_LIST_SHORT_SUMMARY_ENABLED"], ["LIST_READINESS", "READ_API_LIST_READINESS_ENABLED"], ["SUMMARY_FALLBACK", "READ_API_SUMMARY_FALLBACK_ENABLED"], ["SUMMARY_SEARCH", "READ_API_SUMMARY_SEARCH_ENABLED"], ["RELAYFACTS", "READ_API_RELAYFACTS_ENABLED"], ["SESSION_SOURCES", "READ_API_SESSION_SOURCES_ENABLED"], ["ANALYSIS", "READ_API_ANALYSIS_ENABLED"]] as const) {
      // Not live and off (the default dispatch, or no input): unchanged.
      for (const flag of [undefined, "", "off"]) await expect(resolveProfile({ ...env, [key]: flag }, template, get()), `${key}=${flag}`).resolves.toHaveProperty("OWNER_BUFFERED", "on");
      // Not live and on: a new variable, not a collateral change.
      await expect(resolveProfile({ ...env, [key]: "on" }, template, get())).resolves.toHaveProperty("OWNER_BUFFERED", "on");
      // Live "true": repeating on keeps it; off would drop it, so profile/preserve refuses.
      const live = get(version({ ...values, [name]: "true" }));
      await expect(resolveProfile({ ...env, [key]: "on" }, template, live)).resolves.toHaveProperty("OWNER_BUFFERED", "on");
      for (const flag of [undefined, "off"]) await expect(resolveProfile({ ...env, [key]: flag }, template, live), `${key}=${flag}`).rejects.toThrow(new RegExp(`change or drop live setting ${name}`));
      // Only off/on are inputs; nothing else is passed through as a value.
      for (const bad of ["true", "1", "ON", " on", "yes"]) await expect(resolveProfile({ ...env, [key]: bad }, template, get()), `${key}=${bad}`).rejects.toThrow(key === "ANALYSIS" ? /must be off, on or disable/ : /must be off or on/);
    }
  });
  test("the ANALYSIS_ADMIT Hyperdrive binding: added with SESSION_READ_ANALYSIS_HYPERDRIVE_ID, kept when repeated, never silently dropped or changed", async () => {
    const AD = "fedcba9876543210fedcba9876543210";
    const withBinding = (extra: Record<string, string>) => { const v = version(); (v.resources.bindings as Array<Record<string, string>>).push(extra); return v; };
    const live = get(withBinding({ name: "ANALYSIS_ADMIT", type: "hyperdrive", id: AD }));
    // Not live: adding it is a new binding, not a collateral change; without the variable nothing changes.
    await expect(resolveProfile({ ...env, ANALYSIS_HYPERDRIVE_ID: AD }, template, get())).resolves.toHaveProperty("OWNER_BUFFERED", "on");
    await expect(resolveProfile(env, template, get())).resolves.toHaveProperty("OWNER_BUFFERED", "on");
    // Live: the same id keeps it; unset would drop it and another id would change it, so both refuse.
    await expect(resolveProfile({ ...env, ANALYSIS_HYPERDRIVE_ID: AD }, template, live)).resolves.toHaveProperty("OWNER_BUFFERED", "on");
    await expect(resolveProfile(env, template, live)).rejects.toThrow(/change or drop live Hyperdrive binding ANALYSIS_ADMIT/);
    await expect(resolveProfile({ ...env, ANALYSIS_HYPERDRIVE_ID: "abcdefabcdefabcdefabcdefabcdefab" }, template, live)).rejects.toThrow(/change or drop live Hyperdrive binding ANALYSIS_ADMIT/);
    // The render's own validation applies: the read role's config is refused.
    await expect(resolveProfile({ ...env, ANALYSIS_HYPERDRIVE_ID: env.HYPERDRIVE_ID }, template, get())).rejects.toThrow(/read role's Hyperdrive config/);
    expect(profileInputs({ ...env, ANALYSIS_HYPERDRIVE_ID: AD }).analysisHyperdriveId).toBe(AD);
  });
  test("explicit owner_buffered=on/off never silently drops a live analysis flag or ANALYSIS_ADMIT binding; analysis=disable removes them", async () => {
    const AD = "fedcba9876543210fedcba9876543210";
    const live = (extra: Record<string, string>[]) => { const v = version({ ...values, READ_API_ANALYSIS_ENABLED: "true" }); (v.resources.bindings as Array<Record<string, string>>).push(...extra); return get(v); };
    const on = { ...env, OWNER_BUFFERED: "on", OWNER_BUFFERED_WORKSPACES: "*", OWNER_BUFFERED_S3_BUCKET: values.INGEST_S3_BUCKET, OWNER_BUFFERED_S3_REGION: values.INGEST_S3_REGION };
    const off = { ...env, OWNER_BUFFERED: "off" };
    const binding = { name: "ANALYSIS_ADMIT", type: "hyperdrive", id: AD };
    for (const explicit of [on, off, { ...off, DIRECTORY_WORKSPACES_INPUT: "off" }, { ...on, DIRECTORY_WORKSPACES_INPUT: "*" }]) {
      // analysis left at its default: refused, whether or not the directory comes from live.
      for (const analysis of [undefined, "", "off"]) {
        await expect(resolveProfile({ ...explicit, ANALYSIS: analysis, ANALYSIS_HYPERDRIVE_ID: AD }, template, live([binding])), String(analysis)).rejects.toThrow(/drop live setting READ_API_ANALYSIS_ENABLED/);
      }
      // Repeating it (flag and binding) passes; dropping or changing only the binding is refused.
      await expect(resolveProfile({ ...explicit, ANALYSIS: "on", ANALYSIS_HYPERDRIVE_ID: AD }, template, live([binding]))).resolves.toHaveProperty("OWNER_BUFFERED", explicit.OWNER_BUFFERED);
      await expect(resolveProfile({ ...explicit, ANALYSIS: "on" }, template, live([binding]))).rejects.toThrow(/drop live Hyperdrive binding ANALYSIS_ADMIT/);
      await expect(resolveProfile({ ...explicit, ANALYSIS: "on", ANALYSIS_HYPERDRIVE_ID: "abcdefabcdefabcdefabcdefabcdefab" }, template, live([binding]))).rejects.toThrow(/drop live Hyperdrive binding ANALYSIS_ADMIT/);
      // The explicit removal; a set variable may keep the live config but never replace it.
      await expect(resolveProfile({ ...explicit, ANALYSIS: "disable" }, template, live([binding]))).resolves.toHaveProperty("OWNER_BUFFERED", explicit.OWNER_BUFFERED);
      await expect(resolveProfile({ ...explicit, ANALYSIS: "disable", ANALYSIS_HYPERDRIVE_ID: AD }, template, live([binding]))).resolves.toHaveProperty("OWNER_BUFFERED", explicit.OWNER_BUFFERED);
      const breakGlass = "DIRECTORY_WORKSPACES_INPUT" in explicit;
      if (!breakGlass) await expect(resolveProfile({ ...explicit, ANALYSIS: "disable", ANALYSIS_HYPERDRIVE_ID: "abcdefabcdefabcdefabcdefabcdefab" }, template, live([binding]))).rejects.toThrow(/would replace live Hyperdrive binding ANALYSIS_ADMIT/);
      // Live metadata unreadable: refused unless this is the break-glass path.
      await expect(resolveProfile({ ...explicit, ANALYSIS: "on" }, template, async () => { throw Error("unavailable"); })).rejects.toThrow();
      await expect(resolveProfile({ ...explicit, ANALYSIS: "off" }, template, async () => { throw Error("unavailable"); })).rejects.toThrow();
      const unreadable = resolveProfile({ ...explicit, ANALYSIS: "disable" }, template, async () => { throw Error("must not call"); });
      if (breakGlass) await expect(unreadable).resolves.toEqual({ OWNER_BUFFERED: explicit.OWNER_BUFFERED, OWNER_BUFFERED_WORKSPACES: explicit.OWNER_BUFFERED === "on" ? "*" : "", OWNER_BUFFERED_EVENTS: "off", OWNER_BUFFERED_STATEMENT_TIMEOUT_MS: "off", DIRECTORY_WORKSPACES: explicit.DIRECTORY_WORKSPACES_INPUT === "off" ? "" : explicit.DIRECTORY_WORKSPACES_INPUT });
      else await expect(unreadable).rejects.toThrow();
    }
    // Break-glass: explicit off + directory + analysis=disable contacts no live metadata (split, absent or failing API).
    for (const failing of [async () => { throw Error("must not call"); }, async () => ({ deployments: [{ versions: [{ version_id: id, percentage: 50 }] }] })]) {
      await expect(resolveProfile({ ...env, OWNER_BUFFERED: "off", DIRECTORY_WORKSPACES_INPUT: "off", ANALYSIS: "disable" }, template, failing)).resolves.toEqual({ OWNER_BUFFERED: "off", OWNER_BUFFERED_WORKSPACES: "", OWNER_BUFFERED_EVENTS: "off", OWNER_BUFFERED_STATEMENT_TIMEOUT_MS: "off", DIRECTORY_WORKSPACES: "" });
    }
    // preserve with disable: a replacing variable is refused too.
    await expect(resolveProfile({ ...env, ANALYSIS: "disable", ANALYSIS_HYPERDRIVE_ID: "abcdefabcdefabcdefabcdefabcdefab" }, template, live([binding]))).rejects.toThrow(/would replace live Hyperdrive binding ANALYSIS_ADMIT/);
    // profile/preserve: analysis=disable also lets the dispatch drop both.
    await expect(resolveProfile({ ...env, ANALYSIS: "disable" }, template, live([binding]))).resolves.toHaveProperty("OWNER_BUFFERED", "on");
    await expect(resolveProfile({ ...env, ANALYSIS: "off", ANALYSIS_HYPERDRIVE_ID: AD }, template, live([binding]))).rejects.toThrow(/drop live setting READ_API_ANALYSIS_ENABLED/);
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
    expect(await resolveProfile({ ...prod, OWNER_BUFFERED: "off", DIRECTORY_WORKSPACES_INPUT: "off" }, template, get()))
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


describe("047010 exact-session deployment selection", () => {
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


describe("047020 paged stages deployment selection", () => {
  test("preserve and production profile keep the live value; an explicit on is refused", async () => {
    for (const mode of ["preserve", "profile"]) {
      for (const enabled of [false, true]) {
        const live = version({ ...values, READ_API_OWNER_BUFFERED_EXACT_SESSION: "on", ...(enabled ? { READ_API_OWNER_BUFFERED_STAGES_PAGED: "on" } : {}) });
        const base = { ...env, DEPLOY_TARGET: "production", OWNER_BUFFERED: mode, OWNER_BUFFERED_EXACT_SESSION: "off", OWNER_BUFFERED_STAGES_PAGED: "off" };
        const resolved = await resolveProfile(base, template, get(live));
        expect(resolved.OWNER_BUFFERED_STAGES_PAGED).toBe(enabled ? "on" : "off");
        expect(resolved.OWNER_BUFFERED_EXACT_SESSION).toBe("on");
        const rendered = ts.parseConfigFileTextToJson("config.jsonc", renderDeployConfig(template, profileInputs({ ...base, ...resolved }))).config;
        expect(rendered.vars.READ_API_OWNER_BUFFERED_STAGES_PAGED).toBe(enabled ? "on" : undefined);
        await expect(resolveProfile({ ...base, OWNER_BUFFERED_STAGES_PAGED: "on" }, template, get(live))).rejects.toThrow(/feature overrides/);
      }
    }
  });
  test("a live paged flag with an unknown value is refused, never reinterpreted", async () => {
    const live = version({ ...values, READ_API_OWNER_BUFFERED_EXACT_SESSION: "on", READ_API_OWNER_BUFFERED_STAGES_PAGED: "true" });
    await expect(resolveProfile({ ...env, DEPLOY_TARGET: "production", OWNER_BUFFERED: "preserve" }, template, get(live))).rejects.toThrow(/unknown value/);
  });
  test("explicit on renders the flag only together with exact-session on", async () => {
    const base = { ...env, DEPLOY_TARGET: "production", OWNER_BUFFERED: "on", OWNER_BUFFERED_EXACT_SESSION: "on", OWNER_BUFFERED_STAGES_PAGED: "on", OWNER_BUFFERED_S3_BUCKET: values.INGEST_S3_BUCKET, OWNER_BUFFERED_S3_REGION: values.INGEST_S3_REGION };
    const resolved = await resolveProfile(base, template, get());
    const rendered = ts.parseConfigFileTextToJson("config.jsonc", renderDeployConfig(template, profileInputs({ ...base, ...resolved }))).config;
    expect(rendered.vars.READ_API_OWNER_BUFFERED_STAGES_PAGED).toBe("on");
    expect(rendered.vars.READ_API_OWNER_BUFFERED_EXACT_SESSION).toBe("on");
    await expect(resolveProfile({ ...base, OWNER_BUFFERED_EXACT_SESSION: "off" }, template, get())).rejects.toThrow(/stages_paged=on needs/);
  });
});

describe("the raw-store vars (summary_fallback / analysis) in the live-settings guard", () => {
  const raw = { PIPELINE_RAW_S3_BUCKET: env.RAW_S3_BUCKET, PIPELINE_RAW_S3_REGION: env.RAW_S3_REGION, PIPELINE_KEY_PREFIX: TENANT_KEY_BASE, PIPELINE_ORG_FROM_PREFIX: "true" };
  const fallbackLive = get(version({ ...values, READ_API_SUMMARY_FALLBACK_ENABLED: "true", ...raw }));
  const analysisLive = get(version({ ...values, READ_API_ANALYSIS_ENABLED: "true", ...raw }));

  test("profileInputs passes the inputs and the bucket/region through to the render", () => {
    const inputs = profileInputs({ ...env, OWNER_BUFFERED: "off", SUMMARY_FALLBACK: "on", ANALYSIS: "disable" });
    expect(inputs).toMatchObject({ summaryFallback: "on", analysis: "disable", rawS3Bucket: env.RAW_S3_BUCKET, rawS3Region: env.RAW_S3_REGION });
    const config = ts.parseConfigFileTextToJson("c.jsonc", renderDeployConfig(template, inputs)).config;
    expect(config.vars).toMatchObject(raw);
  });

  test("a dispatch that keeps summary_fallback=on keeps the live bucket; it cannot drop or change it", async () => {
    await expect(resolveProfile({ ...env, BRIEF_SUMMARY: "on", SUMMARY_FALLBACK: "on" }, template, get(version({ ...values, READ_API_BRIEF_SUMMARY_ENABLED: "true", READ_API_SUMMARY_FALLBACK_ENABLED: "true", ...raw })))).resolves.toHaveProperty("OWNER_BUFFERED", "on");
    await expect(resolveProfile({ ...env, SUMMARY_FALLBACK: "on" }, template, fallbackLive)).resolves.toHaveProperty("OWNER_BUFFERED", "on");
    // An empty bucket while on cannot render at all (so it can never drop the live one).
    await expect(resolveProfile({ ...env, SUMMARY_FALLBACK: "on", RAW_S3_BUCKET: "" }, template, fallbackLive)).rejects.toThrow(/SESSION_READ_RAW_S3_BUCKET or SESSION_READ_INGEST_S3_BUCKET/);
    await expect(resolveProfile({ ...env, SUMMARY_FALLBACK: "on", RAW_S3_REGION: "" }, template, fallbackLive)).rejects.toThrow(/SESSION_READ_RAW_S3_REGION or SESSION_READ_INGEST_S3_REGION/);
    // Another bucket or region would change a live setting: refused.
    await expect(resolveProfile({ ...env, SUMMARY_FALLBACK: "on", RAW_S3_BUCKET: "example-other-bucket" }, template, fallbackLive)).rejects.toThrow(/change or drop live setting PIPELINE_RAW_S3_BUCKET/);
    await expect(resolveProfile({ ...env, SUMMARY_FALLBACK: "on", RAW_S3_REGION: "eu-west-1" }, template, fallbackLive)).rejects.toThrow(/change or drop live setting PIPELINE_RAW_S3_REGION/);
    // Not live yet: the first on dispatch adds the vars (new settings, not a collateral change).
    await expect(resolveProfile({ ...env, SUMMARY_FALLBACK: "on" }, template, get())).resolves.toHaveProperty("OWNER_BUFFERED", "on");
  });

  test("a dispatch with both off never silently drops live raw-store vars; analysis=disable (without summary_fallback=on) is the explicit removal", async () => {
    for (const name of RAW_STORE_VARS) {
      // Only the raw-store var live (no flag): off would drop it, so refused, in preserve and profile mode.
      const only = get(version({ ...values, [name]: raw[name] }));
      for (const mode of ["preserve", "profile", ""]) {
        for (const off of [{}, { SUMMARY_FALLBACK: "off", ANALYSIS: "off" }]) {
          await expect(resolveProfile({ ...env, OWNER_BUFFERED: mode, ...off }, template, only), `${name} ${mode} ${JSON.stringify(off)}`).rejects.toThrow(new RegExp(`change or drop live setting ${name}`));
        }
      }
      // analysis=disable alone is the removal, but not of a raw store timing/turns/metadata read
      // through (no live READ_API_RAW_STORE_OTHER_READS=off): refused; with the switch it may drop both.
      await expect(resolveProfile({ ...env, ANALYSIS: "disable" }, template, only)).rejects.toThrow(/analysis=disable would drop the live raw-store vars that timing, turns and metadata read through/);
      await expect(resolveProfile({ ...env, ANALYSIS: "disable" }, template, get(version({ ...values, [name]: raw[name], READ_API_RAW_STORE_OTHER_READS: "off" })))).resolves.toHaveProperty("OWNER_BUFFERED", "on");
    }
    await expect(resolveProfile({ ...env, ANALYSIS: "disable" }, template, analysisLive)).rejects.toThrow(/would drop the live raw-store vars/);
    await expect(resolveProfile({ ...env, ANALYSIS: "disable" }, template, get(version({ ...values, READ_API_ANALYSIS_ENABLED: "true", ...raw, READ_API_RAW_STORE_OTHER_READS: "off" })))).resolves.toHaveProperty("OWNER_BUFFERED", "on");
    await expect(resolveProfile({ ...env, ANALYSIS: "on" }, template, analysisLive)).resolves.toHaveProperty("OWNER_BUFFERED", "on");
    await expect(resolveProfile({ ...env, ANALYSIS: "on", RAW_S3_BUCKET: "example-other-bucket" }, template, analysisLive)).rejects.toThrow(/change or drop live setting PIPELINE_RAW_S3_BUCKET/);
    await expect(resolveProfile({ ...env, SUMMARY_FALLBACK: "on", ANALYSIS: "disable", RAW_S3_BUCKET: "example-other-bucket" }, template, get(version({ ...values, READ_API_SUMMARY_FALLBACK_ENABLED: "true", READ_API_ANALYSIS_ENABLED: "true", ...raw })))).rejects.toThrow(/change or drop live setting PIPELINE_RAW_S3_BUCKET/);
    // Summary fallback stays live: analysis=disable must not drop its bucket (the flag guard refuses first without summary_fallback=on).
    // (Without the switch the timing/turns/metadata refusal comes first; with it, the flag guard.)
    await expect(resolveProfile({ ...env, ANALYSIS: "disable" }, template, fallbackLive)).rejects.toThrow(/would drop the live raw-store vars/);
    await expect(resolveProfile({ ...env, ANALYSIS: "disable" }, template, get(version({ ...values, READ_API_SUMMARY_FALLBACK_ENABLED: "true", ...raw, READ_API_RAW_STORE_OTHER_READS: "off" })))).rejects.toThrow(/change or drop live setting READ_API_SUMMARY_FALLBACK_ENABLED/);
    await expect(resolveProfile({ ...env, SUMMARY_FALLBACK: "on", ANALYSIS: "disable" }, template, fallbackLive)).resolves.toHaveProperty("OWNER_BUFFERED", "on");
  });

  test("a live raw-store var stored as a secret is never shadowed by a rendered plain var", async () => {
    const v = version();
    (v.resources.bindings as Array<Record<string, string>>).push({ name: "PIPELINE_RAW_S3_BUCKET", type: "secret_text", text: "MUST_NOT_BE_COPIED" });
    await expect(resolveProfile({ ...env, SUMMARY_FALLBACK: "on" }, template, get(v))).rejects.toThrow(/replacing secret binding PIPELINE_RAW_S3_BUCKET/);
    await expect(resolveProfile(env, template, get(v))).resolves.toHaveProperty("OWNER_BUFFERED", "on");
  });
});

describe("READ_API_RAW_STORE_OTHER_READS: rendering the raw store never changes timing, turns or metadata", () => {
  const raw = { PIPELINE_RAW_S3_BUCKET: env.RAW_S3_BUCKET, PIPELINE_RAW_S3_REGION: env.RAW_S3_REGION, PIPELINE_KEY_PREFIX: TENANT_KEY_BASE, PIPELINE_ORG_FROM_PREFIX: "true" };
  const SWITCH = "READ_API_RAW_STORE_OTHER_READS";
  const rendered = (resolved: Record<string, string>, e: Record<string, string>) =>
    ts.parseConfigFileTextToJson("c.jsonc", renderDeployConfig(template, profileInputs({ ...e, ...resolved }))).config.vars as Record<string, string>;
  const ons: Record<string, string>[] = [{ SUMMARY_FALLBACK: "on" }, { ANALYSIS: "on" }, { SUMMARY_FALLBACK: "on", ANALYSIS: "on" }];
  const explicitOn = { OWNER_BUFFERED: "on", OWNER_BUFFERED_WORKSPACES: "*", OWNER_BUFFERED_S3_BUCKET: values.INGEST_S3_BUCKET, OWNER_BUFFERED_S3_REGION: values.INGEST_S3_REGION };
  const modes: Record<string, string>[] = [{ OWNER_BUFFERED: "preserve" }, { OWNER_BUFFERED: "profile" }, { OWNER_BUFFERED: "profile", DEPLOY_TARGET: "production" }, explicitOn, { OWNER_BUFFERED: "off" }];

  test("fresh add (no raw store live): the render adds the switch off beside the raw-store vars, in every mode", async () => {
    for (const on of ons) for (const mode of modes) {
      const e = { ...env, ...mode, ...on };
      const resolved = await resolveProfile(e, template, get());
      expect(resolved.RAW_STORE_OTHER_READS, JSON.stringify(e)).toBe("off");
      expect(workflowEnvironment(resolved)).toContain("RAW_STORE_OTHER_READS=off\n");
      expect(rendered(resolved, e)).toMatchObject({ ...raw, [SWITCH]: "off" });
    }
  });

  test("a live raw store without the switch: no switch is added, and every raw-store var must already be live as plain text", async () => {
    const live = get(version({ ...values, ...raw }));
    for (const on of ons) for (const mode of modes) {
      const e = { ...env, ...mode, ...on };
      const resolved = await resolveProfile(e, template, live);
      expect(resolved, JSON.stringify(e)).not.toHaveProperty("RAW_STORE_OTHER_READS");
      expect(workflowEnvironment(resolved)).not.toContain("RAW_STORE_OTHER_READS");
      expect(rendered(resolved, e)).not.toHaveProperty(SWITCH);
    }
    // The bucket live but another raw-store var not: the render would change what those reads see.
    for (const name of RAW_STORE_VARS.slice(1)) {
      const partial: Record<string, string> = { ...values, ...raw };
      delete partial[name];
      for (const mode of modes) {
        await expect(resolveProfile({ ...env, ...mode, SUMMARY_FALLBACK: "on" }, template, get(version(partial)))).rejects.toThrow(new RegExp(`has PIPELINE_RAW_S3_BUCKET without ${SWITCH} and without plain ${name}`));
      }
    }
    // A different bucket or region than live is refused in every mode, explicit included (where the live-settings guard does not run).
    for (const mode of modes) {
      await expect(resolveProfile({ ...env, ...mode, SUMMARY_FALLBACK: "on", RAW_S3_BUCKET: "example-other-bucket" }, template, live)).rejects.toThrow(/change or drop live setting PIPELINE_RAW_S3_BUCKET/);
      await expect(resolveProfile({ ...env, ...mode, SUMMARY_FALLBACK: "on", RAW_S3_REGION: "eu-west-1" }, template, live)).rejects.toThrow(/change or drop live setting PIPELINE_RAW_S3_REGION/);
    }
    for (const mode of [explicitOn, { OWNER_BUFFERED: "off" }]) {
      await expect(resolveProfile({ ...env, ...mode, ANALYSIS: "on", RAW_S3_BUCKET: "example-other-bucket" }, template, live)).rejects.toThrow(/change or drop live setting PIPELINE_RAW_S3_BUCKET, which timing, turns and metadata read/);
      await expect(resolveProfile({ ...env, ...mode, ANALYSIS: "on" }, template, get(version({ ...values, ...raw, PIPELINE_KEY_PREFIX: "example-other-base" })))).rejects.toThrow(/change or drop live setting PIPELINE_KEY_PREFIX, which timing, turns and metadata read/);
    }
    // A bucket stored as a secret is never shadowed, in explicit mode too (where the live-settings guard does not run).
    const v = version();
    (v.resources.bindings as Array<Record<string, string>>).push({ name: "PIPELINE_RAW_S3_BUCKET", type: "secret_text", text: "MUST_NOT_BE_COPIED" });
    await expect(resolveProfile({ ...env, ...explicitOn, SUMMARY_FALLBACK: "on" }, template, get(v))).rejects.toThrow(/replacing secret binding PIPELINE_RAW_S3_BUCKET/);
  });

  test("a live switch is kept (only plain \"off\" is accepted) and a dispatch never drops it silently", async () => {
    const live = get(version({ ...values, ...raw, [SWITCH]: "off" }));
    for (const on of ons) for (const mode of modes) {
      const e = { ...env, ...mode, ...on };
      const resolved = await resolveProfile(e, template, live);
      expect(resolved.RAW_STORE_OTHER_READS, JSON.stringify(e)).toBe("off");
      expect(rendered(resolved, e)).toMatchObject({ ...raw, [SWITCH]: "off" });
    }
    // Any other live value, or a secret, is refused rather than reinterpreted.
    for (const text of ["", "on", "OFF", "false"]) {
      await expect(resolveProfile({ ...env, SUMMARY_FALLBACK: "on" }, template, get(version({ ...values, ...raw, [SWITCH]: text })))).rejects.toThrow(/READ_API_RAW_STORE_OTHER_READS is not plain text "off"/);
    }
    const s = version({ ...values, ...raw });
    (s.resources.bindings as Array<Record<string, string>>).push({ name: SWITCH, type: "secret_text", text: "MUST_NOT_BE_COPIED" });
    await expect(resolveProfile({ ...env, SUMMARY_FALLBACK: "on" }, template, get(s))).rejects.toThrow(/is not plain text "off"/);
    // Both off (not the removal): the live switch would be dropped, refused like the vars.
    await expect(resolveProfile(env, template, live)).rejects.toThrow(/change or drop live setting/);
  });

  test("the removal path: drops the vars and the switch together, never a raw store those reads were served through", async () => {
    // Switch live: analysis=disable drops vars and switch; the render carries neither.
    const withSwitch = get(version({ ...values, READ_API_ANALYSIS_ENABLED: "true", ...raw, [SWITCH]: "off" }));
    for (const mode of [{ OWNER_BUFFERED: "preserve" }, explicitOn]) {
      const e = { ...env, ...mode, ANALYSIS: "disable" };
      const resolved = await resolveProfile(e, template, withSwitch);
      expect(resolved).not.toHaveProperty("RAW_STORE_OTHER_READS");
      const vars = rendered(resolved, e);
      for (const name of [...RAW_STORE_VARS, SWITCH]) expect(vars).not.toHaveProperty(name);
    }
    // No switch: timing/turns/metadata were live through the raw store; refused in every mode that reads live.
    const without = get(version({ ...values, READ_API_ANALYSIS_ENABLED: "true", ...raw }));
    for (const mode of [{ OWNER_BUFFERED: "preserve" }, { OWNER_BUFFERED: "profile" }, explicitOn, { OWNER_BUFFERED: "off" }]) {
      await expect(resolveProfile({ ...env, ...mode, ANALYSIS: "disable" }, template, without)).rejects.toThrow(/analysis=disable would drop the live raw-store vars that timing, turns and metadata read through/);
    }
    // A secret bucket stays after the dispatch: dropping the switch would turn those reads on.
    const secret = version({ ...values, READ_API_ANALYSIS_ENABLED: "true", [SWITCH]: "off" });
    (secret.resources.bindings as Array<Record<string, string>>).push({ name: "PIPELINE_RAW_S3_BUCKET", type: "secret_text", text: "MUST_NOT_BE_COPIED" });
    await expect(resolveProfile({ ...env, ANALYSIS: "disable" }, template, get(secret))).rejects.toThrow(/while PIPELINE_RAW_S3_BUCKET stays a secret/);
    // With summary_fallback=on it is not the removal: the vars and the switch stay rendered.
    const both = get(version({ ...values, READ_API_SUMMARY_FALLBACK_ENABLED: "true", READ_API_ANALYSIS_ENABLED: "true", ...raw, [SWITCH]: "off" }));
    const e = { ...env, SUMMARY_FALLBACK: "on", ANALYSIS: "disable" };
    expect(rendered(await resolveProfile(e, template, both), e)).toMatchObject({ ...raw, [SWITCH]: "off" });
  });

  test("break-glass (no live read) refuses summary_fallback=on; the environment cannot inject the switch", async () => {
    const never = async () => { throw new Error("break-glass must not read live"); };
    await expect(resolveProfile({ ...env, OWNER_BUFFERED: "off", DIRECTORY_WORKSPACES_INPUT: "off", ANALYSIS: "disable", SUMMARY_FALLBACK: "on" }, template, never)).rejects.toThrow(/summary_fallback=on needs the live raw-store check/);
    const resolved = await resolveProfile({ ...env, OWNER_BUFFERED: "off", DIRECTORY_WORKSPACES_INPUT: "off", ANALYSIS: "disable" }, template, never);
    expect(resolved).not.toHaveProperty("RAW_STORE_OTHER_READS");
    // A RAW_STORE_OTHER_READS in the dispatch environment is ignored: the value comes from the live Worker only.
    const live = get(version({ ...values, ...raw }));
    const injected = await resolveProfile({ ...env, SUMMARY_FALLBACK: "on", RAW_STORE_OTHER_READS: "off" }, template, live);
    expect(injected).not.toHaveProperty("RAW_STORE_OTHER_READS");
    await expect(resolveProfile({ ...env, RAW_STORE_OTHER_READS: "off" }, template, get())).resolves.not.toHaveProperty("RAW_STORE_OTHER_READS");
  });
});

describe("READ_API_RAW_STORE_OTHER_READS: secrets, explicit both-off, other live PIPELINE_RAW_* settings", () => {
  const raw: Record<string, string> = { PIPELINE_RAW_S3_BUCKET: env.RAW_S3_BUCKET, PIPELINE_RAW_S3_REGION: env.RAW_S3_REGION, PIPELINE_KEY_PREFIX: TENANT_KEY_BASE, PIPELINE_ORG_FROM_PREFIX: "true" };
  const SWITCH = "READ_API_RAW_STORE_OTHER_READS";
  const explicitOn = { OWNER_BUFFERED: "on", OWNER_BUFFERED_WORKSPACES: "*", OWNER_BUFFERED_S3_BUCKET: values.INGEST_S3_BUCKET, OWNER_BUFFERED_S3_REGION: values.INGEST_S3_REGION };
  const explicitModes: Record<string, string>[] = [explicitOn, { OWNER_BUFFERED: "off" }];
  const modes: Record<string, string>[] = [{ OWNER_BUFFERED: "preserve" }, { OWNER_BUFFERED: "profile" }, { OWNER_BUFFERED: "profile", DEPLOY_TARGET: "production" }, ...explicitModes];
  const ons: Record<string, string>[] = [{ SUMMARY_FALLBACK: "on" }, { ANALYSIS: "on" }, { SUMMARY_FALLBACK: "on", ANALYSIS: "on" }];
  const offs: Record<string, string>[] = [{}, { SUMMARY_FALLBACK: "off", ANALYSIS: "off" }];
  const withSecrets = (plain: Record<string, string>, secrets: string[]) => {
    const v = version(plain);
    for (const name of secrets) (v.resources.bindings as Array<Record<string, string>>).push({ name, type: "secret_text", text: "MUST_NOT_BE_COPIED" });
    return v;
  };
  const rendered = (resolved: Record<string, string>, e: Record<string, string>) => renderDeployConfig(template, profileInputs({ ...e, ...resolved }));

  test("a raw-store var stored as a secret is refused before a live switch is accepted, in every mode", async () => {
    for (const name of RAW_STORE_VARS) {
      const plain: Record<string, string> = { ...values, ...raw };
      delete plain[name];
      for (const sw of [{}, { [SWITCH]: "off" }] as Record<string, string>[]) {
        const live = get(withSecrets({ ...plain, ...sw }, [name]));
        for (const mode of modes) for (const on of ons) {
          await expect(resolveProfile({ ...env, ...mode, ...on }, template, live), `${name} ${JSON.stringify({ ...sw, ...mode, ...on })}`).rejects.toThrow(new RegExp(`replacing secret binding ${name}`));
        }
      }
      // Without the rest of the raw store too (no live bucket unless it is the secret itself).
      const alone = get(withSecrets({ ...values, [SWITCH]: "off" }, [name]));
      for (const mode of modes) await expect(resolveProfile({ ...env, ...mode, SUMMARY_FALLBACK: "on" }, template, alone)).rejects.toThrow(new RegExp(`replacing secret binding ${name}`));
    }
  });

  test("explicit owner_buffered=on/off with both inputs off refuses the drops the removal refuses", async () => {
    for (const mode of explicitModes) for (const off of offs) {
      const e = { ...env, ...mode, ...off };
      // (a) live plain raw-store vars without the switch: the dispatch would drop them and turn timing, turns and metadata off.
      for (const live of [raw, ...RAW_STORE_VARS.map((name) => ({ [name]: raw[name]! }))]) {
        await expect(resolveProfile(e, template, get(version({ ...values, ...live }))), `${JSON.stringify(e)} ${Object.keys(live)}`).rejects.toThrow(/would drop the live raw-store vars that timing, turns and metadata read through .*keep summary_fallback=on or analysis=on, or remove them in a separate reviewed change/);
      }
      // (b) a live switch beside a secret bucket: dropping the switch would turn those reads on.
      await expect(resolveProfile(e, template, get(withSecrets({ ...values, [SWITCH]: "off" }, ["PIPELINE_RAW_S3_BUCKET"])))).rejects.toThrow(/would drop live READ_API_RAW_STORE_OTHER_READS while PIPELINE_RAW_S3_BUCKET stays a secret/);
      // Plain vars with the switch: dropping both leaves those reads off (no store), as before.
      const resolved = await resolveProfile(e, template, get(version({ ...values, ...raw, [SWITCH]: "off" })));
      expect(resolved).not.toHaveProperty("RAW_STORE_OTHER_READS");
      // A secret bucket without a switch is untouched by the dispatch: nothing changes.
      await expect(resolveProfile(e, template, get(withSecrets(values, ["PIPELINE_RAW_S3_BUCKET"])))).resolves.not.toHaveProperty("RAW_STORE_OTHER_READS");
    }
    // Profile and preserve keep their own refusal (the live-settings guard), message unchanged.
    for (const mode of [{ OWNER_BUFFERED: "preserve" }, { OWNER_BUFFERED: "profile" }]) {
      await expect(resolveProfile({ ...env, ...mode }, template, get(version({ ...values, ...raw })))).rejects.toThrow(/^Preserve would change or drop live setting PIPELINE_RAW_S3_BUCKET/);
      await expect(resolveProfile({ ...env, ...mode }, template, get(version({ ...values, [SWITCH]: "off" })))).rejects.toThrow(/^Preserve would change or drop live setting READ_API_RAW_STORE_OTHER_READS/);
    }
  });

  test("the ordinary redeploy (no raw-store var, no switch; the credential secrets may be live) resolves and renders as before, in every mode", async () => {
    const creds = ["PIPELINE_RAW_S3_ACCESS_KEY_ID", "PIPELINE_RAW_S3_SECRET_ACCESS_KEY"];
    const pinned = { OWNER_BUFFERED: "on", OWNER_BUFFERED_WORKSPACES: "*", OWNER_BUFFERED_EVENTS: "on", OWNER_BUFFERED_STATEMENT_TIMEOUT_MS: "6000", DIRECTORY_WORKSPACES: "*", PRESERVED_READER_VERSION: id };
    const preserved = { ...pinned, OWNER_BUFFERED_S3_BUCKET: values.INGEST_S3_BUCKET, OWNER_BUFFERED_S3_REGION: values.INGEST_S3_REGION, OWNER_BUFFERED_EXACT_SESSION: "off", OWNER_BUFFERED_STAGES_PAGED: "off", OWNER_BUFFERED_PHASE_LOG: "off", OWNER_BUFFERED_INDEX_VERIFY: "off", BUFFERED_SEARCH_GATE: "off", OWNER_BUFFERED_STAGE_CONCURRENCY: "off" };
    const expected: Array<[Record<string, string>, Record<string, string>]> = [
      [{ OWNER_BUFFERED: "preserve" }, preserved],
      [{ OWNER_BUFFERED: "profile" }, preserved],
      [{ OWNER_BUFFERED: "profile", DEPLOY_TARGET: "production" }, preserved],
      // Explicit on outside production: the unset children render off.
      [explicitOn, { ...pinned, OWNER_BUFFERED_EVENTS: "off", OWNER_BUFFERED_STATEMENT_TIMEOUT_MS: "off" }],
      [{ ...explicitOn, DEPLOY_TARGET: "production" }, pinned],
      [{ OWNER_BUFFERED: "off" }, { OWNER_BUFFERED: "off", OWNER_BUFFERED_WORKSPACES: "", OWNER_BUFFERED_EVENTS: "off", OWNER_BUFFERED_STATEMENT_TIMEOUT_MS: "off", DIRECTORY_WORKSPACES: "*", PRESERVED_READER_VERSION: id }],
    ];
    for (const live of [version(), withSecrets(values, creds.slice(0, 1))]) {
      for (const [mode, want] of expected) for (const off of offs) {
        const e = { ...env, ...mode, ...off };
        const resolved = await resolveProfile(e, template, get(live));
        expect(resolved, JSON.stringify(e)).toEqual(want);
        // Byte-identical to the render without the summary_fallback / analysis / raw-store inputs at all.
        const bare: Record<string, string> = { ...e };
        for (const k of ["SUMMARY_FALLBACK", "ANALYSIS", "RAW_S3_BUCKET", "RAW_S3_REGION"]) delete bare[k];
        expect(rendered(resolved, e)).toBe(rendered(resolved, bare));
        expect(rendered(resolved, e)).not.toMatch(/PIPELINE_RAW_|READ_API_RAW_STORE_OTHER_READS/);
      }
    }
  });

  test("fresh-add detection: another live PIPELINE_RAW_* binding without the bucket is refused; the two credential secrets alone are a fresh add", async () => {
    // rawStoreFromEnv (packages/pipeline/src/lib/pipeline/adapter.ts) reads these besides RAW_STORE_VARS and the credentials.
    const others = ["PIPELINE_RAW_PROVIDER", "PIPELINE_RAW_S3_ENDPOINT", "PIPELINE_RAW_S3_SSE", "PIPELINE_RAW_GCS_TOKEN_URI", "PIPELINE_RAW_EXAMPLE_FUTURE"];
    for (const name of others) {
      for (const sw of [{}, { [SWITCH]: "off" }] as Record<string, string>[]) {
        for (const live of [version({ ...values, ...sw, [name]: "example-value" }), withSecrets({ ...values, ...sw }, [name])]) {
          for (const mode of modes) for (const on of ons) {
            await expect(resolveProfile({ ...env, ...mode, ...on }, template, get(live)), `${name} ${JSON.stringify({ ...sw, ...mode, ...on })}`).rejects.toThrow(new RegExp(`has ${name} without PIPELINE_RAW_S3_BUCKET; .*explicit deployment review required`));
          }
        }
      }
    }
    const creds = get(withSecrets(values, ["PIPELINE_RAW_S3_ACCESS_KEY_ID"]));
    for (const mode of modes) for (const on of ons) {
      const e = { ...env, ...mode, ...on };
      const resolved = await resolveProfile(e, template, creds);
      expect(resolved.RAW_STORE_OTHER_READS, JSON.stringify(e)).toBe("off");
      expect(ts.parseConfigFileTextToJson("c.jsonc", rendered(resolved, e)).config.vars).toMatchObject({ ...raw, [SWITCH]: "off" });
    }
  });

  test("explicit owner_buffered=on/off with a directory and an on input (not break-glass) adds the switch on a fresh add", async () => {
    for (const [mode, directory] of [[explicitOn, "*"], [{ OWNER_BUFFERED: "off" }, "off"]] as const) {
      for (const on of ons) {
        const e = { ...env, ...mode, ...on, DIRECTORY_WORKSPACES_INPUT: directory };
        const resolved = await resolveProfile(e, template, get());
        expect(resolved.RAW_STORE_OTHER_READS, JSON.stringify(e)).toBe("off");
        expect(resolved).not.toHaveProperty("PRESERVED_READER_VERSION");
        expect(ts.parseConfigFileTextToJson("c.jsonc", rendered(resolved, e)).config.vars).toMatchObject({ ...raw, [SWITCH]: "off" });
      }
    }
  });
});
