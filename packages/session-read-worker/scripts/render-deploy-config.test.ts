import { execFileSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import ts from "typescript";
import { describe, expect, test } from "vitest";
import { CONTROL_HEAD_BINDING, HYPERDRIVE_PLACEHOLDER, renderDeployConfig, RenderError } from "./render-deploy-config.ts";

const here = (p: string) => fileURLToPath(new URL(p, import.meta.url));
const TEMPLATE = readFileSync(here("../wrangler.jsonc"), "utf8");
const WORKFLOW = readFileSync(here("../../../.github/workflows/deploy-session-read-worker.yml"), "utf8");
const HD = "0123456789abcdef0123456789abcdef";
const parse = (text: string) => {
  const r = ts.parseConfigFileTextToJson("x.jsonc", text);
  expect(r.error).toBeUndefined();
  return r.config as Record<string, any>;
};
/** The deploy workflow's render before this change, verbatim. */
const sedRender = (id: string) => execFileSync("sed", [`s/"id": "00000000000000000000000000000000"/"id": "${id}"/`], { input: TEMPLATE, encoding: "utf8" });
const ON = { hyperdriveId: HD, ownerBuffered: "on", ownerBufferedWorkspaces: "ws-canary-1, ws-canary-2", s3Bucket: "ingest-bucket-example", s3Region: "us-east-1", directoryWorkspaces: "ws-canary-1,ws-canary-2,ws-other" };

describe("deploy config render, defaults", () => {
  test("no inputs (and an explicit off) is byte-identical to the old sed render: no binding, no flag", () => {
    for (const ownerBuffered of [undefined, "", "off"]) {
      const out = renderDeployConfig(TEMPLATE, { hyperdriveId: HD, ownerBuffered, ownerBufferedWorkspaces: "", directoryWorkspaces: "ws-a" });
      expect(out).toBe(sedRender(HD));
      const config = parse(out);
      expect(config.services).toBeUndefined();
      expect(config.hyperdrive[0].id).toBe(HD);
      expect(config.vars.RELAYHISTORY_READ_OWNER_BUFFERED).toBeUndefined();
      expect(config.vars.READ_API_OWNER_BUFFERED_WORKSPACES).toBeUndefined();
      expect(config.vars.INGEST_S3_BUCKET).toBeUndefined();
    }
  });

  test("wrangler.jsonc itself declares no service binding, no env and no owner-buffered var", () => {
    const config = parse(TEMPLATE);
    expect(config.services).toBeUndefined();
    expect(config.env).toBeUndefined();
    for (const k of ["RELAYHISTORY_READ_OWNER_BUFFERED", "READ_API_OWNER_BUFFERED_WORKSPACES", "INGEST_S3_BUCKET", "INGEST_S3_REGION"]) expect(config.vars[k]).toBeUndefined();
    expect(TEMPLATE).toContain(HYPERDRIVE_PLACEHOLDER);
  });

  test("off with a workspace list, or an unknown mode, is refused", () => {
    expect(() => renderDeployConfig(TEMPLATE, { hyperdriveId: HD, ownerBuffered: "off", ownerBufferedWorkspaces: "ws-a" })).toThrow(RenderError);
    for (const ownerBuffered of ["true", "ON", "1", "yes"]) expect(() => renderDeployConfig(TEMPLATE, { ...ON, ownerBuffered })).toThrow(/"off" or "on"/);
    for (const hyperdriveId of ["", "0".repeat(32), "xyz"]) expect(() => renderDeployConfig(TEMPLATE, { hyperdriveId })).toThrow(/Hyperdrive/);
  });
});

describe("deploy config render, owner_buffered=on", () => {
  test("adds the pinned named-entrypoint binding, the flag, the canary allowlist and the S3 vars; nothing else changes", () => {
    const config = parse(renderDeployConfig(TEMPLATE, ON));
    expect(config.services).toEqual([{ binding: "HISTORY_CONTROL_HEAD", service: "relayhistory-api", entrypoint: "SessionReadPrivacyControls" }]);
    expect(config.vars.RELAYHISTORY_READ_OWNER_BUFFERED).toBe("true");
    expect(config.vars.READ_API_OWNER_BUFFERED_WORKSPACES).toBe("ws-canary-1,ws-canary-2");
    expect(config.vars.INGEST_S3_BUCKET).toBe("ingest-bucket-example");
    expect(config.vars.INGEST_S3_REGION).toBe("us-east-1");
    const base = parse(sedRender(HD));
    const { services: _s, vars, ...rest } = config;
    const { vars: baseVars, ...baseRest } = base;
    expect(rest).toEqual(baseRest);
    for (const [k, v] of Object.entries(baseVars)) expect(vars[k]).toBe(v);
    expect(Object.keys(vars).length).toBe(Object.keys(baseVars).length + 4);
    expect(CONTROL_HEAD_BINDING.binding).toBe("HISTORY_CONTROL_HEAD");
  });

  test("a directory allowlist of * covers any canary id", () => {
    expect(parse(renderDeployConfig(TEMPLATE, { ...ON, directoryWorkspaces: "*" })).vars.READ_API_OWNER_BUFFERED_WORKSPACES).toBe("ws-canary-1,ws-canary-2");
  });

  test("refuses: no canary workspace, *, malformed or duplicate ids, ids off the directory list, missing bucket or region", () => {
    const refused: Array<[Partial<typeof ON>, RegExp]> = [
      [{ ownerBufferedWorkspaces: "" }, /at least one canary/],
      [{ ownerBufferedWorkspaces: "*" }, /not allowed/],
      [{ ownerBufferedWorkspaces: "ws-canary-1,*" }, /not allowed/],
      [{ ownerBufferedWorkspaces: "ws-canary-1,,ws-canary-2" }, /workspace ids/],
      [{ ownerBufferedWorkspaces: "ws canary" }, /workspace ids/],
      [{ ownerBufferedWorkspaces: "ws-canary-1,ws-canary-1" }, /twice/],
      [{ directoryWorkspaces: "ws-canary-1" }, /SESSION_READ_DIRECTORY_WORKSPACES/],
      [{ directoryWorkspaces: "" }, /SESSION_READ_DIRECTORY_WORKSPACES/],
      [{ s3Bucket: "" }, /SESSION_READ_INGEST_S3_BUCKET/],
      [{ s3Bucket: "Bad_Bucket" }, /SESSION_READ_INGEST_S3_BUCKET/],
      [{ s3Region: "" }, /SESSION_READ_INGEST_S3_REGION/],
    ];
    for (const [over, message] of refused) expect(() => renderDeployConfig(TEMPLATE, { ...ON, ...over }), JSON.stringify(over)).toThrow(message);
  });

  test("a template that already binds services or sets the flag is refused", () => {
    const bound = TEMPLATE.replace('"observability"', '"services": [{ "binding": "X", "service": "y" }],\n  "observability"');
    expect(() => renderDeployConfig(bound, ON)).toThrow(/must not declare services/);
    const flagged = TEMPLATE.replace('"CLICKHOUSE_DATABASE"', '"RELAYHISTORY_READ_OWNER_BUFFERED": "true",\n    "CLICKHOUSE_DATABASE"');
    expect(() => renderDeployConfig(flagged, ON)).toThrow(/leave RELAYHISTORY_READ_OWNER_BUFFERED unset/);
  });
});

describe("deploy workflow wiring", () => {
  test("dispatch inputs default to off and none", () => {
    expect(WORKFLOW).toMatch(/owner_buffered:\n\s+description:[^\n]*\n\s+type: choice\n\s+options: \["off", "on"\]\n\s+default: "off"/);
    expect(WORKFLOW).toMatch(/owner_buffered_workspaces:\n\s+description:[^\n]*\n\s+type: string\n\s+default: ""/);
  });

  test("inputs reach shell only through env, and the render runs this script", () => {
    for (const line of WORKFLOW.split("\n")) if (line.includes("${{ inputs.")) expect(line.trim(), line).toMatch(/^[A-Z_]+: \$\{\{ inputs\.[a-z_]+ \}\}$/);
    expect(WORKFLOW).toContain("bun run scripts/render-deploy-config.ts");
    expect(WORKFLOW).not.toMatch(/^\s+sed 's\/"id"/m);
    // The deploy command is unchanged: no owner-buffered --var; the vars live in the on-render only.
    expect(WORKFLOW).not.toContain("--var \"RELAYHISTORY_READ_OWNER_BUFFERED");
  });
});
