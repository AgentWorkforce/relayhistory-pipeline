import { execFileSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import ts from "typescript";
import { describe, expect, test } from "vitest";
import { ALL_WORKSPACES, BUFFERED_SEARCH_GATE_VAR, CONTROL_HEAD_BINDING, HYPERDRIVE_PLACEHOLDER, OWNER_BUFFERED_EVENTS_VAR, OWNER_BUFFERED_INDEX_VERIFY_VAR, OWNER_BUFFERED_PHASE_LOG_VAR, OWNER_BUFFERED_STAGE_CONCURRENCY_VAR, renderDeployConfig, RenderError, type RenderInputs } from "./render-deploy-config.ts";

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

  test("wrangler.jsonc itself declares no service binding, no env and no owner-buffered or gate var", () => {
    const config = parse(TEMPLATE);
    expect(config.services).toBeUndefined();
    expect(config.env).toBeUndefined();
    for (const k of ["RELAYHISTORY_READ_OWNER_BUFFERED", "READ_API_OWNER_BUFFERED_WORKSPACES", "INGEST_S3_BUCKET", "INGEST_S3_REGION", "READ_API_BUFFERED_SEARCH_GATE"]) expect(config.vars[k]).toBeUndefined();
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
      [{ ownerBufferedWorkspaces: "*" }, /exactly "\*"/],
      [{ ownerBufferedWorkspaces: "ws-canary-1,*" }, /cannot be mixed/],
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

describe("deploy config render, all-workspaces mode (owner_buffered_workspaces=*)", () => {
  const ALL = { ...ON, ownerBufferedWorkspaces: "*", directoryWorkspaces: "*" };

  test('"*" with a directory allowlist of "*" renders READ_API_OWNER_BUFFERED_WORKSPACES="*"; everything else as a canary on-render', () => {
    expect(ALL_WORKSPACES).toBe("*");
    for (const over of [{}, { ownerBufferedWorkspaces: " * ", directoryWorkspaces: " * " }]) {
      const config = parse(renderDeployConfig(TEMPLATE, { ...ALL, ...over }));
      expect(config.vars.READ_API_OWNER_BUFFERED_WORKSPACES).toBe("*");
      expect(config.vars.RELAYHISTORY_READ_OWNER_BUFFERED).toBe("true");
      expect(config.services).toEqual([{ ...CONTROL_HEAD_BINDING }]);
      const canary = parse(renderDeployConfig(TEMPLATE, { ...ON, directoryWorkspaces: "*" }));
      expect({ ...config.vars, READ_API_OWNER_BUFFERED_WORKSPACES: "x" }).toEqual({ ...canary.vars, READ_API_OWNER_BUFFERED_WORKSPACES: "x" });
      expect({ ...config, vars: undefined }).toEqual({ ...canary, vars: undefined });
    }
  });

  test("ambiguous all-workspaces configs are refused with a clear error", () => {
    const refused: Array<[Partial<typeof ON>, RegExp]> = [
      // "*" without the directory allowlist being "*" (explicit list, unset, or a list containing "*").
      [{ directoryWorkspaces: "ws-canary-1,ws-canary-2" }, /needs SESSION_READ_DIRECTORY_WORKSPACES to be exactly "\*"/],
      [{ directoryWorkspaces: "" }, /needs SESSION_READ_DIRECTORY_WORKSPACES to be exactly "\*"/],
      [{ directoryWorkspaces: undefined }, /needs SESSION_READ_DIRECTORY_WORKSPACES to be exactly "\*"/],
      [{ directoryWorkspaces: "*,ws-canary-1" }, /needs SESSION_READ_DIRECTORY_WORKSPACES to be exactly "\*"/],
      // "*" mixed with ids, or repeated, even when the directory allowlist is "*".
      [{ ownerBufferedWorkspaces: "*,ws-canary-1" }, /cannot be mixed/],
      [{ ownerBufferedWorkspaces: "ws-canary-1, *" }, /cannot be mixed/],
      [{ ownerBufferedWorkspaces: "*,*" }, /cannot be mixed/],
      // The mode does not relax the S3 requirements.
      [{ s3Bucket: "" }, /SESSION_READ_INGEST_S3_BUCKET/],
      [{ s3Region: "" }, /SESSION_READ_INGEST_S3_REGION/],
    ];
    for (const [over, message] of refused) expect(() => renderDeployConfig(TEMPLATE, { ...ALL, ...over }), JSON.stringify(over)).toThrow(message);
    // "*" with owner_buffered off (or no mode) is the existing ambiguous-dispatch refusal.
    for (const ownerBuffered of [undefined, "", "off"]) {
      expect(() => renderDeployConfig(TEMPLATE, { ...ALL, ownerBuffered })).toThrow(/owner_buffered is off/);
    }
  });

  test("an explicit canary list still validates exactly as before with a directory allowlist of * or a list", () => {
    expect(parse(renderDeployConfig(TEMPLATE, ON)).vars.READ_API_OWNER_BUFFERED_WORKSPACES).toBe("ws-canary-1,ws-canary-2");
    expect(parse(renderDeployConfig(TEMPLATE, { ...ON, directoryWorkspaces: "*" })).vars.READ_API_OWNER_BUFFERED_WORKSPACES).toBe("ws-canary-1,ws-canary-2");
    expect(() => renderDeployConfig(TEMPLATE, { ...ON, directoryWorkspaces: "ws-canary-1" })).toThrow(/not on SESSION_READ_DIRECTORY_WORKSPACES/);
  });
});

describe("deploy config render, buffered_search_gate", () => {
  const GATE = { hyperdriveId: HD, bufferedSearchGate: "on" };
  const ALL = { ...ON, ownerBufferedWorkspaces: "*", directoryWorkspaces: "*" };

  test("gate off (no input, empty, off) is byte-identical to the render without the input, for every existing input combination", () => {
    const combos: RenderInputs[] = [
      { hyperdriveId: HD },
      { hyperdriveId: HD, ownerBuffered: "", ownerBufferedWorkspaces: "", directoryWorkspaces: "ws-a" },
      { hyperdriveId: HD, ownerBuffered: "off", directoryWorkspaces: "*" },
      ON,
      { ...ON, directoryWorkspaces: "*" },
      ALL,
    ];
    for (const combo of combos) {
      const without = renderDeployConfig(TEMPLATE, combo);
      for (const bufferedSearchGate of [undefined, "", " ", "off", " off "]) {
        const out = renderDeployConfig(TEMPLATE, { ...combo, bufferedSearchGate });
        expect(out, JSON.stringify({ combo, bufferedSearchGate })).toBe(without);
        expect(out).not.toContain(BUFFERED_SEARCH_GATE_VAR);
      }
    }
    // And the no-owner-buffered default is still the old sed render.
    expect(renderDeployConfig(TEMPLATE, { hyperdriveId: HD, bufferedSearchGate: "off" })).toBe(sedRender(HD));
  });

  test("gate on alone adds exactly the pinned binding and READ_API_BUFFERED_SEARCH_GATE=on; nothing owner-buffered", () => {
    expect(BUFFERED_SEARCH_GATE_VAR).toBe("READ_API_BUFFERED_SEARCH_GATE");
    for (const bufferedSearchGate of ["on", " on "]) {
      const config = parse(renderDeployConfig(TEMPLATE, { ...GATE, bufferedSearchGate }));
      expect(config.services).toEqual([{ binding: "HISTORY_CONTROL_HEAD", service: "relayhistory-api", entrypoint: "SessionReadPrivacyControls" }]);
      expect(config.vars.READ_API_BUFFERED_SEARCH_GATE).toBe("on");
      for (const k of ["RELAYHISTORY_READ_OWNER_BUFFERED", "READ_API_OWNER_BUFFERED_WORKSPACES", "INGEST_S3_BUCKET", "INGEST_S3_REGION"]) expect(config.vars[k]).toBeUndefined();
      const base = parse(sedRender(HD));
      const { services: _s, vars, ...rest } = config;
      const { vars: baseVars, ...baseRest } = base;
      expect(rest).toEqual(baseRest);
      expect(vars).toEqual({ ...baseVars, READ_API_BUFFERED_SEARCH_GATE: "on" });
    }
  });

  test("gate on with owner_buffered=on (canary or all-workspaces) renders the binding once and both flags", () => {
    for (const owner of [ON, ALL]) {
      const without = parse(renderDeployConfig(TEMPLATE, owner));
      const out = renderDeployConfig(TEMPLATE, { ...owner, bufferedSearchGate: "on" });
      expect(out.split('"binding": "HISTORY_CONTROL_HEAD"').length).toBe(2);
      const config = parse(out);
      expect(config.services).toEqual([{ ...CONTROL_HEAD_BINDING }]);
      expect(config.vars).toEqual({ ...without.vars, READ_API_BUFFERED_SEARCH_GATE: "on" });
      expect({ ...config, vars: undefined }).toEqual({ ...without, vars: undefined });
    }
  });

  test("gate on refuses: unknown values, a template that binds services, declares env or already sets the flag; owner-buffered refusals still apply", () => {
    for (const bufferedSearchGate of ["ON", "true", "1", "yes", "on,off"]) {
      expect(() => renderDeployConfig(TEMPLATE, { ...GATE, bufferedSearchGate })).toThrow(/buffered_search_gate must be "off" or "on"/);
      expect(() => renderDeployConfig(TEMPLATE, { ...ON, bufferedSearchGate })).toThrow(/buffered_search_gate must be "off" or "on"/);
    }
    const bound = TEMPLATE.replace('"observability"', '"services": [{ "binding": "X", "service": "y" }],\n  "observability"');
    const withEnv = TEMPLATE.replace('"observability"', '"env": { "production": {} },\n  "observability"');
    const flagged = TEMPLATE.replace('"CLICKHOUSE_DATABASE"', '"READ_API_BUFFERED_SEARCH_GATE": "on",\n    "CLICKHOUSE_DATABASE"');
    expect(flagged).not.toBe(TEMPLATE);
    for (const inputs of [GATE, { ...ON, bufferedSearchGate: "on" }]) {
      expect(() => renderDeployConfig(bound, inputs)).toThrow(/must not declare services/);
      expect(() => renderDeployConfig(withEnv, inputs)).toThrow(/must not declare services or env/);
      expect(() => renderDeployConfig(flagged, inputs)).toThrow(/leave READ_API_BUFFERED_SEARCH_GATE unset/);
    }
    for (const hyperdriveId of ["", "0".repeat(32)]) expect(() => renderDeployConfig(TEMPLATE, { ...GATE, hyperdriveId })).toThrow(/Hyperdrive/);
    expect(() => renderDeployConfig(TEMPLATE, { ...GATE, ownerBufferedWorkspaces: "ws-a" })).toThrow(/owner_buffered is off/);
    expect(() => renderDeployConfig(TEMPLATE, { ...ON, bufferedSearchGate: "on", s3Bucket: "" })).toThrow(/SESSION_READ_INGEST_S3_BUCKET/);
  });

  test("the CLI fails closed on a bad BUFFERED_SEARCH_GATE value, before writing anything", () => {
    const cli = (env: Record<string, string>) => {
      try {
        return { code: 0, out: execFileSync("bun", ["run", here("./render-deploy-config.ts")], { env: { PATH: process.env.PATH ?? "", HOME: process.env.HOME ?? "", HYPERDRIVE_ID: HD, ...env }, encoding: "utf8", stdio: ["ignore", "pipe", "pipe"] }) };
      } catch (error) {
        const e = error as { status?: number; stdout?: string; stderr?: string };
        return { code: e.status ?? 1, out: `${e.stdout ?? ""}${e.stderr ?? ""}` };
      }
    };
    const bad = cli({ BUFFERED_SEARCH_GATE: "yes" });
    expect(bad.code).not.toBe(0);
    expect(bad.out).toContain('buffered_search_gate must be "off" or "on"');
  });
});

describe("deploy config render, owner_buffered_events", () => {
  const ALL = { ...ON, ownerBufferedWorkspaces: "*", directoryWorkspaces: "*" };

  test("events off (no input, empty, off) is byte-identical to the render without the input, for every existing input combination", () => {
    const combos: RenderInputs[] = [
      { hyperdriveId: HD },
      { hyperdriveId: HD, ownerBuffered: "off", directoryWorkspaces: "*" },
      { hyperdriveId: HD, bufferedSearchGate: "on" },
      ON,
      { ...ON, bufferedSearchGate: "on" },
      ALL,
    ];
    for (const combo of combos) {
      const without = renderDeployConfig(TEMPLATE, combo);
      for (const ownerBufferedEvents of [undefined, "", " ", "off", " off "]) {
        const out = renderDeployConfig(TEMPLATE, { ...combo, ownerBufferedEvents });
        expect(out, JSON.stringify({ combo, ownerBufferedEvents })).toBe(without);
        expect(out).not.toContain(OWNER_BUFFERED_EVENTS_VAR);
      }
    }
    expect(renderDeployConfig(TEMPLATE, { hyperdriveId: HD, ownerBufferedEvents: "off" })).toBe(sedRender(HD));
    expect(TEMPLATE).not.toContain(OWNER_BUFFERED_EVENTS_VAR);
  });

  test("events on with owner_buffered=on adds exactly READ_API_OWNER_BUFFERED_EVENTS=on and nothing else", () => {
    for (const base of [ON, ALL, { ...ON, bufferedSearchGate: "on" }]) {
      const without = parse(renderDeployConfig(TEMPLATE, base));
      const withEvents = parse(renderDeployConfig(TEMPLATE, { ...base, ownerBufferedEvents: " on " }));
      expect(withEvents.vars[OWNER_BUFFERED_EVENTS_VAR]).toBe("on");
      const { [OWNER_BUFFERED_EVENTS_VAR]: _, ...rest } = withEvents.vars;
      expect({ ...withEvents, vars: rest }).toEqual(without);
    }
  });

  test("events on without owner_buffered=on, or a bad value, or a template that sets the var, fails closed", () => {
    for (const ownerBuffered of [undefined, "", "off"]) {
      expect(() => renderDeployConfig(TEMPLATE, { hyperdriveId: HD, ownerBuffered, ownerBufferedEvents: "on" })).toThrow(/owner_buffered_events=on needs owner_buffered=on/);
      expect(() => renderDeployConfig(TEMPLATE, { hyperdriveId: HD, ownerBuffered, bufferedSearchGate: "on", ownerBufferedEvents: "on" })).toThrow(/owner_buffered_events=on needs owner_buffered=on/);
    }
    for (const bad of ["yes", "true", "ON", "1"]) expect(() => renderDeployConfig(TEMPLATE, { ...ON, ownerBufferedEvents: bad })).toThrow(RenderError);
    const flagged = TEMPLATE.replace(/"vars"\s*:\s*\{/, `"vars": { "${OWNER_BUFFERED_EVENTS_VAR}": "on",`);
    expect(flagged).not.toBe(TEMPLATE);
    expect(() => renderDeployConfig(flagged, { ...ON, ownerBufferedEvents: "on" })).toThrow(/leave READ_API_OWNER_BUFFERED_EVENTS unset/);
  });

  test("a template that predeclares READ_API_OWNER_BUFFERED_EVENTS is refused in every mode and for every events value", () => {
    const flagged = TEMPLATE.replace(/"vars"\s*:\s*\{/, `"vars": { "${OWNER_BUFFERED_EVENTS_VAR}": "on",`);
    expect(flagged).not.toBe(TEMPLATE);
    const dispatches: Record<string, RenderInputs> = {
      "default path (owner_buffered off, gate off)": { hyperdriveId: HD, ownerBuffered: "off" },
      "gate-only path (owner_buffered off, gate on)": { hyperdriveId: HD, ownerBuffered: "off", bufferedSearchGate: "on" },
      "valid owner_buffered=on": ON,
    };
    for (const [name, base] of Object.entries(dispatches)) {
      for (const ownerBufferedEvents of [undefined, "", "off", "on"]) {
        const inputs = { ...base, ownerBufferedEvents };
        const label = JSON.stringify({ name, ownerBufferedEvents });
        expect(() => renderDeployConfig(flagged, inputs), label).toThrow(RenderError);
        expect(() => renderDeployConfig(flagged, inputs), label).toThrow(/must leave READ_API_OWNER_BUFFERED_EVENTS unset/);
        if (ownerBufferedEvents !== "on" || base === ON) expect(() => renderDeployConfig(TEMPLATE, inputs), label).not.toThrow();
      }
    }
  });
});

describe("deploy config render, owner_buffered_stage_concurrency and owner_buffered_phase_log", () => {
  const EV = { ...ON, ownerBufferedEvents: "on" };

  test("both off (no input, empty, off) are byte-identical to the render without the inputs, for every existing input combination", () => {
    const combos: RenderInputs[] = [
      { hyperdriveId: HD },
      { hyperdriveId: HD, bufferedSearchGate: "on" },
      ON,
      EV,
      { ...EV, bufferedSearchGate: "on" },
    ];
    for (const combo of combos) {
      const without = renderDeployConfig(TEMPLATE, combo);
      for (const off of [undefined, "", " ", "off", " off "]) {
        const out = renderDeployConfig(TEMPLATE, { ...combo, ownerBufferedStageConcurrency: off, ownerBufferedPhaseLog: off });
        expect(out, JSON.stringify({ combo, off })).toBe(without);
        expect(out).not.toContain(OWNER_BUFFERED_STAGE_CONCURRENCY_VAR);
        expect(out).not.toContain(OWNER_BUFFERED_PHASE_LOG_VAR);
      }
    }
    expect(renderDeployConfig(TEMPLATE, { hyperdriveId: HD, ownerBufferedStageConcurrency: "off", ownerBufferedPhaseLog: "off" })).toBe(sedRender(HD));
    expect(TEMPLATE).not.toContain(OWNER_BUFFERED_STAGE_CONCURRENCY_VAR);
    expect(TEMPLATE).not.toContain(OWNER_BUFFERED_PHASE_LOG_VAR);
  });

  test("with owner_buffered_events=on: concurrency 2..32 adds exactly that var, phase_log=on adds exactly the var \"1\"", () => {
    const without = parse(renderDeployConfig(TEMPLATE, EV));
    for (const n of ["2", "8", " 16 ", "32"]) {
      const out = parse(renderDeployConfig(TEMPLATE, { ...EV, ownerBufferedStageConcurrency: n }));
      expect(out.vars[OWNER_BUFFERED_STAGE_CONCURRENCY_VAR]).toBe(n.trim());
      const { [OWNER_BUFFERED_STAGE_CONCURRENCY_VAR]: _, ...rest } = out.vars;
      expect({ ...out, vars: rest }).toEqual(without);
    }
    const logged = parse(renderDeployConfig(TEMPLATE, { ...EV, ownerBufferedPhaseLog: " on " }));
    expect(logged.vars[OWNER_BUFFERED_PHASE_LOG_VAR]).toBe("1");
    const { [OWNER_BUFFERED_PHASE_LOG_VAR]: _, ...rest } = logged.vars;
    expect({ ...logged, vars: rest }).toEqual(without);
  });

  test("a bad value, either without owner_buffered_events=on, or a template that predeclares either var, fails closed", () => {
    for (const bad of ["0", "1", "33", "02", "-2", "2.0", "on", "yes"]) expect(() => renderDeployConfig(TEMPLATE, { ...EV, ownerBufferedStageConcurrency: bad }), bad).toThrow(/owner_buffered_stage_concurrency must be/);
    for (const bad of ["1", "true", "ON", "yes"]) expect(() => renderDeployConfig(TEMPLATE, { ...EV, ownerBufferedPhaseLog: bad }), bad).toThrow(/owner_buffered_phase_log must be/);
    for (const base of [{ hyperdriveId: HD }, { hyperdriveId: HD, bufferedSearchGate: "on" }, ON, { ...ON, ownerBufferedEvents: "off" }]) {
      expect(() => renderDeployConfig(TEMPLATE, { ...base, ownerBufferedStageConcurrency: "4" })).toThrow(/needs owner_buffered_events=on/);
      expect(() => renderDeployConfig(TEMPLATE, { ...base, ownerBufferedPhaseLog: "on" })).toThrow(/needs owner_buffered_events=on/);
    }
    for (const v of [OWNER_BUFFERED_STAGE_CONCURRENCY_VAR, OWNER_BUFFERED_PHASE_LOG_VAR]) {
      const flagged = TEMPLATE.replace(/"vars"\s*:\s*\{/, `"vars": { "${v}": "1",`);
      expect(flagged).not.toBe(TEMPLATE);
      for (const inputs of [{ hyperdriveId: HD }, { hyperdriveId: HD, bufferedSearchGate: "on" }, ON, EV, { ...EV, ownerBufferedStageConcurrency: "4", ownerBufferedPhaseLog: "on" }]) {
        expect(() => renderDeployConfig(flagged, inputs), JSON.stringify({ v, inputs })).toThrow(new RegExp(`must leave ${v} unset`));
      }
    }
  });
});

describe("deploy config render, owner_buffered_index_verify", () => {
  const EV = { ...ON, ownerBufferedEvents: "on" };
  test("off (no input, empty, off) is byte-identical to the render without it, for every existing input combination", () => {
    const combos: RenderInputs[] = [{ hyperdriveId: HD }, { hyperdriveId: HD, bufferedSearchGate: "on" }, ON, EV, { ...EV, ownerBufferedPhaseLog: "on", ownerBufferedStageConcurrency: "4" }];
    for (const combo of combos) {
      const without = renderDeployConfig(TEMPLATE, combo);
      for (const off of [undefined, "", " ", "off", " off "]) {
        const out = renderDeployConfig(TEMPLATE, { ...combo, ownerBufferedIndexVerify: off });
        expect(out, JSON.stringify({ combo, off })).toBe(without);
        expect(out).not.toContain(OWNER_BUFFERED_INDEX_VERIFY_VAR);
      }
    }
    expect(renderDeployConfig(TEMPLATE, { hyperdriveId: HD, ownerBufferedIndexVerify: "off" })).toBe(sedRender(HD));
    expect(TEMPLATE).not.toContain(OWNER_BUFFERED_INDEX_VERIFY_VAR);
  });

  test("on with owner_buffered=on adds exactly the var \"on\" (with or without events)", () => {
    for (const base of [ON, EV]) {
      const without = parse(renderDeployConfig(TEMPLATE, base));
      const out = parse(renderDeployConfig(TEMPLATE, { ...base, ownerBufferedIndexVerify: " on " }));
      expect(out.vars[OWNER_BUFFERED_INDEX_VERIFY_VAR]).toBe("on");
      const { [OWNER_BUFFERED_INDEX_VERIFY_VAR]: _, ...rest } = out.vars;
      expect({ ...out, vars: rest }).toEqual(without);
    }
  });

  test("a bad value, on without owner_buffered=on, or a template that predeclares the var, fails closed", () => {
    for (const bad of ["1", "true", "ON", "yes"]) expect(() => renderDeployConfig(TEMPLATE, { ...ON, ownerBufferedIndexVerify: bad }), bad).toThrow(/owner_buffered_index_verify must be/);
    for (const base of [{ hyperdriveId: HD }, { hyperdriveId: HD, bufferedSearchGate: "on" }]) {
      expect(() => renderDeployConfig(TEMPLATE, { ...base, ownerBufferedIndexVerify: "on" })).toThrow(/owner_buffered_index_verify=on needs owner_buffered=on/);
    }
    const flagged = TEMPLATE.replace(/"vars"\s*:\s*\{/, `"vars": { "${OWNER_BUFFERED_INDEX_VERIFY_VAR}": "on",`);
    expect(flagged).not.toBe(TEMPLATE);
    for (const inputs of [{ hyperdriveId: HD }, { hyperdriveId: HD, bufferedSearchGate: "on" }, ON, EV, { ...ON, ownerBufferedIndexVerify: "on" }]) {
      expect(() => renderDeployConfig(flagged, inputs), JSON.stringify(inputs)).toThrow(new RegExp(`must leave ${OWNER_BUFFERED_INDEX_VERIFY_VAR} unset`));
    }
  });
});

describe("deploy workflow wiring", () => {
  test("dispatch renders the committed production profile by default; other child overrides remain off and none", () => {
    expect(WORKFLOW).toMatch(/owner_buffered:\n\s+description:[^\n]*\n\s+type: choice\n\s+options: \["profile", "preserve", "on", "off"\]\n\s+default: "profile"/);
    expect(WORKFLOW).toMatch(/owner_buffered_workspaces:\n\s+description:[^\n]*\n\s+type: string\n\s+default: ""/);
    expect(WORKFLOW).toMatch(/buffered_search_gate:\n\s+description:[^\n]*\n\s+type: choice\n\s+options: \["off", "on"\]\n\s+default: "off"/);
    expect(WORKFLOW).toContain("BUFFERED_SEARCH_GATE: ${{ inputs.buffered_search_gate }}");
    expect(WORKFLOW).toMatch(/owner_buffered_events:\n\s+description:[^\n]*\n\s+type: choice\n\s+options: \["profile", "off", "on"\]\n\s+default: "profile"/);
    expect(WORKFLOW).toMatch(/owner_buffered_statement_timeout_ms:\n\s+description:[^\n]*\n\s+type: string\n\s+default: "profile"/);
    expect(WORKFLOW).toContain("OWNER_BUFFERED_EVENTS: ${{ inputs.owner_buffered_events }}");
    expect(WORKFLOW).toMatch(/owner_buffered_stage_concurrency:\n\s+description:[^\n]*\n\s+type: string\n\s+default: "off"/);
    expect(WORKFLOW).toContain("OWNER_BUFFERED_STAGE_CONCURRENCY: ${{ inputs.owner_buffered_stage_concurrency }}");
    expect(WORKFLOW).toMatch(/owner_buffered_phase_log:\n\s+description:[^\n]*\n\s+type: choice\n\s+options: \["off", "on"\]\n\s+default: "off"/);
    expect(WORKFLOW).toContain("OWNER_BUFFERED_PHASE_LOG: ${{ inputs.owner_buffered_phase_log }}");
    expect(WORKFLOW).toMatch(/owner_buffered_index_verify:\n\s+description:[^\n]*\n\s+type: choice\n\s+options: \["off", "on"\]\n\s+default: "off"/);
    expect(WORKFLOW).toContain("OWNER_BUFFERED_INDEX_VERIFY: ${{ inputs.owner_buffered_index_verify }}");
  });

  test("inputs reach shell only through env, and the render runs this script", () => {
    for (const line of WORKFLOW.split("\n")) if (line.includes("${{ inputs.")) expect(line.trim(), line).toMatch(/^[A-Z_]+: \$\{\{ inputs\.[a-z_]+ \}\}$/);
    expect(WORKFLOW).toContain("bun run scripts/render-deploy-config.ts");
    expect(WORKFLOW).not.toMatch(/^\s+sed 's\/"id"/m);
    // The deploy command is unchanged: no owner-buffered --var; the vars live in the on-render only.
    expect(WORKFLOW).not.toContain("--var \"RELAYHISTORY_READ_OWNER_BUFFERED");
    expect(WORKFLOW).not.toContain("--var \"READ_API_BUFFERED_SEARCH_GATE");
    expect(WORKFLOW).not.toContain("--var \"READ_API_OWNER_BUFFERED_EVENTS");
    expect(WORKFLOW).not.toContain("--var \"READ_API_OWNER_BUFFERED_STAGE_CONCURRENCY");
    expect(WORKFLOW).not.toContain("--var \"READ_API_OWNER_BUFFERED_PHASE_LOG");
    expect(WORKFLOW).not.toContain("--var \"READ_API_OWNER_BUFFERED_INDEX_VERIFY");
  });
});
