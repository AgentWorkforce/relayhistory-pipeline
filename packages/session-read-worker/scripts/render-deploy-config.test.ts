import { execFileSync } from "node:child_process";
import { readFileSync } from "node:fs";
import { fileURLToPath } from "node:url";
import ts from "typescript";
import { describe, expect, test } from "vitest";
import { ALL_WORKSPACES, ANALYSIS_ADMIT_BINDING, BUFFERED_SEARCH_GATE_VAR, CONTROL_HEAD_BINDING, HYPERDRIVE_PLACEHOLDER, OWNER_BUFFERED_EVENTS_VAR, OWNER_BUFFERED_INDEX_VERIFY_VAR, OWNER_BUFFERED_PHASE_LOG_VAR, OWNER_BUFFERED_STAGE_CONCURRENCY_VAR, renderDeployConfig, RenderError, type RenderInputs } from "./render-deploy-config.ts";

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

  test("brief_summary / list_short_summary: default off; on adds only the exact \"true\" --var to the deploy command", () => {
    expect(WORKFLOW).toMatch(/brief_summary:\n\s+description:[^\n]*\n\s+type: choice\n\s+options: \["off", "on"\]\n\s+default: "off"/);
    expect(WORKFLOW).toMatch(/list_short_summary:\n\s+description:[^\n]*\n\s+type: choice\n\s+options: \["off", "on"\]\n\s+default: "off"/);
    expect(WORKFLOW).toContain("BRIEF_SUMMARY: ${{ inputs.brief_summary }}");
    expect(WORKFLOW).toContain("LIST_SHORT_SUMMARY: ${{ inputs.list_short_summary }}");
    expect(WORKFLOW).toContain('brief_summary=""; if [ "$BRIEF_SUMMARY" = "on" ]; then brief_summary=true; fi');
    expect(WORKFLOW).toContain('list_short_summary=""; if [ "$LIST_SHORT_SUMMARY" = "on" ]; then list_short_summary=true; fi');
    expect(WORKFLOW).toContain('${brief_summary:+--var "READ_API_BRIEF_SUMMARY_ENABLED:$brief_summary"}');
    expect(WORKFLOW).toContain('${list_short_summary:+--var "READ_API_LIST_SHORT_SUMMARY_ENABLED:$list_short_summary"}');
    // Never in the template or the render: a default deploy carries neither variable.
    expect(TEMPLATE).not.toContain("READ_API_BRIEF_SUMMARY_ENABLED");
    expect(TEMPLATE).not.toContain("READ_API_LIST_SHORT_SUMMARY_ENABLED");
  });

  test("summary_fallback: default off; on needs brief_summary=on and the SUMMARY_MASTER_KEYS secret, and adds only the exact \"true\" --var", () => {
    expect(WORKFLOW).toMatch(/summary_fallback:\n\s+description:[^\n]*\n\s+type: choice\n\s+options: \["off", "on"\]\n\s+default: "off"/);
    expect(WORKFLOW).toContain("SUMMARY_FALLBACK: ${{ inputs.summary_fallback }}");
    expect(WORKFLOW).toContain(`if [ "$SUMMARY_FALLBACK" = "on" ] && [ "$BRIEF_SUMMARY" != "on" ]; then fail 'summary_fallback=on needs brief_summary=on'; fi`);
    expect(WORKFLOW).toContain('summary_fallback=""; if [ "$SUMMARY_FALLBACK" = "on" ]; then summary_fallback=true; fi');
    expect(WORKFLOW).toContain('${summary_fallback:+--var "READ_API_SUMMARY_FALLBACK_ENABLED:$summary_fallback"}');
    expect(WORKFLOW).toContain("for n in SUMMARY_MASTER_KEYS PIPELINE_RAW_S3_ACCESS_KEY_ID PIPELINE_RAW_S3_SECRET_ACCESS_KEY; do");
    expect(TEMPLATE).not.toContain("READ_API_SUMMARY_FALLBACK_ENABLED");
    expect(TEMPLATE).not.toContain("SUMMARY_MASTER_KEYS");
  });

  test("summary_fallback=on: the secret check runs before wrangler deploy, so nothing goes live without SUMMARY_MASTER_KEYS", () => {
    const step = (name: string) => { const i = WORKFLOW.indexOf(`      - name: ${name}`); expect(i, name).toBeGreaterThan(0); return i; };
    const check = step("Check the summary-fallback Worker secrets exist before deploying (names only; summary_fallback=on only)");
    const deploy = step("Deploy");
    const secrets = WORKFLOW.indexOf("for n in SUMMARY_MASTER_KEYS PIPELINE_RAW_S3_ACCESS_KEY_ID PIPELINE_RAW_S3_SECRET_ACCESS_KEY; do");
    expect(WORKFLOW.indexOf("for n in SUMMARY_MASTER_KEYS")).toBe(WORKFLOW.lastIndexOf("for n in SUMMARY_MASTER_KEYS"));
    expect(check).toBeLessThan(secrets);
    expect(secrets).toBeLessThan(deploy);
    expect(deploy).toBeLessThan(WORKFLOW.indexOf("bunx wrangler deploy"));
    expect(WORKFLOW.slice(check, deploy)).toContain("if: env.SUMMARY_FALLBACK == 'on'");
    expect(WORKFLOW.slice(check, deploy)).toContain("exit 1");
  });

  test("summary_search: default off; on adds only the exact \"true\" --var, never the template", () => {
    expect(WORKFLOW).toMatch(/summary_search:\n\s+description:[^\n]*\n\s+type: choice\n\s+options: \["off", "on"\]\n\s+default: "off"/);
    expect(WORKFLOW).toContain("SUMMARY_SEARCH: ${{ inputs.summary_search }}");
    expect(WORKFLOW).toContain(`case "$SUMMARY_SEARCH" in off|on) ;; *) fail 'summary_search must be "off" or "on"' ;; esac`);
    expect(WORKFLOW).toContain('summary_search=""; if [ "$SUMMARY_SEARCH" = "on" ]; then summary_search=true; fi');
    expect(WORKFLOW).toContain('${summary_search:+--var "READ_API_SUMMARY_SEARCH_ENABLED:$summary_search"}');
    expect(TEMPLATE).not.toContain("READ_API_SUMMARY_SEARCH_ENABLED");
  });
});


describe("deploy config render, analysis admission binding (SESSION_READ_ANALYSIS_HYPERDRIVE_ID)", () => {
  const AD = "fedcba9876543210fedcba9876543210";
  const admitBindings = (config: Record<string, any>) => (config.hyperdrive as Array<{ binding: string; id: string }>).filter((h) => h.binding === ANALYSIS_ADMIT_BINDING);

  test("unset or empty: byte-identical to the render without the input, in every mode", () => {
    for (const analysisHyperdriveId of [undefined, "", "  "]) {
      expect(renderDeployConfig(TEMPLATE, { hyperdriveId: HD, analysisHyperdriveId })).toBe(sedRender(HD));
      expect(renderDeployConfig(TEMPLATE, { ...ON, analysisHyperdriveId })).toBe(renderDeployConfig(TEMPLATE, ON));
      expect(renderDeployConfig(TEMPLATE, { hyperdriveId: HD, bufferedSearchGate: "on", analysisHyperdriveId })).toBe(renderDeployConfig(TEMPLATE, { hyperdriveId: HD, bufferedSearchGate: "on" }));
    }
    expect(TEMPLATE).not.toContain(ANALYSIS_ADMIT_BINDING);
  });

  test("set: adds exactly one ANALYSIS_ADMIT Hyperdrive binding after SUPABASE and changes nothing else", () => {
    const base = parse(renderDeployConfig(TEMPLATE, { hyperdriveId: HD }));
    const config = parse(renderDeployConfig(TEMPLATE, { hyperdriveId: HD, analysisHyperdriveId: ` ${AD} ` }));
    expect(config.hyperdrive).toEqual([...base.hyperdrive, { binding: ANALYSIS_ADMIT_BINDING, id: AD }]);
    expect(config.hyperdrive[0]).toEqual({ binding: "SUPABASE", id: HD });
    const { hyperdrive: _a, ...rest } = config;
    const { hyperdrive: _b, ...baseRest } = base;
    expect(rest).toEqual(baseRest);
    expect(config.vars.READ_API_ANALYSIS_ENABLED).toBeUndefined();
    expect(config.services).toBeUndefined();
  });

  test("composes with owner_buffered=on and the search gate (the binding once, every other key unchanged)", () => {
    for (const inputs of [ON, { hyperdriveId: HD, bufferedSearchGate: "on" }, { ...ON, bufferedSearchGate: "on", ownerBufferedEvents: "on" }] as RenderInputs[]) {
      const base = parse(renderDeployConfig(TEMPLATE, inputs));
      const config = parse(renderDeployConfig(TEMPLATE, { ...inputs, analysisHyperdriveId: AD }));
      expect(admitBindings(config)).toEqual([{ binding: ANALYSIS_ADMIT_BINDING, id: AD }]);
      expect({ ...config, hyperdrive: undefined }).toEqual({ ...base, hyperdrive: undefined });
    }
  });

  test("refuses a malformed id, all zeros, and the read role's own Hyperdrive config", () => {
    for (const analysisHyperdriveId of ["xyz", "0".repeat(32), "A".repeat(32), "a".repeat(31), "a".repeat(33)]) {
      expect(() => renderDeployConfig(TEMPLATE, { hyperdriveId: HD, analysisHyperdriveId }), analysisHyperdriveId).toThrow(/SESSION_READ_ANALYSIS_HYPERDRIVE_ID must be/);
    }
    expect(() => renderDeployConfig(TEMPLATE, { hyperdriveId: HD, analysisHyperdriveId: HD })).toThrow(/must not be the read role's Hyperdrive config/);
  });

  test("refuses a template that already declares ANALYSIS_ADMIT, in every mode (also without the input)", () => {
    const declared = TEMPLATE.replace('"binding": "SUPABASE",', '"binding": "SUPABASE",\n      "x": 1\n    },\n    {\n      "binding": "ANALYSIS_ADMIT", "id": "00000000000000000000000000000001",');
    expect(declared).toContain('"ANALYSIS_ADMIT"');
    for (const inputs of [{ hyperdriveId: HD }, { hyperdriveId: HD, analysisHyperdriveId: AD }, ON] as RenderInputs[]) {
      expect(() => renderDeployConfig(declared, inputs)).toThrow(/must not declare the ANALYSIS_ADMIT Hyperdrive binding/);
    }
  });

  test("the CLI renders it from ANALYSIS_HYPERDRIVE_ID", () => {
    expect(readFileSync(here("./render-deploy-config.ts"), "utf8")).toContain("analysisHyperdriveId: process.env.ANALYSIS_HYPERDRIVE_ID");
  });
});

describe("deploy workflow wiring, analysis", () => {
  const step = (name: string) => { const i = WORKFLOW.indexOf(`      - name: ${name}`); expect(i, name).toBeGreaterThan(0); return i; };

  test("analysis: default off; on adds only the exact \"true\" --var, never the template", () => {
    expect(WORKFLOW).toMatch(/analysis:\n\s+description:[^\n]*\n\s+type: choice\n\s+options: \["off", "on", "disable"\]\n\s+default: "off"/);
    expect(WORKFLOW).toContain("ANALYSIS: ${{ inputs.analysis }}");
    expect(WORKFLOW).toContain(`case "$ANALYSIS" in off|on|disable) ;; *) fail 'analysis must be "off", "on" or "disable"' ;; esac`);
    // Only "on" renders the var; disable is the removal, not a value.
    expect(WORKFLOW).toContain('analysis=""; if [ "$ANALYSIS" = "on" ]; then analysis=true; fi');
    expect(WORKFLOW).toContain('analysis=""; if [ "$ANALYSIS" = "on" ]; then analysis=true; fi');
    expect(WORKFLOW).toContain('${analysis:+--var "READ_API_ANALYSIS_ENABLED:$analysis"}');
    expect(TEMPLATE).not.toContain("READ_API_ANALYSIS_ENABLED");
  });

  test("the admission Hyperdrive id comes from the environment variable only, is validated, and is checked live before render", () => {
    expect(WORKFLOW).toContain("ANALYSIS_HYPERDRIVE_ID: ${{ vars.SESSION_READ_ANALYSIS_HYPERDRIVE_ID }}");
    expect(WORKFLOW).not.toContain("inputs.analysis_hyperdrive");
    expect(WORKFLOW).toContain('[ "$ANALYSIS_HYPERDRIVE_ID" != "$HYPERDRIVE_ID" ] || fail');
    const check = step("Check the live admission Hyperdrive safety contract (only when SESSION_READ_ANALYSIS_HYPERDRIVE_ID is set)");
    const render = step("Render the deploy config (Hyperdrive id; control-head binding and flags only when owner_buffered or buffered_search_gate is on)");
    expect(check).toBeLessThan(render);
    const body = WORKFLOW.slice(check, render);
    expect(body).toContain("if: env.ANALYSIS_HYPERDRIVE_ID != ''");
    expect(body).toContain("hyperdrive/configs/$ANALYSIS_HYPERDRIVE_ID");
    expect(body).toContain('HYPERDRIVE_ID="$ANALYSIS_HYPERDRIVE_ID" HYPERDRIVE_ROLE=analysis_admit bun run scripts/check-hyperdrive-config.ts');
    expect(body).toContain("set -o pipefail");
  });

  test("analysis=on: the raw-store secret check runs before wrangler deploy", () => {
    const check = step("Check the analysis Worker secrets exist before deploying (names only; analysis=on only)");
    const deploy = step("Deploy");
    expect(check).toBeLessThan(deploy);
    const body = WORKFLOW.slice(check, deploy);
    expect(body).toContain("if: env.ANALYSIS == 'on'");
    expect(body).toContain("for n in PIPELINE_RAW_S3_ACCESS_KEY_ID PIPELINE_RAW_S3_SECRET_ACCESS_KEY; do");
    expect(body).toContain("exit 1");
  });
});

describe("047010 exact-session RPC render", () => {
  test("default/off leaves the render byte-identical; on adds only its flag", () => {
    const baseline = renderDeployConfig(TEMPLATE, ON);
    for (const value of [undefined, "", "off"]) expect(renderDeployConfig(TEMPLATE, { ...ON, ownerBufferedExactSession: value })).toBe(baseline);
    const config = parse(renderDeployConfig(TEMPLATE, { ...ON, ownerBufferedExactSession: "on" }));
    const before = parse(baseline);
    expect(config).toEqual({ ...before, vars: { ...before.vars, READ_API_OWNER_BUFFERED_EXACT_SESSION: "on" } });
  });
  test("requires owner-buffered on and refuses unknown values or predeclared flags", () => {
    expect(() => renderDeployConfig(TEMPLATE, { hyperdriveId: HD, ownerBufferedExactSession: "on" })).toThrow(/needs owner_buffered=on/);
    for (const value of ["true", "1", "ON"]) expect(() => renderDeployConfig(TEMPLATE, { ...ON, ownerBufferedExactSession: value })).toThrow(/exact_session/);
    const declared = TEMPLATE.replace('"vars": {', '"vars": { "READ_API_OWNER_BUFFERED_EXACT_SESSION": "on",');
    expect(() => renderDeployConfig(declared, ON)).toThrow();
    expect(WORKFLOW).toContain('OWNER_BUFFERED_EXACT_SESSION: ${{ inputs.owner_buffered_exact_session }}');
    expect(WORKFLOW).toMatch(/owner_buffered_exact_session:[\s\S]*?default: "off"/);
  });
});


describe("047020 paged exact-session stages render", () => {
  const EXACT = { ...ON, ownerBufferedExactSession: "on" };
  test("default/off leaves the render byte-identical; on adds only its flag", () => {
    for (const base of [ON, EXACT, { hyperdriveId: HD }]) {
      const baseline = renderDeployConfig(TEMPLATE, base);
      for (const value of [undefined, "", "off", " off "]) expect(renderDeployConfig(TEMPLATE, { ...base, ownerBufferedStagesPaged: value })).toBe(baseline);
    }
    const before = parse(renderDeployConfig(TEMPLATE, EXACT));
    expect(parse(renderDeployConfig(TEMPLATE, { ...EXACT, ownerBufferedStagesPaged: "on" }))).toEqual({ ...before, vars: { ...before.vars, READ_API_OWNER_BUFFERED_STAGES_PAGED: "on" } });
  });
  test("on requires owner_buffered=on AND exact_session=on; unknown values and predeclared flags refused", () => {
    expect(() => renderDeployConfig(TEMPLATE, { hyperdriveId: HD, ownerBufferedStagesPaged: "on" })).toThrow(/stages_paged=on needs owner_buffered=on and owner_buffered_exact_session=on/);
    expect(() => renderDeployConfig(TEMPLATE, { ...ON, ownerBufferedStagesPaged: "on" })).toThrow(/stages_paged=on needs/);
    expect(() => renderDeployConfig(TEMPLATE, { ...ON, ownerBufferedExactSession: "off", ownerBufferedStagesPaged: "on" })).toThrow(/stages_paged=on needs/);
    for (const value of ["true", "1", "ON", "yes"]) expect(() => renderDeployConfig(TEMPLATE, { ...EXACT, ownerBufferedStagesPaged: value })).toThrow(/owner_buffered_stages_paged must be/);
    const declared = TEMPLATE.replace('"vars": {', '"vars": { "READ_API_OWNER_BUFFERED_STAGES_PAGED": "on",');
    expect(() => renderDeployConfig(declared, EXACT)).toThrow();
    expect(WORKFLOW).toContain('OWNER_BUFFERED_STAGES_PAGED: ${{ inputs.owner_buffered_stages_paged }}');
    expect(WORKFLOW).toMatch(/owner_buffered_stages_paged:\n[\s\S]*?type: choice\n\s*options: \["off", "on"\]\n\s*default: "off"/);
  });
});
