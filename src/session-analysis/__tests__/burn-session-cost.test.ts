import { describe, expect, it } from "bun:test";
import {
  burnSourceFor,
  priceSessionUsage,
  type BurnUsagePriceRequest,
  type UsageEventRow,
  type UsagePricer,
} from "../burn-cost/session-cost.ts";

const FX_SHA = "f".repeat(64);
const FX_PRICING = {
  sdkVersion: "5.0.0",
  builtinPricingSha256: FX_SHA,
  overrideSha256: null,
};

/** Fixed per-million tariffs; anything else is unpriced, like burn. */
const FX_TARIFFS: Record<string, { input: number; output: number; cacheRead: number; cacheWrite: number }> = {
  "fx-model-a": { input: 3, output: 15, cacheRead: 0.3, cacheWrite: 3.75 },
  "fx-model-b": { input: 1, output: 2, cacheRead: 0.1, cacheWrite: 1 },
};

function fakePricer(seen: BurnUsagePriceRequest[][] = []): UsagePricer {
  return async ({ requests }) => {
    seen.push(requests);
    return {
      schema: "burn.usage-prices.v1",
      pricing: FX_PRICING,
      prices: requests.map(({ model, usage }) => {
        const tariff = FX_TARIFFS[model];
        if (!tariff) return { costKnown: false, costUsdMicros: null, breakdown: null };
        const usd =
          (usage.input * tariff.input +
            usage.output * tariff.output +
            usage.cacheRead * tariff.cacheRead +
            usage.cacheCreate5m * tariff.cacheWrite) /
          1_000_000;
        return {
          costKnown: true,
          costUsdMicros: Math.round(usd * 1_000_000),
          breakdown: { model, total: usd },
        };
      }),
    };
  };
}

let nextId = 0;
function row(overrides: Partial<UsageEventRow> = {}): UsageEventRow {
  nextId += 1;
  return {
    eventId: `fx-event-${nextId}`,
    machineId: "fx-machine",
    source: "claude",
    sessionId: "fx-session",
    kind: "response",
    model: "fx-model-a",
    provider: "fx-lab",
    usage: { input: 1_000_000, output: 100_000, cacheRead: 0, cacheCreate: 0 },
    costUsdMicros: 0,
    ...overrides,
  };
}

describe("priceSessionUsage", () => {
  it("fills costUSD from burn prices and keeps F1 token definitions", async () => {
    const result = await priceSessionUsage(
      [row(), row({ model: "fx-model-b", usage: { input: 2_000_000, output: 0 } })],
      fakePricer(),
    );
    // a: 3 + 1.5 = 4.5 USD; b: 2 USD.
    expect(result.costUsdMicros).toBe(6_500_000);
    expect(result.usage).toEqual({
      observed: true,
      inputTokens: 3_000_000,
      outputTokens: 100_000,
      cachedInputTokens: 0,
      totalTokens: 3_100_000,
      costUSD: 6.5,
    });
    expect(result.byModel).toEqual([
      { model: "fx-model-a", rows: 1, costUsdMicros: 4_500_000 },
      { model: "fx-model-b", rows: 1, costUsdMicros: 2_000_000 },
    ]);
    expect(result.provenance).toMatchObject({
      engine: "burn",
      pricingSnapshotSha256: FX_SHA,
      pricedRows: 2,
      unpricedRows: 0,
      reportedCostUsdMicros: null,
    });
  });

  it("keeps the session cost null when any row is unpriced", async () => {
    const result = await priceSessionUsage(
      [row(), row({ model: "fx-unknown-model" })],
      fakePricer(),
    );
    expect(result.costUsdMicros).toBeNull();
    expect(result.usage.costUSD).toBeNull();
    expect(result.usage.observed).toBe(true);
    expect(result.provenance.unpricedModels).toEqual(["fx-unknown-model"]);
    expect(result.byModel.find((m) => m.model === "fx-unknown-model")?.costUsdMicros).toBeNull();
    expect(result.byModel.find((m) => m.model === "fx-model-a")?.costUsdMicros).toBe(4_500_000);
  });

  it("keeps the session cost null when a usage row has no model", async () => {
    const result = await priceSessionUsage([row(), row({ model: null })], fakePricer());
    expect(result.costUsdMicros).toBeNull();
    expect(result.provenance.modelessRows).toBe(1);
  });

  it("does not invent a cost for a session with no usage", async () => {
    const seen: BurnUsagePriceRequest[][] = [];
    const result = await priceSessionUsage(
      [row({ usage: null }), row({ usage: { input: 0, output: 0 } })],
      fakePricer(seen),
    );
    expect(seen[0]).toEqual([]);
    expect(result.usage.observed).toBe(false);
    expect(result.usage.costUSD).toBeNull();
    expect(result.costUsdMicros).toBeNull();
  });

  it("prices a replayed row once", async () => {
    const once = row();
    const result = await priceSessionUsage([once, { ...once }], fakePricer());
    expect(result.costUsdMicros).toBe(4_500_000);
    expect(result.usage.inputTokens).toBe(1_000_000);
  });

  it("passes cache writes, reasoning and burn's source through", async () => {
    const seen: BurnUsagePriceRequest[][] = [];
    await priceSessionUsage(
      [
        row({
          source: "codex",
          usage: { input: 10, output: 20, reasoning: 5, cacheRead: 7, cacheCreate: 9, cacheCreate5m: 4, cacheCreate1h: 5 },
        }),
        row({ source: "grok", usage: { input: 1, cacheCreate: 8 } }),
      ],
      fakePricer(seen),
    );
    expect(seen[0]).toEqual([
      {
        model: "fx-model-a",
        source: "codex",
        usage: { input: 10, output: 20, reasoning: 5, cacheRead: 7, cacheCreate5m: 4, cacheCreate1h: 5 },
        measured: true,
      },
      {
        model: "fx-model-a",
        // grok is not a burn harness: null source, priced by model id.
        source: null,
        usage: { input: 1, output: 0, reasoning: 0, cacheRead: 0, cacheCreate5m: 8, cacheCreate1h: 0 },
        measured: true,
      },
    ]);
  });

  it("records a harness-reported cost for cross-checking only", async () => {
    const result = await priceSessionUsage([row({ costUsdMicros: 4_000_000 })], fakePricer());
    expect(result.costUsdMicros).toBe(4_500_000);
    expect(result.provenance.reportedCostUsdMicros).toBe(4_000_000);
  });

  it("accepts bigint micros from the napi binding", async () => {
    const pricer: UsagePricer = async ({ requests }) => ({
      schema: "burn.usage-prices.v1",
      pricing: FX_PRICING,
      prices: requests.map(({ model }) => ({
        costKnown: true,
        costUsdMicros: 7n,
        breakdown: { model, total: 0.000007 },
      })),
    });
    const result = await priceSessionUsage([row()], pricer);
    expect(result.costUsdMicros).toBe(7);
  });

  it("fails loudly when burn answers a different number of rows", async () => {
    const pricer: UsagePricer = async () => ({
      schema: "burn.usage-prices.v1",
      pricing: FX_PRICING,
      prices: [],
    });
    await expect(priceSessionUsage([row()], pricer)).rejects.toThrow("burn priced 0 of 1");
  });
});

describe("costUSD and costUsdMicros agree", () => {
  it("is null together and equal otherwise", async () => {
    const cases: UsageEventRow[][] = [
      [row()],
      [row(), row({ model: "fx-unknown-model" })],
      [row({ model: null })],
      [row({ usage: null })],
      [row({ model: "fx-model-b", usage: { input: 1, output: 1 } })],
    ];
    for (const rows of cases) {
      const result = await priceSessionUsage(rows, fakePricer());
      if (result.costUsdMicros === null) {
        expect(result.usage.costUSD).toBeNull();
      } else {
        expect(result.usage.costUSD).toBe(result.costUsdMicros / 1_000_000);
      }
    }
  });
});

describe("burnSourceFor", () => {
  it("maps only the harnesses burn models", () => {
    expect(burnSourceFor("claude")).toBe("claude-code");
    expect(burnSourceFor("codex")).toBe("codex");
    expect(burnSourceFor("opencode")).toBe("opencode");
    for (const source of ["grok", "muse", "cursor", "devin", "capy"]) {
      expect(burnSourceFor(source)).toBeUndefined();
    }
  });
});

// Opt-in end-to-end check against a locally built @relayburn/sdk (burn
// AgentWorkforce/burn#593). Not a dependency: skipped unless the path is set.
const burnSdkPath = process.env.BURN_SDK_NODE_ENTRY;
describe.skipIf(!burnSdkPath)("priceSessionUsage with the real burn SDK", () => {
  it("prices a known model and leaves an unknown one null", async () => {
    const sdk = await import(burnSdkPath!);
    const pricer: UsagePricer = (opts) => sdk.priceUsage(opts);
    const known = await priceSessionUsage(
      [row({ model: "claude-sonnet-4-5", usage: { input: 10_000, output: 2_000 } })],
      pricer,
    );
    expect(known.costUsdMicros).toBeGreaterThan(0);
    expect(known.provenance.pricingSnapshotSha256).toMatch(/^[0-9a-f]{64}$/);
    const unknown = await priceSessionUsage([row({ model: "fx-unknown-model" })], pricer);
    expect(unknown.costUsdMicros).toBeNull();
  });
});
