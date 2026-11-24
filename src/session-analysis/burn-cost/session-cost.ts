// Proof of concept (review/burn-cost-scope-poc): price a session's own token
// usage with burn's tariffs. Not wired into the runner, processor or engine;
// nothing on main calls this module. See docs/design/burn-session-cost.md.
//
// Division of labor: burn owns tokens -> USD (tariffs, provider precedence,
// context tiers, cache-write TTLs, reasoning rules). This module only maps
// RelayHistory event rows onto burn's `priceUsage` request shape and folds
// the answers back into F1's `metrics.usage` contract, unchanged:
//   - costUSD stays null whenever any usage row is unpriced (unknown, never 0);
//   - costUSD is never imputed for a session with no observed usage.
// Sums across sessions and parent/child dedupe belong to session-intel.
import type { SessionUsageSummary } from "../engine/session-analysis-result.ts";

/** burn's `SourceKind` wire names (relayburn-sdk reader/types.rs). */
export type BurnSource =
  | "claude-code"
  | "codex"
  | "opencode"
  | "copilot-cli"
  | "anthropic-api"
  | "openai-api"
  | "gemini-api";

export interface BurnUsageTokens {
  input: number;
  output: number;
  reasoning: number;
  cacheRead: number;
  cacheCreate5m: number;
  cacheCreate1h: number;
}

export interface BurnUsagePriceRequest {
  model: string;
  /** Required by burn; null for a harness burn does not model. */
  source: BurnSource | null;
  usage: BurnUsageTokens;
  aggregate?: boolean;
  measured?: boolean;
}

export interface BurnUsagePrice {
  costKnown: boolean;
  costUsdMicros: number | bigint | null;
  breakdown: { model: string; total: number } | null;
}

/** `burn.usage-prices.v1` (relayburn-sdk price_usage.rs). */
export interface BurnUsagePrices {
  schema: "burn.usage-prices.v1";
  pricing: {
    sdkVersion: string;
    builtinPricingSha256: string;
    overrideSha256: string | null;
  };
  prices: BurnUsagePrice[];
}

/**
 * The one seam to burn. Production would pass `@relayburn/sdk`'s
 * `priceUsage` (napi) or a `burn` subprocess; tests pass a fixed tariff.
 */
export type UsagePricer = (opts: {
  requests: BurnUsagePriceRequest[];
}) => Promise<BurnUsagePrices>;

/**
 * The fields of an `apiEvent` row (evidence.ts) this module reads. Kept
 * structural so the POC needs no change to evidence.ts.
 */
export interface UsageEventRow {
  eventId: string;
  machineId: string;
  source: string;
  sessionId: string;
  kind?: string | null;
  model: string | null;
  provider?: string | null;
  usage?: {
    input?: number;
    output?: number;
    reasoning?: number;
    cacheRead?: number;
    cacheCreate?: number;
    cacheCreate5m?: number | null;
    cacheCreate1h?: number | null;
  } | null;
  costUsdMicros?: number | null;
}

/** Where `costUSD` came from, for Stage 1 coverage. Additive and optional. */
export interface SessionCostProvenance {
  engine: "burn";
  schema: "burn.usage-prices.v1";
  /** relayburn-sdk version (code-level pricing rules). */
  sdkVersion: string;
  /** SHA-256 of burn's bundled pricing data. */
  pricingSnapshotSha256: string;
  /** SHA-256 of an applied pricing override; null when none. */
  overrideSha256: string | null;
  pricedRows: number;
  unpricedRows: number;
  unpricedModels: string[];
  /** Rows that carried usage but no model id: burn cannot price them. */
  modelessRows: number;
  /** The harness's own reported total, for cross-checking only. */
  reportedCostUsdMicros: number | null;
}

export interface SessionCost {
  usage: SessionUsageSummary;
  costUsdMicros: number | null;
  reasoningTokens: number;
  cacheCreateTokens: number;
  byModel: Array<{
    model: string;
    rows: number;
    costUsdMicros: number | null;
  }>;
  provenance: SessionCostProvenance;
}

const BURN_SOURCES: Record<string, BurnSource> = {
  claude: "claude-code",
  "claude-code": "claude-code",
  codex: "codex",
  opencode: "opencode",
  copilot: "copilot-cli",
  "copilot-cli": "copilot-cli",
};

/**
 * burn's harness for a pipeline `source`. Harnesses burn does not model
 * (grok, muse, cursor, devin, capy) return undefined: burn then prices by
 * model id alone and applies the model's own reasoning rule.
 */
export function burnSourceFor(source: string): BurnSource | undefined {
  return BURN_SOURCES[source];
}

const count = (value: unknown): number =>
  typeof value === "number" && Number.isFinite(value) && value > 0
    ? Math.trunc(value)
    : 0;

/**
 * Cache writes split by TTL. When History only has the total, bill it at the
 * default 5-minute TTL (burn's own fallback for unsplit Claude counters).
 */
function cacheCreateSplit(usage: NonNullable<UsageEventRow["usage"]>): {
  cacheCreate5m: number;
  cacheCreate1h: number;
} {
  const fiveMinute = usage.cacheCreate5m;
  const oneHour = usage.cacheCreate1h;
  if (fiveMinute == null && oneHour == null) {
    return { cacheCreate5m: count(usage.cacheCreate), cacheCreate1h: 0 };
  }
  return { cacheCreate5m: count(fiveMinute), cacheCreate1h: count(oneHour) };
}

function tokensOf(row: UsageEventRow): BurnUsageTokens | null {
  const usage = row.usage;
  if (!usage) return null;
  const tokens: BurnUsageTokens = {
    input: count(usage.input),
    output: count(usage.output),
    reasoning: count(usage.reasoning),
    cacheRead: count(usage.cacheRead),
    ...cacheCreateSplit(usage),
  };
  return Object.values(tokens).some((value) => value > 0) ? tokens : null;
}

/** Same identity session-metrics.ts uses, so a replayed row is priced once. */
function rowKey(row: UsageEventRow): string {
  return JSON.stringify([
    row.source,
    row.sessionId,
    row.machineId,
    row.kind ?? null,
    row.eventId,
  ]);
}

function micros(value: number | bigint | null): number | null {
  if (value === null) return null;
  const n = typeof value === "bigint" ? Number(value) : value;
  return Number.isFinite(n) && n >= 0 ? n : null;
}

/**
 * Price one session's usage rows through burn and fold the result into
 * F1's `SessionUsageSummary`. Never throws for an unpriced model; a pricer
 * failure propagates so the caller can keep the existing null cost.
 */
export async function priceSessionUsage(
  rows: readonly UsageEventRow[],
  pricer: UsagePricer,
): Promise<SessionCost> {
  const seen = new Set<string>();
  const priced: Array<{ model: string; tokens: BurnUsageTokens }> = [];
  const requests: BurnUsagePriceRequest[] = [];
  let modelessRows = 0;
  let reported = 0;
  let sawReported = false;
  let input = 0;
  let output = 0;
  let cacheRead = 0;
  let reasoning = 0;
  let cacheCreate = 0;

  for (const row of rows) {
    const key = rowKey(row);
    if (seen.has(key)) continue;
    seen.add(key);
    const tokens = tokensOf(row);
    if (!tokens) continue;
    input += tokens.input;
    output += tokens.output;
    cacheRead += tokens.cacheRead;
    reasoning += tokens.reasoning;
    cacheCreate += tokens.cacheCreate5m + tokens.cacheCreate1h;
    // History defaults a missing cost to zero, so only a positive value is
    // a reported cost (same rule as evidence.ts normalizedEvent).
    if (count(row.costUsdMicros) > 0) {
      sawReported = true;
      reported += count(row.costUsdMicros);
    }
    if (!row.model) {
      modelessRows += 1;
      continue;
    }
    const source = burnSourceFor(row.source);
    priced.push({ model: row.model, tokens });
    requests.push({
      model: row.model,
      source: source ?? null,
      usage: tokens,
      measured: true,
    });
  }

  const answer = await pricer({ requests });
  if (answer.prices.length !== requests.length) {
    throw new Error(
      `burn priced ${answer.prices.length} of ${requests.length} usage rows`,
    );
  }

  const byModel = new Map<string, { rows: number; costUsdMicros: number | null }>();
  const unpricedModels = new Set<string>();
  let total: number | null = modelessRows > 0 ? null : 0;
  let pricedRows = 0;
  answer.prices.forEach((price, index) => {
    const model = priced[index]!.model;
    const cost = price.costKnown ? micros(price.costUsdMicros) : null;
    const group = byModel.get(model) ?? { rows: 0, costUsdMicros: 0 };
    group.rows += 1;
    if (cost === null) {
      unpricedModels.add(model);
      group.costUsdMicros = null;
      total = null;
    } else {
      pricedRows += 1;
      if (group.costUsdMicros !== null) group.costUsdMicros += cost;
      if (total !== null) total += cost;
    }
    byModel.set(model, group);
  });

  const observed = input + output + cacheRead + reasoning + cacheCreate > 0;
  const costUsdMicros = observed ? total : null;
  return {
    usage: {
      observed,
      inputTokens: input,
      outputTokens: output,
      cachedInputTokens: cacheRead,
      // Same definition as session-metrics.ts (input + output), so the
      // token fields agree with what F1 reports today.
      totalTokens: input + output,
      costUSD: costUsdMicros === null ? null : costUsdMicros / 1_000_000,
    },
    costUsdMicros,
    reasoningTokens: reasoning,
    cacheCreateTokens: cacheCreate,
    byModel: [...byModel]
      .map(([model, group]) => ({ model, ...group }))
      .sort((a, b) => (a.model < b.model ? -1 : a.model > b.model ? 1 : 0)),
    provenance: {
      engine: "burn",
      schema: answer.schema,
      sdkVersion: answer.pricing.sdkVersion,
      pricingSnapshotSha256: answer.pricing.builtinPricingSha256,
      overrideSha256: answer.pricing.overrideSha256,
      pricedRows,
      unpricedRows: requests.length - pricedRows,
      unpricedModels: [...unpricedModels].sort(),
      modelessRows,
      reportedCostUsdMicros: sawReported ? reported : null,
    },
  };
}
