/**
 * Summaries regeneration (push A) for Bob Costas: the flag settings and the summariser config hash
 * the handler and the sweep share. Everything here is computed only when a flag is on, so with every
 * flag off (the default) neither the handler nor the sweep issues a new query or writes a new column.
 */
import { modelKey, modelResponsesUrl } from "../session-brief/config.ts";
import { bobCostasModel, modelSummarizerConfig } from "./model-summarizer.ts";
import { configHash } from "../summary-regen/config-hash.ts";
import { regenActive, regenSettings, type RegenReason, type RegenSettings } from "../summary-regen/settings.ts";
import { BOB_COSTAS_PROMPT_VERSION } from "./versions.ts";

export interface BobCostasRegen {
  settings: RegenSettings;
  /** The current summariser config hash (bobCostasConfigHash); "" when no coverage flag is on. */
  configHash: string;
}

/** Fallback reasons that a later retry may fix (model-summarizer FallbackReason). */
export const RETRYABLE_FALLBACKS: ReadonlySet<string> = new Set(["no_api_key", "model_failed", "budget", "global_cap"]);

/**
 * sha256 over the model id, the Responses URL, the reasoning effort, the prompt version and the key
 * fingerprint (HMAC, never the key). The extractive summariser hashes as model "extractive" with no
 * URL, effort or key.
 */
export function bobCostasConfigHash(env: NodeJS.ProcessEnv = process.env): string {
  const model = bobCostasModel(env);
  return configHash({
    model: model ?? "extractive",
    baseUrl: model ? modelResponsesUrl(env) : "",
    reasoningEffort: model ? modelSummarizerConfig(model, env).reasoningEffort : undefined,
    promptVersion: BOB_COSTAS_PROMPT_VERSION,
    key: model ? modelKey(env) : null,
  });
}

/** null when every flag is off: the handler and sweep then run today's code path only. */
export function bobCostasRegenFromEnv(env: NodeJS.ProcessEnv = process.env): BobCostasRegen | null {
  const settings = regenSettings(env);
  if (!regenActive(settings) && !settings.audit.enabled) return null;
  return { settings, configHash: regenActive(settings) ? bobCostasConfigHash(env) : "" };
}

/** The sweep's workKey suffixes, one per reason family; never the revision key. */
export const REGEN_WORK_SUFFIX = {
  turns: ":regen-turns",
  retry: ":regen-retry",
  config: ":regen-config",
  /** A legacy row current by revision: the handler stamps its coverage, no model call, no audit. */
  stamp: ":regen-stamp",
} as const;

/** The reason a regeneration job was queued for, from its workKey suffix (completion log only). */
export function reasonFromWorkKey(workKey: unknown): "retry_backoff" | "config_changed" | "turns" | undefined {
  if (typeof workKey !== "string") return undefined;
  if (workKey.endsWith(REGEN_WORK_SUFFIX.retry)) return "retry_backoff";
  if (workKey.endsWith(REGEN_WORK_SUFFIX.config)) return "config_changed";
  if (workKey.endsWith(REGEN_WORK_SUFFIX.turns)) return "turns";
  return undefined;
}

export type { RegenReason };
