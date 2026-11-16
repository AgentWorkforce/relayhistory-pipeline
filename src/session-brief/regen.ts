/**
 * Summaries regeneration (push A) for the session brief: the flag settings and the brief model's
 * config hash. Built only by the worker entry points (main.ts, the hosted Lambda); a BriefDeps
 * without `regen` runs today's code path, so every flag off is byte-identical.
 */
import { configHash } from "../summary-regen/config-hash.ts";
import { regenActive, regenSettings, type RegenSettings } from "../summary-regen/settings.ts";
import { modelResponsesUrl } from "./config.ts";
import { SESSION_BRIEF_PROMPT_VERSION } from "./versions.ts";

export interface BriefRegen {
  settings: RegenSettings;
  /** The brief model's config hash; "" when no coverage flag is on (audit only). */
  configHash: string;
}

/**
 * sha256 over the brief model id, the Responses URL, the reasoning effort (the brief engine always
 * sends "none", engine/model.ts), the prompt version and the key fingerprint (HMAC, never the key).
 */
export function briefConfigHash(model: string, key: string | null, env: NodeJS.ProcessEnv = process.env): string {
  return configHash({
    model,
    baseUrl: modelResponsesUrl(env),
    reasoningEffort: "none",
    promptVersion: SESSION_BRIEF_PROMPT_VERSION,
    key,
  });
}

/** null when every flag is off. `key` is the resolved model key (null = no model); never stored. */
export function briefRegenFromEnv(model: string, key: string | null, env: NodeJS.ProcessEnv = process.env): BriefRegen | null {
  const settings = regenSettings(env);
  if (!regenActive(settings) && !settings.audit.enabled) return null;
  return { settings, configHash: regenActive(settings) ? briefConfigHash(model, key, env) : "" };
}
