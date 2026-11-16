/**
 * Bob Costas version stamps. BOB_COSTAS_PROMPT_VERSION names the model prompt (buildPromptInput /
 * buildModelRequest) and enters the config hash; BOB_COSTAS_PIPELINE_VERSION is written to
 * session_summaries.pipeline_version by a regeneration-aware upsert, so a later backfill can select
 * rows produced before a given pipeline change (NULL = before every version). Bump the pipeline
 * version when the digest, the extractive text or the prompt changes in a way that should be
 * regenerable.
 */
export const BOB_COSTAS_PROMPT_VERSION = "bc-prompt-2026-11-v1";
export const BOB_COSTAS_PIPELINE_VERSION = "bc-2026-11-regen-a";
