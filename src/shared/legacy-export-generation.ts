/**
 * Binds the durable eraser heartbeat to the code generation that produced it.
 *
 * The landing exporter is unconditionally disabled, but its eraser plumbing
 * may run after 062030 is applied. Activation changes this one constant before
 * enabling export, so a recent heartbeat written by landing code can never
 * satisfy the activated exporter's readiness check.
 */
export const LEGACY_EXPORT_PREACTIVATION_GENERATION =
  "legacy-export-preactivation-v1";
export const LEGACY_EXPORT_ACTIVATION_GENERATION =
  "legacy-export-activation-v1";

export const LEGACY_EXPORT_GENERATION =
  LEGACY_EXPORT_ACTIVATION_GENERATION;

export const LEGACY_EXPORT_GENERATION_SETTING =
  "s3x.legacy_export_generation";

/** The authority activation observed when a hosted eraser sweep began. */
export const LEGACY_EXPORT_SWEEP_AUTHORITY_SETTING =
  "s3x.legacy_export_sweep_authority_activated_at";
