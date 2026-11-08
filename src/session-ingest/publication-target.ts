/** Explicit canonical publication target selection, read by publication-main.ts.
 *
 *   INGEST_PUBLICATION_TARGET  unset or exactly "aws": the existing hosted runtime
 *                              (publication-runtime.ts: us-east-1, Supabase, IMDSv2,
 *                              Secrets Manager, KMS), unchanged.
 *                              exactly "portable": the self-host runtime
 *                              (publication-portable.ts: S3-compatible endpoint with
 *                              static keys, Postgres URL, explicit TLS profile).
 *                              Anything else, including "", refuses: a typo never
 *                              silently selects either target.
 *
 * No dependencies, so the selector can be read before either runtime is loaded.
 */
export const PUBLICATION_TARGET_VAR = "INGEST_PUBLICATION_TARGET";
export type PublicationTargetKind = "aws" | "portable";
export function publicationTargetKind(env: NodeJS.ProcessEnv): PublicationTargetKind {
  const raw = env[PUBLICATION_TARGET_VAR];
  if (raw === undefined || raw === "aws") return "aws";
  if (raw === "portable") return "portable";
  throw Object.assign(new Error("publication_target_invalid"), { code: "publication_target_invalid" });
}
