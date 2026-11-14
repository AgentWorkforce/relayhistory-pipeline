/**
 * The deployed entry of the session pipeline Lambda (stacks/aws/session-pipeline.ts `main`). It exists
 * only to carry the module-load TypeScript scrub grant (docs/scrub-engine.md, "Default flip"): lambda.ts
 * cannot, because the self-host Node services import it for helpers (supabasePool, transaction,
 * parseSecret, ...) and must never gain the TypeScript engine. The grant import comes first, so it runs
 * before any module of the Lambda graph evaluates; lambda.ts's handler still declares it again on entry.
 * Only hosted entries may import this module (scripts/scrub-default-scan.ts rule "hosted-grant-reach").
 */
import "../../packages/pipeline/src/lib/scrub-engine/hosted-lambda-grant.ts";
export { handler } from "./lambda.ts";
