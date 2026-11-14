/**
 * EventBridge-scheduled Lambda entry for the S3 ledger re-arm sweeper (rearm-sweeper.ts), packaged like
 * buffered-gc-lambda.ts: DEFAULT OFF. With INGEST_REARM_SWEEPER_ENABLED unset or "false" it returns {enabled:false}
 * before reading any setting or credential. Armed, it runs ONE bounded pass with the execution role
 * (INGEST_REARM_CREDENTIALS must be unset or "role"), stopping before its next marker once the invocation's budget is spent.
 * Its position is kept in the bucket (rearm-sweeper.ts rearmCursorKey, create-only then If-Match), so each invocation
 * continues where the last one stopped and wraps to the start once the namespace is done: no marker is starved by the
 * ones before it. The handler never accepts factories, credentials or settings from its invocation payload.
 * One EMF line per pass: counts only.
 */
import "@rhc/lib/scrub-engine/hosted-lambda-grant.ts"; // FIRST import: module-load TS grant (docs/scrub-engine.md "Default flip")
import { declareScrubTsCapability } from "@rhc/lib/scrub-engine/ts-capability.ts";
import { REARM_ENABLE_FLAG, rearmSweeperEnabled } from "./rearm-sweeper-main.ts";

/** Stop listing this long before the Lambda's own deadline (one page's markers must still finish). */
export const REARM_LAMBDA_RESERVE_MS = 60_000;

export async function handler(_event: unknown = {}, context?: { getRemainingTimeInMillis?: () => number }) {
  declareScrubTsCapability("hosted-lambda"); // hosted Lambda zip, no relayscrubber artifacts (docs/scrub-engine.md "Default flip")
  if (!rearmSweeperEnabled(process.env)) return { enabled: false };
  if ((process.env.INGEST_REARM_CREDENTIALS ?? "role") !== "role") throw Error("rearm_sweep_credentials_refused");
  const name = process.env.AWS_LAMBDA_FUNCTION_NAME ?? "rearm-sweeper";
  const metric = (healthy: number, counts: Record<string, number>) => console.log(JSON.stringify({
    _aws: { Timestamp: Date.now(), CloudWatchMetrics: [{ Namespace: "96Bulls/RearmSweeper", Dimensions: [["FunctionName"]], Metrics: [{ Name: "Healthy", Unit: "Count" }] }] },
    FunctionName: /^[A-Za-z0-9_-]{1,64}$/.test(name) ? name : "rearm-sweeper", Healthy: healthy, ...counts,
  }));
  const remaining = context?.getRemainingTimeInMillis?.();
  const deadlineAt = Date.now() + Math.max(0, (typeof remaining === "number" && Number.isFinite(remaining) ? remaining : 300_000) - REARM_LAMBDA_RESERVE_MS);
  try {
    const { runRearmSweeperPass } = await import("./rearm-sweeper-main.ts");
    const { output } = await runRearmSweeperPass(process.env, [], {}, null, { deadlineAt });
    if (output.enabled) metric(output.counts.failed === 0 ? 1 : 0, { ...output.counts, Complete: output.complete ? 1 : 0 });
    return output;
  } catch (e) {
    const code = typeof (e as { code?: unknown })?.code === "string" && /^[A-Za-z0-9_]{1,64}$/.test((e as { code: string }).code) ? (e as { code: string }).code : "failed";
    metric(0, { failed: 1 });
    throw Error("rearm_sweep_" + code);
  }
}

export { REARM_ENABLE_FLAG };
