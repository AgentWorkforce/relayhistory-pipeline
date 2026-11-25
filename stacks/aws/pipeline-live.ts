/**
 * The live session pipeline function, read back for the deploy plan contract
 * (transcript-store.ts TranscriptStorePlanCapture.liveSnapshotText): its env and its event source
 * mappings, by the aws CLI, read-only (lambda:GetFunctionConfiguration,
 * lambda:ListEventSourceMappings). Held in memory only: never written to disk or to the capture files.
 *
 * Alchemy writes the function's whole env on every update and plans against its state, not the live
 * function, so a variable set by hand is visible only here.
 */
import { sessionPipelineNames } from "./session-pipeline.ts";

export type AwsRun = (cmd: string, args: string[]) => string;

/** The live snapshot text, or undefined when the function does not exist yet (a first rollout). */
export function readPipelineLive(run: AwsRun, stage: string, accountId: string, region: string): string | undefined {
  const fn = sessionPipelineNames(stage, accountId, region).function;
  let config: { Environment?: { Variables?: Record<string, string> } };
  try {
    config = JSON.parse(run("aws", ["lambda", "get-function-configuration", "--function-name", fn, "--region", region, "--output", "json"]) || "{}");
  } catch (error) {
    if (/ResourceNotFoundException/.test(String((error as Error)?.message ?? error))) return undefined;
    throw error;
  }
  const listed = JSON.parse(run("aws", ["lambda", "list-event-source-mappings", "--function-name", fn, "--region", region, "--output", "json"]) || "{}") as {
    EventSourceMappings?: { EventSourceArn?: string; State?: string; ScalingConfig?: { MaximumConcurrency?: number } }[];
  };
  return JSON.stringify({
    schema: 1,
    env: config.Environment?.Variables ?? {},
    mappings: (listed.EventSourceMappings ?? []).map((m) => ({
      eventSourceArn: m.EventSourceArn ?? "",
      state: m.State ?? "",
      ...(m.ScalingConfig?.MaximumConcurrency !== undefined ? { maximumConcurrency: m.ScalingConfig.MaximumConcurrency } : {}),
    })),
  });
}
