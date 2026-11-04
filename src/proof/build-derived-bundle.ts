/** Uses Alchemy's actual Rolldown bundler and the same plugin installed on the
 * production function props. This builds files only; it invokes no cloud API. */
import { build, NODE_CONDITION_NAMES } from "alchemy/Bundle/Bundle";
import * as Effect from "effect/Effect";
import { join } from "node:path";
import { fileURLToPath } from "node:url";
import { sessionPipelineFunctionProps } from "../../stacks/aws/session-pipeline.ts";

const ROOT = fileURLToPath(new URL("../..", import.meta.url));
export async function buildDerivedProbeBundle(directory: string): Promise<string> {
  const file = join(directory, "derived-worker.mjs");
  const configured = sessionPipelineFunctionProps({ stage: "fixture", accountId: "000000000000",
    region: "us-east-1", p: {}, tags: () => ({}), bucket: "fixture" }).build;
  await Effect.runPromise(build({
    input: join(ROOT, "src/session-pipeline/derived-bundle-probe.ts"), cwd: ROOT,
    platform: "node", tsconfig: join(ROOT, "tsconfig.json"),
    resolve: { conditionNames: [...NODE_CONDITION_NAMES] },
    plugins: configured.plugins,
  }, { format: "esm", file, codeSplitting: false, sourcemap: false }));
  return file;
}

if (process.argv[1]?.endsWith("build-derived-bundle.ts") && process.argv[2])
  console.log(await buildDerivedProbeBundle(process.argv[2]));
