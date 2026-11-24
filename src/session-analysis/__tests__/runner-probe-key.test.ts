/**
 * The runner's versioning probe writes under <PIPELINE_KEY_PREFIX>/.runner-probe/session-analysis/ (blob-versioning.ts).
 * Nothing downstream may treat such a key as a session: the shared key parser finds no session location, the consumer's
 * pointer indexer and the object-landed builder ignore it as not_a_session_key before any store, database or org
 * resolution, so no org can ever be derived from ".runner-probe".
 */
import { expect, test } from "bun:test";
import { parseSessionKey } from "../../../packages/object-store/src/index.ts";
import { buildObjectLandedMessage } from "../../../packages/pipeline/src/lib/pipeline/adapter.ts";
import { indexPointer, type PointerDeps } from "../../session-pipeline/pointer.ts";
import { runnerProbePrefix } from "../blob-versioning.ts";

const keys = [
  `${runnerProbePrefix("96bulls")}fx-probe-id/probe`,
  `${runnerProbePrefix("fxroot")}00000000-0000-4000-8000-000000000000/probe`,
  runnerProbePrefix("96bulls"),
];

test("a probe key is never a session key, for the consumer or the object-landed path", async () => {
  // Every dependency throws: classification must finish before touching any of them.
  const deps = new Proxy({}, { get: () => { throw new Error("dependency used"); } }) as unknown as PointerDeps;
  for (const key of keys) {
    expect(key).toContain("/.runner-probe/session-analysis/");
    expect(parseSessionKey(key)).toBeUndefined();
    expect(await indexPointer({ key, provider: "azure", bucket: "fx-container" } as never, deps)).toEqual({ status: "ignored", reason: "not_a_session_key" });
    expect(buildObjectLandedMessage({ landed: { key, provider: "azure", bucket: "fx-container" } as never, orgId: "fx-org" }))
      .toEqual({ status: "ignored", reason: "not_a_session_key" });
  }
});
