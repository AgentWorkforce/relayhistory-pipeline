/**
 * A plan that writes nothing to the shared Alchemy state.
 *
 * `AWS.state` is not read-only even for `alchemy plan`: it takes a lease
 * (PUT then DELETE of `__lease__.json`) and reconciles the state bucket's
 * versioning, ownership and encryption on drift. A captured plan therefore runs
 * against a snapshot instead: `96bulls plan <stage> --capture` downloads the
 * stage's state objects with plain GETs (pinned to their version ids), and the
 * stack, when NINETY_SIX_BULLS_STATE_SNAPSHOT names that file, uses Alchemy's
 * in-memory state built from it. Nothing is written to the bucket. The
 * snapshot is refused for anything but a plan.
 */
import { readFileSync } from "node:fs";
import { inMemoryState } from "alchemy";
import { reviveState } from "alchemy/State";

export interface StateSnapshot {
  schema: 1;
  stack: string;
  stage: string;
  bucket: string;
  prefix: string;
  /** Raw object text by key, exactly as stored; each pinned to the version id recorded beside it. */
  objects: { key: string; versionId: string; text: string }[];
}

const OUTPUT_FILE = "__stack_output__.json";

/** In-memory state for one stack/stage, decoded the way BucketState decodes its objects. */
export function snapshotStateLayer(file: string, argv: readonly string[] = process.argv) {
  if (!argv.includes("plan")) throw new Error("96bulls: a state snapshot is for `alchemy plan` only. Nothing was changed.");
  const snap = JSON.parse(readFileSync(file, "utf8")) as StateSnapshot;
  if (snap.schema !== 1) throw new Error(`96bulls: unknown state snapshot schema in ${file}. Nothing was changed.`);
  return inMemoryState(...snapshotContents(snap));
}

export function snapshotContents(snap: StateSnapshot) {
  const resources: Record<string, unknown> = {};
  let output: unknown;
  for (const o of snap.objects) {
    const name = o.key.slice(snap.prefix.length);
    if (name === OUTPUT_FILE) output = JSON.parse(o.text, reviveState);
    else if (name.endsWith(".json") && !name.startsWith("__")) {
      const state = JSON.parse(o.text, reviveState) as { fqn?: string };
      if (!state.fqn) throw new Error(`96bulls: state object ${o.key} has no fqn. Nothing was changed.`);
      resources[state.fqn] = state;
    }
  }
  return [
    { [snap.stack]: { [snap.stage]: resources } } as never,
    (output === undefined ? {} : { [snap.stack]: { [snap.stage]: output } }) as never,
  ] as const;
}
