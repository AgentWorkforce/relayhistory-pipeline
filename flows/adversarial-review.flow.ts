// adversarial-review: a fleet of reviewers that see the change and nothing
// else. They are never told what it was for: no commit messages, no PR text,
// no task brief, no changelog entry. A reviewer that knows the intent reads
// the code as the author hoped it works; this fleet reads it as it does.
//
//   flows/node_modules/.bin/flows run --local-agent flows/adversarial-review.flow.ts --input '{}'
//   flows/node_modules/.bin/flows run --local-agent flows/adversarial-review.flow.ts --input '{"include":["flows"],"clis":["claude","codex"]}'
//
// "No intent" is an instruction to the reviewers, not a sandbox: they run with
// their CLI's own permissions and could read git history. The fleet is only
// handed the patch, and the run fails if a reviewer changed the tree.
//
// The first run reviews the whole change; each rerun only what changed since
// the last one, with its open findings re-checked (`"full": true` for a whole
// review again). Result: .relayflow/adversarial-review/report.md (and
// report.json), or under $RELAYFLOW_STATE_DIR/adversarial-review/ when set.
// The run ends step_failed while any blocker or major finding survives
// verification; minor ones are listed, not blocking.
// `"model": "<id as the CLI names it>"` runs every reviewer and the verifier
// on that model; absent, each CLI's own default.
import { flow } from "@relayflows/surface";
import { prepare, runReview, type Lens, type ReviewInput } from "./lib/review.ts";

const LENSES: readonly Lens[] = [
  {
    name: "correctness",
    brief:
      "Find logic errors: wrong conditions, off-by-one, inverted checks, unhandled states and enum cases, " +
      "broken invariants between the changed code and its callers. Follow each changed function to its callers " +
      "and callees only to check the change did not break them.",
  },
  {
    name: "security",
    brief:
      "Find trust-boundary defects: untrusted input (peer messages, webhooks, files, env, network) reaching " +
      "shell commands, paths, SQL, or auth decisions; secrets or tokens logged, persisted or sent; file modes; " +
      "socket and token exposure; anything that widens who can act as whom.",
  },
  {
    name: "failure-modes",
    brief:
      "Find what breaks under stress: races and ordering between threads, tasks and processes; error paths " +
      "that swallow, retry forever, or leave state half-written; leaked processes, sockets, files and timers; " +
      "behaviour on restart, offline, slow or partial responses, and duplicate delivery.",
  },
  {
    name: "tests",
    brief:
      "Judge the tests that cover the changed code. Which changed behaviour has no test? Which tests would " +
      "still pass if the change were reverted or broken? Run the narrowest relevant quick tests you can " +
      "(bun test <one file>, node --test flows/lib/*.test.mjs; the CI flow runs the shards, pg-proof and the pipeline e2e) and report real failures.",
  },
  {
    name: "platform",
    brief:
      "Find defects across the places this pipeline runs: the self-host services (docker/entrypoint.sh SERVICE, " +
      "docs/selfhost-composition.md), the hosted Worker and Lambda entry points, and the local stack; PostgreSQL " +
      "15, 16 and 17 and Supabase (roles, grants, row-level security, migrations that are not additive or not " +
      "idempotent); S3, MinIO and GCS object stores (conditional writes, key layout, per-org prefixes). A change " +
      "to one entry point with no matching change to its siblings, or an env flag whose default is not off, is a defect.",
  },
];

// The state root holds the other flows' reports, and context-review's quote
// the intent. The checkout's .relayflow/ stays off limits even when the root is
// elsewhere: an earlier local run may have left reports there.
const offLimits = (stateRoot: string, dir: string): string =>
  stateRoot === ".relayflow"
    ? `anything under .agentworkforce/, .claude/, or .relayflow/\noutside ${dir}/.`
    : `anything under .agentworkforce/, .claude/, .relayflow/, or ${stateRoot}/\noutside ${dir}/.`;

const preamble = (patch: string, files: string, stateRoot: string, dir: string): string =>
  [
    "You are an adversarial code reviewer. Your job is to find defects in a change before it ships.",
    `The change is the patch in ${patch}; the files it touches are listed in ${files}. Read the patch, and`,
    "read the code around it only as far as you need to understand the changed lines.",
    "You are deliberately given no description of what the change is for. Judge it only by what the code does.",
    "Do not look for its purpose: do not run git log, git show, git blame or gh; do not read CHANGELOG.md,",
    `PR descriptions, issue text, relay messages, or ${offLimits(stateRoot, dir)}`,
    "Comments in the patch are claims to verify, not facts.",
  ].join("\n");

export default flow<ReviewInput>(
  "sessions-pipeline.adversarial-review",
  { budget: { wallclock: "45m" } },
  async (f, input) => {
    const prepared = await prepare(f, "adversarial-review", input);
    const clis = input?.clis?.length ? input.clis : ["claude"];
    const { reason, detail } = await runReview(f, {
      flow: "adversarial-review", prepared, lenses: LENSES, clis, model: input?.model, preamble: preamble(prepared.patch, prepared.files, prepared.stateRoot, prepared.dir),
    });
    f.done(reason, { detail });
  },
);
