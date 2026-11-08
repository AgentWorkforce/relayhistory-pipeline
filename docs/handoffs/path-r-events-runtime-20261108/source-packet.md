# Path R (owner-buffered session events, #377) runtime source packet: prerequisites only

Source: PR #377, branch `feat/owner-buffered-events-20261108`, head 92c380a2 = merge of `main` 06764c94 (#374,
#375, #376) into the reviewed Path R delta (commits 4528f207 + 28394e2a on top of c858e7fe; no conflicts, the
branch-versus-main delta is unchanged). Every `path:Lx` below is a line in the #377 head tree 92c380a2, i.e. the
tree as it will land. This packet is documentation. It activates nothing, recommends no
activation, and is not an approval. Production identifiers are written as "redacted". No credential value
appears here. Tags: `[source-ready]` = present and default-off in #377's source; `[pending #377 merge]` = true
once #377 is on `main`; `[operator: needs Khaliq OK]` = a production step that only the credentialed operator
lane executes under the authorization that lane holds directly from Khaliq in his own words. This packet does not
verify, grant, transfer or request any such authorization; where an executor session already holds it, the
remaining gate is readiness and execution evidence, not a new approval. `[missing]` = not in source.

## 1. Scope and status

Merged main SHA: f196f637ed6caf4f02d6948aa5830c95b4d20df3 (#377 merged by khaliqgant at 2026-11-08T16:36:14Z
from exact head 92c380a2; parents a5ba9648 = main after #367 and 92c380a2). The merged tree also carries #367
(s3-first async ingest stack: `stacks/cloudflare/session-ingest.ts`, `buffered-queue-provider.ts`,
`scripts/private-ingest-operator.ts`), which does not touch the Path R read-Worker files. At merge time the
`unit-tests` check on 92c380a2 was still in progress (24 of 25 checks green); its terminal result is recorded in
section 7. Cite this exact SHA, never mutable `main`. This SHA is a source reference only: the deploy and image
source stays 8bb18be5 until Khaliq says otherwise.

- "Path R" is the own-session owner-buffered EVENTS reader: `GET /v1/sessions/:sessionId/events` with `source`
  can serve the caller's own buffered events for a session that has no canonical pointer, behind
  `READ_API_OWNER_BUFFERED_EVENTS` (default off).
- Files changed (16; 1271 insertions, 70 deletions):
  - Runtime: `src/session-read-api/app.ts`, `owner-buffered.ts`, `cursor.ts`, `page.ts`,
    `canonical-projection.ts`; `packages/session-read-worker/src/index.ts`.
  - Deploy: `packages/session-read-worker/scripts/render-deploy-config.ts`,
    `.github/workflows/deploy-session-read-worker.yml`.
  - Docs: `docs/session-read-api-design.md` (section 8), `runbooks/sessions-prod-session-read-worker.md`.
  - Tests: `src/session-read-api/__tests__/owner-buffered-events.test.ts` (new),
    `owner-buffered.routes.test.ts`, `owner-buffered.test.ts`, `buffered-privacy-fixtures.test.ts`;
    `packages/session-read-worker/scripts/render-deploy-config.test.ts`,
    `packages/session-read-worker/test/worker.test.ts`.
  - Nothing under `supabase/`, `stacks/`, `src/session-pipeline/`, `src/amp/` or `src/alleyoop/`. No migration.
- What the flag does:
  - Parsing: unset or empty is off, exactly `on` enables, anything else throws `OwnerBufferedEventsFlagError`
    (`src/session-read-api/owner-buffered.ts:83-103`).
  - The Worker refuses any other value before any read: `ConfigError("READ_API_OWNER_BUFFERED_EVENTS")`, logged
    `missing_READ_API_OWNER_BUFFERED_EVENTS`, answer 503 `backend_unavailable` for every request
    (`packages/session-read-worker/src/index.ts:305-314`). A literal `off` is such a value (worker test
    `packages/session-read-worker/test/worker.test.ts:200-230` lists `off` among the 503 cases).
  - On, it sets `events: true` on the owner-buffered deps (`index.ts:433-434`, helper L438-445). The deps exist
    only when `RELAYHISTORY_READ_OWNER_BUFFERED` is on and the bucket and raw-store keys exist (`index.ts:420-425`).
  - The route then runs the buffered branch only if `source` is given and `ownerBufferedFor` returns the deps
    (`src/session-read-api/app.ts:585-586`).
- What the flag does not do:
  - It does not enable owner-buffered reads by itself (`owner-buffered.ts:93-98`; `index.ts:119-126`).
  - It does not touch `GET /v1/events` (workspace search). That route still uses `decodeCursor` and
    `sessionGate` without the buffered branch (`app.ts:619-645`). None of the `app.ts` hunks in #377 touch it.
  - It does not touch activity, timing, catalog, output, digest or delivery routes
    (`docs/session-read-api-design.md:190-191`). It serves no buffered `convergence_event` lines (design L211).
- Prerequisites (both must already be on in the same render, plus both allowlists):
  - `RELAYHISTORY_READ_OWNER_BUFFERED=true` with the `HISTORY_CONTROL_HEAD` binding, rendered only by
    `owner_buffered=on` (`render-deploy-config.ts:187-195`).
  - `READ_API_OWNER_BUFFERED_WORKSPACES` (from the `owner_buffered_workspaces` input) AND
    `READ_API_DIRECTORY_WORKSPACES` (from the GitHub env var `SESSION_READ_DIRECTORY_WORKSPACES`). The caller's
    workspace must be on both, and the token must carry a user (`app.ts:501-514`).
- Precedence (`app.ts:581-605`; design L206):
  - It is decided before `sessionGate`, on `sessionStart` alone (`app.ts:588-589`).
  - Any canonical pointer (live or tombstone) means canonical serves the whole response. The buffered gate, RPC
    and S3 are never called.
  - With zero pointers the gated read runs: `readOwnerBufferedEvents` → `readGatedSession`
    (`owner-buffered.ts:960-979`, `:1010-1020`). The steps are: observe the control head, the stage RPC, the S3
    fetch with sha256 and tenant checks, a recheck, observe again as the last I/O, then admission against the final
    head.
  - Visible rows are served with `X-Relayhistory-Buffered: 1` (`app.ts:597-600`). `sessionGate` (history floor,
    partial-session 501) does not run for them (design L206).
  - Erased at the recheck gives an empty page with the header (`app.ts:592-595`).
  - Hidden, not owned, too large, unverified or refused-projection outcomes fall through to the canonical path,
    which answers exactly as with the flag off (`app.ts:532-557`, `:603-607`). Fenced after a candidate gives 503
    `buffered_fenced` (`app.ts:545-548`).
  - The same `sessionStart` answer is passed into `sessionGate`, so there is one call per request
    (`app.ts:486-491`, `:607`).
- Projection and tenancy:
  - Rows come from `projectEventRows` (`src/session-read-api/canonical-projection.ts:273-319`), the projection
    `readCanonicalScope` now also calls (`canonical-projection.ts:543`).
  - Org, workspace, user, session, source and machine are checked per row, else `TenantMismatchError`
    (`owner-buffered.ts:1035-1059`, check at L1053-1054).
  - Text fields are scrubbed before return (`scrubEventRow`, `owner-buffered.ts:1149-1166`).
  - A projection failure throws `BufferedProjectionFailed`, which hides the session (logged
    `buffered_projection_failed`, `app.ts:550-553`).
- 409 `cursor_backend_transition` (`app.ts:559-566`; design L210):
  - The buffered cursor is the `b96e1:` wire with the six-field position plus a seventh element `"ob1"`
    (`src/session-read-api/cursor.ts:40-56`). `decodeCursor` still refuses seven elements (`cursor.ts:59-61`,
    `:81-82`). Only `decodeBackendCursor` (`cursor.ts:67-69`), called only in the eligible branch (`app.ts:587`),
    reads the marker.
  - A canonical cursor on a buffered page gives 409 (`app.ts:596`).
  - A buffered cursor once the session has a pointer, or is no longer visible as buffered, gives 409
    (`app.ts:603-604`).
  - Paging is `readBufferedEventPage` (`src/session-read-api/page.ts:404-419`), with the canonical sort key and
    the shared `budgetCut` (`page.ts:385-392`).
- Byte-identical flag-off evidence (source; CI status in section 7):
  - Route tests:
    - `owner-buffered.routes.test.ts:525-534`: with owner-buffered on and the events flag absent, the events
      answer equals a harness with no owner-buffered deps, there is no buffered header, and the gate is not
      called.
    - `owner-buffered.routes.test.ts:738-754`: events flag off, owner-buffered disabled, no `source`, or the
      workspace missing from either allowlist all give an answer equal to the flag-off answer, with zero gate
      and zero RPC calls.
    - `owner-buffered.routes.test.ts:871`: hidden outcomes answer exactly as the flag off.
  - Render tests:
    - `render-deploy-config.test.ts:230-249`: events off (no input, empty, whitespace, `off`) is byte-identical
      to the render without the input across six input combinations, and equals the sed render for default
      inputs.
    - `render-deploy-config.test.ts:251-259`: events on adds exactly `READ_API_OWNER_BUFFERED_EVENTS=on` and
      nothing else.
  - Worker test: `worker.test.ts:200-230` checks that unset or empty leaves the deps keys unchanged.
- The 2^53 admission-version clamp fix:
  - The projection's index-row version is the admission version, which is never emitted. A version past
    `Number.MAX_SAFE_INTEGER` used to hide a visible session. It is now clamped to `Number.MAX_SAFE_INTEGER`
    (`owner-buffered.ts:1014-1018`).
  - The gate still compares the exact decimal as BigInt (`admissionVisible`, `owner-buffered.ts:372`).
  - Test: `owner-buffered-events.test.ts:268-275`. `9007199254740993` under head `9007199254740993` is served;
    `9007199254740992` under that head is hidden.
- Commit 28394e2a:
  - The predeclared-var refusal now runs before any return, in every render mode
    (`render-deploy-config.ts:135-141`, called at L153).
  - Test: `render-deploy-config.test.ts:272-291`.

## 2. Bundle and config contract

- Builder and deployer: `.github/workflows/deploy-session-read-worker.yml`. Manual `workflow_dispatch` only
  (L51-72).
  - Both jobs run only on `refs/heads/main` (L80, L87), so Path R cannot be dispatched until #377 is merged.
  - It checks out `github.sha` (L115-118) and runs in the `session-read-worker` environment (L90). The
    concurrency group is `session-read-worker-production` (L75-77).
  - For a `workflow_dispatch` on `main`, `github.sha` resolves to the tip of `main` at dispatch time, which is
    mutable. Before treating a run as the Path R source, record the run's resolved `head_sha` and verify it equals
    f196f637 (or that `packages/session-read-worker/` and `src/session-read-api/` in that tree are identical to
    f196f637's). Never assume `main` still equals f196f637.
  - The `checks` job reuses `session-read-worker-checks.yml` (L79-83): Worker typecheck including the render
    vitest, Worker tests in workerd, and a wrangler dry-run bundle.
  - The Worker is bundled from that main checkout by `wrangler deploy` (L166-176). It is not built from the
    session-pipeline container image.
- Inputs (L54-72): `owner_buffered` (off|on, default off), `owner_buffered_workspaces` (string, default empty),
  `owner_buffered_events` (off|on, default off; new in #377, L63-67), `buffered_search_gate` (off|on, default
  off).
- Env-to-render mapping (workflow job env L95-110; render CLI `render-deploy-config.ts:207-216`):

  | Workflow source | Job env | Render input | Rendered |
  | --- | --- | --- | --- |
  | var `SESSION_READ_HYPERDRIVE_ID` | `HYPERDRIVE_ID` (L95) | `hyperdriveId` | Hyperdrive id in place of the placeholder |
  | input `owner_buffered` | `OWNER_BUFFERED` (L103) | `ownerBuffered` | binding plus four owner-buffered vars when `on` |
  | input `owner_buffered_workspaces` | `OWNER_BUFFERED_WORKSPACES` (L104) | `ownerBufferedWorkspaces` | `READ_API_OWNER_BUFFERED_WORKSPACES` |
  | var `SESSION_READ_INGEST_S3_BUCKET` / `_REGION` | `OWNER_BUFFERED_S3_BUCKET` / `_REGION` (L105-106) | `s3Bucket` / `s3Region` | `INGEST_S3_BUCKET` / `INGEST_S3_REGION` |
  | var `SESSION_READ_DIRECTORY_WORKSPACES` | `DIRECTORY_WORKSPACES` (L99) | `directoryWorkspaces` (cross-check only) | not rendered; deployed via `--var READ_API_DIRECTORY_WORKSPACES` (L173) |
  | input `owner_buffered_events` | `OWNER_BUFFERED_EVENTS` (L108) | `ownerBufferedEvents` | `READ_API_OWNER_BUFFERED_EVENTS=on` when `on` |
  | input `buffered_search_gate` | `BUFFERED_SEARCH_GATE` (L110) | `bufferedSearchGate` | `READ_API_BUFFERED_SEARCH_GATE=on` when `on` |

- Invocation:
  - The render step (L152-156) asserts the placeholder in `wrangler.jsonc`, runs
    `bun run scripts/render-deploy-config.ts`, and asserts the Hyperdrive id in `wrangler.deploy.jsonc`.
  - The script prints no value (`render-deploy-config.ts:48`). With events on it logs
    `deploy config: owner-buffered events ON, READ_API_OWNER_BUFFERED_EVENTS=on` (L223).
- Other deploy steps:
  - Input checks (L128-142): API token present, Hyperdrive id 32 hex and not zero, CA id set, origin exactly the
    approved custom domain, and complete-since ISO or `epoch`.
  - Live Hyperdrive safety contract (L143-151).
  - On `owner_buffered=on` only, the Worker secrets `PIPELINE_RAW_S3_ACCESS_KEY_ID` and
    `PIPELINE_RAW_S3_SECRET_ACCESS_KEY` must exist by name (L157-165).
  - Deploy with `--var` for origin, complete-since, directory, brief-titles and timing (L170-176).
  - ClickHouse secret names (L177-184).
  - Smoke test (L185-189): health 200 and an unauthenticated `/v1/events` 401. The smoke test exercises nothing
    owner-buffered or Path R `[missing]`.
- What `owner_buffered_events` does: L33-37 plus L107-108, which pass it to the render as `OWNER_BUFFERED_EVENTS`.
  It is valid only with `owner_buffered=on`. `off` (or no input) renders exactly the owner_buffered render
  without it. "Every later dispatch without the input turns it off again" (L37).
- Rendered config, baseline versus Path R:
  - Baseline (no inputs): the sed render. Only the Hyperdrive placeholder is replaced, with no `services` and no
    owner-buffered or events var (`render-deploy-config.ts:110-116`, `:156-158`; test
    `render-deploy-config.test.ts:22-33`).
  - `owner_buffered=on`: the parsed template is re-serialised as JSON (`render-deploy-config.ts:182-201`). It
    adds `services: [HISTORY_CONTROL_HEAD -> relayhistory-api#SessionReadPrivacyControls]` (pinned, L60-64,
    L189) and the vars `RELAYHISTORY_READ_OWNER_BUFFERED: "true"`, `READ_API_OWNER_BUFFERED_WORKSPACES`,
    `INGEST_S3_BUCKET` and `INGEST_S3_REGION` (L192-195).
  - Path R dispatch: the `owner_buffered=on` render plus exactly `READ_API_OWNER_BUFFERED_EVENTS: "on"` (L198;
    test `render-deploy-config.test.ts:251-259`).
- Worker refusal: `handle()` answers 503 `backend_unavailable` for any `READ_API_OWNER_BUFFERED_EVENTS` value
  other than unset, empty or `"on"` (`index.ts:305-314`).
  - The render never emits any other value.
  - Hazard: a value set outside the render, for example `off` by hand, takes the whole Worker to 503
    (`worker.test.ts:224-228`).

## 3. Render contract for a Path R canary (documentation only; no dispatch is proposed)

- Dispatch inputs:
  - `owner_buffered=on`.
  - `owner_buffered_workspaces` = an explicit comma-separated list of workspace ids, never `*`. Each id must
    match `^[A-Za-z0-9][A-Za-z0-9_.:-]{0,127}$`, with no duplicates (`render-deploy-config.ts:94`, `:102-108`).
    Each id must also be on `SESSION_READ_DIRECTORY_WORKSPACES` unless that list is `*` (L172-176).
  - `owner_buffered_events=on`.
  - `buffered_search_gate=off` or unset.
- Required GitHub environment variables (existing, values redacted): `SESSION_READ_HYPERDRIVE_ID`,
  `SESSION_READ_SUPABASE_CA_ID`, `SESSION_READ_ORIGIN`, `SESSION_READ_COMPLETE_SINCE`,
  `SESSION_READ_DIRECTORY_WORKSPACES`, `SESSION_READ_INGEST_S3_BUCKET` and `SESSION_READ_INGEST_S3_REGION`
  (workflow L7-12, L26-27, L95-106).
  - Bucket and region must match `BUCKET` and `REGION` (`render-deploy-config.ts:95-96`, `:177-180`).
  - The two raw-store Worker secrets must exist (workflow L157-165).
- "Never `*`" is a rule of this packet, not a source refusal. The render accepts
  `owner_buffered_workspaces="*"` whenever the directory allowlist is exactly `*` (L169-171).
- Refusals that apply (`render-deploy-config.ts`):
  - Hyperdrive id not 32 hex or all zero; template without the placeholder (L144-145).
  - `buffered_search_gate` not off/on (L146-147).
  - `owner_buffered_events` not off/on, e.g. `yes`, `true`, `ON`, `1` (L150-151; test L267).
  - A template whose `vars` already declare `READ_API_OWNER_BUFFERED_EVENTS`. This applies in every mode and for
    every events value, including the default and gate-only renders (L135-141, L153; test L272-291).
  - `owner_buffered_events=on` with `owner_buffered` off or unset, with or without the gate (L155; test
    L261-266).
  - `owner_buffered_workspaces` set while `owner_buffered` is off (L157); `owner_buffered` not off/on (L165).
  - `owner_buffered=on` with no workspaces (L167).
  - `*` with a directory allowlist that is not `*` (L170); `*` mixed with ids; malformed or duplicate ids
    (L102-108).
  - Ids that are not on the directory allowlist (L172-176); bad or missing bucket or region (L177-180).
  - A template that declares `services` or `env` (L121-126), or already sets any owner-buffered var or the
    events var (L184, L186).
- Flags that stay off for this packet:
  - `buffered_search_gate` stays off. `READ_API_BUFFERED_SEARCH_GATE` is not rendered, and its off render is
    byte-identical (`render-deploy-config.test.ts:145`).
  - `INGEST_BUFFERED_SEARCH_FEED` (Lambda) stays off and unset.
  - No other search or feed flag changes.
  - Path R does not depend on the gate or the feeder. The gate filters only `GET /v1/events` search hits, and
    the feeder writes ClickHouse. Path R reads buffered stages from S3 through the 043700 RPCs.
- Routes preserved:
  - `GET /v1/events` is unchanged (`app.ts:619-645`; not in the #377 hunks).
  - `GET /v1/sessions/:id/events` without `source` takes the canonical path unchanged (`app.ts:585`; route test
    L738-754 "no source").
  - With `source` and any canonical pointer, it is canonical as before (`app.ts:589`, `:606-613`).
  - The other session routes (list, turns, metadata, activity, timing, catalog) are not in the #377 hunks.
  - The Amp routes (`src/amp/routes.ts`, mounted by `src/alleyoop/routes.ts`) are outside #377's 16 files, and no
    Amp file changes.

## 4. Rollback

- Path R off, owner-buffered reads kept:
  - Re-dispatch with the same `owner_buffered=on` and workspace list and with `owner_buffered_events=off` (or
    absent). This is `[operator: needs Khaliq OK]`.
  - The render is byte-identical to the render without the input for the same other inputs
    (`render-deploy-config.test.ts:230-249`).
  - The pre-#377 code produces the same bytes for those inputs. The only non-events change on that path is the
    predeclared-var refusal (`render-deploy-config.ts:153`), and `wrangler.jsonc` is not changed by #377.
- Everything owner-buffered off: dispatch without inputs. This gives the sed render, with no binding and no var
  (test L22-33, L247).
- Existing runbook alternative: `wrangler rollback` restores the previous Worker version
  (`runbooks/sessions-prod-session-read-worker.md:213-217`). That also reverts the code, not only the flag.
- Database: nothing to apply or revert. #377 has no migration and no file under `supabase/`.
- Clients holding a buffered cursor after rollback:
  - `decodeCursor` refuses the seven-element cursor, so the answer is 400 `bad_request` (invalid cursor), as
    today for any 7-element cursor (`cursor.ts:81-82`; route test `owner-buffered.routes.test.ts:827-843`). The
    client restarts without a cursor.
  - For a buffered-only session, the restart gets the pre-#377 canonical answer, for example 501
    `history_floor_unknown_session` when a history floor is set (route test L756).
- Clients across a backend transition while Path R is on get 409 `cursor_backend_transition` and restart
  (runbook L79).

## 5. Actor and gates

- This packet authorizes nothing. Each of the following is an operator step that needs Khaliq's own OK in his own
  words:
  - Any image publish.
  - Any read Worker deploy dispatch, including the Path R dispatch and the rollback dispatches.
  - Any flag flip, and any change to `owner_buffered_workspaces`, `SESSION_READ_DIRECTORY_WORKSPACES` or the
    S3 bucket and region vars.
  - Any Worker secret or IAM grant.
  - See workflow L36-37, design L211 and runbook L81.
- 043700 (`supabase/migrations/20261105043700_owner_buffered_read.sql`) is NOT verified applied in production.
  G4 is pending in the operator lane (migration-handover-agent). Bulk G2 parity has not run.
- Path R serves nothing until G4 and the owner's G2 parity row are done:
  - Every buffered response passes the per-user gate. That is the caller's own G2 parity row (rule 4 in
    `sessions.owner_buffered_read_scope`) and the control-head authority gate (`render-deploy-config.ts:21-23`).
  - With no stage visible, the route gives the flag-off answer.
  - An RPC error is fenced or unverified, never a 500: 501 before a candidate and 503 at the recheck (route test
    `owner-buffered.routes.test.ts:444`).
- Owner-buffered prerequisites, each a separate approval (runbook L41; workflow L28-30):
  - The S3 GetObject plus kms:Decrypt grant on `buffered-batches/`.
  - The raw-store Worker secrets.
  - relayhistory-cloud exporting the `SessionReadPrivacyControls` entrypoint on `relayhistory-api`.
  - Whether owner-buffered reads are on in production today is not in source `[missing]`. It is an operator-side
    read.
- Image: the deployed image 8bb18be5 excludes #338, #339, #374, #376 and #377 (trusted program fact).
  - Path R lives entirely in the read Worker, which is bundled from the `main` checkout at dispatch (section 2).
    The Worker deploy, not the image, carries #377 into production.
  - Which commit the production Worker currently runs is not in source `[missing]`. It is an operator-side read.

## 6. Interfaces

- Cloud's `get_shared_session` calls `GET /v1/sessions/:id/events` with `source`, as reported by verify-replica
  from Cloud source (not re-checked here). This is therefore the route the MCP tool uses, and the one Path R
  changes.
- Response shape is unchanged: `{ sessionId, events, nextCursor, correlationId }` (`app.ts:600`, `:613`). Two
  things are new:
  - The `X-Relayhistory-Buffered: 1` header on buffered pages.
  - Two error codes on this route: 409 `cursor_backend_transition` and 503 `buffered_fenced`.
  - Cloud's handling of the 409 (restart without a cursor) and of 503 `buffered_fenced` is not in this repo
    `[missing]`.
- Context text-search remains a separate canonical-only surface, unchanged. In this repo, workspace search is
  `GET /v1/events` (not touched; `owner-buffered.ts:49`; design L204).

## 7. Validation

In CI (source; the clone has no installed dependencies, so nothing was executed for this packet):
- `ci.yml` unit-tests runs `bun test` across the stack (`.github/workflows/ci.yml:80-81`). That covers:
  - `owner-buffered-events.test.ts`: flag parse L55-56; parity with the canonical projection L139; refusal of
    `convergence_event` lines L214; the 2^53 clamp L268-275; paging equal to `readAdapterPage` L349; the cursor
    L385.
  - `owner-buffered.routes.test.ts`: L525-534; the Path R suite L692-871 (served, gating, history floor,
    transitions both ways, tombstone, hidden 409 or flag-off 400, other user, fenced, hidden outcomes).
  - `owner-buffered.test.ts`.
  - `buffered-privacy-fixtures.test.ts`: all 26 causal vectors through list, turns, metadata and events (L166).
- `ci.yml` job `session-read-worker` (L156-157) calls `session-read-worker-checks.yml`:
  - typecheck, which includes `check:deploy-config` and the render vitest
    (`packages/session-read-worker/package.json:9-10`);
  - workerd tests including `worker.test.ts:200-230`;
  - a wrangler dry-run bundle (checks L67-73).
- The deploy workflow re-runs the same checks on the dispatched main SHA (L79-83).
- #377 CI: at head 28394e2a the `session-read-worker / worker`, unit and real-PostgreSQL jobs were green, but the
  pg-proof "ingest publication and frontier" PG15/16 jobs failed because pg-proof checks out the PR head SHA and
  the pre-merge branch (based on c858e7fe) did not yet contain `frontier-plan.postgres.test.ts` from #374. The
  branch was forward-merged with `main` 06764c94 (head 92c380a2). #377 was merged at 16:36:14Z (main f196f637)
  while the `unit-tests` check on 92c380a2 was still in progress; the other 24 checks on 92c380a2 were green.
  Terminal `unit-tests` result on 92c380a2: PENDING at packet time (watcher running; updated when terminal).
- Not covered by CI `[missing]`:
  - The events path against real PostgreSQL RPCs, real S3 and a real control head. `pg-proof.yml` has no Path R
    job. The 043700 privacy guard's real-PG test predates #377.
  - Peak memory per read (design L189 says it has not been measured).

Operator-side validation (read-only, operator lane only, each under Khaliq's OK):
- G4: the 043700 sentinel row in the migration ledger and the read-role verifier.
- G2: a current `owner_buffered_parity` row for each canary owner, matching the live control head.
- The production Worker version and its current vars, read by name; confirm `READ_API_OWNER_BUFFERED_EVENTS` is
  unset before any dispatch.
- After any approved dispatch, as the canary owner:
  - a buffered-only session answers with `X-Relayhistory-Buffered: 1`;
  - a canonical session answers without the header;
  - another user's session answers as with the flag off;
  - the log reasons `buffered_events_served`, `buffered_unverified` and `buffered_projection_failed` appear as
    expected.

## 8. Ordered checklist

Each step is an independent gate. Nothing here is executed from this packet.

1. [source-ready] Path R source at #377 head 92c380a2 (delta 4528f207 + 28394e2a), default off; the flag-off route answers and the render
   are byte-identical (section 1).
2. [done] #377 merged to `main` f196f637 (section 1). The deploy workflow runs only from `main` (section 2);
   any dispatch is `[operator: needs Khaliq OK]` and is not requested by this packet.
3. [operator: needs Khaliq OK] G4: verify 043700 is applied in production (migration-handover-agent lane).
   verify-replica (245226715025592320) reports that the G4 executor session already holds direct authorization;
   not verified here, so the remaining gate is G4 READY and the PASS evidence, not an approval via this packet.
4. [operator: needs Khaliq OK] G2: parity rows for each canary owner. Bulk G2 has not run. Same note as step 3
   for the reviewed bulk G2 executor.
5. [operator: needs Khaliq OK] Owner-buffered prerequisites: the S3/KMS grant on `buffered-batches/`, the
   raw-store Worker secrets, and relayhistory-cloud exporting `SessionReadPrivacyControls` (section 5).
6. [operator: needs Khaliq OK] Choose the canary workspace ids: an explicit list, never `*`, each also on
   `SESSION_READ_DIRECTORY_WORKSPACES`.
7. [operator: needs Khaliq OK] Dispatch *deploy session-read-worker* from `main` with `owner_buffered=on`, the
   explicit `owner_buffered_workspaces`, `owner_buffered_events=on` and `buffered_search_gate=off`.
   `INGEST_BUFFERED_SEARCH_FEED` stays off. Record the run's resolved `head_sha` and check it against f196f637
   (section 2). Bundle, config, binding and rollback receipts belong to the read-Worker actor.
8. [operator: needs Khaliq OK] Run the read-only post-deploy checks (section 7).
9. [operator: needs Khaliq OK] Roll back by re-dispatching with `owner_buffered_events=off`, or without inputs
   (section 4).
10. [missing] An automated Path R smoke test in the deploy workflow; Cloud handling of 409 and 503
    `buffered_fenced`; a real-RPC/S3 test of the events path.

## 9. Open questions for the operator lane

1. Is owner-buffered reading (`owner_buffered=on`) already deployed for any workspace, and with which list? Path R
   reuses that render, and every later dispatch without the inputs turns both off.
2. Does Cloud's `get_shared_session` restart cleanly on 409 `cursor_backend_transition`, and how does it surface
   503 `buffered_fenced` to the MCP caller?
3. Should "never `*`" for Path R be enforced in source (a render refusal of `*` with events on) rather than by
   this packet?
4. Should the deploy workflow refuse or warn when `READ_API_OWNER_BUFFERED_EVENTS` is present on the live Worker
   with a value other than `on`? A hand-set `off` makes the whole Worker 503.
5. Is re-reading the stages on every page (bounded at 32 MiB per session, design L189) acceptable for the canary,
   given that peak memory per read is unmeasured?
6. Which canary owners will have G2 parity rows before the dispatch, and who confirms they match the live control
   head at dispatch time?
