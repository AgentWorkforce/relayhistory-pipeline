# Private ingest operator plan/apply adapter

This is a source-only SSH operator route for Root. It does not authorize execution. The final manual application-deploy exception, exact reviewed capture, state-control prerequisite, storage/seed gates and operator decision remain required. The future protected GitHub route is separately held; this adapter does not bypass its environment protection or dispatch it.

The adapter uses the original exported sessionIngestStage, original assertCloudflareStage, approval binding and four-resource plan contract. It creates a thin Alchemy stack with the same name and resource declaration and an explicit authenticated makeHttpStateStore port. It does not instantiate Cloudflare.state, bootstrap, upgrade, credential refresh or edge-preview helpers. Profile and credential APIs refuse all I/O. Approved account/API token and state URL/token are passed only in RAM/environment; no plaintext Alchemy profile is created. ALCHEMY_HOME points to the ignored run directory for nonsecret lock metadata.

## Preconditions

Root must first create and verify the named control store through its separately reviewed prerequisite. This app adapter requires Cloudflare StateStore version7 and an exact created/updated CloudflareStateStore/Api record naming alchemy-state-store-96bulls in the selected account. It hashes that encoded record into capture authority. The account's live workers.dev subdomain must exactly match the supplied state URL. Missing state, wrong version, wrong token, changed control authority or another account refuses; it never repairs them.

Use a clean immutable reviewed checkout on sf-frame with a real frozen-lockfile install. Inject CLOUDFLARE_ACCOUNT_ID, CLOUDFLARE_API_TOKEN, PRIVATE_INGEST_STATE_URL and PRIVATE_INGEST_STATE_TOKEN through Root's approved memory-only broker. Do not put credentials in command arguments, a plan, a profile or files. The ignored .96bulls/cf-sessions-prod/cloudflare-inputs.json is the existing nonsecret sessionIngest configuration and, for apply, its explicit approval.

## Capture only

With CI=true, run:

```sh
bun scripts/private-ingest-operator.ts plan --expected-sha <reviewed40hexSHA> --inputs-sha256 <semanticInputsSHA256>
```

The inputs digest is the existing deploy-guard inputsDigest over the JSON, excluding approval. The source check accepts only the existing four capture files as checkout differences. It binds source SHA/host/tool versions, account/state URL/control version+hash, exact semantic inputs, original compact+detailed plan hashes and native binding inventory hash.

The real native snapshot must contain exactly the existing four resource IDs/types: two named Queues, the private session-ingest Worker, and its fanout Consumer. No create/update mixture, adoption, replacement, deletion, orphan resource or extra action is admitted by the native and existing plan contracts. Native Worker props must preserve the original private invariant, exact main entrypoint and no secret literals.

Installed Alchemy renders Worker bindings as additional [session-ingest/name] action rows. The original stock contract mistakes these for extra resources. This adapter validates every native binding's exact ID/type/target/action against the authoritative vars, configured Hyperdrive, named fanout resource and three SecretsStore descriptors; the standard off-replica mode has13 bindings. It checks both renderer inventories and their binding-change summary, hashes that native inventory, and removes only these validated binding rows from the view passed to the unchanged four-resource contract. Full original compact/detailed plans are retained and digested. Unknown/duplicate/missing/mismatched bindings refuse. Binding removals are deliberately refused by this narrow route until separately reviewed. Stock Alchemy CLI deployment remains unusable with these full binding-row captures; use only this reviewed adapter.

Capture writes docs/plans/cf-sessions-prod.{plan.txt,plan.detailed.txt,inputs.sanitized.json,provenance.json} and emits safe generation/digests. It permits metadata/state GETs only. It neither mutates Cloudflare resources nor writes remote state during planning.

## Reviewed application apply

After Root reviews the exact capture and obtains the final explicit manual exception, the ignored approval record must retain the original decision/by/date/quote/capture binding and additionally set execution to ssh-sf-frame-retained-plan. A real date within30days, Khaliq Gant, verbatim authorization quote and exact provenance/plan/inputs/source binding are required.

```sh
CI=true bun scripts/private-ingest-operator.ts deploy --expected-sha <sameReviewedSHA> --inputs-sha256 <sameSemanticInputsSHA256> --provenance-sha256 <reviewedProvenanceFileSHA256>
```

The program obtains a fresh native plan, validates it, compares both full plans and all authority, rechecks the clean source and current control record, then passes that same retained snapshot object to Stack.apply with updateStateStore:false. The CLI has no --yes, destroy, alternate stage, alternate entrypoint or profile option. The programmatic retained-deploy entry also requires the bound approval evidence before any transport.

The network capability permits only the selected private Worker upload/settings and workers.dev-disable operations, the two exact named queue creates and observed owned queue IDs, and that fanout queue's Consumer. State writes are limited to the four app resource keys/output under96bulls-cloudflare/cf-sessions-prod. Control-state writes, queue/resource deletion, arbitrary domains/routes, public Worker URL enablement, SecretsStore mutation, foreign accounts and edge-preview paths refuse. Existing foreign-domain cleanup is not silently authorized; an unexpected attachment causes refusal and needs a new reviewed action. HTTP requests have explicit deadlines; metadata reads are bounded.

Any partial app apply must be reconciled through a new actual plan/capture and approval. The authoritative state service records the normal engine progress; this adapter does not roll back or retry the whole operation automatically. A source/generation/input/control-state change requires a new capture. This command does not flip DELIVERY_FORWARD or any read/write routing flag.

## Evidence and remaining operational gates

Offline tests use the actual installed Alchemy planner, original four-resource declaration and original renderer, synthetic HTTP state/Hyperdrive/Queue metadata, and refusing fallback transports. They prove the exact4-resource/13-binding plan, malformed actual-snapshot refusal, state/profile/upgrade boundaries, and exact retained-snapshot identity at an explicitly stubbed apply boundary. No actual cloud apply, Worker bundle upload/propagation, queue consumer delivery or private service-binding request has been proven by those tests. Root must independently verify live permissions, plan, actual post-apply private surface and smoke contracts.

Buffered Queue/declaration support is described in private-ingest-buffered-queues.md. It requires the separate faithful buffered Worker receipt/outbox and publication-state components before deployment. Do not treat a paused queue or inline0 alone as a successful acceptance path.
