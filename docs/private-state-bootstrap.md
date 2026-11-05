# Create-only 96Bulls control-state prerequisite

This operator adapter creates the empty named Alchemy control state store. It does not plan or apply the ingest application. It is a reviewed source path, not evidence that live provisioning succeeded. The future GitHub route remains blocked by unavailable required-reviewer environment protection.

The installed official Alchemy/Cloudflare packages are pinned to 2.0.0-beta.84. The adapter composes their exported StateStore Api, TokenValue/AuthToken, RandomProvider and inMemoryState, then retains the actual Stack.plan snapshot and applies that same object with updateStateStore:false. It does not call the convenience bootstrap/upgrade/login helpers.

## Exact footprint

The accepted plan contains five creates and one adoption:

| Logical resource                                                         | Action |
| ------------------------------------------------------------------------ | ------ |
| StateStoreAuthTokenValue (Alchemy.Random)                                | create |
| Api (Cloudflare.Worker), physical alchemy-state-store-96bulls            | create |
| StateStoreSecrets (Cloudflare.SecretsStore), exact existing shared store | adopt  |
| AlchemyStateStoreToken (Cloudflare.SecretsStore.Secret)                  | create |
| StateStoreEncryptionKeyValue (Alchemy.Random, 32 bytes)                  | create |
| StateStoreEncryptionKey (Cloudflare.SecretsStore.Secret)                 | create |

Api binds exactly AlchemyStateStoreToken, StateStoreEncryptionKey and the Store Durable Object export. These three binding creates are counted separately by Alchemy. The DO is part of this official Worker definition, not an extra independently managed resource.

The control Worker uses its authenticated workers.dev URL and the official previews-enabled setting. This is a bearer-protected public control API, not the private ingest Worker. No ingest Worker, application binding, schema, IAM, Hyperdrive, app flag, edge-preview credential broker or other app resource is applied. No existing Worker upgrade or state store creation is allowed; the one pinned existing Secrets Store is adopted without changing it.

## Preconditions and capture

Root must inspect the actual account and complete namespace/secret metadata inventory and retain exclusive ownership of this control-plane name during creation. The adapter repeats those reads and requires exact Worker settings HTTP 404/code 10007, the expected shared store, absent exact two secret names and no matching DO namespace. An inventory failure or incomplete pagination refuses.

The Cloudflare API has an upsert Worker PUT, not an atomic create-if-absent primitive. The immediate pre-PUT absence check narrows the race but cannot remove it. Concurrent external provisioning is an explicit operational refusal condition.

Use an isolated sf-frame checkout at the reviewed immutable source SHA, with a real frozen-lockfile install. Inject CLOUDFLARE_ACCOUNT_ID and CLOUDFLARE_API_TOKEN only through Root's approved in-memory credential path. Do not put credentials in shell arguments, files, profiles or capture output. The nonsecret ignored input file .96bulls/state-bootstrap/inputs.json contains only accountId, secretsStoreId and onePasswordVaultId (full IDs, never names); later it also carries the explicit approval record.

Run the following **capture only** command after substituting the independently reviewed immutable SHA:

```sh
CI=true bun scripts/private-state-bootstrap.ts plan --expected-sha <reviewed-40-hex-SHA> --capture
```

The command permits no Cloudflare writes. It captures the original Alchemy compact and detailed plans, sanitized inputs and provenance to docs/plans/cf-state-bootstrap-96bulls.{plan.txt,plan.detailed.txt,inputs.sanitized.json,provenance.json}. Its safe JSON output contains the generation and exact capture binding digests. It binds source SHA/clean files/host/tool versions, target account/store/name/version/vault, both plan hashes and metadata admission. Unknown source changes, input fields, resource types/actions or credentials in plans refuse. ALCHEMY_HOME is redirected under the ignored runtime directory; profile/credential APIs are replaced by refusing memory ports.

## Review and execution contract

Root must review the capture and obtain the specific bootstrap approval before any create command. Add approval to the ignored input JSON: decision approved, by Khaliq Gant, a real date within 30 days, the verbatim authorization quote, execution ssh-sf-frame-state-bootstrap-only, and capture equal to the emitted provenanceSha256/planSha256/inputsSha256/sourceCommit object. Approval does not change the semantic inputs digest.

Only after that review, Root can run:

```sh
CI=true bun scripts/private-state-bootstrap.ts create --expected-sha <same-reviewed-SHA> --reviewed-provenance-sha <reviewed-provenance-file-SHA256>
```

This command is documented for Root; the author has not executed it against live services. It revalidates the exact local capture before touching 1Password, reserves a dedicated Secure Note using JSON stdin, and verifies that exact item/vault can be read and edited. Then it obtains a fresh metadata admission and native plan, compares the reviewed generation, rechecks source and applies the retained snapshot. It refuses changed/existing resources and unplanned writes. A second Worker upload, foreign account, new shared store, edge preview, delete or app resource write is denied.

## Credentials, recovery and state hoist

The sink sends the new URL, auth token and encoded control-resource state (including recovery material) only to the dedicated 1Password item's concealed bootstrap_recovery field via JSON stdin. Child stdout/stderr are captured in RAM. Secrets are never CLI arguments, plan files, receipts or printed errors. The sink reads the item back and requires exact serialized-envelope equality and the captured vault/item identity. Root's approved op authentication must already be available; the adapter never signs in or changes the credential setup.

Only a confirmed sink receipt permits opening the new remote state service. It requires official version 7 and empty remote stacks, then hoists exactly the six CloudflareStateStore control records and reads each one back with the official encoding. State HTTP capability is limited to version/list and those six exact control keys. No ingest state is written.

Any apply or hoist failure after token generation attempts a second concealed partial recovery checkpoint. There is no automatic retry of the overall create, upgrade, deletion or rollback. A partial create must be reconciled by Root using the saved recovery item and exact live metadata; rerunning the create-only command will refuse the existing Worker/secrets. If 1Password itself becomes unavailable after provisioning starts, recovery persistence cannot be guaranteed; the adapter refuses and reports a fixed recovery-sink failure without printing secrets. This remains an operational risk, not a claimed atomic transaction.

## Evidence and limits

Offline tests exercise the real installed official Stack.plan and original plan renderer against synthetic bounded Cloudflare metadata, strict capabilities, approval/source/plan changes, 1Password JSON-stdin/readback ports and control-state hoist ports. No live Cloudflare create, 1Password item mutation, DO readiness or real state-hoist success is established by those tests. Actual operator metadata/capture and independent review remain prerequisites. Credentials in memory are not cryptographically erased from the process heap.

The normal ingest application keeps its existing guard, capture and approval contract. Bootstrapping this prerequisite does not authorize the subsequent app apply or the session cutover.
