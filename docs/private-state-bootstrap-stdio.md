# Private state bootstrap through a fixed Mini recovery broker

This is an operator-only private IPC adapter for the existing reviewed control-store
bootstrap. It does not provision the ingest application. The original native
source/capture, approval, fresh admission, five-create/one-adopt, and retained
snapshot checks remain authoritative. The normal CLI's default local 1P port is
unchanged; this entrypoint injects a restricted Mini port.

Do not invoke create without the final explicit operator exception and an exact
fresh capture on the final clean source. This source change grants no execution
approval. GitHub's unprotected environments are not an approval substitute.

## Fixed capability

The broker reads the normal Mini Wrangler OAuth cache in RAM and sends its bearer
only through SSH stdin to the exact sf-frame worktree. It never writes a profile,
token file, state credential file, or secret log. The remote script uses the same
environment-only native Cloudflare capability and no edge-preview helpers.

The only 1P capability is the normal account
`C33CTNNS4RFQRAIYU4BQ3P3KFI`, vault
`6hmhqxvcbcmhh24yahdflbbkge` (96Bulls), and ONE newly created secure note for the
reviewed generation. Initial creation must contain only a concealed pending
recovery slot. Later get/edit requests must name that exact new ID. There is no
list/search, existing-item lookup, arbitrary command, other account/vault, second
create, or create retry after an uncertain response. Item JSON is projected to
the allowed title/category/vault/concealed slot rather than forwarded wholesale.

Recovery envelopes bind generation, account, exact URL derived from the reviewed
capture's subdomain, and the six native control keys. A decoded Cloudflare API
bearer is rejected before any 1P invocation. The 1P child environment strips all
OP_* overrides, Cloudflare/CF variables, and any other environment value containing
that bearer. The new state-store auth token and encoded control state ARE intended
to enter the concealed recovery slot; the Cloudflare API bearer is not.

Private frames have a 20 MiB byte ceiling before JSON assembly, strict UTF-8,
bounded waits and sequential reply IDs. Python also rejects duplicate JSON keys
and nonfinite numbers. The native input pump observes EOF/malformed input while
CF work is running and aborts its fetch signal. A stop frame aborts CF work while
leaving same-item get/partial-edit replies available. The broker's bounded failure
drain cannot create an item or report successful provisioning.

Public output is a newly constructed allowlist: capture digests/source/generation,
or the completed control URL/new item ID/count and false ingest/preview flags.
Unknown fields, secret-bearing values, additional frames, partial recovery,
truncation, timeout and nonzero child exit refuse. No success is printed until
SSH exits zero. A broken stdin pipe still reaches owned-child wait/cleanup.

## Reviewed commands (Root only, not executed by the author)

Copy the reviewed broker source to Mini without changing its bytes. The remote
checkout must contain this same reviewed commit, be clean, and have its own
installed dependencies. Place only sanitized native capture/approval artifacts
at the existing native paths; the broker does not manufacture approval.

Capture-only, using the final immutable union SHA and exact checkout slug:

```sh
python3 scripts/state-bootstrap-broker.py plan \
  --source-sha FINAL_UNION_SHA \
  --checkout upload-release-union-20261104
```

After independent review of the exact native capture and explicit approval bound
to that source/generation/plan, create-only uses the SAME reviewed provenance
bytes locally and remotely:

```sh
python3 scripts/state-bootstrap-broker.py create \
  --source-sha FINAL_UNION_SHA \
  --checkout upload-release-union-20261104 \
  --reviewed-provenance /path/to/sanitized/provenance.json \
  --reviewed-provenance-sha EXACT_SHA256
```

Neither command has been invoked against live services by this implementation.
An offline proof cannot distinguish actual normal-account 1P authorization or
live SSH/OAuth operation; Root must establish those separately. The earlier
capture on another source does not validate this adapter's final source.

## Failure boundary

CF resource creation and 1P are separate services, not an atomic transaction.
The dedicated note is created/read/edit verified BEFORE CF provisioning and
complete recovery is verified BEFORE remote state hoist. A handled stop can save
partial state to that same note. A hard transport loss may prevent that save and
leave uncertain control resources or a pending recovery note. Do not auto-retry,
upgrade, delete, resume, or infer rollback. Preserve the new item locator and
review the actual control inventory/recovery state with the operator.

Only the SSH subprocess created by this broker may be terminated after its
bounded close/wait fails. No unrelated process, Worker or service is stopped.

## Offline evidence

Run on sf-frame, with Node24.21/Bun1.4.2. No production profile or CLI entrypoint
is invoked:

```sh
python3 -B -m unittest scripts/test_state_bootstrap_broker.py
bun test ./stacks/cloudflare/__tests__/private-state-bootstrap-stdio.test.ts \
  ./stacks/cloudflare/__tests__/private-state-bootstrap.test.ts
bun run typecheck
```

The Python suite executes the real private Bun transport and native
`prepareOnePasswordSink` against an in-memory fake 1P runner with network
forbidden. It also exercises binary subprocess EOF, oversized/truncated output,
initial exit, nonzero result exit and same-item partial recovery after stop.
Seven independent guard reversions each fail the corresponding control: child
exit, public allowlist, pending-only creation, uncertain-create retry exclusion,
decoded bearer exclusion, exact capture URL, and partial-only stop edits.
