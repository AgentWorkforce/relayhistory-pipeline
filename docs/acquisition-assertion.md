# Acquisition assertion verifier

`src/session-ingest/acquisition-assertion.ts` implements the assertion and grant
encoding from `factory-acquisition-owner-design.md` §3. The acquisition installer
can use this module before calling its restricted SQL functions.

The operations envelope is `{payload, signature}`. `payload` contains exactly
the operation's assertion fields plus `evidenceSha256` and `requestSha256`.
Every assertion field is a string; an absent optional UUID or expiry is `""`.
Revisions and epochs use canonical decimal strings up to PostgreSQL's signed
64-bit maximum, preserving values above JavaScript's safe-integer limit.
`signature` is the unpadded base64url encoding of a 64-byte Ed25519 signature.
The route supplies the operation kind: `install`, `revoke`, or `rebind`.

Call `verifyAcquisitionAssertion(kind, envelope, issuers, authorizeScope)` with
the authenticated service's server-pinned issuer registry and a policy derived
from the authenticated control-plane scope. Registry entries contain an
Ed25519 public `KeyObject` and the issuer class (`cloud` or `native`). The
scope policy must explicitly return `true`. The verifier checks the issuer
class, exact canonical assertion bytes, signature, both envelope digests and
scope policy. It returns a detached, frozen `{kind, payload}` whose payload is
ready for the matching SQL function. Failures expose only the static typed
codes `invalid_request` or `not_authorized`.

`acquisitionPayload` validates unsigned fields, and
`acquisitionAssertionBytes` builds the domain-separated assertion bytes.
`acquisitionAssertionDigests` computes the completed evidence and request
digests without including either digest in the signed assertion.
`acquisitionGrantDigest` hashes the session-scoped grant secret. The transport
and intent store own authentication, durable retries, encryption and delivery;
this module has no database, network or configuration side effects.

The focused tests include the published grant and RFC 8032 signature vectors,
recomputed public hashes on tampered assertions, issuer/scope refusal,
operation-class separation, canonical scalar and Unicode rejection, and
precision-preserving bigint boundaries:

```sh
bun test src/session-ingest/__tests__/acquisition-assertion.test.ts
```
