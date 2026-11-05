# Dormant full-History summary adapter

GET /v1/canonical/summaries/:id?workspace=...&source=...&kind=short|long
requires CANONICAL_READ_ENABLED=true, CANONICAL_READ_MODE=pages and
CANONICAL_HISTORY_READ_ENABLED=true. It is independent of the v1 analysis cache.

The SAME caller bearer and fixed four RPC arguments reach the narrow JWT-scoped
session_directory_history_summary before any object/key IO. The pure reader
validates identity, required/ready source, full authority, fixed storage and pointer;
it verifies real v2 cipher/hash/AAD, decrypts through keyById only, and rereads the
entire original claim. A final change refuses plaintext. Model-suggested long
state is not qualified Referee state; only the current claimstate is returned.

The public response is version1/identity/kind/ready|empty/summary/model/builtAt/
source(epoch string, fingerprint)/qualifiedstate/correlationId. It exposes no
bucket, object key, key ID or key. Missing source/pointer/key is unavailable, not
invented empty. There is no raw, old brief, SQL, CH or Neon fallback.

The stock entrypoint injects NO keyring and remains unable to serve this new
read even if flags are set. createSessionReadWorker({summaryKeyring}) is the
explicit reviewed backend composition seam. The readonly port is:
keyById({orgId,workspaceId},keyId,signal):Promise<CryptoKey>.
Remote implementations must honor cancellation. There is no env/currentKey/
owner/key-derivation fallback and no production provisioning action in this PR.
A separately reviewed actual key binding/broker and normal credential path is
still required. Existing configured S3 storage must exactly match the claim.

The committed corpus contains synthetic ORIGINAL09cc producer ciphers, not
new-reader-generated data. Both short/long interop in actual workerd, preserve
large integer epochs and Unicode/lone-surrogates, and retain qualified active
state when the encrypted model suggests done. The portable generator requires
a clean09cc producer and a NEW output directory; it never calls real services.

Existing page-mode replay has NO raw fallback. current:false refuses before
objects; current artifacts recheck replay publication and do not separately
call the raw pointer RPC. Native620 must deny every managed-required scope in
unqualified raw pointer/capture paths, even when ready, so old readers cannot
resurrect unmasked winners.

Remaining: immutable620 SQL/JWT-role/profile/grant composition and public-role
proofs, Cloud/UI consuming the new truthful RHC summary contract, actual key-port
wiring, erasure/currentness integration, independent reviews/current-head CI.
Initial upload299, source79 and existing v1 analysis/title behavior are unchanged.
No live schema/role/grant/secret/configuration/deploy/flag was changed.
