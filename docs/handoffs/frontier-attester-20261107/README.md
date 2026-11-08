These are the original synthetic contract fixtures for the attester v3 design
(`51d358b293790030dadcd5839358ac18109d570b2b6d05e97433b21efe5ec398`),
published unchanged for the 050000 contract review and codec conformance work.

- [Contract vectors](publication-frontier-contract-vectors-20261107.json):
  `fd3f8908524b708b24e1511dc88795eb365304f7e3d74cb6267f4491a3231abe`
- [Bigint known answers](publication-frontier-component-bigint-known-answers-20261107.json):
  `867f6b3567ad976c1a01f4fa7940e6b0ce89d4ae7d5454d45a484d1486eb3e38`

The files retain their original design and implementation-status labels as
historical provenance. Their synthetic counts, receipts, trust bindings and
expected SQL outcomes are contract data, not production coverage evidence or
execution authority. The bigint examples test decimal-string byte preservation,
including values above JavaScript's safe integer range; they cannot qualify a
frontier.

Validation for this publication: both complete file hashes match the originals;
both files parse as JSON; all four positive canonical byte strings match their
inputs and SHA256 known answers. This does not claim execution of the future
attester RPC, SQL cases or an independent coverage verifier.

Verify the original artifact bytes from this directory with:

```sh
shasum -a 256 -c SHA256SUMS
```
