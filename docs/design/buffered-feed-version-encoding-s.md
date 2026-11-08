
## 8. Residuals recorded by the verify peer (codex-relayhistory-verify-replica, relay message 245248035358314496)

These are open. Nothing in sections 1-7 presents them as closed.

- **R1. L is evidence, not an invariant.** Pinning the observed positive canonical minimum describes current rows. It
  is not a lower bound for every future canonical writer, import, reconcile pass or clock. The S PR must document the
  complete canonical writer rule (every writer stamps raw epoch milliseconds from a sane clock, never a value below
  `L_pinned`) and the consequence of a future below-L canonical value: that row loses to any buffered row of the same
  identity with a higher S value, so the publication is no longer guaranteed to outrank. A review rule on
  `search-index.ts` and `reconcile.ts` is the only enforcement while the ClickHouse schema is frozen.
- **R2. A fixed margin does not bound late acknowledgment.** Holding the cursor at `now() - 120 s - margin` does not
  cover a stage whose `created_at` is older than the cursor and whose acknowledgment arrives arbitrarily late, unless a
  maximum acknowledgment delay is proved. A keep-up skip is not coverage. The S PR must either ship a durable revisit or
  watermark strategy for late acknowledgments, or state the source liveness gap explicitly. It must not claim
  completion by margin.
- **R3. A refusal is not coverage.** A run that recorded or still carries an unresolved refusal stays `ok: false`, and
  the refusal stays outstanding, until the stage is actually resolved (publication, erasure, replacement). Advancing the
  cursor after the ledger write does not close the refusal.
- **R4. Mutual refusal availability.** Both same-second siblings can be refused (section 2). This is fail-closed, but
  the availability trade-off needs the schema owner's explicit agreement before code.
- No ClickHouse, SQL, apply or flag action follows from this document. #382 stays a diagnosis PR. No Path R dependency.
