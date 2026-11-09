# Public idempotency selectors in tool input

Ruleset 7 preserves the exact case-insensitive `idempotency_key` identifier in unquoted assignments. This field selects a replay receipt and is not an authentication credential. The broader assignment rule had redacted its benign value; a second scrub of the escaped JSON literal could then replace the entire tool-input string with a placeholder.

Only this exact identifier is exempt. Prefixed secret assignments, quoted secret-key context, provider token shapes, Bearer credentials, JWTs, private keys, and the escaped-literal fallback retain their protections. A recognizable credential stored under this field still gets scrubbed.

Version 6 and its digest remain immutable, with a frozen source fixture. Newly sealed objects use version 7 and its new manifest digest; existing stored objects are not rewritten. Tool-use display classification is a separate projection concern.
