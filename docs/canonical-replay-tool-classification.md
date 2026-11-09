# Codex tool-use classification in canonical replay

A Codex tool call can carry two delivered facts: a `session_event` with
`role=assistant` and `kind=tool_use`, and a structured `tool_call`. The generic
delivery projection uses the role as its display type. That can make a scrubbed
tool input appear as an assistant chat message.

Canonical read/index projection restores `type=tool_use` and the recorded tool
name only when the verified record union supplies an unambiguous tool-call
witness with the same origin, physical session and nonempty message ID. An equal
timestamp, a redaction placeholder or similar content is insufficient. Other
sources, normal assistant messages, absent/ambiguous witnesses and mismatched
identities keep their existing classification.

This does not remove events, recover redacted text or rewrite objects. Event
identities, counts, ordering and content remain intact. The transcript renderer
already treats `tool_use` as activity rather than a conversational speaker. A
normal assistant message containing a scrubbed placeholder remains an assistant
message.

The correction applies to full canonical object replay and future canonical
index writes. Existing index-only rows need the normal qualified re-projection
path before their classification changes. The generic SQL-parity delivery
projection, schema generation, grants, authentication, erasure checks and
source-generation cursor rules are unchanged.
