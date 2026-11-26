# Codex tool-use classification in canonical replay

A delivered `session_event` carries its speaker in `payload.role` and may
identify its content in `payload.kind`. The delivery stream projection uses
explicit `tool_use`, `tool_result` and `thinking` kinds as the display type,
preserving the speaker separately in `actor_role`. Other session events keep
the role-based display type, including ordinary assistant messages containing
tool-shaped or redacted text.

For Codex tool-use events, the canonical read/index projection can also supply
the recorded tool name from a unique structured `tool_call` in the verified
record union with the same origin, physical session and nonempty message ID.
Unpaired or ambiguous captures keep the explicit event type without a tool
name. The classification preserves event identities, counts, ordering and content.

This applies to newly projected delivery streams and canonical object replay.
Previously materialized rows need a separately qualified reprojection to change.
