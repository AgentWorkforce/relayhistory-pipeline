# Recorded agent name on self-host session lists

The self-host read API accepts `includeAgentName=recorded` on
`GET /v1/sessions`. This is an explicit response extension: the parameter must
appear exactly once with that exact value. The self-host read surface must be
configured, and the existing token, workspace allowlist, tenant and cursor
checks still apply. It grants no new authority.

Without the parameter, response keys and mapping remain unchanged for the
published strict `@relayhistory/cloud-client` 0.2.0 contract. That published
contract does not accept `agentName`, even though newer SDK source declares it.
An older default client must not receive this extension implicitly. Consumers
requesting it must validate the explicit optional top-level string themselves;
do not relax unknown-field validation on default SDK responses or add a nested
`catalog.agentName` field.

The opted-in summary includes `agentName` only when the projected catalog has
exactly one valid recorded name. The source is session-kind `agent_name`
metadata projected into `session_catalog_agents`, not user/history actors,
titles, tools or model labels. The recorded name is not a claim about a live or
primary agent. Historical multi-name sets, missing/unprojected catalogs and
invalid metadata omit the field. Buffered sessions also omit it: their list
metadata does not establish a recorded name.

Plain and query lists share this mapping. The existing list query already
selects the recorded names; this extension adds no catalog requests, object
reads, SQL functions, role grants or canonical-writer changes. A cursor still
identifies the same ordered rows when the display extension is toggled.

The self-host Control proxy must validate and forward the same parameter on
its workspace-scoped list route before the dashboard requests it. The dashboard
must request it on both its Control and direct-reader list paths, render the
explicit top-level name as plain text beside harness/model, and retain the old
subtitle when it is absent. An older Control may reject the new parameter;
adopt the validated proxy contract first, rather than retrying arbitrary 400s
or inventing a fallback name.

The dashboard's operator setting `DASHBOARD_SESSION_AGENT_NAMES_ENABLED`
(`sessionAgentNames === true` in runtime configuration) remains false by
default. Enable it only after the matching Control and reader contract is
qualified. A frontend deployment alone must not send the parameter to older
Control installations.
