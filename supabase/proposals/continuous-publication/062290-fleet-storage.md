# Proposed fleet scheduler storage for 062290

The up and down components live outside migration discovery. The release owner must compose them with the separately reviewed recovery component and the profile's normal ledger, checksum and latest-step guards. Schema installation leaves the scheduler dormant.

The access script requires a separate owner transaction for each approved scope. Supply the dedicated direct database login through publication_scheduler.login, the exact fleet digest through publication_scheduler.fleet and the scheduler's scope digest through publication_scheduler.scope. The latter binds tenant, origin, target, generation, admission, plan and frontier as defined by publicationFleetScopeKey. The owner must independently verify the roster and digests. The fleet login must have no role memberships, schema ownership or privileged attributes, and must connect directly. A pool that switches roles cannot use this policy.

Before activation, the database owner must verify the effective login and schema grants, protect role membership and ownership from drift, and approve the dedicated bindings. The access script grants only scheduler state privileges. Scoped source reads and publication RPC execution need separate review. Neither applying this proposal nor binding a login starts a worker.

Removing a scope binding hides its checkpoint and holds. Retain all durable rows during revocation. The down component requires an outer transaction and refuses rollback while any binding, reservation history, checkpoint or retry/hold remains. Leave the component installed during rollback of a running fleet. A separately authorized retirement and reconciliation procedure would be needed before discarding durable state.
