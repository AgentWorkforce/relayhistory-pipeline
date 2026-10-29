# Runbook: local stack

```
bin/96bulls up local            # idempotent; reuses .96bulls/local secrets, re-mints the courier token
bin/96bulls status local        # docker ps for the stack
bin/96bulls smoke local         # injects one fresh session, follows it, scans telemetry; exit 0 = pass
bin/96bulls seed local --fresh  # rebuild the Concourse fixtures (then restart the courier or run up)
bin/96bulls token local         # a 1h JWT for manual calls
bin/96bulls down local          # stop, destroy, wait for removal; keeps secrets and seed
bin/96bulls down local --purge  # also forgets secrets, seed, volumes and Alchemy state
bin/96bulls plan local          # alchemy plan (dry run)
```

Logs: `docker logs 96b-<component>`. Spans:
`docker run --rm -v 96b-telemetry:/t alpine cat /t/spans.jsonl`.
Database: `psql postgres://hoop:<password from .96bulls/local/inputs.json>@127.0.0.1:15432/hoopdb`.

If `down` reports containers stuck in "removal in progress", wait; the Docker
daemon finishes eventually (seen: 25 minutes). Do not restart Docker on a shared
machine without checking `docker info --format '{{.LiveRestoreEnabled}}'` and
who else is running on it.

A second stack on the same machine: `bin/96bulls up local-<name>` suffixes every
container and volume name (ports are shared, so only one may be up at a time).
