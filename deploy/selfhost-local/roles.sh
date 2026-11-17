#!/bin/sh
# The operator step after migrate (docs/selfhost-composition.md section 4), run
# by the `roles` one-shot in compose.yaml: make the service logins usable and
# give each its password. Passwords come from the environment and reach psql as
# variables on stdin (quoted by psql), never on a command line, in a file or in
# the output. Safe to rerun: every statement is idempotent.
set -eu
psql() { command psql -v ON_ERROR_STOP=1 -X -q "$@"; }
setpw() { # role, environment variable holding its password
  printf '%s\n' "\\set pw \`printenv $2\`" "ALTER ROLE $1 PASSWORD :'pw';" | psql
}
psql -f /bootstrap/session_ingest_role.sql
setpw bulls96_session_ingest RH_INGEST_PASSWORD
psql -f /bootstrap/session_ingest_role_verify.sql
psql -f /bootstrap/session_read_api_role.sql
setpw bulls96_read_api RH_READ_PASSWORD
psql -f /bootstrap/session_read_api_role_verify.sql
setpw search_consumer_runtime RH_CONSUMER_PASSWORD
# The read role's search grants ("Postgres search prerequisites", read-api design).
psql <<'SQL'
GRANT USAGE ON SCHEMA search_index TO bulls96_read_api;
GRANT SELECT ON ALL TABLES IN SCHEMA search_index TO bulls96_read_api;
SQL
