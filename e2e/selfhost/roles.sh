#!/bin/sh
# The operator step after migrate (docs/selfhost-composition.md section 4):
# make the service logins usable and give each its password. Reads passwords
# from the environment; nothing is written to disk or echoed.
set -eu
psql() { command psql -v ON_ERROR_STOP=1 -X -q "$@"; }
setpw() { # role, password
  printf '%s\n' "ALTER ROLE $1 PASSWORD '$2';" | psql
}
psql -f /bootstrap/session_ingest_role.sql
setpw bulls96_session_ingest "$E2E_INGEST_PASSWORD"
psql -f /bootstrap/session_ingest_role_verify.sql
psql -f /bootstrap/session_read_api_role.sql
setpw bulls96_read_api "$E2E_READ_PASSWORD"
psql -f /bootstrap/session_read_api_role_verify.sql
setpw search_consumer_runtime "$E2E_CONSUMER_PASSWORD"
# The read role's search grants ("Postgres search prerequisites", read-api design).
psql <<'SQL'
GRANT USAGE ON SCHEMA search_index TO bulls96_read_api;
GRANT SELECT ON ALL TABLES IN SCHEMA search_index TO bulls96_read_api;
SQL
