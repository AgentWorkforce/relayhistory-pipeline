-- PROPOSED managed LOGIN profile. Privileged prerequisite, no credentials,
-- role creation, grants, owner transfer or model call. Final Root review held.
-- psql -X -v ON_ERROR_STOP=1 -f ... AFTER bare bootstrap and630 migration.
-- Password/managed secret provisioning is separate; never put it in this file.
\set ON_ERROR_STOP 1
BEGIN;
-- Refuse unsafe attrs/membership/data/function rights BEFORE enabling LOGIN.
\ir session_history_model_verify.sql
ALTER ROLE bulls96_session_model LOGIN NOINHERIT;
ALTER ROLE bulls96_session_model SET statement_timeout='5s';
ALTER ROLE bulls96_session_model SET lock_timeout='2s';
ALTER ROLE bulls96_session_model SET idle_in_transaction_session_timeout='10s';
\ir session_history_model_verify.sql
COMMIT;
