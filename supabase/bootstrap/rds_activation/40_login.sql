-- rds-activation/v1 fragment 40: arm LOGIN on every LOGIN role of the spec (hoop,
-- sessions_pipeline_runtime and the service logins), as the executor. The executor
-- has ADMIN on each of them through its creator edge.
--
-- Each password is a SCRAM-SHA-256 verifier computed by the driver, so the plaintext
-- never reaches the server. The verifiers are bound once with set_config(..., true) as
-- a bind parameter, used through EXECUTE (so no top-level statement text holds them),
-- and then cleared. Then the executor grants itself SET (not INHERIT) on hoop. That
-- edge carries the provisioner's SET ROLE chain for the ownership handoff in 50, and
-- it disappears with the executor in 60.
DO $rds_activation_40$
DECLARE
  v_verifiers jsonb := current_setting('hoopdb_activation.verifiers')::jsonb;
  v_name text;
  v_verifier text;
BEGIN
  IF current_user::text <> 'hoopdb_activation_executor' THEN
    RAISE EXCEPTION USING ERRCODE = '42501', MESSAGE = 'rds-activation fragment 40 must run as hoopdb_activation_executor';
  END IF;
  FOR v_name, v_verifier IN SELECT key, value FROM jsonb_each_text(v_verifiers) ORDER BY key LOOP
    EXECUTE format('ALTER ROLE %I LOGIN PASSWORD %L', v_name, v_verifier);
  END LOOP;
  PERFORM set_config('hoopdb_activation.verifiers', '{}', true);
  v_verifiers := NULL;
  IF EXISTS (SELECT 1 FROM jsonb_array_elements(current_setting('hoopdb_activation.role_spec')::jsonb) e
               JOIN pg_catalog.pg_roles r ON r.rolname = e->>'name'
              WHERE r.rolcanlogin IS DISTINCT FROM (e->>'login')::boolean) THEN
    RAISE EXCEPTION USING ERRCODE = '55000', MESSAGE = 'rds-activation: LOGIN does not match the role spec';
  END IF;
  GRANT hoop TO hoopdb_activation_executor WITH INHERIT FALSE, SET TRUE;
END
$rds_activation_40$;
