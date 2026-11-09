-- rds-activation/v1 fragment 10: function defaults of the two non-HoopDb roles that
-- can CREATE in sessions on RDS. Runs as the provisioning login, before any role exists.
--
-- On a real RDS instance rds_superuser has effective CREATE on schemas it does not own
-- (#389; the emulation grants it directly), and the provisioner inherits that. The
-- migrator's rds function-boundary gate therefore counts both as sessions creators, and
-- each needs a global function default of exactly {<self>:EXECUTE}. Only a role that
-- inherits rds_superuser can set rds_superuser's default, and hoop deliberately does
-- not, so the provisioner sets both here.
--
-- Scope: pg_default_acl is per database. These two rows exist only in hoopdb, a
-- database dedicated to HoopDb in the separate-database layout (infra #107). No other
-- service's database, and no other role's defaults, change. This is the "owner function
-- defaults per owner" rule of docs/shared-database-isolation.md section 3, applied to
-- every sessions creator. Fragment 00 has already refused a poisoned starting row.
DO $rds_activation_10$
DECLARE
  v_target text;
  v_default text[];
BEGIN
  IF current_user::text <> current_setting('hoopdb_activation.provisioner') THEN
    RAISE EXCEPTION USING ERRCODE = '42501', MESSAGE = 'rds-activation fragment 10 must run as the provisioner';
  END IF;
  FOREACH v_target IN ARRAY ARRAY['rds_superuser', current_user::text] LOOP
    EXECUTE format('ALTER DEFAULT PRIVILEGES FOR ROLE %I REVOKE EXECUTE ON FUNCTIONS FROM PUBLIC', v_target);
    SELECT coalesce(array_agg(e ORDER BY e), '{}') INTO v_default
      FROM (SELECT CASE WHEN a.grantee = d.defaclrole THEN '<self>' ELSE coalesce(pg_get_userbyid(a.grantee), 'PUBLIC') END
                   || ':' || a.privilege_type || CASE WHEN a.is_grantable THEN '*' ELSE '' END AS e
              FROM pg_default_acl d, aclexplode(d.defaclacl) a
             WHERE d.defaclrole = v_target::regrole AND d.defaclnamespace = 0 AND d.defaclobjtype = 'f') x;
    IF v_default IS DISTINCT FROM ARRAY['<self>:EXECUTE']::text[] THEN
      RAISE EXCEPTION USING ERRCODE = '55000',
        MESSAGE = format('rds-activation: %I global function default is not exactly {<self>:EXECUTE}: %s', v_target, v_default);
    END IF;
  END LOOP;
END
$rds_activation_10$;
