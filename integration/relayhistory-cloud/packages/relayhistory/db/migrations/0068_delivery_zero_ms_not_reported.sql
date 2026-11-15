-- PROPOSED for AgentWorkforce/relayhistory-cloud packages/relayhistory/db/migrations (renumber to
-- the next free slot when it lands there). Not applied by anything in this repository: the
-- vendored chain in vendor/relayhistory-cloud is a verbatim pin of upstream
-- (scripts/sync-relayhistory-schema.sh), and these functions are upstream's, so the fix belongs
-- upstream. Its TypeScript twin, packages/pipeline/src/lib/pipeline/relayhistory-projection.ts
-- (activityMs, catalogTime), already behaves this way; packages/pipeline/test/
-- relayhistory-zero-ms.test.ts applies this file over the pinned chain and proves the two agree.
--
-- The relayhistory client parsers write *_ms = 0 for a record that carries no time. 0023 and
-- 0022 accepted 0 (BETWEEN 0 AND ...), so such an event projected at 1970-01-01 and every
-- timeless tool call settled under it inherited the epoch.
--
-- delivery_activity_ms: a key whose value is 0 is "not reported" and the next key is tried;
-- with none reported the projection falls back to received_at, as for a missing key. Any other
-- number outside 1..8640000000000000 still gives NULL, as before (lower bound now 1).
-- delivery_catalog_time: 0 gives NULL (lower bound now 1).
--
-- NULLIF inside the CASE keeps 0023's evaluation order: a non-number is never cast.
-- Same signatures, volatility, search_path and return types (CREATE OR REPLACE keeps owner and
-- ACL). No index or generated column uses either function. Existing rows are not rewritten:
-- re-project affected sessions (runbooks/epoch-event-time-repair.md in sessions-pipeline).

CREATE OR REPLACE FUNCTION sessions.delivery_activity_ms(p_payload jsonb) RETURNS numeric
LANGUAGE sql IMMUTABLE AS $activity$
  SELECT candidate FROM (SELECT COALESCE(
    CASE WHEN jsonb_typeof(p_payload->'updated_ms') = 'number' THEN NULLIF((p_payload->>'updated_ms')::numeric, 0) END,
    CASE WHEN jsonb_typeof(p_payload->'ts_ms') = 'number' THEN NULLIF((p_payload->>'ts_ms')::numeric, 0) END,
    CASE WHEN jsonb_typeof(p_payload->'timestamp_ms') = 'number' THEN NULLIF((p_payload->>'timestamp_ms')::numeric, 0) END,
    CASE WHEN jsonb_typeof(p_payload->'last_activity_ms') = 'number' THEN NULLIF((p_payload->>'last_activity_ms')::numeric, 0) END
  ) AS candidate) AS activity
  WHERE candidate BETWEEN 1 AND 8640000000000000
$activity$;

CREATE OR REPLACE FUNCTION sessions.delivery_catalog_time(value jsonb)
RETURNS timestamptz LANGUAGE plpgsql IMMUTABLE
SET search_path = pg_catalog, sessions, pg_temp AS $catalog_time$
DECLARE
  milliseconds numeric;
BEGIN
  IF jsonb_typeof(value) IS DISTINCT FROM 'number' THEN RETURN NULL; END IF;
  milliseconds := (value #>> '{}')::numeric;
  IF milliseconds NOT BETWEEN 1 AND 8640000000000000 THEN RETURN NULL; END IF;
  RETURN to_timestamp((milliseconds / 1000)::double precision);
END;
$catalog_time$;
