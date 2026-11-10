import { BUFFERED_SEARCH_FEED_RUNTIME_ROLE } from "../../src/session-pipeline/buffered-feed-role.ts";
import type { VerifierTarget } from "./types.ts";

const quoted = (sigs: readonly string[]) => sigs.map((k) => " '" + k + "'").join(",\n");

/**
 * Role verifiers whose approved-function arrays are derived from the composed ACL. A lane that
 * introduces a role with its own verifier (e.g. bulls96_project_events, #237) adds it here.
 */
export const VERIFIERS: readonly VerifierTarget[] = [
  {
    role: "bulls96_session_ingest",
    path: "supabase/bootstrap/session_ingest_verify.sql",
    pattern: /v_allowed text\[\] := ARRAY\[[\s\S]*?\]::text\[\];/,
    render: (sigs) => "v_allowed text[] := ARRAY[\n" + quoted(sigs) + "\n ]::text[];",
  },
  {
    role: "session_directory_search_worker",
    path: "supabase/bootstrap/session_directory_search_worker_verify.sql",
    pattern: /v_approved text\[\] := ARRAY\[[\s\S]*?\];/,
    render: (sigs) => "v_approved text[] := ARRAY[\n" + quoted(sigs) + "\n ];",
  },
  {
    // The read API role may EXECUTE no sessions SECURITY DEFINER function but these (allowed,
    // not required, so the verifier also passes before 20261105034000 is applied).
    role: "bulls96_read_api",
    path: "supabase/bootstrap/session_read_api_role_verify.sql",
    pattern: /AND p\.oid::regprocedure::text NOT IN \([\s\S]*?\)\n  LOOP/,
    render: (sigs) => "AND p.oid::regprocedure::text NOT IN (\n" + sigs.map((k) => "         '" + k + "'").join(",\n") + ")\n  LOOP",
  },
  {
    role: "bulls96_project_events",
    path: "supabase/bootstrap/canonical_project_events_verify.sql",
    template: "supabase/bootstrap/session_ingest_verify.sql",
    pattern: /v_allowed text\[\] := ARRAY\[[\s\S]*?\]::text\[\];/,
    render: (sigs) => "v_allowed text[] := ARRAY[\n" + quoted(sigs) + "\n ]::text[];",
    transform: (text) => text.replaceAll("bulls96_session_ingest", "bulls96_project_events").replaceAll("session ingest", "canonical project events"),
  },
  {
    // The E3 transcript eraser under the Supabase privacy authority (20261105041000): only the four eraser read RPCs.
    role: "bulls96_privacy_eraser",
    path: "supabase/bootstrap/privacy_eraser_verify.sql",
    template: "supabase/bootstrap/session_ingest_verify.sql",
    pattern: /v_allowed text\[\] := ARRAY\[[\s\S]*?\]::text\[\];/,
    render: (sigs) => "v_allowed text[] := ARRAY[\n" + quoted(sigs) + "\n ]::text[];",
    transform: (text) => text.replaceAll("bulls96_session_ingest", "bulls96_privacy_eraser").replaceAll("session ingest", "privacy eraser"),
  },
  {
    // The E3 erasure phase's registry login (20261105042000): only the retention claim/fence/complete RPCs.
    role: "bulls96_babysitter_retention",
    path: "supabase/bootstrap/babysitter_retention_verify.sql",
    template: "supabase/bootstrap/session_ingest_verify.sql",
    pattern: /v_allowed text\[\] := ARRAY\[[\s\S]*?\]::text\[\];/,
    render: (sigs) => "v_allowed text[] := ARRAY[\n" + quoted(sigs) + "\n ]::text[];",
    transform: (text) => text.replaceAll("bulls96_session_ingest", "bulls96_babysitter_retention").replaceAll("session ingest", "babysitter retention"),
  },
  {
    // The Factory acquisition control role (slot 043850): only install/revoke/rebind/status.
    role: "bulls96_acquisition_installer",
    path: "supabase/bootstrap/acquisition_installer_verify.sql",
    template: "supabase/bootstrap/session_ingest_verify.sql",
    pattern: /v_allowed text\[\] := ARRAY\[[\s\S]*?\]::text\[\];/,
    render: (sigs) => "v_allowed text[] := ARRAY[\n" + quoted(sigs) + "\n ]::text[];",
    transform: (text) => text.replaceAll("bulls96_session_ingest", "bulls96_acquisition_installer").replaceAll("session ingest", "acquisition installer"),
  },
  {
    // The buffered search feed's runtime login (slot 043870): only the three buffered_search_feed_* RPCs.
    role: BUFFERED_SEARCH_FEED_RUNTIME_ROLE,
    path: "supabase/bootstrap/buffered_search_feed_runtime_verify.sql",
    template: "supabase/bootstrap/session_ingest_verify.sql",
    pattern: /v_allowed text\[\] := ARRAY\[[\s\S]*?\]::text\[\];/,
    render: (sigs) => "v_allowed text[] := ARRAY[\n" + quoted(sigs) + "\n ]::text[];",
    transform: (text) => text.replaceAll("bulls96_session_ingest", BUFFERED_SEARCH_FEED_RUNTIME_ROLE).replaceAll("session ingest", "buffered search feed runtime"),
  },
];
