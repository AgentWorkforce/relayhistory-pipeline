-- Only the read surface is removed; generated briefs and pointers are retained.
BEGIN;
DROP FUNCTION sessions.session_brief_summary_for(text,text,text);
COMMIT;
