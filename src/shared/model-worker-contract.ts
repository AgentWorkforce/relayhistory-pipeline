/** Proposed dedicated managed model caller. Source-only; no credentials or
 * privilege fallback. Generated SQL verifier derives from this exact registry. */
export const MODEL_WORKER_ROLE="bulls96_session_model";
export const MODEL_WORKER_FUNCTIONS=Object.freeze([
 "sessions.session_history_capture(jsonb)",
 "sessions.session_history_model_checkpoint(jsonb)",
 "sessions.session_history_model_claim(jsonb)",
 "sessions.session_history_model_enqueue(jsonb)",
 "sessions.session_history_model_discover(jsonb)",
 "sessions.session_history_model_heartbeat(jsonb)",
 "sessions.session_history_register_object(jsonb)",
 "sessions.session_history_replay_descriptor(jsonb)",
]as const);
