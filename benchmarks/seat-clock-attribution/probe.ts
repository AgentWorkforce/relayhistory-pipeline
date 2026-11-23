/**
 * Bench-only probe instrumentation of sessions.ingest_lazy_ack_batch for the attribution harness. It reads the function
 * body exactly as installed by the migrations (pg_get_functiondef), inserts timing probes at fixed anchors and installs the
 * probed copy into the THROWAWAY database only. Nothing under supabase/ or src/ changes. Each anchor must match exactly
 * once, or the harness refuses to run.
 *
 * A probe is three PL/pgSQL assignments: _x := now in microseconds; _acc[k] += _x - _t; _t := _x. Step k is the time
 * between the previous probe and probe k. At the end of the body the accumulated steps, the absolute server stamps and
 * the per-step counts are written with set_config('scb.trace', ..., false): a session-level setting, so it survives the
 * COMMIT and the load worker reads it on the same connection after COMMIT (outside the measured hold). A failed ACK
 * rolls the setting back; only successful commits carry a trace.
 */
export const STEPS = [
  /*1*/ "guard", /*2*/ "lock_fences", /*3*/ "seal", /*4*/ "gate_read", /*5*/ "clock_lock",
  /*6*/ "origin_lock", /*7*/ "stage_row", /*8*/ "attempt_check1", /*9*/ "attempt_row",
  // main loop (per record, summed)
  /*10*/ "loop_fetch", /*11*/ "loop_key", /*12*/ "loop_slot_lock", /*13*/ "loop_checks", /*14*/ "loop_seat",
  /*15*/ "loop_slot_update", /*16*/ "loop_decision_insert", /*17*/ "loop_decision_check",
  /*18*/ "last_seat_update", /*19*/ "final_check",
  // t2 branch
  /*20*/ "t2_A_candidates", /*21*/ "t2_B_slot_locks", /*22*/ "t2_C1_plan", /*23*/ "t2_upgrade", /*24*/ "t2_C2_number",
  /*25*/ "t2_D_seats", /*26*/ "t2_D_slots", /*27*/ "t2_D_decisions", /*28*/ "t2_E_count",
] as const;
const K = (s: (typeof STEPS)[number]) => STEPS.indexOf(s) + 1;
const P = (s: (typeof STEPS)[number]) => `_x:=extract(epoch from clock_timestamp())*1e6;_acc[${K(s)}]:=_acc[${K(s)}]+(_x-_t);_t:=_x;`;

export function instrument(def: string): string {
  const once = (src: string, find: string, repl: string) => {
    const n = src.split(find).length - 1;
    if (n !== 1) throw Error(`probe anchor matched ${n} times: ${find.slice(0, 80)}`);
    return src.replace(find, repl);
  };
  let s = def;
  s = once(s, "v_t2 boolean;", `_acc float8[]:=array_fill(0::float8,ARRAY[${STEPS.length}]);_t float8;_x float8;_t0 float8;_tclock float8;_tex float8;_n integer:=0;v_t2 boolean;`);
  s = once(s, "BEGIN\n PERFORM sessions.ingest_lazy_guard(p->>'org_id');", `BEGIN\n _t:=extract(epoch from clock_timestamp())*1e6;_t0:=_t;\n PERFORM sessions.ingest_lazy_guard(p->>'org_id');${P("guard")}`);
  s = once(s, " PERFORM sessions.ingest_lock_fences(p->>'org_id',p->>'workspace_id',p->>'user_id');\n", ` PERFORM sessions.ingest_lock_fences(p->>'org_id',p->>'workspace_id',p->>'user_id');${P("lock_fences")}\n`);
  s = once(s, "MESSAGE='session ingest: real user seal required before any receipt';END IF;", `MESSAGE='session ingest: real user seal required before any receipt';END IF;${P("seal")}`);
  s = once(s, " v_t2:=v_gate='fenced' AND v_mode='t2';", ` v_t2:=v_gate='fenced' AND v_mode='t2';${P("gate_read")}`);
  s = once(s, "WHERE singleton FOR NO KEY UPDATE;v_last:=c.last_seat;", `WHERE singleton FOR NO KEY UPDATE;v_last:=c.last_seat;${P("clock_lock")}_tclock:=_t;`);
  s = once(s, "  SELECT * INTO STRICT c FROM sessions.ingest_receipt_seat_clock WHERE singleton FOR KEY SHARE;\n END IF;",
    `  SELECT * INTO STRICT c FROM sessions.ingest_receipt_seat_clock WHERE singleton FOR KEY SHARE;${P("clock_lock")}\n END IF;`);
  s = once(s, "p->>'origin_id')::text,0));", `p->>'origin_id')::text,0));${P("origin_lock")}`);
  s = once(s, "MESSAGE='session ingest: current original stage required';END IF;\n PERFORM sessions.ingest_lazy_attempt(t.claim_token);",
    `MESSAGE='session ingest: current original stage required';END IF;${P("stage_row")}\n PERFORM sessions.ingest_lazy_attempt(t.claim_token);${P("attempt_check1")}`);
  s = once(s, "MESSAGE='session ingest: original candidate order changed';END IF;", `MESSAGE='session ingest: original candidate order changed';END IF;${P("attempt_row")}`);
  // main loop
  s = once(s, "  ORDER BY (a.source_processing_ranks->>q.ord)::integer LOOP\n", `  ORDER BY (a.source_processing_ranks->>q.ord)::integer LOOP\n${P("loop_fetch")}_n:=_n+1;\n`);
  s = once(s, "  v_key:=encode(sha256(convert_to(v_material::text,'UTF8')),'hex');\n", `  v_key:=encode(sha256(convert_to(v_material::text,'UTF8')),'hex');${P("loop_key")}\n`);
  s = once(s, "  SELECT * INTO slot FROM sessions.ingest_lazy_native_slots WHERE key_sha256=v_key FOR UPDATE;\n", `  SELECT * INTO slot FROM sessions.ingest_lazy_native_slots WHERE key_sha256=v_key FOR UPDATE;${P("loop_slot_lock")}\n`);
  s = once(s, "  v_first:=CASE WHEN v_accept THEN r.candidate_digest ELSE slot.current_digest END;\n", `  v_first:=CASE WHEN v_accept THEN r.candidate_digest ELSE slot.current_digest END;${P("loop_checks")}\n`);
  s = once(s, "    r.key_material,v_last,v_first,r.key_material->>10,r.key_material->>11,r.fence_epoch) RETURNING * INTO s;\n  END IF;\n",
    `    r.key_material,v_last,v_first,r.key_material->>10,r.key_material->>11,r.fence_epoch) RETURNING * INTO s;\n  END IF;${P("loop_seat")}\n`);
  s = once(s, "    current_seat_key=s.key_sha256,current_legacy_key=NULL WHERE key_sha256=v_key;\n  END IF;\n",
    `    current_seat_key=s.key_sha256,current_legacy_key=NULL WHERE key_sha256=v_key;\n  END IF;${P("loop_slot_update")}\n`);
  s = once(s, "   a.schema_generation,a.source_control_version,a.source_witness_sha256) ON CONFLICT DO NOTHING;\n",
    `   a.schema_generation,a.source_control_version,a.source_witness_sha256) ON CONFLICT DO NOTHING;${P("loop_decision_insert")}\n`);
  s = once(s, "MESSAGE='session ingest: immutable ACK decision adoption refused';END IF;\n END LOOP;",
    `MESSAGE='session ingest: immutable ACK decision adoption refused';END IF;${P("loop_decision_check")}\n END LOOP;`);
  // t2 branch
  s = once(s, "    FROM sessions.ingest_receipt_seat_records(t.claim_token,t.group_ordinals) q)z;\n", `    FROM sessions.ingest_receipt_seat_records(t.claim_token,t.group_ordinals) q)z;${P("t2_A_candidates")}\n`);
  s = once(s, "   ORDER BY k.first_pos FOR UPDATE OF n;\n", `   ORDER BY k.first_pos FOR UPDATE OF n;${P("t2_B_slot_locks")}\n`);
  s = once(s, "   INTO v_plan FROM l8 f;\n", `   INTO v_plan FROM l8 f;${P("t2_C1_plan")}\n`);
  s = once(s, "WHERE ck.singleton FOR NO KEY UPDATE;\n", `WHERE ck.singleton FOR NO KEY UPDATE;${P("t2_upgrade")}_tclock:=_t;\n`);
  s = once(s, "  v_lim:=coalesce(v_epos,2147483647);\n", `  v_lim:=coalesce(v_epos,2147483647);${P("t2_C2_number")}\n`);
  s = once(s, "   WHERE q.alloc AND q.pos<v_lim ORDER BY q.pos;\n", `   WHERE q.alloc AND q.pos<v_lim ORDER BY q.pos;${P("t2_D_seats")}\n`);
  s = once(s, "    WHERE q.accept AND q.pos<v_lim AND q.acc_cum=i AND n.key_sha256=q.nk;\n   END LOOP;\n  END IF;\n",
    `    WHERE q.accept AND q.pos<v_lim AND q.acc_cum=i AND n.key_sha256=q.nk;\n   END LOOP;\n  END IF;${P("t2_D_slots")}\n`);
  s = once(s, "   WHERE q.pos<v_lim ORDER BY q.pos ON CONFLICT DO NOTHING;\n", `   WHERE q.pos<v_lim ORDER BY q.pos ON CONFLICT DO NOTHING;${P("t2_D_decisions")}\n`);
  s = once(s, "  v_last:=v_last+v_allocs;\n", `  v_last:=v_last+v_allocs;${P("t2_E_count")}\n`);
  // tail
  s = once(s, " UPDATE sessions.ingest_receipt_seat_clock SET last_seat=v_last WHERE singleton;\n", ` UPDATE sessions.ingest_receipt_seat_clock SET last_seat=v_last WHERE singleton;${P("last_seat_update")}\n`);
  s = once(s, " PERFORM sessions.ingest_lazy_attempt(t.claim_token);\nEND",
    ` PERFORM sessions.ingest_lazy_attempt(t.claim_token);${P("final_check")}\n` +
    ` PERFORM set_config('scb.trace',jsonb_build_object('t2',v_t2,'n',_n,'t0',_t0,'tclock',_tclock,'tend',_t,'acc',to_jsonb(_acc))::text,false);\nEND`);
  return s;
}

/** Probed copy of the origin-scoped commit wrapper: stamps entry, after the origin-claim advisory enter, and exit. */
export function instrumentWrapper(def: string): string {
  const find = " v_mark:=sessions.ingest_origin_claim_enter(p);\n v_result:=sessions.ingest_commit_buffered_batch(p);\n";
  if (def.split(find).length !== 2) throw Error("wrapper anchor");
  const us = "extract(epoch from clock_timestamp())*1e6";
  return def.replace("DECLARE v_mark xid8;v_result jsonb;", "DECLARE v_mark xid8;v_result jsonb;_w0 float8;_w1 float8;")
    .replace(find, ` _w0:=${us};\n v_mark:=sessions.ingest_origin_claim_enter(p);_w1:=${us};\n v_result:=sessions.ingest_commit_buffered_batch(p);\n`)
    .replace(" RETURN v_result;\nEND", ` PERFORM set_config('scb.wrap',jsonb_build_object('w0',_w0,'w1',_w1,'w2',${us})::text,false);\n RETURN v_result;\nEND`);
}
