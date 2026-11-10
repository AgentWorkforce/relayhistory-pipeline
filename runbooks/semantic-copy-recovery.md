# Copier checkpoint recovery and cancellation repair

This follows 191 bd184df and closes its independent P1/P2 findings. Runtime remains disabled by default; no new metadata migration, grant, IAM or source API is introduced.

An exact global profile is not per-kind checkpoint ownership. A claim with baselineH=null must find its own kind's feature table empty and no authority history for that kind, before capturing a replacement H. The first global profile also requires all feature/authority tables empty. Existing sibling-kind data is allowed for the other kind's first initialization. A retained H/cursor may resume normally. Lost metadata with historical rows/authority refuses permanently across retries; no automatic delete/adoption is performed. Restore/rebuild is an explicit operator procedure.

After the fresh-kind fence, the job durably persists H before its dirty authority ACK. Dirty-ACK failure leaves a harmless retained H and no feature write; retry uses that original H. Source/feature/SQL CAS, dirty 2*g/final 2*g+1, exact hash/readback and current paired authority checks are unchanged.

Every production source/metadata operation uses SemanticPostgresExecutor. The dedicated pools bound connection acquisition to 1s. Each logical operation runs BEGIN + SET LOCAL search_path/statement_timeout/lock_timeout, its parameterized query/RPC, then COMMIT. Statement timeout is at most 15s and the remaining job budget; lock timeout at most 5s and remaining budget. Aborted/failed operations do not queue a COMMIT and dispose their own connection. Normal success resets local settings at commit. Application values remain PostgreSQL protocol parameters; only validated integer timeout values enter SET SQL. No role defaults or backend termination are used.

Abort sends the standard pg cancellation packet for that exact client's PID/secret, waits for query settlement and closes its own connection if an unresponsive transport exceeds the bounded cleanup grace. An acquisition completing after abort is destroyed without dispatching SQL. A per-page heartbeat has its own merged signal; it is stopped/canceled/drained before checkpoint commit or error cleanup, so finally never waits on an uncancelled heartbeat. Global abort still cancels all job-owned operations. The job budget has up to 1.5s bounded acquisition/cleanup grace under transport failure; no indefinite wait is advertised.

The vendored CH helper stays byte-identical. createSemanticClickHouseClient uses its existing fetcher injection hook, merges job abort/remaining budget with the vendor request timeout, and returns the scoped client required by runSemanticCopyTick. Command,select,insert and response reads share the signal; insert cannot retain its old 300s default after job expiry. Synchronous ACK/readback requirements remain. A failed/aborted partial feature insert is retried under the same H/cursor; it cannot advance SQL readiness.

New transport interface:
SemanticBoundedSql extends SqlExecutor with queryBounded(sql,args,{signal,deadline,now?}).
scopeSemanticSql returns query + queryWithSignal for heartbeat-local cancellation.
SemanticPostgresExecutor(pool,readonlySource=false) is the concrete implementation.
createSemanticClickHouseClient(config,fetcher?) adds scoped(context) to old CH methods.
Feature table inventory is explicit {event,brief}, avoiding sibling bootstrap/adoption confusion.

A PostgreSQL logical query now costs three wire round trips (BEGIN+three SET LOCAL statements in one message, RPC/read, COMMIT) and six server statements. Throughput prototype/composition must count physical transport, not just logical query calls. No throughput speedup is claimed by this repair.

Synthetic evidence includes the original actual Node24/PG17 builtin/stages/CH checkpoint-loss reproduction, repeated refusal with features and authority-only history, valid sibling initialization/retained-H retry, actual 10s REGISTER stall canceled near 1000ms with no late binding/active RPC, and an actual 10s heartbeat canceled/drained under 500ms with no late mutation. Independent reviewer will rerun exact immutable final code before any readiness claim. Production install/grants/source coverage/baseline capacity/consumer config and activation remain separate gates.
