import type { NodeReadProfile } from "../session-read-api/node-read-profile.ts";
/** Capture/full-union hydration/immutable writes/CAS. All database calls use backend
 * RPCs; the public claim RPC has the existing search-worker JWT policy. */
import * as zlib from "node:zlib";
import type { SqlExecutor } from "../../packages/pipeline/src/lib/pipeline/queue.ts";
import { canonicalProducerSnapshot, canonicalSourceGeneration, type CanonicalPointer, type CanonicalIdentity } from "../session-read-api/canonical.ts";
import { ARTIFACT_MAX_SOURCE_POINTERS, type ArtifactSource, type ArtifactPublication } from "../session-read-api/canonical-artifact.ts";
import type { ObjectStore } from "../../packages/object-store/src/index.ts";
import type { SessionScope } from "./pointer.ts";
import { canonicalTimingProjection } from "../session-read-api/canonical-timing.ts";
import { artifactDigest, ArtifactBuildError, materializeCanonicalArtifacts } from "./replay-pages.ts";
import { historyReplayPrefix, registerHistoryArtifactObject, publishHistoryReplay, type HistorySourceCapture } from "../shared/history-source-authority.ts";
export interface ReplayCapture {
  exists: boolean; blocked: boolean; identity: CanonicalIdentity; source: ArtifactSource;
  snapshot: { epoch: string; pointers: CanonicalPointer[]; [key: string]: unknown };
}
export interface ReplayProducerDeps {
  profile?: NodeReadProfile;
  /** Required for620-managed scopes; exact original mask/evidence, no recapture fallback. */
  history?: HistorySourceCapture;
  largeLease?: string;
  publish?: (params: unknown[]) => Promise<boolean>;
  db: SqlExecutor; store: ObjectStore; provider: string; bucket: string;
  prefix: { PIPELINE_ORG_FROM_PREFIX?: string; PIPELINE_KEY_PREFIX?: string };
  decompress?: (bytes: Uint8Array, maxBytes: number) => Uint8Array;
}
export interface ReplayProducerResult { published: boolean; reason?: "absent" | "source_blocked"; publication?: ArtifactPublication; rows?: number; objects?: number; bytes?: number }
const fail = (code: string): never => { throw new ArtifactBuildError(code); };
const scopeOf = (i: CanonicalIdentity): SessionScope => ({ orgId: i.orgId, workspaceId: i.workspaceId, harness: i.source, sessionId: i.sessionId });
async function capture(db: SqlExecutor, scope: SessionScope): Promise<ReplayCapture> {
  const { rows } = await db.query<{captured: ReplayCapture}>("SELECT sessions.capture_session_replay_pages($1,$2,$3,$4) AS captured", [scope.orgId,scope.workspaceId,scope.harness,scope.sessionId]);
  const c = rows[0]?.captured;
  if (!c || typeof c.exists !== "boolean" || typeof c.blocked !== "boolean" || c.identity?.orgId !== scope.orgId || c.identity?.workspaceId !== scope.workspaceId || c.identity?.source !== scope.harness || typeof c.identity.sessionId !== "string" || !c.identity.sessionId || !/^[0-9a-f]{64}$/u.test(c.source?.fingerprint ?? "") || !Array.isArray(c.snapshot?.pointers)) fail("artifact_capture_invalid");
  if (c.exists && (!/^[1-9][0-9]*$/u.test(c.source.epoch) || c.snapshot.epoch !== c.source.epoch || !c.snapshot.pointers.length || c.snapshot.pointers.length > ARTIFACT_MAX_SOURCE_POINTERS)) fail("artifact_capture_bound");
  if (!c.exists && c.snapshot.pointers.length) fail("artifact_capture_invalid");
  return c;
}
/** Artifact generations are explicitly v1, separate from unchanged raw-provider cursors. */
export function canonicalArtifactGeneration(sourceGeneration: string, source: ArtifactSource): string {
  return artifactDigest(JSON.stringify(["canonical-read-artifact-v1",sourceGeneration,source.epoch,source.fingerprint]));
}
export async function produceCanonicalReplay(scope: SessionScope, deps: ReplayProducerDeps): Promise<ReplayProducerResult> {
  const first = await capture(deps.db, scope);
  if (!first.exists) return { published: false, reason: "absent" };
  if (first.blocked) return { published: false, reason: "source_blocked" };
  const identity = first.identity, managed = (first.snapshot.history as {required?:unknown}|undefined)?.required === true;
  const history = deps.history;
  if(managed && (!history || history.identity.orgId!==identity.orgId || history.identity.workspaceId!==identity.workspaceId || history.identity.source!==identity.source || history.identity.sessionId!==identity.sessionId || history.source.epoch!==first.source.epoch || history.source.fingerprint!==first.source.fingerprint)) fail("artifact_history_authority_required");
  if(history && !managed) fail("artifact_history_authority_uninstalled");
  // Each re-read includes the monotonic source epoch, not merely pointer values.
  const current = async () => {
    const c = await capture(deps.db, scopeOf(identity));
    if (!c.exists || c.blocked || c.source.epoch !== first.source.epoch || c.source.fingerprint !== first.source.fingerprint || JSON.stringify(c.identity) !== JSON.stringify(identity)) fail("artifact_source_changed");
    return c.snapshot.pointers;
  };
  const snapshot = await canonicalProducerSnapshot({
    store: deps.store, provider: deps.provider, bucket: deps.bucket, prefix: deps.prefix,
    pointers: current,
    ...(history ? {historyMask:history.mask} : {}),
    decompress: deps.decompress ?? ((bytes,maxBytes) => new Uint8Array(zlib.zstdDecompressSync(bytes,{maxOutputLength:maxBytes}))),
  }, identity, deps.profile);
  if (!snapshot.exists) fail("artifact_source_changed");
  const sourceGeneration = canonicalSourceGeneration(first.snapshot.pointers,identity);
  if (snapshot.generation !== sourceGeneration) fail("artifact_source_changed");
  const generation = canonicalArtifactGeneration(sourceGeneration,first.source);
  const anchor = [...first.snapshot.pointers].sort((a,b) => Buffer.compare(Buffer.from(a.manifest_key),Buffer.from(b.manifest_key)))[0]!;
  if (!anchor.manifest_key.endsWith("/manifest.json")) fail("artifact_anchor_invalid");
  const rawSha = anchor.content_sha256.replace(/^\\x/u, "").toLowerCase();
  if (!/^[0-9a-f]{64}$/u.test(rawSha)) fail("artifact_anchor_invalid");
  const prefix = history ? historyReplayPrefix(history,generation) : `${anchor.manifest_key.slice(0,-"manifest.json".length)}normalized/read-projection-v1/${rawSha}/${generation}/`;
  const timing = canonicalTimingProjection(identity,snapshot.rows,snapshot.legacyCoverage !== null);
  const rows = timing ? [...snapshot.rows,timing] : snapshot.rows;
  const store = history ? new Proxy(deps.store,{get(target,key){
    if(key==="put") return async(k:Parameters<ObjectStore["put"]>[0],body:Uint8Array,options?:Parameters<ObjectStore["put"]>[2])=>{
      if(options?.ifNoneMatch!==true || options.ifMatch) fail("artifact_history_create_only");
      await registerHistoryArtifactObject(deps.db,history,{key:String(k),sha256:artifactDigest(body),bytes:body.byteLength});
      return target.put(k,body,options);
    };
    const value=Reflect.get(target,key);return typeof value==="function"?value.bind(target):value;
  }}) : deps.store;
  const made = await materializeCanonicalArtifacts(store,{ identity,generation,source:first.source,prefix,rows,profile:deps.profile,legacyCoverage:snapshot.legacyCoverage });
  await current(); // Objects are verified, but stale work still must publish nothing.
  const params = [identity.orgId,identity.workspaceId,identity.source,identity.sessionId,first.source.epoch,first.source.fingerprint,JSON.stringify(first.snapshot),generation,sourceGeneration,prefix,made.index.key,made.index.sha256,made.index.bytes];
  const published = history ? await publishHistoryReplay(deps.db,history,params,deps.largeLease??null) : deps.publish ? await deps.publish(params) : (await deps.db.query<{published:boolean}>("SELECT sessions.publish_session_replay_pages($1,$2,$3,$4,$5::bigint,$6,$7::jsonb,$8,$9,$10,$11,$12,$13::integer) AS published",params)).rows[0]?.published;
  if (published !== true) fail("artifact_publish_cas_failed");
  return { published:true, publication:{version:1,identity,generation,source:first.source,prefix,index:made.index}, rows:snapshot.rows.length, objects:made.objects, bytes:made.bytes };
}
