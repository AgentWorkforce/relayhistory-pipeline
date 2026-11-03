import { rawStoreFromEnv } from "../../pipeline/src/lib/pipeline/adapter.ts";
import { objectKey } from "../../object-store/src/index.ts";
import { CanonicalReadError } from "../../../src/session-read-api/canonical.ts";
import type { ArtifactIdentity } from "../../../src/session-read-api/canonical-artifact.ts";
import {
  readSessionAnalysisCache,
  type AnalysisCacheDeps,
} from "../../../src/session-read-api/session-analysis-reader.ts";
import {
  canonicalClaimReader,
  canonicalTokenTenant,
  type CanonicalEnv,
} from "./canonical.ts";
export interface AnalysisHandleDeps extends AnalysisCacheDeps {
  wake: (identity: ArtifactIdentity, signal: AbortSignal) => Promise<unknown>;
}
function wakeAck(value: unknown, identity: ArtifactIdentity) {
  if (Array.isArray(value)) {
    if (value.length !== 1) throw new CanonicalReadError("analysis_wakeup");
    value = value[0];
  }
  const v = value as {
    version?: unknown;
    accepted?: unknown;
    exists?: unknown;
    identity?: ArtifactIdentity;
  } | null;
  if (
    !v ||
    v.version !== 1 ||
    typeof v.accepted !== "boolean" ||
    typeof v.exists !== "boolean" ||
    v.identity?.orgId !== identity.orgId ||
    v.identity.workspaceId !== identity.workspaceId ||
    v.identity.source !== identity.source ||
    v.identity.sessionId !== identity.sessionId
  )
    throw new CanonicalReadError("analysis_wakeup");
}
export async function canonicalAnalysisHandle(
  request: Request,
  env: CanonicalEnv,
  fetcher: typeof fetch = fetch,
  override?: AnalysisHandleDeps,
): Promise<Response> {
  const controller = new AbortController(),
    correlationId = crypto.randomUUID();
  let timer: ReturnType<typeof setTimeout> | undefined;
  try {
    if (
      env.CANONICAL_READ_ENABLED !== "true" ||
      env.CANONICAL_READ_MODE !== "pages"
    )
      throw new CanonicalReadError("not_configured");
    const url = new URL(request.url),
      match = /^\/v1\/canonical\/analyses\/([^/]+)$/u.exec(url.pathname);
    if (request.method !== "GET" || !match)
      throw new CanonicalReadError("route", 400);
    for (const key of url.searchParams.keys())
      if (
        !["workspace", "source"].includes(key) ||
        url.searchParams.getAll(key).length !== 1
      )
        throw new CanonicalReadError("parameter", 400);
    const workspace = url.searchParams.get("workspace"),
      source = url.searchParams.get("source"),
      sessionId = decodeURIComponent(match[1]!);
    if (
      !workspace ||
      !source ||
      !/^[a-z0-9][a-z0-9._-]{0,63}$/u.test(source) ||
      !sessionId ||
      [...sessionId].length > 255
    )
      throw new CanonicalReadError("parameter", 400);
    const bearer = /^Bearer\s+(\S+)$/iu.exec(
      request.headers.get("authorization") ?? "",
    )?.[1];
    if (!bearer) throw new CanonicalReadError("authorization", 401);
    const identity = {
        ...canonicalTokenTenant(bearer, workspace),
        source,
        sessionId,
      },
      signal = AbortSignal.any([
        controller.signal,
        request.signal,
        ...(override ? [override.signal] : []),
      ]);
    const boundedFetch: typeof fetch = (input, init) =>
      fetcher(input, {
        ...init,
        signal: init?.signal ? AbortSignal.any([init.signal, signal]) : signal,
      });
    let deps = override;
    if (!deps) {
      const raw = rawStoreFromEnv(env, boundedFetch);
      if (!raw) throw new CanonicalReadError("not_configured");
      const wake = canonicalClaimReader(
          env,
          bearer,
          signal,
          "request_session_analysis_cache",
          boundedFetch,
        ),
        publication = canonicalClaimReader(
          env,
          bearer,
          signal,
          "session_directory_analysis_cache",
          boundedFetch,
        );
      deps = {
        signal,
        wake,
        publication,
        read: async (key) => (await raw.store.getObject(objectKey(key))).body,
      };
    }
    const selected = deps;
    const result = await Promise.race([
      (async () => {
        wakeAck(await selected.wake(identity, signal), identity);
        return readSessionAnalysisCache({ ...selected, signal }, identity);
      })(),
      new Promise<never>((_, reject) => {
        timer = setTimeout(() => {
          controller.abort();
          reject(new CanonicalReadError("deadline"));
        }, 8000);
      }),
    ]);
    return Response.json(
      {
        orgId: identity.orgId,
        workspaceId: workspace,
        source,
        sessionId,
        ...result,
        correlationId,
      },
      {
        headers: {
          "X-Relayhistory-Workspace-Id": workspace,
          "Cache-Control": "private, no-store",
        },
      },
    );
  } catch (error) {
    const e =
      error instanceof CanonicalReadError
        ? error
        : new CanonicalReadError("analysis_cache_unavailable");
    return Response.json(
      {
        error: {
          code:
            e.status === 400
              ? "bad_request"
              : e.status === 401
                ? "unauthorized"
                : "session_read_unavailable",
          message: e.message,
        },
        correlationId,
      },
      {
        status: e.status,
        headers: {
          "X-Session-Read-Reason": e.reason,
          "Cache-Control": "private, no-store",
        },
      },
    );
  } finally {
    if (timer) clearTimeout(timer);
    controller.abort();
  }
}
