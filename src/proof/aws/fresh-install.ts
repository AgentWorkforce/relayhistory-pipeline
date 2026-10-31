/**
 * Check 1, fresh install: every running container image is digest-pinned, the
 * kubelet actually runs that digest, and the digest is the one the released
 * chart (or the operator's recorded input) says it should be. Chart versions
 * and digests are recorded as evidence.
 */

export interface PodList {
  items: Array<{
    metadata: { name: string; namespace: string };
    spec: { containers: Array<{ name: string; image: string }>; initContainers?: Array<{ name: string; image: string }> };
    status: {
      phase?: string;
      containerStatuses?: Array<{ name: string; image: string; imageID?: string }>;
      initContainerStatuses?: Array<{ name: string; image: string; imageID?: string }>;
    };
  }>;
}

export interface HelmRelease {
  name: string;
  namespace: string;
  revision: string;
  chart: string;
  app_version: string;
  status: string;
}

export interface ImageRecord {
  pod: string;
  namespace: string;
  container: string;
  init: boolean;
  image: string;
  repository: string;
  specDigest?: string;
  runningDigest?: string;
}

export interface FreshInstallResult {
  ok: boolean;
  /** No expectations were supplied, so "matches the release" could not be decided. */
  blocked: boolean;
  failures: string[];
  images: ImageRecord[];
  charts: Array<Pick<HelmRelease, "name" | "namespace" | "chart" | "app_version" | "revision" | "status">>;
}

const DIGEST = /@(sha256:[0-9a-f]{64})$/;

/** Repository without tag or digest, with Docker Hub's implicit prefixes made explicit. */
export function repositoryOf(image: string): string {
  let ref = image.replace(DIGEST, "");
  const lastSlash = ref.lastIndexOf("/");
  const colon = ref.lastIndexOf(":");
  if (colon > lastSlash) ref = ref.slice(0, colon);
  const first = ref.split("/")[0]!;
  const hasRegistry = ref.includes("/") && (first.includes(".") || first.includes(":") || first === "localhost");
  if (!hasRegistry) ref = `docker.io/${ref.includes("/") ? ref : `library/${ref}`}`;
  return ref;
}

export function digestOf(ref: string | undefined): string | undefined {
  return ref ? DIGEST.exec(ref)?.[1] : undefined;
}

export function collectImages(pods: PodList, opts: { runningOnly?: boolean } = {}): ImageRecord[] {
  const out: ImageRecord[] = [];
  for (const pod of pods.items) {
    if (opts.runningOnly !== false && pod.status.phase !== "Running") continue;
    const statuses = new Map([...(pod.status.containerStatuses ?? []), ...(pod.status.initContainerStatuses ?? [])].map((s) => [s.name, s]));
    const add = (c: { name: string; image: string }, init: boolean) => {
      const st = statuses.get(c.name);
      out.push({
        pod: pod.metadata.name,
        namespace: pod.metadata.namespace,
        container: c.name,
        init,
        image: c.image,
        repository: repositoryOf(c.image),
        specDigest: digestOf(c.image),
        runningDigest: digestOf(st?.imageID),
      });
    };
    for (const c of pod.spec.containers) add(c, false);
    for (const c of pod.spec.initContainers ?? []) add(c, true);
  }
  return out;
}

export function evaluateFreshInstall(pods: PodList, releases: HelmRelease[], expected: Record<string, string>): FreshInstallResult {
  const images = collectImages(pods);
  const failures: string[] = [];
  const expectations = new Map(Object.entries(expected).map(([repo, d]) => [repositoryOf(repo), d]));
  let unmatched = 0;
  for (const i of images) {
    const where = `${i.namespace}/${i.pod}/${i.container}`;
    if (!i.specDigest) {
      failures.push(`${where}: image is not digest-pinned (${i.image})`);
      continue;
    }
    if (!i.runningDigest) failures.push(`${where}: kubelet reports no running digest`);
    else if (i.runningDigest !== i.specDigest) failures.push(`${where}: running ${i.runningDigest} differs from spec ${i.specDigest}`);
    const want = expectations.get(i.repository);
    if (!want) unmatched += 1;
    else if (want !== i.specDigest) failures.push(`${where}: ${i.specDigest} is not the released/input digest ${want} for ${i.repository}`);
  }
  if (images.length === 0) failures.push("no running pods found");
  if (expectations.size > 0 && unmatched > 0)
    for (const i of images.filter((x) => x.specDigest && !expectations.has(x.repository)))
      failures.push(`${i.namespace}/${i.pod}/${i.container}: no released/input digest recorded for ${i.repository}`);
  const charts = releases.map(({ name, namespace, chart, app_version, revision, status }) => ({ name, namespace, chart, app_version, revision, status }));
  for (const r of charts) if (r.status !== "deployed") failures.push(`helm release ${r.namespace}/${r.name} is ${r.status}`);
  const blocked = failures.length === 0 && expectations.size === 0;
  return { ok: failures.length === 0 && !blocked, blocked, failures, images, charts };
}

/**
 * Expected digests from a chart's `helm show values` output: every
 * `<registry/repo>[:tag]@sha256:...` string, plus `repository:` followed by
 * `digest: sha256:...` or `tag: <tag>@sha256:...`. Inputs from aws-inputs.json (the per-account ECR digest) are merged on
 * top by the caller, so an operator input always wins over a chart default.
 */
export function expectedFromValues(valuesYaml: string): Record<string, string> {
  const out: Record<string, string> = {};
  for (const m of valuesYaml.matchAll(/([a-z0-9.-]+(?::\d+)?\/[A-Za-z0-9._/-]+)(?::[A-Za-z0-9._-]+)?@(sha256:[0-9a-f]{64})/g))
    out[repositoryOf(`${m[1]}@${m[2]}`)] = m[2]!;
  const lines = valuesYaml.split("\n");
  for (let i = 0; i < lines.length; i++) {
    const repo = /^(\s*)repository:\s*["']?([^"'\s#]+)/.exec(lines[i]!);
    if (!repo) continue;
    for (let j = i + 1; j < Math.min(lines.length, i + 6); j++) {
      const d = /^\s*(?:digest:\s*["']?|tag:\s*["']?[A-Za-z0-9._-]*@)(sha256:[0-9a-f]{64})/.exec(lines[j]!);
      if (d) {
        out[repositoryOf(repo[2]!)] = d[1]!;
        break;
      }
    }
  }
  return out;
}

/** "relayauth-0.1.0" -> { name: "relayauth", version: "0.1.0" } (helm list's chart column). */
export function parseChartRef(chart: string): { name: string; version: string } | undefined {
  const m = /^(.+)-(\d+\.\d+\.\d+(?:[-+][0-9A-Za-z.-]+)?)$/.exec(chart);
  return m ? { name: m[1]!, version: m[2]! } : undefined;
}
