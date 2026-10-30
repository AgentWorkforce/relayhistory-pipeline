# Relaycast self-hosted conformance evidence

Evidence captured on 2026-10-30 against the published `relaycast` chart
`0.1.0`. This is a local kind proof of the portable Relaycast runtime. It does
not clear the AWS stage or change the production 96Bulls architecture:
production continues to use Supabase for its database and AWS S3 for object
storage.

## Provenance

| Component | Repository | Exact source | Published artifact |
| --- | --- | --- | --- |
| Consumer contract | `AgentWorkforce/96BullSessions` | `500ecc0839a05952eda4a3d84b7ecb2117cfce0b` | n/a |
| Helm chart | `AgentWorkforce/helm-charts` | tag `relaycast-0.1.0`, commit `d4963fd98e5a56ef11aae97e0ef5a82129abbe3b` | `relaycast-0.1.0.tgz`, SHA-256 `cdfc41cb6202fd81beb17cb57d5fbbdecaf27fa427f51faa07c5ec0fc116744f` |
| Runtime | `AgentWorkforce/relaycast` | tag `v8.0.0`, commit `f49b9c214f1762d1b1591acc75e65ced26e753fe` | `ghcr.io/agentworkforce/relaycast-self-host@sha256:5c86c095a335259060dc34ce7aa2e90809267395af915977a1ae608f93a83b98` |

The GitHub release asset digest, locally downloaded chart digest, chart value,
rendered Deployment image, and running pod image all matched the values above.

## Static and render conformance

The chart was tested from the exact `relaycast-0.1.0` tag, not a mutable
checkout:

```sh
helm_charts_repo=/path/to/helm-charts
proof_worktree=$(mktemp -d /tmp/helm-relaycast-proof.XXXXXX)
rmdir "$proof_worktree"
git -C "$helm_charts_repo" worktree add --detach "$proof_worktree" relaycast-0.1.0
(
  cd "$proof_worktree"
  scripts/ci/charts.sh static relaycast
  scripts/ci/charts.sh dryrun relaycast
)
git -C "$helm_charts_repo" worktree remove "$proof_worktree"
```

Results:

- strict Helm lint, deterministic render, hook ban, and Kubernetes schema
  validation passed; the render contained four valid resources and no invalid
  or skipped resources;
- Kubernetes server-side dry-run passed against the kind API server;
- all 26 chart rejection cases passed, covering mutable or malformed image
  digests, unsafe origins, direct public exposure, multiple or zero SQLite
  writers, unimplemented backends, unsafe persistence, unsupported or reserved
  environment variables, and invalid retention/mailbox values;
- the rendered workload used one replica, `Recreate`, `ClusterIP`, a retained
  RWO PVC, a read-only root filesystem, dropped Linux capabilities,
  `RuntimeDefault` seccomp, and no mounted service-account token.

Artifact provenance can be checked independently:

```sh
artifact_dir=$(mktemp -d /tmp/relaycast-chart.XXXXXX)
relaycast_repo=/path/to/relaycast
helm repo add awf https://AgentWorkforce.github.io/helm-charts
helm repo update awf
helm pull awf/relaycast --version 0.1.0 --destination "$artifact_dir"
sha256sum "$artifact_dir/relaycast-0.1.0.tgz"
helm show chart awf/relaycast --version 0.1.0
helm show values awf/relaycast --version 0.1.0 | sed -n '/^image:/,/^[^ ]/p'
git -C "$relaycast_repo" rev-parse 'v8.0.0^{commit}'
gh release view relaycast-0.1.0 --repo AgentWorkforce/helm-charts \
  --json tagName,targetCommitish,publishedAt,assets
```

## Live kind conformance

The already-installed release was inspected in cluster
`kind-96bulls-e2e`, namespace `relaycast-conformance`:

```sh
helm --kube-context kind-96bulls-e2e list -n relaycast-conformance
helm --kube-context kind-96bulls-e2e get values \
  relaycast-conformance -n relaycast-conformance -a
helm --kube-context kind-96bulls-e2e get manifest \
  relaycast-conformance -n relaycast-conformance
kubectl --context kind-96bulls-e2e -n relaycast-conformance \
  get deployment,pod,service,pvc -o wide
kubectl --context kind-96bulls-e2e -n relaycast-conformance \
  get deployment relaycast-conformance \
  -o jsonpath='{.spec.template.spec.containers[0].image}{"\n"}'
```

The release was Helm revision `3`, chart `relaycast-0.1.0`, app version
`8.0.0`. The pod was `1/1` Ready with zero restarts and the 10 Gi RWO PVC was
Bound.

The live probe kept generated workspace, agent, and observer credentials only
in process memory. Output was restricted to phase results. It exercised:

1. `GET /health`;
2. workspace creation, agent registration, a REST message write/read, and
   delivery of that message over `/v1/ws` using a scoped observer token
   (`stream:read`, `messages:read`, `channels:read`, `files:read`);
3. the signed local-file flow: request upload, PUT exact bytes, complete,
   request download, and compare exact bytes;
4. deletion and replacement of the kind pod, followed by verification that the
   workspace/message SQLite rows and exact uploaded bytes remained on the
   retained PVC.

The WebSocket check deliberately used an observer token. Relaycast `v8.0.0`
rejects agent and workspace keys on the workspace stream; agent realtime uses
the node transport instead. Treating an agent token as valid `/v1/ws` evidence
would test the wrong contract.

Sanitized result transcript:

```text
health_status=200 body={"ok":true,"version":"0.1.0","build":"unknown"}
PASS REST and observer WebSocket
PASS signed file upload/download
PASS post-restart SQLite row and file byte verification
```

The pod replacement was confined to the local kind namespace. The Helm
release, PVC, and chart revision remained in place.

## Mutation boundary

No Alchemy plan or apply ran. No AWS API, IAM, database, bucket, deployment,
release publication, or production operation ran. No Helm install or upgrade
ran during this proof. The only writes to runtime state were disposable
conformance fixtures and pod replacements in the existing local kind release.
