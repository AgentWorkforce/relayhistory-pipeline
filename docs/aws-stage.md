# AWS stage (not cleared)

Stages named `aws-*` are declared in `alchemy.run.ts` and refuse to run. Nothing
in this repository creates a cloud resource, credential or IAM entity until
Khaliq clears the AWS account work in his own words.

What the stage will need (each item needs an explicit OK): a target account
(per-agent sandbox under the Sandbox OU via Pippin Tools, or a named dev
account); which credential to use (SSO profile or a `96bulls-deployer` role
with GitHub OIDC trust scoped to this repo); an Alchemy state bucket; HoopDb
(RDS Postgres 17 + pgvector, or Supabase); RelayAuth JWKS + issuer; image push
rights (GHCR/ECR, by digest); compute (ECS Fargate first, EKS via the
`96bulls-worker` chart later); secrets names in 1Password; a budget cap.

Sandbox rules (settled Oct 28, 2026): accounts are per agent; a sweeper on a
schedule and on session close wipes everything sandbox-tagged and quarantines
any account it cannot verify empty (zero sandbox-tagged resources, zero
non-baseline IAM entities, empty workspace bucket prefix); quiet timeout 3h,
extensions in 3h blocks with no cap, any activity or deployment resets the
clock. `96bulls sandbox up|down|extend <agent>` is the planned interface; the
provisioning API lives in Pippin Tools.

## Decisions (Khaliq, 2026-10-28)

- **Compute:** EKS (Auto Mode) running the `96bulls` umbrella chart from
  AgentWorkforce/helm-charts, pinned to the published services image digest in
  `versions.lock.json` (`stacks/aws/eks.ts`).
- **Dev HoopDb:** a Supabase project in the org the 96Bulls team owns, declared
  with the `Supabase.Project` resource; its password comes from an `op://`
  reference at create time only.
- **Sandbox HoopDb:** Postgres (pgvector) as a container inside the sandbox's own
  compute; RDS only when a test needs managed Postgres.
- **Services image:** published `ghcr.io/agentworkforce/96bulls-services@sha256:46cf380e...`
  from main 4703c8e.

Still gated: nothing here runs until `.96bulls/<stage>/aws-inputs.json` carries
Khaliq's clearance and the account id is confirmed on the command line.

## Image registry (Khaliq, 2026-10-28)

The services image is the hosted product and stays private. The AWS stage
creates a private, tag-immutable ECR repository (`96bulls-services`, scan on
push, last 50 images kept); EKS nodes pull from it with their node role. The
`publish-image-ecr` workflow pushes to it via GitHub OIDC (repo variables
`AWS_DEPLOYER_ROLE_ARN`, `AWS_REGION`), manual and Khaliq-only. The EKS stage refuses
to declare anything until `aws-inputs.json` carries `eks.imageDigest`, the digest
published to that account's repository (each account builds its own). Only
jobs in the `image-publish` and `deploy-<stage>` GitHub environments can assume
the deployer role. The GHCR
copy stays private for kind and self-host tests.

Order for the first environment: apply the baseline (creates ECR and the
deployer role) -> set the two repo variables -> run publish-image-ecr -> put the
digest in `aws-inputs.json` -> apply again with `eks` inputs.

## Self-hosted RelayAuth (Khaliq, 2026-10-29: "the entire stack self hostable")

Each environment can run its own RelayAuth (helm-charts `relayauth`, one
replica on an encrypted gp3 volume) instead of trusting hosted relayauth.dev.
Turn it on with `eks.relayauth` in `aws-inputs.json`:

```json
"relayauth": { "chartVersion": "0.1.0", "secretName": "relayauth" }
```

and remove `eks.relayauthIssuer` (the stack refuses both). AlleyOop's issuer and
JWKS URL then become the in-cluster Service, `http://relayauth.relayauth.svc:8787`,
whatever `relayauthJwksUrl` says. Neither the token API nor JWKS is public:
only pods in the `96bulls` namespace may call it (NetworkPolicy, where enforced).

**Keys (once per environment, before the deploy).** They are generated on the
Mac straight into 1Password and reach the cluster through a pipe, never a file
on an agent machine. The 1Password item is `<stage> relayauth` in the
environment's vault (`96Bulls-dev` for sessions-dev), with fields
`internal-secret`, `signing-key.pem` (unencrypted PKCS#8 RSA 3072,
`BEGIN PRIVATE KEY`) and `bootstrap-api-key` (`rak_` + 64 hex):

```sh
# on the Mac (Khaliq approves each op call)
k=$(openssl genpkey -algorithm RSA -pkeyopt rsa_keygen_bits:3072 2>/dev/null)
op item create --vault 96Bulls-dev --category "API Credential" --title "sessions-dev relayauth" \
  "internal-secret[password]=$(openssl rand -hex 32)" \
  "bootstrap-api-key[password]=rak_$(openssl rand -hex 32)" \
  "signing-key.pem[password]=$k"; unset k
```

```sh
# from the operator shell, cluster credentials in env vars only
kubectl create namespace relayauth --dry-run=client -o yaml | kubectl apply -f -
ssh kjg-lap "zsh -lc 'op item get \"sessions-dev relayauth\" --vault 96Bulls-dev --reveal --format json'" \
  | jq '{apiVersion:"v1",kind:"Secret",type:"Opaque",metadata:{name:"relayauth",namespace:"relayauth"},
         stringData:(.fields|map(select(.label|IN("internal-secret","signing-key.pem","bootstrap-api-key")))|map({(.label):.value})|add)}' \
  | kubectl apply -f -
```

RelayAuth reads the keys at boot: after changing the Secret, run
`kubectl -n relayauth rollout restart deploy/relayauth`. Rotation:
`signingKeyNext` / `signingKeyPrevious` in the chart (helm-charts README).

**Minting a RelayHistory assertion** (the bootstrap key has exactly the scope
`relayauth:assertion:create:relayhistory`):

```sh
kubectl -n relayauth port-forward svc/relayauth 18787:8787 &
curl -s http://127.0.0.1:18787/v1/tokens/relayhistory-assertion \
  -H "x-api-key: $BOOTSTRAP_KEY" -H 'content-type: application/json' \
  -d '{"orgId":"org_sessions_dev","workspaceId":"default","sponsorId":"<user id>","scopes":["rth:sync","rth:read"],"expiresIn":3600,"historyOrigin":"https://sessions-dev.agentrelay.com"}'
# -> 201 {"accessToken": ...}; send it to AlleyOop as Authorization: Bearer <token>
```

The token's `aud` is exactly `historyOrigin`, which must equal the stage's
`relayhistoryOrigin` (AlleyOop refuses anything else).

**Transport.** The Service is plain HTTP. That is safe for what crosses it today:
AlleyOop fetches only the public JWKS, and minting goes through
`kubectl port-forward`, which tunnels inside the API server's TLS. Pod-to-pod
traffic on EKS is not encrypted by default, so **nothing may send the bootstrap
key over the cluster network until RelayAuth has TLS** (a mesh with mTLS, or TLS
terminated in front of RelayAuth). That applies to an in-cluster courier or
broker, and to a public token endpoint, which is a separate decision.
