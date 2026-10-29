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
