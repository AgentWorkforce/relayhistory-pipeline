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
