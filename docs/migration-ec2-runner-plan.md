# Temporary EC2 migration compute

Pippen owns the account lifecycle; sessions-pipeline owns this compute template. Use the existing sessions-prod workload account941433935742 inus-east-1. Production session data must not be placed in a disposable Pippen sandbox. The template is independent of the existing pipeline Lambda and application routing.

## Resources and sizing

`stacks/aws/migration-ec2-runner.template.json` creates one EC2 instance, one security group, an EC2-only SSM role and an instance profile. Default r7i.xlarge is4vCPU/32GiB and fits the account's current5-vCPU quota. The8-vCPU/64GiB r7i.2xlarge option matches the earlier measured large-session envelope; its quota increase is separate.32GiB provisioning does not establish large-session capacity. Do not run a48GiB Node heap on this host.

Pass a pinned Amazon-owned AL2023 x86_64 AMI with /dev/xvda root device, an existing VPC and a subnet with working HTTPS egress. Current provisioned inputs were ami-0c230065b91270bc6, vpc-082645846ced87247 and subnet-0417f37645858cd06. Verify the AWS caller account and region before creating or executing a change set; inspect every resource action and replacement.

## Runtime boundary

No inbound ports or SSH key. Outbound TCP443/5432/6543/8443 supports package/SSM HTTPS, database TLS and the existing ClickHouse HTTPS8443 endpoint. IMDSv2 is mandatory with hop limit1. Root storage is encrypted100GiB gp3 and deleted on this instance's termination.

Boot installs Docker and enables SSM, creates an application-disabled marker and writes a persistent absolute UTC expiry timer before package installation. The timer powers off this instance6hours after first boot; EC2 shutdown behavior terminates it. The deadline survives reboot. Root tracks and later deletes the remaining stack IAM/profile/security-group resources.

AmazonSSMManagedInstanceCore otherwise allows Parameter Store reads; the role explicitly denies GetParameter/GetParameters/GetParameterHistory/GetParametersByPath. It has no application secret/S3/KMS policy. A bootstrap command must not retrieve application credentials or start migration jobs.

## Required verification

Validate the template, review CREATE/UPDATE change sets, and verify the AMI owner/architecture/root device. After creation require EC2 instance and system checks, SSM Online, Docker active, the disabled marker and a persistent expiry timer with the expected absolute deadline. A green CloudFormation stack alone does not prove bootstrap succeeded.

The reviewed deployment passed actual AWS IAM simulation: all four Parameter Store value/history actions explicitly denied; instance update/control-channel actions allowed. The disabled startup SSM command succeeded with Docker25.0.18 and the persistent timer enabled. Adding8443 was a separate change set containing exactly one non-replacing security-group modification.

## Application release and cleanup

Application execution needs a final composed code artifact, schema/ACL readiness and its reviewed runtime policy. Pin the existing PipelineSecret ARN, current raw-S3 reads and normalized-only output writes with applicable KMS access. No raw overwrite/delete, Neon writes or unrelated roles. Keep credentials out of userdata, SSM command parameters, logs and disk; Docker environment configuration also persists secrets, so use a reviewed memory-only bootstrap rather than embedding secret values in docker --env arguments/configuration.

The canonical Node entry is /app/large-read-main.mjs; semantic copying uses its separately frozen Node bundle. Runtime flags remain disabled until release gates pass. Existing source and target freshness, coverage, erasure acknowledgement and rollback checks remain required. Old ClickHouse tail deletion backlog is not readiness authority for the new tables.

Delete only this newly-created named stack after its own jobs stop. Never target the old pipeline, existing buckets or a Pippen sandbox sweep. Record exact template SHA, stack/instance IDs, application-disabled state, initialization evidence, absolute expiry and final cleanup result in the recovery record.
