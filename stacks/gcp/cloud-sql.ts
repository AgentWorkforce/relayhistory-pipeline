/**
 * Private Cloud SQL attachment. The pinned Alchemy provider cannot express it.
 *
 * `@alchemy.run/gcp@2.0.0-beta.84` `GCP.SQL.Instance` writes only
 * `ipConfiguration.ipv4Enabled`. It has no private network, allocated range,
 * or Private Service Connect input. A public instance is not a private path,
 * and disabling `ipv4Enabled` without a private network would create an
 * instance with no address. This module refuses both.
 */

export interface CloudSqlPrivateAttachment {
  ipv4Enabled: false;
  privateNetwork: string;
  allocatedIpRange: string;
  /** Auth proxy must use the private address. The connection name is appended by the caller. */
  proxyArgs: readonly ["--private-ip", "--address=0.0.0.0", "--port=5432"];
}

export interface SqlProviderCapabilities {
  /** True only when `GCP.SQL.Instance` accepts `privateNetwork` and passes it to Cloud SQL. */
  privateNetwork: boolean;
}

/** Capability of the pin used by this lane. Do not flip this until the provider passes the field through. */
export const ALCHEMY_GCP_BETA_84_SQL: SqlProviderCapabilities = { privateNetwork: false };

export type CloudSqlPlan =
  | { declare: false; reason: string }
  | { declare: true; attachment: CloudSqlPrivateAttachment };

function attachmentName(value: unknown): string | undefined {
  return typeof value === "string" && value.trim() ? value.trim() : undefined;
}

export function cloudSqlPlan(
  input: { network?: unknown; allocatedIpRange?: unknown },
  provider: SqlProviderCapabilities = ALCHEMY_GCP_BETA_84_SQL,
): CloudSqlPlan {
  if (!provider.privateNetwork) {
    return {
      declare: false,
      reason:
        "96bulls: Cloud SQL is not declared. This Alchemy GCP provider cannot attach a private network, and a public instance is refused. Nothing was created.",
    };
  }
  const network = attachmentName(input.network);
  const allocatedIpRange = attachmentName(input.allocatedIpRange);
  if (!network || !allocatedIpRange) {
    throw new Error(
      "96bulls: Cloud SQL private attachment needs the VPC network and the Private Service Access range. Nothing was created.",
    );
  }
  return {
    declare: true,
    attachment: {
      ipv4Enabled: false,
      privateNetwork: network,
      allocatedIpRange,
      proxyArgs: ["--private-ip", "--address=0.0.0.0", "--port=5432"],
    },
  };
}
