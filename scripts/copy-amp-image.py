#!/usr/bin/env python3
"""Copy only a verified native image, using Linux anonymous-memory auth."""
import base64
import contextlib
import fcntl
import hashlib
import json
import os
from pathlib import Path
import re
import subprocess
import sys
import time


def token_expiry(token, now):
    try:
        parts = token.split(".")
        if len(parts) != 3:
            raise ValueError()
        payload = json.loads(base64.urlsafe_b64decode(parts[1] + "=" * (-len(parts[1]) % 4)))
        expiry = payload["exp"]
        if type(expiry) is not int or expiry - now < 900:
            raise ValueError()
        return expiry
    except (ValueError, KeyError, TypeError):
        raise RuntimeError("CF registry credential must retain at least 15 minutes of validity") from None


@contextlib.contextmanager
def memory_auth(github_actor, github_token, registry_token):
    if not hasattr(os, "memfd_create"):
        raise RuntimeError("Linux anonymous-memory authentication required")
    descriptor = os.memfd_create("amp-registry-auth", os.MFD_CLOEXEC | os.MFD_ALLOW_SEALING)
    try:
        entries = {host: {"auth": base64.b64encode(f"{user}:{secret}".encode()).decode()}
                   for host, user, secret in [("ghcr.io", github_actor, github_token),
                                               ("registry.cloudflare.com", "v1", registry_token)]}
        os.write(descriptor, json.dumps({"auths": entries}).encode())
        fcntl.fcntl(descriptor, fcntl.F_ADD_SEALS,
                    fcntl.F_SEAL_WRITE | fcntl.F_SEAL_GROW | fcntl.F_SEAL_SHRINK | fcntl.F_SEAL_SEAL)
        yield descriptor, f"/proc/self/fd/{descriptor}"
    finally:
        os.close(descriptor)


def validate_plan(env, plan):
    for name, value, pattern in [("source", plan.get("source_sha"), r"[a-f0-9]{40}"),
                                 ("index", plan.get("signed_index_digest"), r"sha256:[a-f0-9]{64}"),
                                 ("native", plan.get("linux_amd64_digest"), r"sha256:[a-f0-9]{64}"),
                                 ("account", plan.get("account_id"), r"[a-f0-9]{32}")]:
        if not isinstance(value, str) or not re.fullmatch(pattern, value):
            raise RuntimeError("Verified copy plan identity invalid")
    if (plan["source_sha"] != env.get("SOURCE_SHA") or plan["signed_index_digest"] != env.get("IMAGE_DIGEST")
            or plan["account_id"] != env.get("CLOUDFLARE_ACCOUNT_ID")
            or plan.get("publication_run_id") != env.get("PUBLISH_RUN_ID")
            or plan.get("copy_workflow_sha") != env.get("GITHUB_SHA")
            or env.get("GITHUB_ACTOR") != "khaliqgant"
            or not env.get("GH_TOKEN") or not env.get("CF_REGISTRY_TOKEN")):
        raise RuntimeError("Verified plan and runtime authority differ")


def copy_image(env, run=subprocess.run, auth=memory_auth, clock=time.time):
    root = Path(env.get("ARTIFACT_DIR", "."))
    if not root.is_absolute():
        raise RuntimeError("Verified artifact directory required")
    plan = json.loads((root / "amp-copy-plan.json").read_text())
    validate_plan(env, plan)
    expiry = token_expiry(env["CF_REGISTRY_TOKEN"], clock())
    allowed = {"PATH", "HOME", "TMPDIR", "SSL_CERT_FILE", "SSL_CERT_DIR", "HTTP_PROXY",
               "HTTPS_PROXY", "NO_PROXY", "http_proxy", "https_proxy", "no_proxy"}
    child_env = {key: value for key, value in env.items() if key in allowed}
    native = plan["linux_amd64_digest"]
    repo = f"registry.cloudflare.com/{plan['account_id']}/96bulls-services"
    target = f"docker://{repo}:sha-{plan['source_sha']}"
    layout = root / "approved-native"
    with auth(env["GITHUB_ACTOR"], env["GH_TOKEN"], env["CF_REGISTRY_TOKEN"]) as (fd, path):
        options = {"check": True, "pass_fds": (fd,), "env": child_env,
                   "stdout": subprocess.PIPE, "stderr": subprocess.PIPE}
        print("Amp registry copy phase: source-copy", flush=True)
        run(["skopeo", "copy", "--preserve-digests", "--authfile", path,
             f"docker://ghcr.io/agentworkforce/96bulls-services@{native}",
             f"oci:{layout}:approved"], timeout=300, **options)
        print("Amp registry copy phase: source-verification", flush=True)
        raw = (layout / "blobs" / "sha256" / native[7:]).read_bytes()
        if "sha256:" + hashlib.sha256(raw).hexdigest() != native:
            raise RuntimeError("Source OCI manifest differs from the signed native descriptor")
        config = run(["skopeo", "inspect", "--config", f"oci:{layout}:approved"],
                     timeout=30, **options)
        config = json.loads(config.stdout)
        if config.get("os") != "linux" or config.get("architecture") != "amd64":
            raise RuntimeError("Native image platform differs from its verified descriptor")
        print("Amp registry copy phase: destination-copy", flush=True)
        token_expiry(env["CF_REGISTRY_TOKEN"], clock())
        run(["skopeo", "copy", "--preserve-digests",
             "--authfile", path, f"oci:{layout}:approved", target], timeout=600, **options)
        for phase, reference in [("tag-verification", target),
                                 ("digest-verification", f"docker://{repo}@{native}")]:
            print("Amp registry copy phase:", phase, flush=True)
            result = run(["skopeo", "inspect", "--raw", "--authfile", path, reference],
                         timeout=60, **options)
            if not isinstance(result.stdout, bytes) or "sha256:" + hashlib.sha256(result.stdout).hexdigest() != native:
                raise RuntimeError("Destination manifest differs from the signed native descriptor")
    return {**plan, "cloudflare_image": f"{repo}@{native}", "registry_credential_expires_at": expiry}


if __name__ == "__main__":
    try:
        receipt = copy_image(os.environ)
        Path(os.environ["ARTIFACT_DIR"], "amp-copy-receipt.json").write_text(json.dumps(receipt, indent=2) + "\n")
        print("Verified native image copied:", receipt["cloudflare_image"])
    except (OSError, ValueError, RuntimeError, subprocess.SubprocessError):
        print("Amp registry copy failed; no verified copy receipt issued", file=sys.stderr)
        sys.exit(1)
