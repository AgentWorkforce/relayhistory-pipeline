#!/usr/bin/env python3
"""Copy the signed OCI archive without saving registry credentials to disk."""
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


@contextlib.contextmanager
def memory_auth(actor, token):
    if not hasattr(os, "memfd_create"):
        raise RuntimeError("Linux anonymous-memory authentication is required")
    descriptor = os.memfd_create("registry-auth", os.MFD_CLOEXEC | os.MFD_ALLOW_SEALING)
    try:
        auth = base64.b64encode(f"{actor}:{token}".encode()).decode()
        os.write(descriptor, json.dumps({"auths": {"ghcr.io": {"auth": auth}}}).encode())
        os.lseek(descriptor, 0, os.SEEK_SET)
        fcntl.fcntl(descriptor, fcntl.F_ADD_SEALS,
                    fcntl.F_SEAL_WRITE | fcntl.F_SEAL_GROW | fcntl.F_SEAL_SHRINK | fcntl.F_SEAL_SEAL)
        yield descriptor, f"/proc/self/fd/{descriptor}"
    finally:
        os.close(descriptor)


def copy_image(env, run=subprocess.run, auth_context=memory_auth):
    source, digest, actor, token = (env.get(k, "") for k in
                                    ["SOURCE_SHA", "IMAGE_DIGEST", "GITHUB_ACTOR", "GH_TOKEN"])
    root = Path(env.get("RUNNER_TEMP", "."))
    if not re.fullmatch(r"[a-f0-9]{40}", source) or not re.fullmatch(r"sha256:[a-f0-9]{64}", digest):
        raise RuntimeError("Exact source and signed digest required")
    if actor != "khaliqgant" or not token or not root.is_absolute() or not (root / "services.oci.tar").is_file():
        raise RuntimeError("Approved actor, credential and OCI archive required")
    target = f"docker://ghcr.io/agentworkforce/96bulls-services:sha-{source}"
    # The copy tool needs only its inherited memory descriptor, not GitHub/OIDC
    # credentials in its environment. Preserve normal executable/TLS/proxy setup.
    allowed = {"PATH", "HOME", "TMPDIR", "SSL_CERT_FILE", "SSL_CERT_DIR", "HTTP_PROXY",
               "HTTPS_PROXY", "NO_PROXY", "http_proxy", "https_proxy", "no_proxy"}
    child_env = {key: value for key, value in env.items() if key in allowed}
    with auth_context(actor, token) as (descriptor, path):
        options = {"check": True, "pass_fds": (descriptor,), "env": child_env}
        run(["skopeo", "copy", "--all", "--preserve-digests", "--authfile", path,
             f"oci-archive:{root / 'services.oci.tar'}", target], **options)
        result = run(["skopeo", "inspect", "--raw", "--authfile", path, target],
                     stdout=subprocess.PIPE, **options)
        if not isinstance(result.stdout, bytes) or "sha256:" + hashlib.sha256(result.stdout).hexdigest() != digest:
            raise RuntimeError("Registry index does not match the signed digest")
    return digest


if __name__ == "__main__":
    try:
        print("Copied and verified signed OCI index:", copy_image(os.environ))
    except (OSError, RuntimeError, subprocess.SubprocessError):
        print("Reviewed image copy failed; no release receipt issued", file=sys.stderr)
        sys.exit(1)
