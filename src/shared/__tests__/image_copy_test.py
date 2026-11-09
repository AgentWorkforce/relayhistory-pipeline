import base64
import contextlib
import hashlib
import importlib.util
import json
import os
import sys
import subprocess
from pathlib import Path
import tempfile
from types import SimpleNamespace
import unittest

sys.dont_write_bytecode = True

spec = importlib.util.spec_from_file_location("image_copy", Path(__file__).parents[3] / "scripts/copy-reviewed-image.py")
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)


class ImageCopyTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="image-copy-fixture-")
        Path(self.temp.name, "services.oci.tar").write_bytes(b"fixture archive")
        self.index = b"fixture index"
        self.env = {"SOURCE_SHA": "a" * 40, "IMAGE_DIGEST": "sha256:" + hashlib.sha256(self.index).hexdigest(),
                    "GITHUB_ACTOR": "khaliqgant", "GH_TOKEN": "fixture-only-token",
                    "ACTIONS_ID_TOKEN_REQUEST_TOKEN": "fixture-oidc", "RUNNER_TEMP": self.temp.name, "PATH": "/usr/bin"}

    def tearDown(self):
        self.temp.cleanup()

    @contextlib.contextmanager
    def auth(self, actor, token):
        self.assertEqual((actor, token), ("khaliqgant", "fixture-only-token"))
        yield 999, "/proc/self/fd/999"

    def test_copy_uses_inherited_descriptor_and_exact_index_without_child_tokens(self):
        calls = []

        def run(args, **options):
            calls.append(args)
            self.assertNotIn("fixture-only-token", " ".join(args))
            self.assertEqual(options["pass_fds"], (999,))
            self.assertTrue(options["check"])
            self.assertNotIn("GH_TOKEN", options["env"])
            self.assertNotIn("ACTIONS_ID_TOKEN_REQUEST_TOKEN", options["env"])
            self.assertEqual(args[args.index("--authfile") + 1], "/proc/self/fd/999")
            return SimpleNamespace(stdout=self.index)

        self.assertEqual(module.copy_image(self.env, run, self.auth), self.env["IMAGE_DIGEST"])
        self.assertEqual([args[1] for args in calls], ["copy", "inspect", "copy", "inspect"])
        for copy in (calls[0], calls[2]):
            self.assertIn("--all", copy)
            self.assertIn("--preserve-digests", copy)
        tag = "sha-" + self.env["SOURCE_SHA"]
        self.assertEqual([args[-1] for args in calls],
                         [f"docker://ghcr.io/agentworkforce/sessions-pipeline:{tag}"] * 2 +
                         [f"docker://ghcr.io/agentworkforce/96bulls-services:{tag}"] * 2)

    def test_repositories_match_the_signed_dual_publish_list(self):
        source = (Path(__file__).parents[3] / "scripts/image-release-provenance.mjs").read_text()
        self.assertEqual(module.REPOSITORIES, ("ghcr.io/agentworkforce/sessions-pipeline", "ghcr.io/agentworkforce/96bulls-services"))
        for repository in module.REPOSITORIES:
            self.assertIn(f'"{repository}"', source)

    def test_second_repository_mismatch_issues_no_success(self):
        calls = []
        def run(args, **options):
            calls.append(args)
            inspected = sum(1 for call in calls if call[1] == "inspect")
            return SimpleNamespace(stdout=self.index if inspected < 2 else b"changed")
        with self.assertRaisesRegex(RuntimeError, "signed digest"):
            module.copy_image(self.env, run, self.auth)
        self.assertTrue(calls[-1][-1].startswith("docker://ghcr.io/agentworkforce/96bulls-services:"))

    def test_foreign_or_malformed_authority_never_calls_registry(self):
        for field, value in [("SOURCE_SHA", "main"), ("IMAGE_DIGEST", "latest"),
                             ("GITHUB_ACTOR", "foreign"), ("GH_TOKEN", "")]:
            def unexpected(*args, **kwargs):
                self.fail("Registry reached before validation")
            with self.assertRaises(RuntimeError):
                module.copy_image({**self.env, field: value}, unexpected, self.auth)

    def test_changed_registry_bytes_cannot_issue_a_success_receipt(self):
        with self.assertRaisesRegex(RuntimeError, "signed digest"):
            module.copy_image(self.env, lambda *args, **kwargs: SimpleNamespace(stdout=b"changed"), self.auth)

    def test_failing_registry_client_keeps_capability_url_out_of_inherited_logs(self):
        sensitive="https://registry.example/upload?credential=fixture-private-upload-capability"
        def failed(args,**options):
            self.assertEqual(options.get("stdout"),subprocess.PIPE)
            self.assertEqual(options.get("stderr"),subprocess.PIPE)
            return subprocess.run([sys.executable,"-c","import sys; print("+repr(sensitive)+"); print("+repr(sensitive)+",file=sys.stderr); sys.exit(17)"],
                                  check=True,stdout=options.get("stdout"),stderr=options.get("stderr"),env=options["env"])
        with self.assertRaises(subprocess.CalledProcessError) as caught:
            module.copy_image(self.env,failed,self.auth)
        self.assertIn(sensitive.encode(),caught.exception.stdout)
        self.assertIn(sensitive.encode(),caught.exception.stderr)
    @unittest.skipUnless(hasattr(os, "memfd_create"), "requires the Linux publisher kernel")
    def test_actual_memory_auth_is_sealed_rereadable_and_closed(self):
        import fcntl
        with module.memory_auth("khaliqgant", "fixture-only-token") as (descriptor, path):
            for _ in range(2):
                with open(path) as stream:
                    auth = json.load(stream)["auths"]["ghcr.io"]["auth"]
                self.assertEqual(base64.b64decode(auth).decode(), "khaliqgant:fixture-only-token")
            self.assertTrue(fcntl.fcntl(descriptor, fcntl.F_GET_SEALS) & fcntl.F_SEAL_WRITE)
            with self.assertRaises(OSError):
                os.write(descriptor, b"modified")
        with self.assertRaises(OSError):
            os.fstat(descriptor)


if __name__ == "__main__":
    unittest.main()
