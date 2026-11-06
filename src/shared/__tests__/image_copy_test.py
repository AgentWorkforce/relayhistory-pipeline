import base64
import contextlib
import hashlib
import importlib.util
import json
import os
import sys
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
        self.assertEqual([args[1] for args in calls], ["copy", "inspect"])
        self.assertIn("--all", calls[0])
        self.assertIn("--preserve-digests", calls[0])

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
