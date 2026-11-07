import base64
import contextlib
import hashlib
import importlib.util
import io
import json
import os
from pathlib import Path
import sys
import subprocess
import tempfile
from types import SimpleNamespace
import unittest
sys.dont_write_bytecode = True
spec = importlib.util.spec_from_file_location("amp_copy", Path(__file__).parents[3] / "scripts/copy-amp-image.py")
module = importlib.util.module_from_spec(spec)
spec.loader.exec_module(module)

class AmpImageCopyTests(unittest.TestCase):
    def setUp(self):
        self.temp = tempfile.TemporaryDirectory(prefix="amp-copy-")
        self.root = Path(self.temp.name)
        self.raw = b'{"schemaVersion":2,"fixture":"native manifest"}'
        self.native = "sha256:" + hashlib.sha256(self.raw).hexdigest()
        self.plan = {"source_sha":"a"*40,"signed_index_digest":"sha256:"+"b"*64,"linux_amd64_digest":self.native,
                     "account_id":"c"*32,"publication_run_id":"123","copy_workflow_sha":"d"*40}
        (self.root/"amp-copy-plan.json").write_text(json.dumps(self.plan))
        payload = base64.urlsafe_b64encode(json.dumps({"exp":4600}).encode()).decode().rstrip("=")
        self.env = {"ARTIFACT_DIR":self.temp.name,"SOURCE_SHA":self.plan["source_sha"],"IMAGE_DIGEST":self.plan["signed_index_digest"],
                    "CLOUDFLARE_ACCOUNT_ID":self.plan["account_id"],"PUBLISH_RUN_ID":"123","GITHUB_SHA":"d"*40,
                    "GITHUB_ACTOR":"khaliqgant","GH_TOKEN":"fixture-github","CF_REGISTRY_TOKEN":"fixture."+payload+".signature"}
        self.calls=[]
    def tearDown(self): self.temp.cleanup()
    @contextlib.contextmanager
    def auth(self,*args): yield 999,"/proc/self/fd/999"
    def fake_run(self,args,**options):
        self.calls.append(args)
        self.assertNotIn("GH_TOKEN",options["env"])
        self.assertNotIn("CF_REGISTRY_TOKEN",options["env"])
        self.assertEqual(options["pass_fds"],(999,))
        self.assertNotIn(self.env["CF_REGISTRY_TOKEN"]," ".join(args))
        if args[1]=="copy" and args[-1].startswith("oci:"):
            target=self.root/"approved-native/blobs/sha256"/self.native[7:]
            target.parent.mkdir(parents=True);target.write_bytes(self.raw)
        if "--config" in args: return SimpleNamespace(stdout=b'{"os":"linux","architecture":"amd64"}')
        return SimpleNamespace(stdout=self.raw)
    def test_decouples_source_and_destination_and_verifies_both_destination_addresses(self):
        receipt=module.copy_image(self.env,self.fake_run,self.auth,lambda:1000)
        self.assertEqual([args[1] for args in self.calls],["copy","inspect","copy","inspect","inspect"])
        self.assertTrue(receipt["cloudflare_image"].endswith("@"+self.native))
        self.assertNotIn("CF_REGISTRY_TOKEN",receipt)
        self.assertEqual(receipt["registry_credential_expires_at"],4600)
    def test_runner_skopeo_1_13_supports_copy_arguments_and_reports_only_fixed_phases(self):
        def skopeo_1_13(args,**options):
            if "--image-parallel-copies" in args:
                raise subprocess.CalledProcessError(1,args,stderr=b"unknown flag: --image-parallel-copies")
            return self.fake_run(args,**options)
        progress=io.StringIO()
        with contextlib.redirect_stdout(progress):
            receipt=module.copy_image(self.env,skopeo_1_13,self.auth,lambda:1000)
        self.assertTrue(receipt["cloudflare_image"].endswith("@"+self.native))
        self.assertEqual(progress.getvalue().splitlines(),[
            "Amp registry copy phase: source-copy",
            "Amp registry copy phase: source-verification",
            "Amp registry copy phase: destination-copy",
            "Amp registry copy phase: tag-verification",
            "Amp registry copy phase: digest-verification",
        ])
    def test_destination_failure_reports_phase_without_client_diagnostics(self):
        sensitive=b"https://registry.example/upload?credential=fixture-private-upload-capability"
        def failed(args,**options):
            if args[1]=="copy" and args[-1].startswith("docker://registry.cloudflare.com/"):
                self.assertEqual(options.get("stdout"),subprocess.PIPE)
                self.assertEqual(options.get("stderr"),subprocess.PIPE)
                raise subprocess.CalledProcessError(17,args,output=sensitive,stderr=sensitive)
            return self.fake_run(args,**options)
        progress,diagnostics=io.StringIO(),io.StringIO()
        with contextlib.redirect_stdout(progress),contextlib.redirect_stderr(diagnostics):
            with self.assertRaises(subprocess.CalledProcessError) as caught:
                module.copy_image(self.env,failed,self.auth,lambda:1000)
        self.assertEqual(progress.getvalue().splitlines()[-1],"Amp registry copy phase: destination-copy")
        self.assertNotIn(sensitive.decode(),progress.getvalue()+diagnostics.getvalue())
        self.assertEqual(diagnostics.getvalue(),"")
        self.assertEqual(caught.exception.stdout,sensitive)
        self.assertEqual(caught.exception.stderr,sensitive)
    def test_expired_or_substituted_authority_never_reaches_registry(self):
        for key,value in [("SOURCE_SHA","main"),("CLOUDFLARE_ACCOUNT_ID","e"*32),("GH_TOKEN",""),("CF_REGISTRY_TOKEN","bad"),("GITHUB_ACTOR","other")]:
            with self.assertRaises((RuntimeError,ValueError)):
                module.copy_image({**self.env,key:value},self.fake_run,self.auth,lambda:1000)
        self.assertEqual(self.calls,[])
        with self.assertRaises(RuntimeError): module.copy_image(self.env,self.fake_run,self.auth,lambda:4500)
        self.assertEqual(self.calls,[])
    def test_failing_registry_client_keeps_capability_url_out_of_inherited_logs(self):
        sensitive="https://registry.example/upload?credential=fixture-private-upload-capability"
        def failed(args,**options):
            self.assertEqual(options.get("stdout"),subprocess.PIPE)
            self.assertEqual(options.get("stderr"),subprocess.PIPE)
            return subprocess.run([sys.executable,"-c","import sys; print("+repr(sensitive)+"); print("+repr(sensitive)+",file=sys.stderr); sys.exit(17)"],
                                  check=True,stdout=options.get("stdout"),stderr=options.get("stderr"),env=options["env"])
        with self.assertRaises(subprocess.CalledProcessError) as caught:
            module.copy_image(self.env,failed,self.auth,lambda:1000)
        self.assertIn(sensitive.encode(),caught.exception.stdout)
        self.assertIn(sensitive.encode(),caught.exception.stderr)
    @unittest.skipUnless(hasattr(os,"memfd_create"),"requires Linux copy runner")
    def test_actual_auth_descriptor_has_both_hosts_is_sealed_and_closes(self):
        import fcntl
        with module.memory_auth("khaliqgant","fixture-gh","fixture-cf") as (fd,path):
            for _ in range(2):
                with open(path) as stream: entries=json.load(stream)["auths"]
                self.assertEqual(set(entries),{"ghcr.io","registry.cloudflare.com"})
            self.assertTrue(fcntl.fcntl(fd,fcntl.F_GET_SEALS)&fcntl.F_SEAL_WRITE)
            with self.assertRaises(OSError):os.write(fd,b"changed")
        with self.assertRaises(OSError):os.fstat(fd)
    def test_destination_tampering_never_returns_receipt(self):
        def changed(args,**options):
            result=self.fake_run(args,**options)
            if "--raw" in args:return SimpleNamespace(stdout=b"changed destination")
            return result
        with self.assertRaisesRegex(RuntimeError,"Destination manifest"):
            module.copy_image(self.env,changed,self.auth,lambda:1000)

if __name__=="__main__": unittest.main()
