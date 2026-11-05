"""Offline only. No op, Wrangler profile, SSH, Cloudflare or credential access."""
import contextlib
import io
import copy
import hashlib
import importlib.util
import json
import os
import pathlib
import subprocess
import sys
import unittest

HERE = pathlib.Path(__file__).resolve().parent
spec = importlib.util.spec_from_file_location("broker", HERE / "state-bootstrap-broker.py")
b = importlib.util.module_from_spec(spec)
spec.loader.exec_module(b)
GEN, SHA, TOKEN = "a" * 64, "b" * 40, "synthetic-cf-bearer-not-a-real-token"
URL = "https://alchemy-state-store-96bulls.synthetic.workers.dev"
ITEM = "a" * 26


def item(content="pending", id=None):
    value = {"title": "alchemy-state-store-96bulls bootstrap " + GEN, "category": "SECURE_NOTE",
             "vault": {"id": b.VAULT}, "fields": [{"id": "bootstrap_recovery", "type": "CONCEALED", "value": content}]}
    if id:
        value["id"] = id
    return value


def envelope(status="created"):
    return {"protocol": "private-state-bootstrap-v1", "status": status, "generation": GEN,
            "accountId": b.CF_ACCOUNT, "url": URL, "authToken": "c" * 64,
            "encodedBootstrapState": {k: {"synthetic": True} for k in b.STATE_KEYS}}


class FakeOp:
    def __init__(self):
        self.calls = []
        self.value = None
    def __call__(self, args, raw):
        self.calls.append((args, raw))
        if args[1] in ("create", "edit"):
            self.value = json.loads(raw)
            self.value["id"] = ITEM
        return copy.deepcopy(self.value)


def reserve(policy, runner):
    return policy.execute(["item", "create", "--vault", b.VAULT, "--format=json", "-"], json.dumps(item()), runner)


def edit(policy, runner, content, stopping=False):
    return policy.execute(["item", "edit", ITEM, "--vault", b.VAULT, "--format=json"],
                          json.dumps(item(content, ITEM)), runner, stopping=stopping)


def plan_result():
    return {"operation": "captured-only", "sourceCommit": SHA, "generation": GEN,
            "capture": {"provenanceSha256": GEN, "planSha256": GEN, "inputsSha256": GEN, "sourceCommit": SHA}}


class PolicyTests(unittest.TestCase):
    def setUp(self):
        self.policy = b.RecoveryPolicy(GEN, URL, TOKEN)
        self.runner = FakeOp()

    def test_actual_new_item_sequence_and_secret_readback(self):
        reserve(self.policy, self.runner)
        value = self.policy.execute(["item", "get", ITEM, "--vault", b.VAULT, "--format=json", "--reveal"], None, self.runner)
        self.assertEqual(value["id"], ITEM)
        edit(self.policy, self.runner, json.dumps(envelope()))
        self.assertTrue(self.policy.recovery_written)
        self.assertEqual(self.policy.last_status, "created")
        self.assertEqual(len(self.runner.calls), 3)
        self.assertNotIn(TOKEN, json.dumps(self.runner.calls))

    def test_no_old_item_get_arbitrary_exec_or_second_create(self):
        for args in (["item", "list"], ["item", "get", ITEM, "--reveal"], ["run", "--", "sh"]):
            with self.assertRaises(b.Refused):
                self.policy.execute(args, None, self.runner)
        reserve(self.policy, self.runner)
        with self.assertRaises(b.Refused):
            reserve(self.policy, self.runner)
        self.assertEqual(len(self.runner.calls), 1)

    def test_initial_create_must_be_pending(self):
        with self.assertRaises(b.Refused):
            self.policy.execute(["item", "create", "--vault", b.VAULT, "--format=json", "-"],
                                json.dumps(item(json.dumps(envelope()))), self.runner)
        self.assertEqual(self.runner.calls, [])

    def test_uncertain_create_is_not_retried(self):
        def broken(args, raw):
            raise RuntimeError("synthetic")
        with self.assertRaises(RuntimeError):
            reserve(self.policy, broken)
        with self.assertRaises(b.Refused):
            reserve(self.policy, self.runner)
        self.assertEqual(self.runner.calls, [])

    def test_foreign_url_vault_item_fields_and_api_token_refused_before_op(self):
        reserve(self.policy, self.runner)
        bad = envelope()
        bad["url"] = URL.replace("synthetic", "foreign")
        values = [item(json.dumps(bad), ITEM), item(json.dumps(envelope()), "z" * 26)]
        v = item(json.dumps(envelope()), ITEM); v["vault"]["id"] = "z" * 26; values.append(v)
        v = item(json.dumps(envelope()), ITEM); v["fields"].append({"id": "unexpected", "type": "STRING", "value": "x"}); values.append(v)
        v = envelope(); v["encodedBootstrapState"]["Api"] = {"bearer": TOKEN}; values.append(item(json.dumps(v).replace("synthetic", "\\u0073ynthetic"), ITEM))
        for value in values:
            with self.subTest(value=value), self.assertRaises(b.Refused):
                self.policy.execute(["item", "edit", ITEM, "--vault", b.VAULT, "--format=json"], json.dumps(value), self.runner)
        self.assertEqual(len(self.runner.calls), 1)

    def test_one_password_output_fields_are_checked(self):
        def contaminated(args, raw):
            value = item(id=ITEM)
            value["fields"][0]["type"] = "STRING"
            return value
        with self.assertRaises(b.Refused):
            reserve(self.policy, contaminated)

    def test_stop_allows_same_item_partial_only(self):
        with self.assertRaises(b.Refused):
            self.policy.execute(["item", "create", "--vault", b.VAULT, "--format=json", "-"],
                                json.dumps(item()), self.runner, stopping=True)
        reserve(self.policy, self.runner)
        with self.assertRaises(b.Refused):
            edit(self.policy, self.runner, json.dumps(envelope()), stopping=True)
        edit(self.policy, self.runner, json.dumps(envelope("partial")), stopping=True)
        with self.assertRaises(b.Refused):
            edit(self.policy, self.runner, "pending")

    def test_result_is_exact_allowlist_and_created_requires_complete_recovery(self):
        value = plan_result()
        self.assertEqual(b.public_result(value, SHA, None, TOKEN), value)
        for field in ("authToken", "debug"):
            bad = {**value, field: "concealed-secret"}
            with self.assertRaises(b.Refused):
                b.public_result(bad, SHA, None, TOKEN)
        reserve(self.policy, self.runner)
        value = {"operation": "created-control-store", "generation": GEN, "url": URL, "credentialItemId": ITEM,
                 "controlResources": 6, "ingestApplied": False, "edgePreview": False}
        with self.assertRaises(b.Refused):
            b.public_result(value, SHA, self.policy, TOKEN)
        edit(self.policy, self.runner, json.dumps(envelope()))
        self.assertEqual(b.public_result(value, SHA, self.policy, TOKEN), value)
        edit(self.policy, self.runner, json.dumps(envelope("partial")))
        with self.assertRaises(b.Refused):
            b.public_result(value, SHA, self.policy, TOKEN)

    def test_capture_binds_exact_url_and_identity(self):
        capture = {"schema": 1, "kind": "private-state-bootstrap-v1", "stage": "alchemy-state-store-96bulls",
                   "generation": GEN, "source": {"commit": SHA},
                   "target": {"accountId": b.CF_ACCOUNT, "secretsStoreId": "d" * 32, "onePasswordVaultId": b.VAULT,
                              "stateScript": "alchemy-state-store-96bulls", "stateVersion": 7},
                   "admission": {"accountId": b.CF_ACCOUNT, "storeId": "d" * 32, "subdomain": "synthetic",
                                 "workerAbsent": True, "secretsAbsent": True, "namespaceAbsent": True}}
        raw = json.dumps(capture).encode()
        self.assertEqual(b.policy_from_capture(raw, hashlib.sha256(raw).hexdigest(), SHA, TOKEN).url, URL)
        for key in ("workerAbsent", "secretsAbsent", "namespaceAbsent"):
            bad = copy.deepcopy(capture); bad["admission"][key] = False
            raw = json.dumps(bad).encode()
            with self.assertRaises(b.Refused):
                b.policy_from_capture(raw, hashlib.sha256(raw).hexdigest(), SHA, TOKEN)

    def test_op_environment_no_other_auth_or_cf_token(self):
        value = b.op_environment({"PATH": "/bin", "HOME": "/synthetic", "CLOUDFLARE_API_TOKEN": TOKEN,
                                 "OP_CONNECT_TOKEN": "x", "OP_SERVICE_ACCOUNT_TOKEN": "x", "OP_SESSION_abc": "x",
                                 "OTHER": TOKEN}, TOKEN)
        self.assertEqual(value, {"PATH": "/bin", "HOME": "/synthetic"})

    def test_duplicate_keys_and_nonfinite_json_refuse(self):
        for raw in ('{"id":1,"id":2}', '{"id":NaN}'):
            with self.assertRaises(b.Refused):
                b.parse(raw)


class BridgeTests(unittest.TestCase):
    def child(self, frames, status=0, tail="", wait_input=True):
        code = "import sys,json; sys.stdin.buffer.readline(); " + (
            "sys.stdout.write(" + (repr("".join(json.dumps(f) + "\n" for f in frames)) + " + " + (repr(tail) if len(tail) < 10000 else "(\"x\" * " + str(len(tail)) + ")")) + "); sys.stdout.flush(); " +
            ("sys.stdin.buffer.read(); " if wait_input else "") + "sys.exit(" + str(status) + ")")
        return subprocess.Popen([sys.executable, "-c", code], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)

    def run_bridge(self, child, policy=None, runner=None):
        try:
            return b.bridge(child, SHA, TOKEN, policy, runner or FakeOp(), timeout=3 if policy else 0.3)
        finally:
            child.stdout.close()

    def test_success_waits_for_zero_child_exit(self):
        frame = {"kind": "result", "value": plan_result()}
        self.assertEqual(self.run_bridge(self.child([frame]))["operation"], "captured-only")
        captured = io.StringIO()
        with contextlib.redirect_stdout(captured), self.assertRaises(b.Refused):
            self.run_bridge(self.child([frame], status=7))
        self.assertEqual(captured.getvalue(), "")

    def test_extra_result_frame_secret_and_nonzero_never_publish(self):
        frame = {"kind": "result", "value": plan_result()}
        for frames in ([frame, frame], [{"kind": "result", "value": {**plan_result(), "authToken": "hidden"}}],
                       [{"kind": "one_password", "id": 1, "args": ["item", "list"], "input": None}]):
            with self.assertRaises(b.Refused):
                self.run_bridge(self.child(frames, wait_input=False))

    def test_truncated_or_oversized_or_timeout_refuses_and_reaps(self):
        for tail in ("{", "x" * (b.FRAME_LIMIT + 1)):
            child = self.child([], tail=tail, wait_input=False)
            with self.assertRaises((b.Refused, ValueError)):
                self.run_bridge(child)
            self.assertIsNotNone(child.returncode)
        child = self.child([], wait_input=True)
        with self.assertRaises(b.Refused):
            self.run_bridge(child)
        self.assertIsNotNone(child.returncode)


    def test_stop_drains_only_same_item_partial_recovery(self):
        create = {"kind": "one_password", "id": 1, "args": ["item", "create", "--vault", b.VAULT, "--format=json", "-"], "input": json.dumps(item())}
        get = {"kind": "one_password", "id": 2, "args": ["item", "get", ITEM, "--vault", b.VAULT, "--format=json", "--reveal"], "input": None}
        patch = {"kind": "one_password", "id": 3, "args": ["item", "edit", ITEM, "--vault", b.VAULT, "--format=json"], "input": json.dumps(item(json.dumps(envelope("partial")), ITEM))}
        code = """
import sys,json
def read(): return json.loads(sys.stdin.buffer.readline())
def send(v): print(json.dumps(v),flush=True)
read()
send(CREATE)
assert read()["ok"]
send({"kind":"malformed"})
assert read() == {"kind":"stop"}
send(GET)
assert read()["ok"]
send(PATCH)
assert read()["ok"]
send({"kind":"refused","code":"synthetic_stop"})
sys.exit(1)
"""
        code = code.replace("CREATE", repr(create)).replace("GET", repr(get)).replace("PATCH", repr(patch))
        child = subprocess.Popen([sys.executable, "-c", code], stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
        policy, runner = b.RecoveryPolicy(GEN, URL, TOKEN), FakeOp()
        with self.assertRaises(b.Refused):
            self.run_bridge(child, policy, runner)
        self.assertEqual([call[0][1] for call in runner.calls], ["create", "get", "edit"])
        self.assertEqual(policy.last_status, "partial")
        self.assertEqual(child.returncode, 1)

    def test_child_initial_exit_is_always_reaped(self):
        child = subprocess.Popen([sys.executable, "-c", "raise SystemExit(9)"],
                                 stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
        child.wait(timeout=2)
        with self.assertRaises(b.Refused):
            self.run_bridge(child)
        self.assertEqual(child.returncode, 9)

    def test_actual_bun_private_transport_and_native_sink_no_network(self):
        child = subprocess.Popen(["bun", str(HERE / "fixtures/state-bootstrap-stdio-proof.ts")],
                                 stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
        policy = b.RecoveryPolicy(GEN, URL, TOKEN)
        runner = FakeOp()
        value = self.run_bridge(child, policy, runner)
        self.assertEqual(value["credentialItemId"], ITEM)
        self.assertEqual([call[0][1] for call in runner.calls], ["create", "get", "edit", "get", "edit", "get"])
        self.assertTrue(policy.recovery_written)
        self.assertNotIn(TOKEN, json.dumps(runner.calls))


if __name__ == "__main__":
    unittest.main()
