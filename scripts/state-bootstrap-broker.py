"""Fixed Mini -> sf-frame private IPC. No secret files, logs, arbitrary op or old items."""
import argparse
import datetime
import hashlib
import json
import os
import pathlib
import re
import select
import shlex
import subprocess
import time
import tomllib

ACCOUNT = "C33CTNNS4RFQRAIYU4BQ3P3KFI"
VAULT = "6hmhqxvcbcmhh24yahdflbbkge"
CF_ACCOUNT = "f7232cb80f6fab86a95426302af243e4"
FRAME_LIMIT = 20 * 1024 * 1024
STATE_KEYS = {
    "StateStoreAuthTokenValue", "Api", "StateStoreSecrets",
    "AlchemyStateStoreToken", "StateStoreEncryptionKeyValue", "StateStoreEncryptionKey",
}


class Refused(Exception):
    pass


def require(condition):
    if not condition:
        raise Refused()


def parse(raw):
    def pairs(items):
        result = {}
        for key, value in items:
            require(key not in result)
            result[key] = value
        return result
    require(isinstance(raw, (str, bytes)) and len(raw.encode() if isinstance(raw, str) else raw) <= FRAME_LIMIT)
    return json.loads(raw, object_pairs_hook=pairs, parse_constant=lambda _: (_ for _ in ()).throw(Refused()))


def exact(value, names):
    return isinstance(value, dict) and set(value) == set(names.split(","))


def hex_value(value, length):
    return isinstance(value, str) and re.fullmatch("[0-9a-f]{" + str(length) + "}", value) is not None


def no_token(value, token):
    if isinstance(value, str):
        require(token not in value)
    elif isinstance(value, list):
        for item in value:
            no_token(item, token)
    elif isinstance(value, dict):
        for key, item in value.items():
            no_token(key, token)
            no_token(item, token)


class RecoveryPolicy:
    def __init__(self, generation, url, api_token):
        require(hex_value(generation, 64))
        require(isinstance(url, str) and re.fullmatch(r"https://alchemy-state-store-96bulls\.[a-z0-9][a-z0-9-]{0,62}\.workers\.dev", url))
        require(isinstance(api_token, str) and len(api_token) >= 16)
        self.generation, self.url, self.api_token = generation, url, api_token
        self.item_id = None
        self.create_attempted = False
        self.recovery_written = False
        self.last_status = None
        self.title = "alchemy-state-store-96bulls bootstrap " + generation

    def item(self, value, identified):
        require(isinstance(value, dict) and value.get("title") == self.title and
                value.get("category") == "SECURE_NOTE" and value.get("vault", {}).get("id") == VAULT)
        no_token(value, self.api_token)
        if identified:
            require(isinstance(value.get("id"), str) and re.fullmatch(r"[a-z0-9]{26}", value["id"]))
            require(self.item_id is None or value["id"] == self.item_id)
        fields = value.get("fields")
        require(isinstance(fields, list) and len(fields) in (1, 2) and all(isinstance(f, dict) for f in fields))
        recovery = [f for f in fields if f.get("id") == "bootstrap_recovery"]
        require(len(recovery) == 1 and recovery[0].get("type") == "CONCEALED")
        for field in fields:
            require(field.get("id") == "bootstrap_recovery" or
                    (field.get("id") == "notesPlain" and field.get("type") == "STRING" and field.get("value", "") == ""))
        raw = recovery[0].get("value")
        if raw == "pending":
            require(not self.recovery_written)
        else:
            require(isinstance(raw, str) and len(raw.encode()) <= 8 * 1024 * 1024)
            envelope = parse(raw)
            require(exact(envelope, "protocol,status,generation,accountId,url,authToken,encodedBootstrapState"))
            require(envelope["protocol"] == "private-state-bootstrap-v1" and
                    envelope["generation"] == self.generation and envelope["accountId"] == CF_ACCOUNT and
                    envelope["url"] == self.url and envelope["status"] in ("created", "partial"))
            no_token(envelope, self.api_token)
            require(hex_value(envelope["authToken"], 64) or
                    (envelope["status"] == "partial" and envelope["authToken"] is None))
            state = envelope["encodedBootstrapState"]
            require(isinstance(state, dict) and set(state) <= STATE_KEYS)
            if envelope["status"] == "created":
                require(set(state) == STATE_KEYS)
        result = {"title": self.title, "category": "SECURE_NOTE", "vault": {"id": VAULT},
                  "fields": [{"id": "bootstrap_recovery", "label": "bootstrap_recovery",
                              "type": "CONCEALED", "value": raw}]}
        if identified:
            result["id"] = value["id"]
        return result, raw

    def execute(self, args, raw, runner, stopping=False):
        create = ["item", "create", "--vault", VAULT, "--format=json", "-"]
        get = ["item", "get", self.item_id, "--vault", VAULT, "--format=json", "--reveal"]
        edit = ["item", "edit", self.item_id, "--vault", VAULT, "--format=json"]
        creating = args == create and self.item_id is None and not self.create_attempted
        if creating:
            require(not stopping)
            value, content = self.item(parse(raw), False)
            require(content == "pending")  # Reserve before any control resource write.
            self.create_attempted = True  # An uncertain create must never be retried.
        elif self.item_id and args == get:
            require(raw is None)
            value = None
        elif self.item_id and args == edit:
            value, content = self.item(parse(raw), True)
            if stopping:
                require(content != "pending" and parse(content)["status"] == "partial")
        else:
            raise Refused()
        safe_input = json.dumps(value, separators=(",", ":")) if value is not None else None
        result = runner(args, safe_input)
        # Only this invocation's newly created ID can ever become readable/editable.
        if creating:
            self.item(result, True)
            self.item_id = result["id"]
        safe, content = self.item(result, True)
        if content != "pending":
            self.recovery_written = True
            self.last_status = parse(content)["status"]
        return safe


def policy_from_capture(raw, digest, source_sha, token):
    require(hex_value(digest, 64) and len(raw) <= 2 * 1024 * 1024 and hashlib.sha256(raw).hexdigest() == digest)
    capture = parse(raw)
    target, admission = capture.get("target", {}), capture.get("admission", {})
    require(capture.get("schema") == 1 and capture.get("kind") == "private-state-bootstrap-v1" and
            capture.get("stage") == "alchemy-state-store-96bulls" and
            target.get("stateScript") == "alchemy-state-store-96bulls" and target.get("stateVersion") == 7 and
            capture.get("source", {}).get("commit") == source_sha and
            target.get("onePasswordVaultId") == VAULT and target.get("accountId") == CF_ACCOUNT and
            admission.get("accountId") == CF_ACCOUNT and
            admission.get("storeId") == target.get("secretsStoreId") and
            all(admission.get(k) is True for k in ("workerAbsent", "secretsAbsent", "namespaceAbsent")))
    subdomain = admission.get("subdomain")
    require(isinstance(subdomain, str) and re.fullmatch(r"[a-z0-9][a-z0-9-]{0,62}", subdomain))
    return RecoveryPolicy(capture.get("generation"), "https://alchemy-state-store-96bulls." + subdomain + ".workers.dev", token)


def public_result(value, source_sha, policy, token):
    no_token(value, token)
    require(isinstance(value, dict))
    if policy is None:
        require(exact(value, "operation,sourceCommit,generation,capture") and value["operation"] == "captured-only" and
                value["sourceCommit"] == source_sha and hex_value(value["generation"], 64))
        c = value["capture"]
        require(exact(c, "provenanceSha256,planSha256,inputsSha256,sourceCommit") and c["sourceCommit"] == source_sha and
                all(hex_value(c[k], 64) for k in ("provenanceSha256", "planSha256", "inputsSha256")))
        return {"operation": "captured-only", "sourceCommit": source_sha, "generation": value["generation"],
                "capture": {k: c[k] for k in ("provenanceSha256", "planSha256", "inputsSha256", "sourceCommit")}}
    require(exact(value, "operation,generation,url,credentialItemId,controlResources,ingestApplied,edgePreview") and
            value["operation"] == "created-control-store" and value["generation"] == policy.generation and
            value["url"] == policy.url and policy.item_id is not None and value["credentialItemId"] == policy.item_id and
            type(value["controlResources"]) is int and value["controlResources"] == 6 and
            value["ingestApplied"] is False and value["edgePreview"] is False and policy.recovery_written and policy.last_status == "created")
    return {k: value[k] for k in ("operation", "generation", "url", "credentialItemId", "controlResources", "ingestApplied", "edgePreview")}


class Frames:
    """Binary byte bound before JSON allocation; bounded wait, strict UTF8/duplicate keys."""
    def __init__(self, stream, timeout=300, limit=FRAME_LIMIT):
        self.fd = stream.fileno()
        self.buffer = bytearray()
        self.timeout, self.limit = timeout, limit

    def next(self, eof=False):
        end = time.monotonic() + self.timeout
        while True:
            split = self.buffer.find(b"\n")
            if split >= 0:
                require(split <= self.limit)
                raw = bytes(self.buffer[:split])
                del self.buffer[:split + 1]
                return parse(raw.decode("utf-8", errors="strict"))
            require(len(self.buffer) <= self.limit)
            remaining = end - time.monotonic()
            require(remaining > 0 and select.select([self.fd], [], [], remaining)[0])
            chunk = os.read(self.fd, min(65536, self.limit + 1 - len(self.buffer)))
            if not chunk:
                require(eof and not self.buffer)
                return None
            self.buffer.extend(chunk)


def bridge(child, source_sha, token, policy, runner, timeout=300):
    frames = Frames(child.stdout, timeout)
    def send(value):
        raw = json.dumps(value, separators=(",", ":")).encode()
        require(len(raw) <= FRAME_LIMIT)
        child.stdin.write(raw + b"\n")
        child.stdin.flush()
    expected_id, result, failure = 1, None, None
    try:
        send({"accountId": CF_ACCOUNT, "apiToken": token})
        while True:
            frame = frames.next(eof=result is not None)
            if frame is None:
                break
            require(result is None and isinstance(frame, dict))
            if exact(frame, "kind,value") and frame["kind"] == "result":
                result = public_result(frame["value"], source_sha, policy, token)
                # EOF tells the remote pump the result was received. It has already
                # finished its native operation and no further op frames are legal.
                child.stdin.close()
                continue
            require(exact(frame, "kind,id,args,input") and frame["kind"] == "one_password" and
                    policy is not None and type(frame["id"]) is int and frame["id"] == expected_id <= 40)
            expected_id += 1
            try:
                value = policy.execute(frame["args"], frame["input"], runner)
                send({"id": frame["id"], "ok": True, "value": value})
            except Exception:
                send({"id": frame["id"], "ok": False})
                raise Refused()
        require(result is not None)
    except Exception as error:
        failure = error
        try:
            send({"kind": "stop"})
            # A stop aborts CF fetch but leaves this private channel available for
            # the native catch path to save partial control state in THE SAME item.
            # No create or successful public result is possible during recovery.
            frames.timeout = min(timeout, 15)
            for _ in range(8):
                frame = frames.next()
                if exact(frame, "kind,code") and frame["kind"] == "refused":
                    break
                require(policy is not None and exact(frame, "kind,id,args,input") and
                        frame["kind"] == "one_password" and type(frame["id"]) is int and
                        frame["id"] == expected_id <= 40)
                expected_id += 1
                value = policy.execute(frame["args"], frame["input"], runner, stopping=True)
                send({"id": frame["id"], "ok": True, "value": value})
        except Exception:
            pass
    finally:
        if not child.stdin.closed:
            try:
                child.stdin.close()
            except (BrokenPipeError, OSError):
                pass
        try:
            status = child.wait(timeout=10)
        except subprocess.TimeoutExpired:
            child.terminate()  # Only the SSH child created by this invocation.
            status = child.wait(timeout=10)
        require(status == 0 and failure is None and result is not None)
    return result  # Caller prints only after successful wait, never child raw JSON.


def op_environment(environ, token=None):
    # Normal desktop account only: never fall through to service/connect tokens,
    # and never pass the Cloudflare API bearer to the 1P child process.
    return {k: v for k, v in environ.items() if not (
        k.startswith(("CLOUDFLARE_", "CF_", "OP_"))
        or k in ("OP_SESSION", "OP_ACCOUNT", "OP_VAULT")
        or (token is not None and token in v))}


def main():
    parser = argparse.ArgumentParser()
    parser.add_argument("mode", choices=["plan", "create"])
    parser.add_argument("--source-sha", required=True)
    parser.add_argument("--checkout", required=True)
    parser.add_argument("--reviewed-provenance")
    parser.add_argument("--reviewed-provenance-sha")
    p = parser.parse_args()
    require(hex_value(p.source_sha, 40) and re.fullmatch(r"[a-z0-9-]+", p.checkout))
    profile = pathlib.Path.home() / "Library/Preferences/.wrangler/config/default.toml"
    credential = tomllib.loads(profile.read_text())
    expiry = datetime.datetime.fromisoformat(credential["expiration_time"].replace("Z", "+00:00"))
    require(expiry > datetime.datetime.now(datetime.timezone.utc))
    token = credential["oauth_token"]
    require(isinstance(token, str) and re.fullmatch(r"[!-~]{16,8192}", token))
    policy = None
    if p.mode == "create":
        require(p.reviewed_provenance and p.reviewed_provenance_sha)
        with open(p.reviewed_provenance, "rb") as capture:
            raw = capture.read(2 * 1024 * 1024 + 1)
        policy = policy_from_capture(raw, p.reviewed_provenance_sha, p.source_sha, token)
    else:
        require(not p.reviewed_provenance and not p.reviewed_provenance_sha)
    root = "/home/khaliqgant/Projects/AgentWorkforce/sessions-pipeline-worktrees/" + p.checkout
    argv = [p.mode, "--expected-sha", p.source_sha] + (["--capture"] if p.mode == "plan" else
            ["--reviewed-provenance-sha", p.reviewed_provenance_sha])
    command = "cd " + shlex.quote(root) + " && PATH=/home/khaliqgant/.local/share/mise/installs/node/24.21.0/bin:/home/khaliqgant/.local/share/mise/installs/bun/1.4.2/bin:/usr/bin:/bin bun scripts/private-state-bootstrap-stdio.ts " + " ".join(map(shlex.quote, argv))
    def runner(args, raw):
        run = subprocess.run(["/opt/homebrew/bin/op", *args, "--account", ACCOUNT], input=raw,
                             capture_output=True, text=True, timeout=90, env=op_environment(os.environ, token))
        require(run.returncode == 0 and len(run.stdout.encode()) <= FRAME_LIMIT)
        return parse(run.stdout)
    child = subprocess.Popen(["ssh", "-o", "BatchMode=yes", "sf-frame", command],
                             stdin=subprocess.PIPE, stdout=subprocess.PIPE, stderr=subprocess.DEVNULL)
    print(json.dumps(bridge(child, p.source_sha, token, policy, runner), separators=(",", ":")))


if __name__ == "__main__":
    try:
        main()
    except Exception:
        print(json.dumps({"status": "refused", "code": "state_bootstrap_broker_refused"}))
        raise SystemExit(1)
