"""Run from the repository root after cargo test -p minga-ctl -p minga-api."""
import http.server
import json
import os
import pathlib
import pty
import select
import subprocess
import tempfile
import threading
import time

fixture = {"id": "cr-review", "service_id": "web", "kind": "deploy", "status": "pending",
           "changes": [{"field": "image", "before": "web:1", "after": "web:\u202e2\U000e0001"},
                       {"field": "replicas", "before": 1, "after": 2}]}


def check(answer):
    calls = []

    class Handler(http.server.BaseHTTPRequestHandler):
        def do_GET(self):
            calls.append(("GET", self.path))
            self.reply([fixture])

        def do_POST(self):
            calls.append(("POST", self.path))
            self.reply(dict(fixture, status="approved"))

        def reply(self, value):
            payload = json.dumps(value).encode()
            self.send_response(200)
            self.send_header("Content-Length", str(len(payload)))
            self.end_headers()
            self.wfile.write(payload)

        def log_message(self, *_):
            pass

    server = http.server.ThreadingHTTPServer(("127.0.0.1", 0), Handler)
    thread = threading.Thread(target=server.serve_forever, daemon=True)
    thread.start()
    master, slave = pty.openpty()
    try:
        with tempfile.TemporaryDirectory(prefix="minga-134-terminal-") as scratch:
            env = {k: v for k, v in os.environ.items() if not k.startswith("MINGA_")}
            env.update(XDG_CONFIG_HOME=scratch, XDG_STATE_HOME=scratch,
                       MINGA_CTL_ENDPOINT=f"http://127.0.0.1:{server.server_port}")
            child = subprocess.Popen([str(pathlib.Path("target/debug/minga-ctl").resolve()),
                                      "change", "approve", "cr-review"],
                                     stdin=slave, stdout=subprocess.PIPE,
                                     stderr=subprocess.PIPE, env=env)
            try:
                evidence = b""
                deadline = time.monotonic() + 10
                while b"Continue? [y/N]" not in evidence:
                    assert time.monotonic() < deadline, evidence
                    ready, _, _ = select.select([child.stderr], [], [], 0.2)
                    if ready:
                        chunk = os.read(child.stderr.fileno(), 8192)
                        assert chunk, evidence
                        evidence += chunk
                assert len(calls) == 1, calls  # no mutation before user answers
                text = evidence.decode()
                assert 'image: "web:1" -> "web:\\u202e2\\udb40\\udc01"' in text, text
                assert "replicas: 1 -> 2" in text, text
                assert text.isascii(), repr(text)
                os.write(master, answer.encode() + b"\n")
                stdout, stderr = child.communicate(timeout=10)
                if answer == "y":
                    assert child.returncode == 0, stderr
                    assert calls == [("GET", "/minga/change-requests?id=cr-review"),
                                     ("POST", "/minga/change-requests/cr-review/approve")], calls
                else:
                    assert child.returncode != 0 and len(calls) == 1, calls
                    assert b"cancelled; nothing changed" in stderr, stderr
                print(f"PASS answer={answer!r}: diff before prompt, ASCII escapes, calls={calls}")
                print(text)
            finally:
                if child.poll() is None:
                    child.kill()
                    child.wait()
    finally:
        os.close(master)
        os.close(slave)
        server.shutdown()
        server.server_close()
        thread.join()


for response in ("n", "y"):
    check(response)
