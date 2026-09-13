#!/usr/bin/env python3
"""Real official-image supervisor contract; needs Docker, creates no cluster resources."""

import json
import subprocess
import tempfile
import time
import urllib.error
import urllib.request
import uuid
from pathlib import Path

IMAGE = (
    "prom/statsd-exporter:v0.29.0@sha256:"
    "632f705804922d50c1c95ba8ff9c8c0cc18d4bbb0cc265dc4f9ae708271c95b3"
)
ROOT = Path(__file__).resolve().parents[2]


def docker(*args):
    return subprocess.check_output(["docker", *args], text=True).strip()


def main():
    name = "dsh-exporter-supervisor-" + uuid.uuid4().hex[:8]
    with tempfile.TemporaryDirectory(prefix="dsh-exporter-") as directory:
        mapping = Path(directory) / "statsd-mapping.yml"
        mapping.write_text("mappings: []\n")
        try:
            docker(
                "run", "-d", "--name", name, "--user", "10001:10001",
                "--read-only", "--memory", "128m", "--cpus", "0.1",
                "--cap-drop", "ALL", "--security-opt", "no-new-privileges",
                "--tmpfs", "/run/droggol-monitoring:rw,size=16777216,uid=10001,gid=10001,mode=770",
                "-p", "127.0.0.1::9102", "-e", "GOMEMLIMIT=96MiB", "-e", "GOMAXPROCS=1",
                "-v", f"{ROOT / 'scripts/monitoring-exporter.sh'}:/supervisor.sh:ro",
                "-v", f"{mapping}:/mapping.yml:ro", "--entrypoint", "/bin/sh", IMAGE,
                "/supervisor.sh", "--statsd.listen-udp=", "--statsd.listen-tcp=",
                "--statsd.listen-unixgram=/run/droggol-monitoring/statsd.sock",
                "--statsd.unixsocket-mode=660", "--statsd.mapping-config=/mapping.yml",
                "--statsd.event-queue-size=2048", "--statsd.udp-packet-queue-size=256",
            )
            address = docker("port", name, "9102/tcp")

            def wait_ready():
                deadline = time.monotonic() + 20
                while time.monotonic() < deadline:
                    try:
                        with urllib.request.urlopen(f"http://{address}/metrics", timeout=2) as response:
                            assert b"statsd_exporter" in response.read()
                        return
                    except (OSError, urllib.error.URLError):
                        time.sleep(0.2)
                raise AssertionError("exporter did not recover")

            wait_ready()
            original = json.loads(docker("inspect", name))[0]["State"]["Pid"]
            child = docker("exec", name, "pidof", "statsd_exporter")
            docker("exec", name, "kill", "-9", child)
            # The socket survives SIGKILL; the new child must unlink it successfully.
            time.sleep(0.5)
            assert json.loads(docker("inspect", name))[0]["State"]["Running"]
            wait_ready()
            assert docker("exec", name, "pidof", "statsd_exporter") != child
            inspect = json.loads(docker("inspect", name))[0]
            assert inspect["State"]["Pid"] == original and inspect["RestartCount"] == 0
            started = time.monotonic()
            docker("stop", "--time", "5", name)
            assert time.monotonic() - started < 5, "TERM must stop the exporter child promptly"
            assert json.loads(docker("inspect", name))[0]["State"]["ExitCode"] == 0
            print("PASS: child SIGKILL recovers across stale socket; supervisor stays running; TERM exits cleanly")
        finally:
            subprocess.run(["docker", "rm", "-f", name], check=False, stdout=subprocess.DEVNULL)


if __name__ == "__main__":
    main()
