#!/usr/bin/env python3
"""Run SQLStore integration tests against a new disposable local PostgreSQL.

Uses an already available postgres:16 image, a private random password, a
loopback-only randomly assigned port, and tmpfs. Never attaches an existing
database or host data volume. The container is removed even when tests fail.
"""
import json
import os
import secrets
import signal
import subprocess
import sys
import time
from pathlib import Path

def main():
    name = "goim-remote-test-" + secrets.token_hex(6)
    password = secrets.token_urlsafe(32)
    env = os.environ.copy()
    env["POSTGRES_PASSWORD"] = password
    started = False
    def interrupted(_signum, _frame):
        raise SystemExit(130)
    signal.signal(signal.SIGTERM, interrupted)
    signal.signal(signal.SIGINT, interrupted)
    try:
        subprocess.run(
            ["docker", "run", "--pull=never", "--rm", "-d", "--name", name,
             "--network", "bridge", "-p", "127.0.0.1::5432",
             "--tmpfs", "/var/lib/postgresql/data:rw,size=256m,mode=0700",
             "-e", "POSTGRES_PASSWORD", "postgres:16"],
            env=env, check=True, stdout=subprocess.DEVNULL,
        )
        started = True
        for _ in range(120):
            ready = subprocess.run(
                ["docker", "exec", name, "pg_isready", "-q",
                 "-U", "postgres", "-h", "127.0.0.1"],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            )
            if ready.returncode == 0:
                break
            time.sleep(0.25)
        else:
            raise RuntimeError("disposable PostgreSQL did not become ready")
        ports = json.loads(subprocess.check_output(
            ["docker", "inspect", "--format", "{{json .NetworkSettings.Ports}}", name],
            text=True,
        ))
        binding = ports["5432/tcp"][0]
        if binding["HostIp"] != "127.0.0.1":
            raise RuntimeError("test database is not bound to loopback")
        env["GOIM_REMOTE_TEST_POSTGRES_DSN"] = (
            "postgres://postgres:" + password + "@127.0.0.1:"
            + binding["HostPort"] + "/postgres?sslmode=disable"
        )
        print("Running SQLite and disposable PostgreSQL integration tests.", flush=True)
        result = subprocess.run(
            ["go", "test", "-race", "-v", "./..."],
            cwd=Path(__file__).resolve().parent, env=env,
        )
        return result.returncode
    finally:
        if started:
            subprocess.run(
                ["docker", "rm", "-f", name],
                stdout=subprocess.DEVNULL, stderr=subprocess.DEVNULL,
            )
            print("Removed disposable PostgreSQL test container.", flush=True)

if __name__ == "__main__":
    sys.exit(main())
