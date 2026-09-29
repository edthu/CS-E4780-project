"""Bring the Compose stack up and down for a test run, and find its containers.

Every run uses the main docker-compose.yml plus tests/docker-compose.test.yml
under its own project name, so the normal stack (and its volumes) is never
touched. Containers are reached by their bridge IP, which is routable from a
Linux host; that works for any `--scale streams=N` without publishing ports.
"""

from __future__ import annotations

import os
import subprocess
import time
from dataclasses import dataclass
from pathlib import Path

import docker
import requests

ROOT = Path(__file__).resolve().parents[2]
WORK_DIR = ROOT / "tests" / ".work"
OUTPUT_DIR = WORK_DIR / "output"
EVENTS_FILE = OUTPUT_DIR / "events.ndjson"
COMPOSE_FILES = [ROOT / "docker-compose.yml", ROOT / "tests" / "docker-compose.test.yml"]

KAFKA_BOOTSTRAP = "localhost:29092"
UI_URL = "http://localhost:18501"
JMX_PORT = 9404
QUERY_PORT = 7070
JVM_SERVICES = ("kafka", "streams", "producer", "consumer")
APP_SERVICES = ("kafka", "kafka-init", "streams", "producer", "consumer", "ui")
JARS = ("producer", "consumer", "streams")


@dataclass(frozen=True)
class Container:
    service: str
    name: str
    id: str
    pid: int
    ip: str

    @property
    def cgroup_dir(self) -> Path:
        # "0::/system.slice/docker-<id>.scope" on cgroup v2; resolving through
        # the pid works for both the systemd and cgroupfs Docker drivers.
        line = Path(f"/proc/{self.pid}/cgroup").read_text().strip().splitlines()[-1]
        return Path("/sys/fs/cgroup") / line.split("::", 1)[1].lstrip("/")


class Stack:
    def __init__(self, project: str):
        self.project = project
        self.client = docker.from_env()

    # -- lifecycle -----------------------------------------------------------

    def compose(self, *args: str, check: bool = True) -> subprocess.CompletedProcess:
        cmd = ["docker", "compose", "-p", self.project]
        for f in COMPOSE_FILES:
            cmd += ["-f", str(f)]
        return subprocess.run(cmd + list(args), cwd=ROOT, check=check,
                              capture_output=True, text=True)

    def build_jars(self) -> None:
        if os.getenv("E2E_SKIP_BUILD") == "1":
            return
        tasks = [f"{m}/assembly" for m in JARS]
        subprocess.run(["sbt", "-batch", *tasks], cwd=ROOT, check=True)

    def up(self, streams_replicas: int = 1, build_images: bool = True) -> None:
        """Start from a clean slate: no topics, no state, an empty input file."""
        missing = [p for p in (ROOT / "tests" / "jmx" / "agent.jar",) if not p.exists()]
        if missing:
            raise RuntimeError("JMX agent missing; run tests/jmx/fetch-agent.sh first")
        self.down()
        OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
        EVENTS_FILE.write_text("")
        if build_images:
            self.compose("build", "producer", "consumer", "streams", "ui")
        self.compose("up", "-d", "--scale", f"streams={streams_replicas}", *APP_SERVICES)
        self.wait_streams_running(streams_replicas)
        self.wait_for(lambda: requests.get(f"{UI_URL}/_stcore/health", timeout=2).ok,
                      "ui healthy", timeout=120)

    def down(self) -> None:
        self.compose("down", "-v", "--remove-orphans", check=False)

    def scale_streams(self, replicas: int) -> None:
        self.compose("up", "-d", "--no-recreate", "--scale", f"streams={replicas}", "streams")
        self.wait_streams_running(replicas)

    # -- discovery -----------------------------------------------------------

    def containers(self, service: str | None = None) -> list[Container]:
        filters = {"label": [f"com.docker.compose.project={self.project}"], "status": "running"}
        if service:
            filters["label"].append(f"com.docker.compose.service={service}")
        result = []
        for c in self.client.containers.list(filters=filters):
            c.reload()
            nets = c.attrs["NetworkSettings"]["Networks"]
            ip = next(iter(nets.values()))["IPAddress"] if nets else ""
            result.append(Container(
                service=c.labels["com.docker.compose.service"],
                name=c.name, id=c.id, pid=c.attrs["State"]["Pid"], ip=ip,
            ))
        return sorted(result, key=lambda c: c.name)

    def streams_urls(self) -> list[str]:
        return [f"http://{c.ip}:{QUERY_PORT}" for c in self.containers("streams")]

    # -- waiting -------------------------------------------------------------

    @staticmethod
    def wait_for(predicate, what: str, timeout: float = 90, interval: float = 0.5):
        deadline = time.monotonic() + timeout
        last_exc = None
        while time.monotonic() < deadline:
            try:
                value = predicate()
                if value:
                    return value
            except Exception as exc:  # noqa: BLE001 - retried until the deadline
                last_exc = exc
            time.sleep(interval)
        raise TimeoutError(f"timed out waiting for {what} (last error: {last_exc})")

    def wait_streams_running(self, replicas: int, timeout: float = 180) -> None:
        def all_running() -> bool:
            urls = self.streams_urls()
            if len(urls) != replicas:
                return False
            states = [requests.get(f"{u}/health", timeout=2).json()["state"] for u in urls]
            return all(s == "RUNNING" for s in states)

        self.wait_for(all_running, f"{replicas} streams replica(s) RUNNING", timeout=timeout)
