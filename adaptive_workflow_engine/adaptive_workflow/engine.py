"""
Adaptive Workflow Engine (simple base)

Features
- Tasks with dependencies (DAG), executed in dependency order
- Conditional execution (skip a task based on shared context)
- Retries with exponential backoff
- Adaptive behaviour:
    * learns per-task average duration and failure rate (persisted to JSON)
    * flaky tasks automatically get extra retries on later runs
    * among ready tasks, historically fastest run first
    * tasks can add new tasks at runtime (dynamic workflow)
- Fallback task when a task finally fails
"""
from __future__ import annotations

import json
import time
from dataclasses import dataclass, field
from pathlib import Path
from typing import Any, Callable, Optional


@dataclass
class Task:
    name: str
    func: Callable[[dict, "Engine"], Any]
    deps: list[str] = field(default_factory=list)
    condition: Optional[Callable[[dict], bool]] = None   # run only if True
    retries: int = 1
    fallback: Optional[Callable[[dict, "Engine"], Any]] = None
    status: str = "pending"   # pending | success | failed | skipped
    result: Any = None


class Stats:
    """Remembers how each task behaved in past runs."""

    def __init__(self, path: str | None = None):
        self.path = Path(path) if path else None
        self.data: dict[str, dict] = {}
        if self.path and self.path.exists():
            self.data = json.loads(self.path.read_text())

    def record(self, name: str, duration: float, ok: bool):
        s = self.data.setdefault(name, {"runs": 0, "fails": 0, "avg": 0.0})
        s["runs"] += 1
        s["fails"] += 0 if ok else 1
        s["avg"] += (duration - s["avg"]) / s["runs"]   # running mean
        if self.path:
            self.path.write_text(json.dumps(self.data, indent=2))

    def avg_time(self, name: str) -> float:
        return self.data.get(name, {}).get("avg", 0.0)

    def fail_rate(self, name: str) -> float:
        s = self.data.get(name)
        return s["fails"] / s["runs"] if s and s["runs"] else 0.0


class Engine:
    def __init__(self, stats_file: str | None = None, backoff: float = 0.1):
        self.tasks: dict[str, Task] = {}
        self.ctx: dict[str, Any] = {}
        self.stats = Stats(stats_file)
        self.backoff = backoff

    # ---- building the workflow -------------------------------------------
    def add_task(self, task: Task) -> Task:
        self.tasks[task.name] = task
        return task

    def task(self, name=None, deps=(), **kw):
        """Decorator: @engine.task(deps=["a"], retries=2)"""
        def wrap(fn):
            self.add_task(Task(name or fn.__name__, fn, list(deps), **kw))
            return fn
        return wrap

    # ---- adaptive helpers --------------------------------------------------
    def _effective_retries(self, t: Task) -> int:
        # flaky history -> extra retries (up to +3)
        return t.retries + min(3, int(self.stats.fail_rate(t.name) * 4))

    def _ready(self) -> list[Task]:
        ready = [
            t for t in self.tasks.values()
            if t.status == "pending"
            and all(self.tasks[d].status in ("success", "skipped") for d in t.deps)
        ]
        return sorted(ready, key=lambda t: self.stats.avg_time(t.name))  # fastest first

    # ---- execution ---------------------------------------------------------
    def _run_task(self, t: Task):
        if t.condition and not t.condition(self.ctx):
            t.status = "skipped"
            print(f"  - {t.name}: skipped (condition false)")
            return
        attempts = self._effective_retries(t) + 1
        for i in range(1, attempts + 1):
            start = time.time()
            try:
                t.result = t.func(self.ctx, self)
                self.stats.record(t.name, time.time() - start, True)
                t.status = "success"
                print(f"  ✓ {t.name} (attempt {i}/{attempts})")
                return
            except Exception as e:
                self.stats.record(t.name, time.time() - start, False)
                print(f"  ✗ {t.name} attempt {i}/{attempts} failed: {e}")
                if i < attempts:
                    time.sleep(self.backoff * 2 ** (i - 1))
        if t.fallback:
            try:
                t.result = t.fallback(self.ctx, self)
                t.status = "success"
                print(f"  ↺ {t.name}: fallback succeeded")
                return
            except Exception as e:
                print(f"  ✗ {t.name} fallback failed: {e}")
        t.status = "failed"

    def run(self) -> dict[str, str]:
        missing = {d for t in self.tasks.values() for d in t.deps} - self.tasks.keys()
        if missing:
            raise ValueError(f"Unknown dependencies: {missing}")
        print("Running workflow...")
        while True:
            ready = self._ready()
            if not ready:
                break
            for t in ready:
                self._run_task(t)
        for t in self.tasks.values():           # blocked by failed deps / cycles
            if t.status == "pending":
                t.status = "skipped"
                print(f"  - {t.name}: skipped (blocked or cyclic dependency)")
        return {n: t.status for n, t in self.tasks.items()}


