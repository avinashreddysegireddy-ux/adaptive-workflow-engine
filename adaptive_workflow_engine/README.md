# Adaptive Workflow Engine

A small workflow engine in pure Python (3.9+) that runs tasks in dependency
order and adapts based on what it learns from past runs.

## Features
- Task dependencies (DAG)
- Conditional execution (`condition=lambda ctx: ...`)
- Retries with exponential backoff, plus optional `fallback`
- Adaptive behaviour:
  - Learns average duration and failure rate per task (`workflow_stats.json`)
  - Flaky tasks automatically get extra retries (up to +3)
  - Fastest historical tasks run first among ready tasks
  - Tasks can add new tasks at runtime

## Structure
    adaptive_workflow/engine.py   core engine (Engine, Task, Stats)
    examples/demo.py              runnable demo
    tests/test_engine.py          unit tests

## Run
    python examples/demo.py
    python -m unittest discover tests

## Usage
    from adaptive_workflow import Engine

    engine = Engine(stats_file="workflow_stats.json")

    @engine.task()
    def fetch(ctx, eng): ctx["n"] = 5

    @engine.task(deps=["fetch"], condition=lambda c: c["n"] > 3, retries=2)
    def process(ctx, eng): ...

    print(engine.run())
