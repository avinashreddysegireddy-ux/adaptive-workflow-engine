import os, random, sys
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from adaptive_workflow import Engine, Task

engine = Engine(stats_file="workflow_stats.json")


@engine.task()
def fetch_data(ctx, eng):
    ctx["rows"] = random.randint(0, 200)


@engine.task(deps=["fetch_data"], retries=1)
def flaky_api_call(ctx, eng):
    if random.random() < 0.6:
        raise RuntimeError("API timeout")
    ctx["enriched"] = True


@engine.task(deps=["flaky_api_call"], condition=lambda c: c["rows"] > 100)
def heavy_processing(ctx, eng):
    for i in range(3):  # tasks added at runtime
        eng.add_task(Task(f"chunk_{i}",
                          lambda c, e, i=i: c.setdefault("chunks", []).append(i),
                          deps=["heavy_processing"]))


@engine.task(deps=["flaky_api_call"], condition=lambda c: c["rows"] <= 100)
def light_processing(ctx, eng):
    ctx["light"] = True


@engine.task(deps=["heavy_processing", "light_processing"])
def report(ctx, eng):
    print("  Report ctx:", dict(ctx))


if __name__ == "__main__":
    print(engine.run())
