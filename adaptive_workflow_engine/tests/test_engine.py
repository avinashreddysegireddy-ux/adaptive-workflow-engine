import os, sys, tempfile, unittest
sys.path.insert(0, os.path.join(os.path.dirname(__file__), ".."))
from adaptive_workflow import Engine, Task


class EngineTests(unittest.TestCase):
    def test_dependency_order(self):
        e, order = Engine(backoff=0), []
        e.add_task(Task("b", lambda c, en: order.append("b"), deps=["a"]))
        e.add_task(Task("a", lambda c, en: order.append("a")))
        e.run()
        self.assertEqual(order, ["a", "b"])

    def test_condition_skips(self):
        e = Engine(backoff=0)
        e.add_task(Task("x", lambda c, en: 1, condition=lambda c: False))
        self.assertEqual(e.run()["x"], "skipped")

    def test_retry_then_success(self):
        e, calls = Engine(backoff=0), []
        def flaky(c, en):
            calls.append(1)
            if len(calls) < 3:
                raise RuntimeError("fail")
        e.add_task(Task("f", flaky, retries=3))
        self.assertEqual(e.run()["f"], "success")

    def test_fallback(self):
        e = Engine(backoff=0)
        def bad(c, en): raise RuntimeError("x")
        e.add_task(Task("t", bad, retries=0, fallback=lambda c, en: "ok"))
        self.assertEqual(e.run()["t"], "success")

    def test_failed_dep_blocks_children(self):
        e = Engine(backoff=0)
        def bad(c, en): raise RuntimeError("x")
        e.add_task(Task("a", bad, retries=0))
        e.add_task(Task("b", lambda c, en: 1, deps=["a"]))
        r = e.run()
        self.assertEqual((r["a"], r["b"]), ("failed", "skipped"))

    def test_dynamic_tasks_and_stats(self):
        path = os.path.join(tempfile.mkdtemp(), "s.json")
        e = Engine(stats_file=path, backoff=0)
        def parent(c, en):
            en.add_task(Task("child", lambda c2, e2: 1, deps=["parent"]))
        e.add_task(Task("parent", parent))
        r = e.run()
        self.assertEqual(r["child"], "success")
        self.assertTrue(os.path.exists(path))


if __name__ == "__main__":
    unittest.main()
