"""Regression tests for the installed extension, not the old standalone prototype."""
import importlib
import json
import os
from pathlib import Path
import sys
import tempfile
import threading
import unittest

EXT = Path(os.environ.get("PI_TRACE_EXTENSION_DIR", str(Path(__file__).resolve().parents[1] / "extensions" / "trace")))
sys.path.insert(0, str(EXT))
from live_index import EventIndex
import serve
import trace_to_html


class LiveTests(unittest.TestCase):
    def setUp(self):
        self.tmp = tempfile.TemporaryDirectory()
        self.session = Path(self.tmp.name) / "session-a"
        self.session.mkdir()
        self.file = self.session / "events.jsonl"
        self.events = [
            {"type": "session_start", "ts": 100, "cwd": "/tmp/demo"},
            {"type": "interaction_start", "ts": 110, "prompt": "test"},
            {"type": "turn_start", "ts": 120, "turnIndex": 1},
            {"type": "step_start", "ts": 130, "stepIndex": 1, "turnIndex": 1},
            {"type": "llm_request", "ts": 140, "stepIndex": 1, "input": {"messages": [{"content": "x" * 100000}], "api_key": "secret"}},
            {"type": "step_end", "ts": 150, "stepIndex": 1, "turnIndex": 1, "text": "answer", "usage": {"input": 10, "output": 20}, "stopReason": "stop"},
        ]
        self.file.write_text("".join(json.dumps(e) + "\n" for e in self.events))

    def tearDown(self):
        self.tmp.cleanup()

    def test_lazy_input_and_no_repeat_read(self):
        index = EventIndex(self.session)
        events = index.events()
        count = index.bytes_read
        self.assertLess(len(json.dumps(events)), 3000)
        self.assertEqual(index.input(events[4]["input"]["__traceInput"]), self.events[4]["input"])
        self.assertEqual(events, index.events())
        self.assertEqual(count, index.bytes_read)
        index.close()

    def test_append_partial_and_restart(self):
        index = EventIndex(self.session)
        self.assertEqual(len(index.events()), 6)
        with self.file.open("a") as f:
            f.write('{"type":"turn_end"')
        self.assertEqual(len(index.events()), 6)
        with self.file.open("a") as f:
            f.write('}\nnot-json\n')
        self.assertEqual(len(index.events()), 7)
        index.close()
        restored = EventIndex(self.session)
        self.assertEqual(len(restored.events()), 7)
        self.assertEqual(restored.bytes_read, 0)
        restored.close()

    def test_truncate(self):
        index = EventIndex(self.session)
        index.events()
        self.file.write_text('{"type":"session_start","ts":999}\n')
        self.assertEqual(index.events(), [{"type": "session_start", "ts": 999}])
        index.close()

    def test_tree_and_summary_match_original(self):
        cache = serve.TreeCache()
        root, nodes, light, dag = cache.get("session-a", self.session)
        original = trace_to_html.build_tree(self.events, session_dir=str(self.session))
        self.assertEqual(set(nodes), set(serve.index_nodes(original)))
        self.assertEqual(cache.summary(self.session), trace_to_html.extract_summary(self.session))
        self.assertNotIn("x" * 1000, json.dumps(light))
        llm = next(n for n in nodes.values() if n["type"] == "step")
        self.assertTrue(llm["data"]["input"]["__lazy"])
        self.assertEqual(trace_to_html.sanitize(self.events[4]["input"])["api_key"], "***REDACTED***")

    def test_single_flight(self):
        cache = serve.TreeCache()
        results = []
        threads = [threading.Thread(target=lambda: results.append(cache.get("session-a", self.session))) for _ in range(4)]
        for t in threads: t.start()
        for t in threads: t.join()
        self.assertEqual(len(results), 4)
        self.assertTrue(all(r[0] is results[0][0] for r in results))

    def test_session_paths(self):
        self.assertTrue(serve.valid_session_path("session-a/subagents/child-a"))
        self.assertFalse(serve.valid_session_path("session-a/../child-a"))


if __name__ == "__main__":
    unittest.main()
