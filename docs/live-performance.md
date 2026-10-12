# Live viewer performance

The existing Trace server remains on loopback port 4317. No additional service or runtime dependency is introduced.

## Behavior changes

- Session shutdown flushes the event stream without automatically rendering static HTML. `/trace` and `/trace all` still generate HTML explicitly, using asynchronous subprocesses.
- New provider request events contain the latest message, total message count, request options and tool descriptions. `traceCapture` marks this as a preview, not the exact full provider request. The Pi session retains the conversation, but it is not a substitute for exact provider-request replay.
- Each session gets an automatically generated `live-index.sqlite3` beside its original event log. It stores lightweight events and byte offsets for historical Input; unchanged logs are not reread and appended complete lines are indexed incrementally.
- Timeline data excludes node details. Selecting a node loads its details; selecting Input reads only its corresponding historical event. Deep tree rows, nested JSON and long strings are expanded on demand.
- Historical event logs are not rewritten or deleted. A first access without an index still requires a full scan; explicit full-tree expansion/search or large Input expansion may still produce noticeable work.

## Checks

```sh
python3 -m unittest discover -s tests -p 'test_live_performance.py' -v
python3 extensions/trace/viewer/build.py
node --check extensions/trace/viewer/viewer.js
# jiti must be available for the test only; it is not a new runtime dependency.
PI_TRACE_TEST_JITI=/path/to/jiti node tests/test_recorder.cjs
```

Six Python regression tests cover lazy Input, unchanged-log reads, append/partial lines, persisted-index restart, truncation, summary/tree compatibility and single-flight access. The recorder fixture exercises 10,000 historical messages, 1,000 thinking deltas and shutdown without static export.

Local Chromium testing against port 4317 with a 2.61 GB / 3,733-node historical trace measured approximately 545 ms to open the session, 56 ms to select node details and 108 ms to load Input. First index creation took 13.8 s; the 9.75 MB persisted index reduced a fresh-process summary read to 87 ms with zero original-log bytes read. These are local measurements, not latency guarantees; explicit whole-tree search/expansion still produced an approximately 838 ms long task.
