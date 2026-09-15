#!/usr/bin/env python3
"""
Pi Trace Live: loopback-only live server for pi-trace-extension.

Reads ~/.pi/agent/traces/<session-id>/events.jsonl, which the upstream
extension writes, and serves the existing viewer/dashboard templates through
a small HTTP + SSE server. This file never writes trace data.

Usage:
    python3 serve.py [--traces DIR] [--port 4317] [--idle-ms 30000] [--no-idle-exit]

Lifecycle (方案 A):
    - pi processes POST /api/lease and renew every 10s; session_shutdown DELETEs.
    - A lease expires after LEASE_TTL_MS without heartbeat or when its PID dies.
    - After the last lease is gone for --idle-ms, the server shuts down.
    - With --no-idle-exit the server stays up (manual browsing of history).
"""
from __future__ import annotations

import argparse
import json
import os
import queue
import re
import signal
import sys
import threading
import time
import urllib.request
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import urlparse

HERE = Path(__file__).resolve().parent
if str(HERE) not in sys.path:
    sys.path.insert(0, str(HERE))

import trace_to_html  # noqa: E402  same-dir import; loads viewer/assets.json at import

SESSION_ID_RE = re.compile(r"^[A-Za-z0-9][A-Za-z0-9._-]{0,255}$")
DEFAULT_TRACES = Path.home() / ".pi" / "agent" / "traces"
LEASE_TTL_MS = 30_000
MAX_BODY_BYTES = 64 * 1024

CSP = (
    "default-src 'self'; base-uri 'none'; connect-src 'self'; frame-ancestors 'none'; "
    "form-action 'none'; object-src 'none'; img-src 'self' data:; "
    "script-src 'self' 'unsafe-inline'; style-src 'self' 'unsafe-inline'"
)

def dashboard_live_script() -> str:
    # 会话列表页很小，变化后直接自动重载。
    return (
        "<script>\n(() => {\n"
        '  const es = new EventSource("/api/stream");\n'
        "  let timer = null;\n"
        '  es.addEventListener("changed", () => { clearTimeout(timer); timer = setTimeout(() => location.reload(), 1200); });\n'
        '  es.addEventListener("server_closing", () => es.close());\n'
        "})();\n</script>\n"
    )


def session_live_script(session_id: str) -> str:
    # 单会话页：拉新树后原地更新，保留折叠状态 / 选中节点 / 滚动位置。
    tree_url = json.dumps(f"/api/session/{session_id}/tree")
    scope = json.dumps(f"/api/session/{session_id}/stream")
    return (
        "<script>\n(() => {\n"
        f"  const es = new EventSource({scope});\n"
        f"  const treeUrl = {tree_url};\n"
        "  let timer = null, pending = false;\n"
        "  async function pull() {\n"
        "    if (pending) return;\n"
        "    pending = true;\n"
        "    try {\n"
        "      const res = await fetch(treeUrl, { headers: { accept: 'application/json' }, cache: 'no-store' });\n"
        "      if (res.ok) { const data = await res.json(); if (window.__piTraceApply) window.__piTraceApply(data.tree, data.dag); }\n"
        "    } catch (error) {}\n"
        "    pending = false;\n"
        "  }\n"
        '  es.addEventListener("changed", () => { clearTimeout(timer); timer = setTimeout(pull, 1200); });\n'
        '  es.addEventListener("server_closing", () => es.close());\n'
        "})();\n</script>\n"
    )


def inject_before_body(html: str, snippet: str) -> str:
    index = html.rfind("</body>")
    if index == -1:
        return html + snippet
    return html[:index] + snippet + html[index:]


def pid_alive(pid: object) -> bool:
    if not isinstance(pid, int) or isinstance(pid, bool) or pid <= 0:
        return False
    try:
        os.kill(pid, 0)
        return True
    except ProcessLookupError:
        return False
    except PermissionError:
        return True


def events_file(traces: Path, session_id: str) -> Path:
    return traces / session_id / "events.jsonl"


def signature(path: Path) -> tuple[int, int] | None:
    try:
        stat = path.stat()
        return (stat.st_size, stat.st_mtime_ns)
    except OSError:
        return None


def list_session_ids(traces: Path) -> list[str]:
    try:
        return sorted(
            p.name for p in traces.iterdir() if p.is_dir() and SESSION_ID_RE.match(p.name)
        )
    except OSError:
        return []


def summaries(traces: Path) -> list[dict]:
    out = []
    for session_id in list_session_ids(traces):
        try:
            summary = trace_to_html.extract_summary(traces / session_id)
        except Exception:
            summary = None
        if summary:
            out.append(summary)
    out.sort(key=lambda item: item.get("startedAt") or 0, reverse=True)
    return out


def health_ok(port: int, timeout: float = 0.5) -> bool:
    try:
        with urllib.request.urlopen(f"http://127.0.0.1:{port}/api/health", timeout=timeout) as response:
            return response.status == 200
    except Exception:
        return False


class LeaseStore:
    def __init__(self) -> None:
        self._lock = threading.Lock()
        self._leases: dict[str, dict] = {}
        self._had_lease = False
        self._drain_since: float | None = None

    def register(self, run_id: str, pid: object, session_id: object, directory: object) -> None:
        with self._lock:
            self._leases[run_id] = {
                "pid": pid,
                "sessionId": session_id,
                "dir": directory,
                "last": time.time(),
            }
            self._had_lease = True
            self._drain_since = None

    def unregister(self, run_id: str) -> None:
        with self._lock:
            self._leases.pop(run_id, None)

    def heartbeat(self, run_id: str) -> None:
        with self._lock:
            lease = self._leases.get(run_id)
            if lease:
                lease["last"] = time.time()

    def expire(self, ttl_ms: int) -> None:
        now = time.time()
        with self._lock:
            for run_id in list(self._leases):
                lease = self._leases[run_id]
                if now - lease["last"] > ttl_ms / 1000 or not pid_alive(lease["pid"]):
                    del self._leases[run_id]

    def count(self) -> int:
        with self._lock:
            return len(self._leases)

    def should_exit(self, idle_ms: int, allow_exit: bool) -> bool:
        with self._lock:
            if not allow_exit or not self._had_lease or self._leases:
                self._drain_since = None
                return False
            if self._drain_since is None:
                self._drain_since = time.time()
                return False
            return (time.time() - self._drain_since) * 1000 >= idle_ms


class Hub:
    """Fan out throttled 'changed' signals to SSE subscribers."""

    def __init__(self, throttle_seconds: float = 2.0) -> None:
        self._lock = threading.Lock()
        self._subs: set[queue.Queue] = set()
        self._last_published: dict[str, float] = {}
        self._throttle = throttle_seconds

    def subscribe(self) -> queue.Queue:
        channel: queue.Queue = queue.Queue(maxsize=8)
        with self._lock:
            self._subs.add(channel)
        return channel

    def unsubscribe(self, channel: queue.Queue) -> None:
        with self._lock:
            self._subs.discard(channel)

    def publish(self, key: str) -> None:
        now = time.time()
        with self._lock:
            if now - self._last_published.get(key, 0) < self._throttle:
                return
            self._last_published[key] = now
            channels = list(self._subs)
        for channel in channels:
            try:
                channel.put_nowait("changed")
            except queue.Full:
                pass

    def close_all(self) -> None:
        with self._lock:
            channels = list(self._subs)
        for channel in channels:
            try:
                channel.put_nowait("closing")
            except queue.Full:
                pass


def watcher(hub: Hub, traces: Path, stop: threading.Event) -> None:
    known: dict[str, tuple[int, int] | None] = {}
    while not stop.wait(1.0):
        current = {sid: signature(events_file(traces, sid)) for sid in list_session_ids(traces)}
        if set(current) != set(known):
            hub.publish("__global__")
        for sid, sig in current.items():
            if known.get(sid) != sig:
                hub.publish(sid)
        known = current


class Handler(BaseHTTPRequestHandler):
    server_version = "PiTraceLive/0.1"

    def log_message(self, *args) -> None:  # silence per-request logging
        pass

    def _headers(self, content_type: str, status: int = 200, length: int | None = None) -> None:
        self.send_response(status)
        self.send_header("content-type", content_type)
        self.send_header("cache-control", "no-store")
        self.send_header("x-content-type-options", "nosniff")
        self.send_header("content-security-policy", CSP)
        if length is not None:
            self.send_header("content-length", str(length))
        self.end_headers()

    def _json(self, value: object, status: int = 200) -> None:
        body = json.dumps(value, ensure_ascii=False).encode("utf-8")
        self._headers("application/json; charset=utf-8", status, len(body))
        self.wfile.write(body)

    def _html(self, value: str, status: int = 200) -> None:
        body = value.encode("utf-8")
        self._headers("text/html; charset=utf-8", status, len(body))
        self.wfile.write(body)

    def _read_json(self) -> dict | None:
        try:
            length = int(self.headers.get("content-length") or 0)
        except ValueError:
            return None
        if length <= 0 or length > MAX_BODY_BYTES:
            return None
        try:
            value = json.loads(self.rfile.read(length).decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError):
            return None
        return value if isinstance(value, dict) else None

    def do_GET(self) -> None:  # noqa: N802 (http.server API)
        path = urlparse(self.path).path
        if path == "/":
            self._dashboard()
        elif path == "/api/health":
            self._health()
        elif path == "/api/sessions":
            self._json({"sessions": summaries(self.server.traces)})  # type: ignore[attr-defined]
        elif path == "/api/stream":
            self._stream(None)
        else:
            match = re.fullmatch(r"/api/session/([^/]+)/tree", path)
            if match:
                self._tree(match.group(1))
                return
            match = re.fullmatch(r"/api/session/([^/]+)/stream", path)
            if match:
                self._stream(match.group(1))
                return
            self._viewer_route(path)

    def _viewer_route(self, path: str) -> None:
        """把上游 viewer/dashboard 的相对链接映射到 serve 路由。

        上游假定 file://，dashboard 链接 `./<sid>/trace.html`，viewer 子 trace
        链接 `subagents/<child>/trace.html`。走 HTTP 后分别落到
        `/<sid>/trace.html` 和 `/session/<sid>/subagents/<child>/trace.html`。
        """
        if path.startswith("/session/"):
            rest = path[len("/session/"):]
            segments = [segment for segment in rest.split("/") if segment]
            if not segments:
                self._json({"error": "not found"}, 404)
                return
            if rest == segments[0]:
                # 重定向到带尾斜杠的地址，相对链接 subagents/... 才能落在本 session 下。
                self.send_response(301)
                self.send_header("location", f"/session/{segments[0]}/")
                self.send_header("content-length", "0")
                self.end_headers()
                return
        else:
            segments = [segment for segment in path.split("/") if segment]
        if segments and segments[-1] == "trace.html":
            segments = segments[:-1]
        self._serve_viewer(segments)

    def _serve_viewer(self, segments: list[str]) -> None:
        traces = self.server.traces  # type: ignore[attr-defined]
        if not segments:
            self._json({"error": "not found"}, 404)
            return
        for segment in segments:
            if not SESSION_ID_RE.match(segment):
                self._json({"error": "invalid path"}, 400)
                return
        session = traces.joinpath(*segments)
        if not (session / "events.jsonl").is_file():
            self._json({"error": "session not found"}, 404)
            return
        try:
            html = trace_to_html.render_session_html(session)
        except Exception as exc:
            self._json({"error": f"render failed: {exc}"}, 500)
            return
        live = session_live_script(segments[0]) if len(segments) == 1 else ""
        self._html(inject_before_body(html, live))

    def do_POST(self) -> None:  # noqa: N802
        if urlparse(self.path).path != "/api/lease":
            self._json({"error": "not found"}, 404)
            return
        body = self._read_json()
        if body is None:
            self._json({"error": "invalid body"}, 400)
            return
        run_id = body.get("runId")
        if not isinstance(run_id, str) or not SESSION_ID_RE.match(run_id):
            self._json({"error": "invalid runId"}, 400)
            return
        self.server.leases.register(  # type: ignore[attr-defined]
            run_id, body.get("pid"), body.get("sessionId"), body.get("dir")
        )
        self._json({"ok": True, "leases": self.server.leases.count()})  # type: ignore[attr-defined]

    def do_DELETE(self) -> None:  # noqa: N802
        if urlparse(self.path).path != "/api/lease":
            self._json({"error": "not found"}, 404)
            return
        body = self._read_json() or {}
        run_id = body.get("runId")
        if isinstance(run_id, str):
            self.server.leases.unregister(run_id)  # type: ignore[attr-defined]
        self._json({"ok": True, "leases": self.server.leases.count()})  # type: ignore[attr-defined]

    def _health(self) -> None:
        self._json(
            {
                "version": 1,
                "pid": os.getpid(),
                "url": f"http://127.0.0.1:{self.server.server_address[1]}",  # type: ignore[attr-defined]
                "leases": self.server.leases.count(),  # type: ignore[attr-defined]
                "traces": str(self.server.traces),  # type: ignore[attr-defined]
            }
        )

    def _dashboard(self) -> None:
        try:
            html = trace_to_html.render_dashboard(summaries(self.server.traces))  # type: ignore[attr-defined]
        except Exception as exc:  # pragma: no cover - defensive
            self._json({"error": f"dashboard render failed: {exc}"}, 500)
            return
        self._html(inject_before_body(html, dashboard_live_script()))

    def _tree(self, session_id: str) -> None:
        if not SESSION_ID_RE.match(session_id):
            self._json({"error": "invalid session id"}, 400)
            return
        session = self.server.traces / session_id  # type: ignore[attr-defined]
        if not (session / "events.jsonl").is_file():
            self._json({"error": "session not found"}, 404)
            return
        events = trace_to_html.load_events(session)
        root = trace_to_html.build_tree(events, session_dir=str(session))
        trace_to_html.scrub_tree(root)
        dag = trace_to_html.collect_dag_stats(root)
        self._json({"tree": root, "dag": dag})

    def _stream(self, session_id: str | None) -> None:
        if session_id is not None and not SESSION_ID_RE.match(session_id):
            self._json({"error": "invalid session id"}, 400)
            return
        self._headers("text/event-stream; charset=utf-8")
        channel = self.server.hub.subscribe()  # type: ignore[attr-defined]
        try:
            self.wfile.write(b": connected\n\n")
            self.wfile.flush()
            while not self.server.stop.is_set():  # type: ignore[attr-defined]
                try:
                    event = channel.get(timeout=15)
                except queue.Empty:
                    self.wfile.write(b": keepalive\n\n")
                    self.wfile.flush()
                    continue
                if event == "closing":
                    self.wfile.write(b"event: server_closing\ndata: {}\n\n")
                    self.wfile.flush()
                    return
                self.wfile.write(b"event: changed\ndata: {}\n\n")
                self.wfile.flush()
        except (BrokenPipeError, ConnectionResetError, OSError):
            pass
        finally:
            self.server.hub.unsubscribe(channel)  # type: ignore[attr-defined]


def parse_args(argv: list[str]) -> argparse.Namespace:
    parser = argparse.ArgumentParser(description="Pi Trace Live server")
    parser.add_argument("--traces", default=str(DEFAULT_TRACES), help="trace root directory")
    parser.add_argument("--port", type=int, default=int(os.environ.get("PI_TRACE_WEB_PORT", "4317")))
    parser.add_argument("--idle-ms", type=int, default=int(os.environ.get("PI_TRACE_WEB_IDLE_MS", "30000")))
    parser.add_argument("--no-idle-exit", action="store_true", help="do not exit when leases are gone")
    return parser.parse_args(argv)


def main(argv: list[str] | None = None) -> int:
    args = parse_args(sys.argv[1:] if argv is None else argv)
    traces = Path(args.traces).expanduser().resolve()
    idle_ms = max(5_000, args.idle_ms)
    stop = threading.Event()

    try:
        server = ThreadingHTTPServer(("127.0.0.1", args.port), Handler)
    except OSError as exc:
        if health_ok(args.port):
            print(f"[pi-trace-live] already running on http://127.0.0.1:{args.port}", flush=True)
            return 0
        print(f"[pi-trace-live] cannot bind 127.0.0.1:{args.port}: {exc}", file=sys.stderr)
        print("[pi-trace-live] set PI_TRACE_WEB_PORT to an unused port", file=sys.stderr)
        return 1

    server.daemon_threads = True
    server.traces = traces  # type: ignore[attr-defined]
    server.hub = Hub()  # type: ignore[attr-defined]
    server.leases = LeaseStore()  # type: ignore[attr-defined]
    server.stop = stop  # type: ignore[attr-defined]

    def lease_loop() -> None:
        while not stop.is_set():
            server.leases.expire(LEASE_TTL_MS)  # type: ignore[attr-defined]
            if server.leases.should_exit(idle_ms, not args.no_idle_exit):  # type: ignore[attr-defined]
                stop.set()
                server.hub.close_all()  # type: ignore[attr-defined]
                threading.Thread(target=server.shutdown, daemon=True).start()
                return
            stop.wait(2.0)

    def handle_signal(signum, frame) -> None:  # noqa: ARG001
        stop.set()
        server.hub.close_all()  # type: ignore[attr-defined]
        threading.Thread(target=server.shutdown, daemon=True).start()

    threading.Thread(target=lease_loop, daemon=True).start()
    threading.Thread(target=watcher, args=(server.hub, traces, stop), daemon=True).start()  # type: ignore[attr-defined]
    signal.signal(signal.SIGTERM, handle_signal)
    signal.signal(signal.SIGINT, handle_signal)

    print(f"[pi-trace-live] serving {traces} on http://127.0.0.1:{args.port}", flush=True)
    try:
        server.serve_forever(poll_interval=0.5)
    finally:
        stop.set()
        server.server_close()
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
