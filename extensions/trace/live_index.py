"""Persistent lightweight event index for the existing loopback trace server."""
import json
import sqlite3
import threading
from pathlib import Path


class EventIndex:
    def __init__(self, session):
        self.session = Path(session)
        self.lock = threading.Lock()
        self.bytes_read = 0
        self.db = sqlite3.connect(str(self.session / "live-index.sqlite3"), check_same_thread=False)
        self.db.execute("CREATE TABLE IF NOT EXISTS events (offset INTEGER PRIMARY KEY, size INTEGER, body TEXT)")
        self.db.execute("CREATE TABLE IF NOT EXISTS meta (key TEXT PRIMARY KEY, value TEXT)")
        self.db.commit()

    def update(self):
        with self.lock:
            source = self.session / "events.jsonl"
            stat = source.stat()
            meta = dict(self.db.execute("SELECT key, value FROM meta"))
            offset = int(meta.get("offset", "0"))
            identity = f"{stat.st_dev}:{stat.st_ino}"
            if meta.get("identity") != identity or stat.st_size < offset or (
                stat.st_size == offset and meta.get("mtime") != str(stat.st_mtime_ns)
            ):
                self.db.execute("DELETE FROM events")
                offset = 0
            if stat.st_size == offset:
                return
            with source.open("rb") as stream:
                stream.seek(offset)
                while True:
                    start = stream.tell()
                    line = stream.readline()
                    self.bytes_read += len(line)
                    if not line or not line.endswith(b"\n"):
                        offset = start
                        break
                    offset = stream.tell()
                    try:
                        event = json.loads(line)
                    except (ValueError, UnicodeDecodeError):
                        continue
                    if event.get("type") == "llm_request" and event.get("input") is not None:
                        event["input"] = {"__lazy": True, "__traceInput": {"offset": start, "size": len(line)}}
                    # Per-step content is already present in step_end; don't retain its turn duplicate.
                    if event.get("type") == "turn_summary":
                        event.pop("steps", None)
                    self.db.execute("INSERT OR REPLACE INTO events VALUES (?, ?, ?)",
                                    (start, len(line), json.dumps(event, ensure_ascii=False)))
            self.db.executemany("INSERT OR REPLACE INTO meta VALUES (?, ?)",
                                [("offset", str(offset)), ("identity", identity), ("mtime", str(stat.st_mtime_ns))])
            self.db.commit()

    def events(self):
        self.update()
        with self.lock:
            return [json.loads(row[0]) for row in self.db.execute("SELECT body FROM events ORDER BY offset")]

    def input(self, reference):
        with (self.session / "events.jsonl").open("rb") as stream:
            stream.seek(reference["offset"])
            return json.loads(stream.read(reference["size"]))["input"]

    def close(self):
        self.db.close()


def event_summary(session, events):
    started = ended = None
    cwd = prompt = None
    interactions = turns = tools = tool_errors = step_errors = aborted = 0
    total_input = total_output = cache_read = 0
    cost = 0
    models = set()
    for e in events:
        ts = e.get("ts")
        if ts:
            if started is None:
                started = ts
            ended = ts
        t = e.get("type")
        if t == "session_start":
            cwd = e.get("cwd") or cwd
        elif t == "interaction_start":
            interactions += 1
            if prompt is None:
                p = e.get("prompt") or ""
                prompt = p[:60] + ("…" if len(p) > 60 else "")
        elif t == "turn_start":
            turns += 1
        elif t in ("tool_start", "tool_execution_start"):
            tools += 1
        elif t in ("tool_end", "tool_execution_end"):
            tool_errors += bool(e.get("isError"))
        elif t in ("step_end", "message_end", "llm_completion"):
            u = e.get("usage") or {}
            total_input += int(u.get("input") or u.get("inputTokens") or 0)
            total_output += int(u.get("output") or u.get("outputTokens") or 0)
            cache_read += int(u.get("cacheRead") or u.get("cacheReadTokens") or 0)
            c = u.get("cost")
            cost += (c.get("total") or 0) if isinstance(c, dict) else (c or 0)
            if e.get("model"):
                models.add(e["model"])
            step_errors += e.get("stopReason") == "error"
            aborted += e.get("stopReason") == "aborted"
    if started is None or not interactions:
        return None
    return dict(id=Path(session).name, startedAt=started, endedAt=ended,
                durationMs=max(0, (ended or started) - started), cwd=cwd,
                cwdName=Path(cwd).name if cwd else None, firstPrompt=prompt or "",
                interactionCount=interactions, turnCount=turns, toolCount=tools,
                errorCount=tool_errors + step_errors, toolErrorCount=tool_errors,
                stepErrorCount=step_errors, abortedCount=aborted,
                totalInput=total_input, totalOutput=total_output, totalCacheRead=cache_read,
                totalCost=cost, models=sorted(models))
