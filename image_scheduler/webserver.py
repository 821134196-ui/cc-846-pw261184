"""Web 服务器：静态仪表盘 + JSON API，带后台自动步进线程。

仅使用标准库（http.server / threading），无第三方依赖。
"""

from __future__ import annotations

import json
import threading
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path
from urllib.parse import parse_qs, urlparse

from .engine import SimEngine
from .topology import SCENARIOS, build_scenario

WEB_DIR = Path(__file__).resolve().parent / "web"


class SimulationRunner:
    def __init__(self) -> None:
        self.lock = threading.RLock()
        self.engine: SimEngine | None = None
        self.scenario_name = "burst"
        self.auto = True
        self.interval = 0.8
        self._thread: threading.Thread | None = None
        self._stop = threading.Event()
        self.reset(self.scenario_name)
        self._start_thread()

    # ------------------------------------------------------------------ 控制

    def reset(self, scenario_name: str) -> None:
        with self.lock:
            self.engine = SimEngine(build_scenario(scenario_name))
            self.scenario_name = scenario_name

    def tick_once(self) -> None:
        with self.lock:
            if not self.engine.is_done():
                self.engine.step()

    def set_auto(self, on: bool) -> None:
        with self.lock:
            self.auto = on

    def set_interval(self, seconds: float) -> None:
        with self.lock:
            self.interval = max(0.1, float(seconds))

    def set_registry_factor(self, factor: float) -> None:
        with self.lock:
            self.engine.set_registry_factor(factor)

    def add_job(self, node_id: str, image: str, deadline: int) -> None:
        with self.lock:
            self.engine.add_job(node_id, image, deadline)

    def snapshot(self) -> dict:
        with self.lock:
            return self.engine.snapshot()

    # ------------------------------------------------------------------ 后台线程

    def _start_thread(self) -> None:
        self._stop.clear()
        self._thread = threading.Thread(
            target=self._loop, name="sim-ticker", daemon=True
        )
        self._thread.start()

    def _loop(self) -> None:
        while not self._stop.is_set():
            if self.auto and self.engine is not None:
                with self.lock:
                    if not self.engine.is_done():
                        self.engine.step()
            self._stop.wait(self.interval)

    def shutdown(self) -> None:
        self._stop.set()


class Handler(BaseHTTPRequestHandler):
    runner: SimulationRunner  # 由 main 注入

    def log_message(self, *args) -> None:  # 静默访问日志
        pass

    # ------------------------------------------------------------------ 工具

    def _json(self, payload: dict, status: int = 200) -> None:
        body = json.dumps(payload, ensure_ascii=False).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(body)))
        self.end_headers()
        self.wfile.write(body)

    def _file(self, rel: str, content_type: str) -> None:
        path = (WEB_DIR / rel).resolve()
        if not str(path).startswith(str(WEB_DIR)) or not path.is_file():
            self.send_error(404)
            return
        data = path.read_bytes()
        self.send_response(200)
        self.send_header("Content-Type", content_type)
        self.send_header("Content-Length", str(len(data)))
        self.end_headers()
        self.wfile.write(data)

    # ------------------------------------------------------------------ 路由

    def do_GET(self) -> None:
        parsed = urlparse(self.path)
        route = parsed.path
        if route in ("/", "/index.html"):
            self._file("index.html", "text/html; charset=utf-8")
        elif route == "/app.js":
            self._file("app.js", "application/javascript; charset=utf-8")
        elif route == "/styles.css":
            self._file("styles.css", "text/css; charset=utf-8")
        elif route == "/api/state":
            snap = self.runner.snapshot()
            snap["auto"] = self.runner.auto
            snap["interval"] = self.runner.interval
            self._json(snap)
        elif route == "/api/scenarios":
            self._json({"scenarios": [
                {"name": k, "description": v} for k, v in SCENARIOS.items()
            ]})
        else:
            self.send_error(404)

    def do_POST(self) -> None:
        parsed = urlparse(self.path)
        try:
            length = int(self.headers.get("Content-Length", 0))
            raw = self.rfile.read(length) if length else b"{}"
            data = json.loads(raw.decode("utf-8") or "{}")
        except (ValueError, json.JSONDecodeError):
            self._json({"error": "invalid JSON"}, 400)
            return

        if parsed.path == "/api/control":
            action = data.get("action")
            if action == "tick":
                self.runner.tick_once()
            elif action == "play":
                self.runner.set_auto(True)
            elif action == "pause":
                self.runner.set_auto(False)
            elif action == "reset":
                self.runner.reset(data.get("scenario", "burst"))
            elif action == "speed":
                self.runner.set_interval(float(data.get("interval", 0.8)))
            elif action == "registry":
                self.runner.set_registry_factor(float(data.get("factor", 1.0)))
            elif action == "add_job":
                self.runner.add_job(
                    data["node_id"], data["image"], int(data["deadline"])
                )
            else:
                self._json({"error": f"unknown action {action}"}, 400)
                return
            self._json({"ok": True})
        else:
            self.send_error(404)


def serve(host: str = "0.0.0.0", port: int = 8000) -> None:
    runner = SimulationRunner()
    Handler.runner = runner
    httpd = ThreadingHTTPServer((host, port), Handler)
    try:
        print(f"镜像拉取调度模拟器 → http://{host}:{port}")
        httpd.serve_forever()
    except KeyboardInterrupt:
        pass
    finally:
        runner.shutdown()
        httpd.server_close()


if __name__ == "__main__":
    serve()
