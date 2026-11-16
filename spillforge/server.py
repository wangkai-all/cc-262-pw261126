"""Dependency-free HTTP server for the operator UI and JSON API.

Endpoints
---------
GET  /                       -> single-page UI
GET  /static/app.js,app.css  -> assets
GET  /api/state?after=N      -> full snapshot + event tail
GET  /api/jobs/<id>          -> job detail (events, ledger, artifacts)
GET  /api/jobs/<id>/preview  -> first lines of the result
GET  /api/jobs/<id>/result   -> download result
GET  /api/ledger             -> recent ledger journal
POST /api/uploads            -> raw body upload (X-Filename header)
POST /api/demo               -> generate synthetic input
POST /api/jobs               -> submit {kind, source_upload, second_upload, name}
POST /api/jobs/<id>/cancel   -> cancel
POST /api/jobs/<id>/resume   -> resume
POST /api/jobs/<id>/purge    -> delete result
POST /api/policy             -> {policy}
POST /api/recover            -> run recovery pass now
POST /api/faults             -> {points: [...]}
POST /api/reset              -> test helper: wipe all state (loopback only)
"""

from __future__ import annotations

import json
import mimetypes
import os
import shutil
import threading
import time
import urllib.parse
from http import HTTPStatus
from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
from pathlib import Path

from .config import Config
from .errors import ConfigError, SpillForgeError
from .service import Service

WEB_DIR = Path(__file__).parent / "web"


class ApiError(Exception):
    def __init__(self, status: int, message: str):
        self.status = status
        self.message = message
        super().__init__(message)


class _BoundedReader:
    """Expose exactly Content-Length bytes of the request body.

    ``rfile.read(n)`` blocks until ``n`` bytes arrive, so passing the raw socket
    to ``save_upload`` hangs whenever the client sends its body in several
    packets (curl and browsers both do for files > one TCP window).
    """

    def __init__(self, rfile, length: int):
        self._rfile = rfile
        self._remaining = length

    def read(self, n: int = -1) -> bytes:
        if self._remaining <= 0:
            return b""
        if n is None or n < 0:
            n = self._remaining
        chunk = self._rfile.read(min(n, self._remaining))
        self._remaining -= len(chunk)
        return chunk


class Handler(BaseHTTPRequestHandler):
    server_version = "SpillForge/0.1"

    @property
    def service(self) -> Service:
        return self.server.service  # type: ignore[attr-defined]

    def log_message(self, fmt, *args):  # quieter test output
        if os.environ.get("SPILLFORGE_HTTP_LOG"):
            super().log_message(fmt, *args)

    # ------------------------------------------------------------------ http

    def do_GET(self):
        try:
            self._route_get()
        except ApiError as exc:
            self._json({"error": exc.message}, status=exc.status)
        except BrokenPipeError:
            pass
        except Exception as exc:
            self._json({"error": f"{type(exc).__name__}: {exc}"}, status=500)

    def do_POST(self):
        try:
            self._route_post()
        except ApiError as exc:
            self._json({"error": exc.message}, status=exc.status)
        except BrokenPipeError:
            pass
        except ConfigError as exc:
            self._json({"error": str(exc)}, status=400)
        except Exception as exc:
            self._json({"error": f"{type(exc).__name__}: {exc}"}, status=500)

    # ---------------------------------------------------------------- routes

    def _route_get(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        query = urllib.parse.parse_qs(parsed.query)
        if path in ("/", "/index.html"):
            return self._file(WEB_DIR / "index.html", "text/html; charset=utf-8")
        if path.startswith("/static/"):
            name = path[len("/static/"):]
            if "/" in name or name.startswith("."):
                raise ApiError(404, "not found")
            return self._file(WEB_DIR / name, None)
        if path == "/api/state":
            after = int(query.get("after", ["0"])[0] or 0)
            return self._json(self.service.state(event_cursor=after))
        if path == "/api/ledger":
            limit = int(query.get("limit", ["200"])[0] or 200)
            return self._json({"entries": self.service.ledger_log(limit)})
        if path.startswith("/api/jobs/"):
            parts = path.strip("/").split("/")
            if len(parts) < 3:
                raise ApiError(404, "not found")
            job_id = parts[2]
            if len(parts) == 3:
                detail = self.service.job_detail(job_id)
                if detail is None:
                    raise ApiError(404, "job not found")
                return self._json(detail)
            if len(parts) == 4 and parts[3] == "preview":
                return self._json({"lines": self.service.preview(job_id)})
            if len(parts) == 4 and parts[3] == "result":
                return self._download(job_id)
        raise ApiError(404, "not found")

    def _route_post(self):
        parsed = urllib.parse.urlparse(self.path)
        path = parsed.path
        if path == "/api/uploads":
            return self._upload()
        body = self._read_json()
        if path == "/api/demo":
            upload = self.service.generate_demo(
                int(body.get("records") or 5000),
                skew=int(body.get("skew") or 20),
                sorted_input=bool(body.get("sorted")),
            )
            return self._json({"upload": _upload_public(upload)}, status=201)
        if path == "/api/jobs":
            job = self.service.submit_job(
                body.get("kind", "sort"),
                body.get("source_upload") or body.get("source_upload_id"),
                body.get("second_upload") or body.get("second_upload_id"),
                name=body.get("name"),
                priority=int(body.get("priority") or 0),
            )
            return self._json({"job": self.service._job_public(job)}, status=201)
        if path.startswith("/api/jobs/"):
            parts = path.strip("/").split("/")
            if len(parts) == 4:
                job_id, action = parts[2], parts[3]
                if action == "cancel":
                    ok = self.service.cancel(job_id)
                    return self._json({"ok": ok})
                if action == "resume":
                    ok = self.service.resume(job_id)
                    return self._json({"ok": ok})
                if action == "purge":
                    ok = self.service.purge(job_id)
                    return self._json({"ok": ok})
        if path == "/api/policy":
            self.service.set_policy(body.get("policy", "fairness"))
            return self._json({"policy": self.service.scheduler.policy})
        if path == "/api/recover":
            return self._json(self.service.recover())
        if path == "/api/faults":
            points = self.service.set_faults(body.get("points") or [])
            return self._json({"points": points})
        if path == "/api/reset":
            return self._reset()
        raise ApiError(404, "not found")

    # --------------------------------------------------------------- helpers

    def _read_json(self) -> dict:
        length = int(self.headers.get("Content-Length") or 0)
        if length <= 0:
            return {}
        raw = self.rfile.read(length)
        try:
            return json.loads(raw.decode("utf-8"))
        except (UnicodeDecodeError, json.JSONDecodeError) as exc:
            raise ApiError(400, f"invalid JSON body: {exc}")

    def _json(self, payload, status: int = 200):
        raw = json.dumps(payload, default=str).encode("utf-8")
        self.send_response(status)
        self.send_header("Content-Type", "application/json; charset=utf-8")
        self.send_header("Content-Length", str(len(raw)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(raw)

    def _file(self, path: Path, content_type: str | None):
        if not path.is_file():
            raise ApiError(404, "not found")
        raw = path.read_bytes()
        ctype = content_type or mimetypes.guess_type(str(path))[0] or "application/octet-stream"
        self.send_response(200)
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(len(raw)))
        self.send_header("Cache-Control", "no-store")
        self.end_headers()
        self.wfile.write(raw)

    def _upload(self):
        name = self.headers.get("X-Filename") or "upload.tsv"
        name = urllib.parse.unquote(name)
        length = int(self.headers.get("Content-Length") or 0)
        if length <= 0:
            raise ApiError(400, "empty upload")
        upload = self.service.save_upload(name, _BoundedReader(self.rfile, length),
                                          content_length=length)
        self._json({"upload": _upload_public(upload)}, status=201)

    def _download(self, job_id: str):
        path = self.service.result_path(job_id)
        if not path:
            raise ApiError(404, "result not available")
        size = path.stat().st_size
        self.send_response(200)
        ctype = mimetypes.guess_type(str(path))[0] or "application/octet-stream"
        self.send_header("Content-Type", ctype)
        self.send_header("Content-Length", str(size))
        self.send_header(
            "Content-Disposition", f'attachment; filename="{job_id}-result.dat"'
        )
        self.end_headers()
        with open(path, "rb") as fh:
            shutil.copyfileobj(fh, self.wfile)

    def _reset(self):
        """Test-only reset. Only allowed from loopback peers."""
        host = self.client_address[0]
        if host not in ("127.0.0.1", "::1"):
            raise ApiError(403, "reset is loopback-only")
        svc = self.service
        # Stop workers by cancelling, then wipe rows and files.
        for job in svc.store.list_jobs():
            try:
                svc.scheduler.cancel(job["id"])
            except Exception:
                pass
        time.sleep(0.05)
        with svc.store.tx() as cur:
            for table in (
                "artifact_refs", "artifacts", "reservations", "checkpoints",
                "heartbeats", "leases", "ledger_entries", "events", "uploads",
                "jobs",
            ):
                cur.execute(f"DELETE FROM {table}")
        for sub in (svc.config.tmp_dir, svc.config.files_dir):
            if sub.exists():
                shutil.rmtree(sub, ignore_errors=True)
            sub.mkdir(parents=True, exist_ok=True)
        svc.scheduler.wake()
        self._json({"ok": True})


def _upload_public(upload: dict) -> dict:
    return {k: upload[k] for k in ("id", "name", "bytes", "sha256", "created_at")}


class Server(ThreadingHTTPServer):
    daemon_threads = True
    allow_reuse_address = True

    def __init__(self, address, service: Service):
        self.service = service
        super().__init__(address, Handler)


def serve(config: Config, service: Service | None = None) -> Server:
    svc = service or Service(config)
    svc.start()
    server = Server((config.host, config.port), svc)
    return server


def run_forever(config: Config, service: Service | None = None) -> None:
    server = serve(config, service)
    host, port = server.server_address[:2]
    print(f"SpillForge listening on http://{host}:{port}")
    print(f"data dir: {config.data_dir}  budget: {config.disk_budget_bytes} bytes")
    try:
        server.serve_forever(poll_interval=0.25)
    except KeyboardInterrupt:
        pass
    finally:
        server.shutdown()
        server.service.stop()
