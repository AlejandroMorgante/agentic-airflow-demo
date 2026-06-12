from __future__ import annotations

import json
import logging
import os
from http.server import BaseHTTPRequestHandler, HTTPServer
from typing import Any, Callable

log = logging.getLogger(__name__)


class LocalInvocationApp:
    def __init__(self) -> None:
        self._handler: Callable[[dict[str, Any]], dict[str, Any]] | None = None

    def entrypoint(self, func: Callable[[dict[str, Any]], dict[str, Any]]):
        self._handler = func
        return func

    def run(self) -> None:
        if self._handler is None:
            raise RuntimeError("No entrypoint registered")

        handler_func = self._handler

        class InvocationHandler(BaseHTTPRequestHandler):
            def do_GET(self) -> None:
                if self.path not in {"/", "/health"}:
                    self.send_response(404)
                    self.end_headers()
                    return
                body = b'{"ok": true}'
                self.send_response(200)
                self.send_header("content-type", "application/json")
                self.send_header("content-length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def do_POST(self) -> None:
                if self.path not in {"/invocations", "/api/reasoning_engine"}:
                    self.send_response(404)
                    self.end_headers()
                    return

                length = int(self.headers.get("content-length", "0"))
                payload = json.loads(self.rfile.read(length) or b"{}")
                if self.path == "/api/reasoning_engine":
                    input_val = payload.get("input") or payload
                    if isinstance(input_val, str):
                        input_val = json.loads(input_val)
                    response = {"output": handler_func(input_val)}
                else:
                    response = handler_func(payload)
                body = json.dumps(response).encode()
                self.send_response(200)
                self.send_header("content-type", "application/json")
                self.send_header("content-length", str(len(body)))
                self.end_headers()
                self.wfile.write(body)

            def log_message(self, format: str, *args: Any) -> None:
                log.info(format, *args)

        port = int(os.environ.get("PORT", "8080"))
        log.info("Starting local invocation server on :%s", port)
        HTTPServer(("0.0.0.0", port), InvocationHandler).serve_forever()
