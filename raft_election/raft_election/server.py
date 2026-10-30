"""Status REST endpoints and the two election RPC endpoints."""

from http.server import BaseHTTPRequestHandler, ThreadingHTTPServer
import json
import socket
from urllib.parse import urlsplit

from .node import NodeUnavailable, RaftNode
from .protocol import AppendEntries, MAX_BODY_BYTES, RequestVote


def make_server(node: RaftNode) -> ThreadingHTTPServer:
    class Handler(BaseHTTPRequestHandler):
        def setup(self) -> None:
            super().setup()
            self.connection.settimeout(2)

        def log_message(self, format: str, *args) -> None:
            pass  # Role/term changes are logged by the node, rather than every heartbeat.

        def _reply(self, status: int, body: dict) -> None:
            data = json.dumps(body, ensure_ascii=False).encode("utf-8")
            try:
                self.send_response(status)
                self.send_header("Content-Type", "application/json; charset=utf-8")
                self.send_header("Content-Length", str(len(data)))
                self.send_header("Connection", "close")
                self.end_headers()
                self.wfile.write(data)
            except (OSError, TimeoutError):
                pass  # A timed-out client may already have disconnected.
            self.close_connection = True

        def do_GET(self) -> None:
            path = urlsplit(self.path).path
            if path not in ("/status", "/health"):
                self._reply(404, {"error": "Unknown endpoint"})
                return
            try:
                snapshot = node.status()
                body = snapshot if path == "/status" else {
                    "status": "ok", "nodeId": snapshot["nodeId"]}
                self._reply(200, body)
            except NodeUnavailable as exc:
                self._reply(503, {"error": str(exc)})

        def do_POST(self) -> None:
            path = urlsplit(self.path).path
            if path not in ("/raft/request-vote", "/raft/append-entries"):
                self._reply(404, {"error": "Unknown endpoint"})
                return
            try:
                if self.headers.get("Transfer-Encoding"):
                    raise ValueError("Transfer-Encoding is not supported")
                length = int(self.headers.get("Content-Length", "0"))
                if length <= 0:
                    raise ValueError("A JSON request body is required")
                if length > MAX_BODY_BYTES:
                    self._reply(413, {"error": "Request body exceeds 64 KiB"})
                    return
                data = self.rfile.read(length)
                if len(data) != length:
                    raise ValueError("Request body is shorter than Content-Length")
                body = json.loads(data.decode("utf-8"))
                if not isinstance(body, dict):
                    raise ValueError("Request body must be a JSON object")
                if path == "/raft/request-vote":
                    request = RequestVote.parse(body, node.config.nodes)
                    response = node.request_vote(request)
                else:
                    request = AppendEntries.parse(body, node.config.nodes)
                    response = node.append_entries(request)
                self._reply(200, response)
            except (ValueError, UnicodeError) as exc:
                self._reply(400, {"error": str(exc)})
            except (OSError, TimeoutError):
                self._reply(408, {"error": "Timed out reading request"})
            except NodeUnavailable as exc:
                self._reply(503, {"error": str(exc)})

    address = urlsplit(node.config.nodes[node.node_id])

    class Server(ThreadingHTTPServer):
        address_family = socket.AF_INET6 if ":" in address.hostname else socket.AF_INET
        daemon_threads = True

    return Server((address.hostname, address.port), Handler)
