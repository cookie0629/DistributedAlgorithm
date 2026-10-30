"""One bounded HTTP request per call; no proxy or redirect handling."""

import http.client
import json
from urllib.parse import urlsplit

from .protocol import MAX_BODY_BYTES


class HTTPTransport:
    def __init__(self, timeout_seconds: float):
        self.timeout = timeout_seconds

    def call(self, address: str, path: str, body: dict) -> dict:
        url = urlsplit(address)
        connection = http.client.HTTPConnection(url.hostname, url.port,
                                                 timeout=self.timeout)
        try:
            connection.request("POST", path, json.dumps(body).encode("utf-8"),
                               {"Content-Type": "application/json"})
            response = connection.getresponse()
            data = response.read(MAX_BODY_BYTES + 1)
            if response.status != 200:
                raise ValueError(f"RPC returned HTTP {response.status}")
            if len(data) > MAX_BODY_BYTES:
                raise ValueError("RPC response is too large")
            result = json.loads(data)
            if not isinstance(result, dict):
                raise ValueError("RPC response must be a JSON object")
            return result
        finally:
            connection.close()
