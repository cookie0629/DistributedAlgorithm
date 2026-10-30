"""Static membership and timing configuration (all durations are milliseconds)."""

from dataclasses import dataclass
import json
import math
from pathlib import Path
from urllib.parse import urlsplit


@dataclass(frozen=True)
class Config:
    nodes: dict[str, str]
    heartbeat_interval_ms: float = 100
    election_timeout_min_ms: float = 500
    election_timeout_max_ms: float = 1000
    rpc_timeout_ms: float = 200

    @classmethod
    def load(cls, path: str, node_id: str) -> "Config":
        raw = json.loads(Path(path).read_text(encoding="utf-8-sig"))
        if not isinstance(raw, dict):
            raise ValueError("Configuration must be a JSON object")
        allowed = {"nodes", "heartbeat_interval_ms", "election_timeout_min_ms",
                   "election_timeout_max_ms", "rpc_timeout_ms"}
        if raw.keys() - allowed:
            raise ValueError(f"Unknown configuration keys: {sorted(raw.keys() - allowed)}")
        members = raw.get("nodes")
        if not isinstance(members, dict) or not members:
            raise ValueError("nodes must be a nonempty ID-to-HTTP-address object")
        nodes = {}
        addresses = set()
        for member_id, address in members.items():
            if not isinstance(member_id, str) or not member_id.strip():
                raise ValueError("Node IDs must be nonempty strings")
            if not isinstance(address, str) or any(c.isspace() for c in address):
                raise ValueError(f"Invalid address for {member_id}")
            url = urlsplit(address)
            if (url.scheme != "http" or not url.hostname or not url.port
                    or url.username is not None or url.password is not None
                    or url.path not in ("", "/") or url.query or url.fragment):
                raise ValueError(f"Address for {member_id} must be http://host:port")
            endpoint = (url.hostname.lower(), url.port)
            if endpoint in addresses:
                raise ValueError(f"Duplicate node address: {address}")
            addresses.add(endpoint)
            nodes[member_id] = address.rstrip("/")
        if node_id not in nodes:
            raise ValueError(f"Node {node_id!r} is not in nodes")
        timing = {}
        for name in allowed - {"nodes"}:
            value = raw.get(name, getattr(cls, name))
            if (type(value) not in (int, float) or not math.isfinite(value)
                    or value <= 0):
                raise ValueError(f"{name} must be a finite positive number")
            timing[name] = value
        config = cls(nodes=nodes, **timing)
        if not (config.heartbeat_interval_ms < config.election_timeout_min_ms
                < config.election_timeout_max_ms):
            raise ValueError("Require heartbeat_interval_ms < election_timeout_min_ms "
                             "< election_timeout_max_ms")
        if config.rpc_timeout_ms >= config.election_timeout_min_ms:
            raise ValueError("rpc_timeout_ms must be less than election_timeout_min_ms")
        return config
