"""Small JSON logging helper."""

from datetime import datetime, timezone
import json
import logging


def emit(event: str, *, level: int = logging.INFO, **fields) -> None:
    logging.getLogger("raft_election").log(level, json.dumps({
        "timestamp": datetime.now(timezone.utc).isoformat(),
        "event": event,
        **fields,
    }, ensure_ascii=False))
