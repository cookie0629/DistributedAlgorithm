import argparse
import logging
import signal
import threading

from .config import Config
from .events import emit
from .node import RaftNode
from .server import make_server


def main() -> int:
    parser = argparse.ArgumentParser(description="Raft leader election and status API")
    parser.add_argument("--node-id", required=True)
    parser.add_argument("--config", required=True, help="Static cluster JSON configuration")
    args = parser.parse_args()
    logging.basicConfig(level=logging.INFO, format="%(message)s")
    shutdown = threading.Event()
    node = server = http_thread = None
    exit_code = 0

    def request_shutdown(signum, frame):
        shutdown.set()

    signal.signal(signal.SIGINT, request_shutdown)
    signal.signal(signal.SIGTERM, request_shutdown)
    try:
        config = Config.load(args.config, args.node_id)
        node = RaftNode(args.node_id, config)
        server = make_server(node)
        node.start()
        http_thread = threading.Thread(target=server.serve_forever,
                                       kwargs={"poll_interval": 0.1}, name="raft-http")
        http_thread.start()
        emit("node_started", nodeId=args.node_id, address=config.nodes[args.node_id],
             currentTerm=node.status()["currentTerm"], storage="memory")
        while not shutdown.wait(0.25):
            pass
    except Exception as exc:
        emit("startup_error", level=logging.ERROR, nodeId=args.node_id, error=str(exc))
        exit_code = 1
    finally:
        if node is not None:
            node.stop()
        if server is not None:
            if http_thread is not None and http_thread.is_alive():
                server.shutdown()
                http_thread.join()
            server.server_close()
        emit("node_stopped", nodeId=args.node_id, exitCode=exit_code)
    return exit_code


if __name__ == "__main__":
    raise SystemExit(main())
