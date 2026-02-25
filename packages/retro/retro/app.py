"""Desktop app entry point — pywebview + Flask + background worker."""

import socket
import threading

from werkzeug.serving import make_server

from agenttrace.watcher import IngestionWorker
from retro.server import app as flask_app, set_worker


def _find_free_port() -> int:
    """Find an available TCP port on localhost."""
    with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
        s.bind(("127.0.0.1", 0))
        return s.getsockname()[1]


def _needs_initial_ingest() -> bool:
    """Check if the DB has no sessions yet."""
    from agenttrace.db import get_writer

    # Ensure schema is initialized by getting writer
    conn = get_writer()
    count = conn.execute("SELECT COUNT(*) FROM sessions").fetchone()[0]
    return count == 0


def launch():
    """Start Flask server + ingestion worker, then open a pywebview window."""
    import webview

    port = _find_free_port()
    url = f"http://127.0.0.1:{port}"

    # Start Flask in a daemon thread
    server = make_server("127.0.0.1", port, flask_app)
    flask_thread = threading.Thread(
        target=server.serve_forever, daemon=True, name="flask-server"
    )
    flask_thread.start()

    # Start background ingestion worker — run pipeline immediately if DB is empty
    worker = IngestionWorker(run_immediately=_needs_initial_ingest())
    set_worker(worker)
    worker.start()

    # Create and run the native window (blocks until closed)
    webview.create_window("Claude Retro", url, width=1280, height=860)
    webview.start()

    # Cleanup
    server.shutdown()
    worker.stop()
