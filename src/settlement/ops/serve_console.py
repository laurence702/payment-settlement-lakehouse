"""Entry point: `python -m settlement.ops.serve_console`.

Applies pending migrations, then serves the console. Host and port come from
the environment so the same command works in a container and on a laptop.
"""

from __future__ import annotations

import os

import uvicorn

from settlement.config import get_settings
from settlement.ops.app import create_app
from settlement.ops.migrate import migrate
from settlement.ops.postgres_store import PostgresCaseStore


def main() -> None:
    settings = get_settings()
    migrate(settings)
    app = create_app(PostgresCaseStore(settings))
    uvicorn.run(
        app,
        host=os.getenv("OPS_BIND_HOST", "127.0.0.1"),
        port=int(os.getenv("OPS_PORT", "8080")),
        log_level="info",
    )


if __name__ == "__main__":
    main()
