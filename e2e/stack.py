"""Connection settings for the locally running stack, read from the project .env."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def read_env(path: Path = ROOT / ".env") -> dict[str, str]:
    """Parse KEY=VALUE lines; surrounding quotes are stripped, comments ignored."""
    values: dict[str, str] = {}
    for raw in path.read_text().splitlines():
        line = raw.strip()
        if not line or line.startswith("#") or "=" not in line:
            continue
        key, _, value = line.partition("=")
        values[key.strip()] = value.strip().strip('"').strip("'")
    return values


@dataclass(frozen=True)
class Stack:
    airflow_url: str
    ops_url: str
    airflow_user: str
    airflow_password: str

    @classmethod
    def from_env_file(cls) -> Stack:
        env = read_env()
        return cls(
            airflow_url=f"http://localhost:{env['PORT_AIRFLOW']}",
            ops_url=f"http://127.0.0.1:{env.get('PORT_OPS', '8090')}",
            airflow_user=env["AIRFLOW_ADMIN_USER"],
            airflow_password=env["AIRFLOW_ADMIN_PASSWORD"],
        )
