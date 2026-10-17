"""
Shared fixtures for CLI tests: run the built pg_backup_auditor binary
against small catalogs created in a temporary directory.

The binary is taken from PG_BACKUP_AUDITOR, or ./pg_backup_auditor in
the repository root when the variable is not set.
"""

import os
import subprocess
from pathlib import Path

import pytest

REPO_ROOT = Path(__file__).resolve().parents[2]


@pytest.fixture(scope="session")
def auditor_bin():
    path = Path(os.environ.get("PG_BACKUP_AUDITOR", REPO_ROOT / "pg_backup_auditor"))
    if not path.is_file():
        pytest.exit(f"pg_backup_auditor binary not found: {path} (build it first)", returncode=2)
    return path


@pytest.fixture
def auditor(auditor_bin):
    """Run the binary; returns CompletedProcess with text stdout/stderr."""

    def run(*args, tz="UTC", env=None):
        run_env = dict(os.environ, TZ=tz)
        if env:
            run_env.update(env)
        return subprocess.run(
            [str(auditor_bin), *map(str, args)],
            capture_output=True,
            text=True,
            env=run_env,
            timeout=60,
        )

    return run


@pytest.fixture
def probackup_catalog(tmp_path):
    """
    Build a pg_probackup catalog: backups/<instance>/<id>/backup.control
    plus the database/ directory pg_probackup always creates.

    Each backup is (backup_id, mode, status) or a dict with extra
    backup.control keys, e.g. {"id": "B", "mode": "PAGE", "status": "OK",
    "parent-backup-id": "A"}.
    """

    def build(*backups, instance="node1"):
        root = tmp_path / "catalog"
        for spec in backups:
            if isinstance(spec, tuple):
                spec = {"id": spec[0], "mode": spec[1], "status": spec[2]}
            fields = {
                "backup-id": spec["id"],
                "backup-mode": spec["mode"],
                "status": spec["status"],
                "timeline": "1",
                "start-lsn": "0/2000028",
                "stop-lsn": "0/2000100",
                "start-time": "'2026-10-01 10:00:00+00'",
                "end-time": "'2026-10-01 10:05:00+00'",
                "stream": "true",
            }
            fields.update({k: v for k, v in spec.items() if k not in ("id", "mode", "status")})

            backup_dir = root / "backups" / instance / spec["id"]
            (backup_dir / "database").mkdir(parents=True)
            (backup_dir / "backup.control").write_text(
                "".join(f"{k} = {v}\n" for k, v in fields.items())
            )
        return root

    return build
