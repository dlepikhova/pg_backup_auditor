"""check: exit codes and messages a script or monitoring relies on."""

import pytest


def test_backup_id_not_found(auditor, probackup_catalog):
    catalog = probackup_catalog(("TST001", "FULL", "OK"))

    r = auditor("check", "-B", catalog, "--backup-id", "NOPE")

    assert r.returncode == 1
    assert "Backup not found: NOPE" in r.stderr


def test_nothing_eligible_for_validation(auditor, probackup_catalog):
    catalog = probackup_catalog(("TST001", "FULL", "ERROR"))

    r = auditor("check", "-B", catalog)

    assert r.returncode == 2
    assert "NO VALIDATION PERFORMED" in r.stdout


@pytest.mark.xfail(reason="color codes are written to non-terminal output (ROADMAP v0.4.0: --no-color)")
def test_interrupted_merge_skipped_with_hint(auditor, probackup_catalog):
    catalog = probackup_catalog(("TST001", "FULL", "MERGED"))

    r = auditor("check", "-B", catalog)

    assert "[SKIPPED] Status: MERGED" in r.stdout
    assert "rerun pg_probackup merge" in r.stdout
