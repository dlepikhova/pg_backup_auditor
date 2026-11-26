"""WAL between consecutive backups in audit and check: what was actually checked."""

import pytest

SEGMENTS = [f"0000000100000000000000{n:02X}" for n in range(1, 6)]


def backup(backup_id, status, start, stop):
    return {"id": backup_id, "mode": "FULL", "status": status,
            "start-lsn": start, "stop-lsn": stop, "stream": "false"}


def run(auditor, cmd, catalog):
    if cmd == "check":
        return auditor("check", "-B", catalog, "--level=checksums")
    return auditor("audit", "-B", catalog)


@pytest.mark.parametrize("cmd", ["audit", "check"])
def test_one_backup_not_checked(auditor, probackup_catalog, cmd):
    catalog = probackup_catalog(backup("TST001", "OK", "0/5000028", "0/5000100"),
                                wal=SEGMENTS)

    r = run(auditor, cmd, catalog)

    assert "[NOT CHECKED]" in r.stdout, r.stdout
    assert "needs at least 2 completed backups (OK, DONE or WARNING), found 1: TST001" in r.stdout
    assert "no WAL gaps" not in r.stdout


@pytest.mark.parametrize("cmd", ["audit", "check"])
def test_ok_names_first_and_last_backup(auditor, probackup_catalog, cmd):
    catalog = probackup_catalog(backup("TST001", "OK", "0/2000028", "0/2000100"),
                                backup("TST002", "DONE", "0/5000028", "0/5000100"),
                                wal=SEGMENTS)

    r = run(auditor, cmd, catalog)

    assert "no WAL gaps between TST001 and TST002 (2 completed backups)" in r.stdout, r.stdout


@pytest.mark.parametrize("cmd", ["audit", "check"])
def test_error_backup_not_counted(auditor, probackup_catalog, cmd):
    catalog = probackup_catalog(backup("TST001", "OK", "0/2000028", "0/2000100"),
                                backup("TST002", "ERROR", "0/3000028", "0/3000100"),
                                wal=SEGMENTS)

    r = run(auditor, cmd, catalog)

    assert "found 1: TST001" in r.stdout, r.stdout
