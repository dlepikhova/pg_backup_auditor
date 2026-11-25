"""Color output: --no-color anywhere on the command line, none outside a terminal."""

import pytest

ESC = "\x1b["

COMMANDS = [
    ("list",),
    ("check",),
    ("audit",),
    ("stat",),
    ("info", "--backup-id", "TST001"),
]


@pytest.fixture
def catalog(probackup_catalog):
    return probackup_catalog(("TST001", "FULL", "OK"))


@pytest.mark.parametrize("cmd", COMMANDS, ids=lambda c: c[0])
def test_no_color_accepted_after_command(auditor, catalog, cmd):
    r = auditor(cmd[0], "-B", catalog, *cmd[1:], "--no-color")

    assert r.returncode != 4, r.stderr
    assert "unrecognized option" not in r.stderr


def test_no_color_accepted_before_command(auditor, catalog):
    r = auditor("--no-color", "list", "-B", catalog)

    assert r.returncode == 0, r.stderr
    assert "TST001" in r.stdout


@pytest.mark.parametrize("cmd", COMMANDS, ids=lambda c: c[0])
def test_no_color_codes_when_not_a_terminal(auditor, catalog, cmd):
    r = auditor(cmd[0], "-B", catalog, *cmd[1:])

    assert ESC not in r.stdout
