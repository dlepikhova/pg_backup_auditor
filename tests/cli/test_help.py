"""help command: general and per-command usage."""

import pytest


def test_help_prints_general_usage(auditor):
    r = auditor("help")

    assert r.returncode == 0, r.stderr
    assert r.stdout == auditor("--help").stdout


@pytest.mark.parametrize("cmd", ["list", "check", "info", "audit", "stat"])
def test_help_command_matches_command_help(auditor, cmd):
    r = auditor("help", cmd)

    assert r.returncode == 0, r.stderr
    assert r.stdout == auditor(cmd, "--help").stdout


def test_help_unknown_command(auditor):
    r = auditor("help", "nope")

    assert r.returncode == 4
    assert "Unknown command 'nope'" in r.stderr
