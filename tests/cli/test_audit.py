"""audit command: output that scripts and operators act on."""


def test_audit_does_not_advise_wal_removal(auditor, probackup_catalog):
    # The backup starts in segment ...05; it must never be offered for removal
    catalog = probackup_catalog(
        {"id": "TST001", "mode": "FULL", "status": "OK",
         "start-lsn": "0/5000028", "stop-lsn": "0/5000100", "stream": "false"},
        wal=[f"0000000100000000000000{n:02X}" for n in range(1, 6)],
    )

    r = auditor("audit", "-B", catalog)

    assert "Segments:   5" in r.stdout, r.stdout
    assert "Cleanup" not in r.stdout
    assert "can be safely removed" not in r.stdout
