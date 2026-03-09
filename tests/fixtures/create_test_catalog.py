#!/usr/bin/env python3
"""
Create a synthetic pg_probackup test catalog for CI integration tests.

Usage:
    python3 create_test_catalog.py <output_dir>

Creates the following layout:

    <output_dir>/
        backups/
            testinstance/
                TB50D5/
                    backup.control
                    database/          <- required by pg_probackup_detect()
        wal/
            testinstance/
                000000010000000000000001   (40-byte WAL header)
                000000010000000000000002
                ...
                000000010000000000000010  (hex A)

Environment variables to use with the generated catalog:
    PG_BACKUP_AUDITOR_TEST_CATALOG  = <output_dir>
    PG_BACKUP_AUDITOR_TEST_INSTANCE = testinstance
    PG_BACKUP_AUDITOR_TEST_BACKUP_ID = TB50D5
"""

import os
import sys
import struct

# ------------------------------------------------------------------ #
# Fixture constants                                                    #
# ------------------------------------------------------------------ #

INSTANCE  = "testinstance"
BACKUP_ID = "TB50D5"
TIMELINE  = 1
SEG_SIZE  = 16 * 1024 * 1024   # 16 MB  (default PostgreSQL segment size)
BLCKSZ    = 8192                # standard PostgreSQL block size

# Non-zero system identifier (arbitrary; just needs to be != 0)
SYSID = 7308243541062227871

# Backup spans segments 1..10 (hex 1..A)
# start_lsn = 0/1000000  (start of segment 1)
# stop_lsn  = 0/A000000  (start of segment 10 = 0xA)
START_SEG = 1
STOP_SEG  = 10   # 0xA in hex


# ------------------------------------------------------------------ #
# WAL header construction                                              #
# ------------------------------------------------------------------ #

def wal_header(seg_id):
    """
    Return a 40-byte XLogLongPageHeaderData for the given segment.

    Layout (all fields little-endian):
      Offset  0-1  xlp_magic       uint16   PG16/17 magic; any non-zero value
      Offset  2-3  xlp_info        uint16   XLP_LONG_HEADER = 0x0002
      Offset  4-7  xlp_tli         uint32   timeline
      Offset  8-15 xlp_pageaddr    uint64   seg_id * SEG_SIZE
      Offset 16-19 xlp_rem_len     uint32   0  (complete record, no continuation)
      Offset 20-23 <padding>       4 bytes  alignment for xlp_sysid
      Offset 24-31 xlp_sysid       uint64   arbitrary non-zero system ID
      Offset 32-35 xlp_seg_size    uint32   must be power-of-2 in [1 MB, 1 GB]
      Offset 36-39 xlp_xlog_blcksz uint32  must be power-of-2 in [512, 65536]

    Because the file is only 40 bytes (< 64 bytes), wal_validator.c skips
    the XLogRecord CRC check, so no real WAL record data is needed.
    """
    magic    = 0xD071          # PG16/17 WAL page magic
    info     = 0x0002          # XLP_LONG_HEADER
    tli      = TIMELINE
    pageaddr = seg_id * SEG_SIZE
    rem_len  = 0

    return struct.pack('<HHIQIxxxxQII',
                       magic, info, tli, pageaddr, rem_len,
                       SYSID, SEG_SIZE, BLCKSZ)


# ------------------------------------------------------------------ #
# Helpers                                                              #
# ------------------------------------------------------------------ #

def seg_filename(tli, seg_id):
    """
    Format WAL segment filename: TTTTTTTTLLLLLLLLSSSSSSSS  (24 hex chars).
    For segment sizes <= 16 MB the high-32 part of the LSN is always 0.
    """
    return f'{tli:08X}{0:08X}{seg_id:08X}'


def lsn_str(seg_id):
    """Format the start LSN of seg_id as '0/HEXVALUE'."""
    return f'0/{seg_id * SEG_SIZE:X}'


# ------------------------------------------------------------------ #
# Catalog creation                                                     #
# ------------------------------------------------------------------ #

def create(base_dir):
    backup_dir = os.path.join(base_dir, 'backups', INSTANCE, BACKUP_ID)
    db_dir     = os.path.join(backup_dir, 'database')
    wal_dir    = os.path.join(base_dir, 'wal', INSTANCE)

    os.makedirs(db_dir,  exist_ok=True)
    os.makedirs(wal_dir, exist_ok=True)

    # --- backup.control ---
    control = (
        f'backup-id = {BACKUP_ID}\n'
        f'backup-mode = FULL\n'
        f'status = OK\n'
        f'start-lsn = {lsn_str(START_SEG)}\n'
        f'stop-lsn = {lsn_str(STOP_SEG)}\n'
        f'timeline = {TIMELINE}\n'
        f'stream = false\n'
    )
    with open(os.path.join(backup_dir, 'backup.control'), 'w') as fh:
        fh.write(control)

    # --- WAL segments (START_SEG .. STOP_SEG inclusive) ---
    for seg_id in range(START_SEG, STOP_SEG + 1):
        path = os.path.join(wal_dir, seg_filename(TIMELINE, seg_id))
        with open(path, 'wb') as fh:
            fh.write(wal_header(seg_id))

    print(f'Catalog created: {base_dir}')
    print(f'  backup : {backup_dir}')
    print(f'  wal    : {wal_dir}')
    print(f'  segments: {START_SEG}..{STOP_SEG} '
          f'({STOP_SEG - START_SEG + 1} files, each 40 bytes)')


# ------------------------------------------------------------------ #
# Entry point                                                          #
# ------------------------------------------------------------------ #

if __name__ == '__main__':
    if len(sys.argv) != 2:
        print(f'Usage: {sys.argv[0]} <output_dir>', file=sys.stderr)
        sys.exit(1)
    create(sys.argv[1])
