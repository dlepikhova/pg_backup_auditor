# pg_backup_auditor

Cross-platform PostgreSQL backup auditor - unified analysis and validation tool for PostgreSQL backups.

## Overview

`pg_backup_auditor` is a command-line utility for auditing, validating, and analyzing PostgreSQL backups created by various backup tools (pg_basebackup, pg_probackup, and more in the future).

## Features

- Unified interface for pg_basebackup and pg_probackup 2.5.X
- pg_basebackup: plain and tar formats, all compressions (gzip, bzip2, xz, lz4), pg_combinebackup (PG 17+)
- pg_probackup: FULL, PAGE, DELTA, PTRACK backup types, all statuses
- Backup listing with filtering, sorting, grouping by directory
- Detailed backup info: timing, storage, PostgreSQL metadata (LSN, timeline, WAL)
- Backup validation with 4 levels (basic → standard → checksums → full):
  - Level 2 (standard): metadata consistency — timestamps, LSN range, timeline, version
  - Level 3 (checksums): WAL availability + continuity + timeline history + full WAL header and per-record CRC32C validation
- Color output with `--no-color` option

## Quick Start

### Prerequisites

**Required:**
- C11-compatible compiler (GCC 7+, Clang 10+)
- macOS, Linux, or FreeBSD

**Optional:**
- PostgreSQL 10+ headers (for extended PostgreSQL integration)
- Meson >= 0.55.0 and Ninja (for Meson build system)
- zlib (for compressed backup support)

### Build

**Makefile:**
```bash
make
```

**Meson (recommended):**
```bash
meson setup builddir
meson compile -C builddir
```

See [Meson Build Guide](docs/MESON_BUILD.md) for detailed instructions.

### Install

**Makefile:**
```bash
sudo make install PREFIX=/usr/local
```

**Meson:**
```bash
sudo meson install -C builddir
```

### Usage

```bash
# List all backups in a directory
pg_backup_auditor list --backup-dir=/var/lib/pgbackup

# List with filtering and sorting (newest first, only OK pg_probackup)
pg_backup_auditor list -B /var/lib/pgbackup \
  --type=pg_probackup --status=ok --sort-by=start_time --reverse

# Limit output and control scanning depth
pg_backup_auditor list -B /var/lib/pgbackup --limit=10 --max-depth=2

# Show detailed backup information
pg_backup_auditor info --backup-dir=/var/lib/pgbackup --backup-id=T92Y4K
pg_backup_auditor info --backup-path=/var/lib/pgbackup/instance/T92Y4K

# Validate backups (standard level by default)
pg_backup_auditor check --backup-dir=/var/lib/pgbackup
pg_backup_auditor check -B /var/lib/pgbackup --level=full

# Disable color output
pg_backup_auditor list -B /var/lib/pgbackup --no-color
```

## Commands

### `list`

Display all found backups with detailed information including total size.

```bash
pg_backup_auditor list --backup-dir=PATH [OPTIONS]
```

Options:
- `--backup-dir=PATH` - Backup directory (required)
- `--type=TYPE` - Filter by type (auto|pg_basebackup|pg_probackup)
- `--status=STATUS` - Filter by status (all|ok|warning|error|corrupt|orphan)
- `--sort-by=FIELD` - Sort by field (start_time|end_time|name|size)
- `--reverse, -r` - Reverse sort order (newest first)
- `--limit=N, -n N` - Limit output to N backups
- `--max-depth=N, -d N` - Maximum recursion depth (-1 = unlimited, 0 = current dir only)
- `--help, -h` - Show comprehensive help

**Output**: backups grouped by directory, instance names shown for pg_probackup, summary with total count and total size.

### `check`

Validate backup consistency. Scans all backups in the directory and runs validation at the specified level. Backups with status ERROR or CORRUPT are skipped automatically.

```bash
pg_backup_auditor check --backup-dir=PATH [OPTIONS]
```

Options:
- `--backup-dir=PATH, -B PATH` - Backup directory (required)
- `--backup-id=ID, -i ID` - Validate only the specified backup
- `--wal-archive=PATH, -w PATH` - External WAL archive directory (optional, needed for level 3)
- `--level=LEVEL, -l LEVEL` - Validation level (default: standard)
- `--skip-wal` - Skip all WAL checks
- `--help, -h` - Show help

**Validation levels** (cumulative — each level includes all checks from previous levels):

| Level | Name | Checks |
|-------|------|--------|
| 1 | `basic` | File structure, backup chain connectivity, WAL presence in backup |
| 2 | `standard` | Metadata: timestamps, LSN range, timeline, PostgreSQL version *(default)* |
| 3 | `checksums` | WAL availability (all required segments present) + WAL header validation |
| 4 | `full` | All checks + pg_verifybackup |

**Level 3 WAL checks** (require `--wal-archive` or auto-detected for pg_probackup):
- WAL availability: every segment in [start_lsn, stop_lsn] exists in the archive
- WAL continuity: no gaps between consecutive segments in the archive
- WAL timeline history: `.history` file present for timeline > 1
- WAL header validation: reads `XLogLongPageHeaderData` (40 bytes) from each segment and verifies:
  - non-zero magic, `XLP_LONG_HEADER` flag set
  - timeline matches backup, page address matches segment start
  - segment size is a valid power of two (1 MB – 1 GB), block size in [512, 65536]
  - CRC32C of the first `XLogRecord`
- WAL per-record CRC32C: reads the segment page by page and validates the CRC32C of every complete single-page `XLogRecord`; records that span a page boundary or have `xl_crc=0` (synthetic pg_probackup records) are intentionally skipped

**Output**: per-backup results with [OK] / [WARNING] / [ERROR] labels, summary with Total / Validated / Skipped counts, and overall result (OK / WARNING / FAILED).

### `info`

Show detailed information about a specific backup.

```bash
# By backup ID (searches in backup directory)
pg_backup_auditor info --backup-dir=PATH --backup-id=BACKUP_ID

# By direct path
pg_backup_auditor info --backup-path=PATH
```

Options:
- `--backup-dir=PATH, -B PATH` - Backup directory for searching by ID
- `--backup-id=ID, -i ID` - Backup ID to search for
- `--backup-path=PATH, -p PATH` - Direct path to backup directory
- `--help, -h` - Show help

Either `--backup-path` or (`--backup-dir` + `--backup-id`) is required.

**Output sections**:
- GENERAL — ID, node, instance, type, tool, tool version, status
- TIMING — start time, end time, duration
- STORAGE — path, data size, WAL size
- POSTGRESQL — version, timeline, start/stop LSN, WAL range, WAL start file, backup method, backup from, label

## Exit Codes

- `0` - Success
- `1` - General error
- `2` - Validation failed (check command)
- `3` - Critical error / No backups found
- `4` - Invalid arguments

## Supported Platforms

### Tier 1 (Primary)
- macOS 12+ (Intel and Apple Silicon)
- Ubuntu 20.04+, 22.04+, 24.04+
- Debian 11, 12
- RHEL/Rocky/AlmaLinux 8, 9

### Tier 2 (Best effort)
- FreeBSD 13.x, 14.x
- Arch Linux, openSUSE, Alpine

## Testing

### Run Unit Tests

**Meson:**
```bash
meson test -C builddir
```

**Makefile:**
```bash
cd tests/unit
make run
```

### Test Coverage

**Test suite**: 130 unit and integration tests (100% passing)

- **WAL validator**: availability, continuity, timeline history, header validation, per-record CRC32C (valid / mismatch / invalid tot_len / zero CRC skipped / multi-page skipped)
- **Adapters**: pg_basebackup, pg_probackup (scan, WAL path detection, end-to-end)
- **Common utilities**: string_utils, xlog (LSN/segment parsing and formatting), INI parser
- **Integration tests** with synthetic pg_probackup backup catalog (auto-generated; skipped if env vars not set)

## Development

### Debug Build

**Makefile:**
```bash
make DEBUG=1
```

**Meson:**
```bash
meson setup builddir --buildtype=debug
meson compile -C builddir
```

### Clean Build

**Makefile:**
```bash
make clean
make
```

**Meson:**
```bash
rm -rf builddir
meson setup builddir
meson compile -C builddir
```

### Dependencies

**Makefile:**
```bash
make depend
```

**Meson:**
```bash
# Dependencies are handled automatically
meson setup builddir --reconfigure
```

## Documentation

- [Installation Guide](docs/INSTALL.md) - Platform-specific installation
- [Meson Build Guide](docs/MESON_BUILD.md) - Detailed Meson instructions

## License

GNU General Public License v3 or later (GPL-3.0-or-later). See [LICENSE](LICENSE).

## Authors

- Daria Lepikhova daria.n.lepikhova@gmail.com

