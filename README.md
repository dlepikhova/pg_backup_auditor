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
- Backup validation with 4 levels (basic → standard → checksums → full)
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
- `--level=LEVEL` - Validation level (default: standard)
- `--help, -h` - Show help

**Validation levels** (cumulative — each level includes all checks from the previous):
- `basic` - Backup status from metadata only
- `standard` - Metadata checks (backup_id, timestamps, LSN consistency)
- `checksums` - Checksum verification (if available in backup)
- `full` - All checks including WAL continuity

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

Current test coverage: **~68% overall**

- **Adapters**: 85% (pg_basebackup), 67% (pg_probackup)
- **Common utilities**: 80% (string_utils, logging, file_utils)
- **CLI commands**: 60% (list, info, check)
- **Scanners**: 70% (fs_scanner)

**Test suite**:
- 45 unit tests (all passing)
- Integration tests via `test_real_backups.sh`
- Tests for extended metadata extraction (9 tests)
- Tests for backup detection, scanning, and parsing

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

