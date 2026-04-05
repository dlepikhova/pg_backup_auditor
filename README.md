# pg_backup_auditor

Cross-platform PostgreSQL backup auditor — unified analysis and validation tool for pg_basebackup, pg_probackup, and pgBackRest backups.

## Features

- **Three adapters**: pg_basebackup, pg_probackup 2.5/2.8, pgBackRest
- **Backup listing** with filtering by tool/status, sorting, grouping by directory
- **Detailed backup info**: timing, storage, PostgreSQL metadata (LSN, timeline, WAL)
- **Backup validation** with 4 levels (basic → standard → checksums → full):
  - Level 1 (basic): on-disk structure — required files and directories, backup chain integrity
  - Level 2 (standard): metadata consistency — timestamps, LSN range, timeline, pg version
  - Level 3 (checksums): per-file checksums + WAL availability, continuity, restore chain, header validation
  - Level 4 (full): archive-wide WAL header scan (detects segment swaps outside backup LSN ranges)
- **Per-tool structure checks**:
  - pg_basebackup: `base/`, `global/pg_control`, `PG_VERSION`, `backup_label`/`backup_manifest`, stream WAL
  - pg_probackup: `database/`, `database_map`, `pg_control`, `PG_VERSION`, `backup_label` (archive mode), stream WAL
  - pgBackRest: `backup.manifest`, `backup-error` detection, `pg_data/pg_control`, `PG_VERSION`, `manifest.copy`
- **Per-file checksum validation**:
  - pg_basebackup: SHA256 and CRC32C from `backup_manifest` + manifest self-checksum
  - pg_probackup: CRC32C from `backup_content.control`
  - pgBackRest: SHA1 from `backup.manifest` `[target:file]` section
- **Chain validation**: pg_probackup (FULL/DELTA/PAGE/PTRACK) and pgBackRest (FULL/DIFF/INCR)
- **STREAM backup WAL**: embedded `database/pg_wal/` and `pg_wal/` scanned automatically
- **WAL segment size** auto-detected from segment headers (1 MB – 1 GB)
- Color output with `--no-color`

## Quick Start

### Prerequisites

**Required:**
- C11-compatible compiler (GCC 7+, Clang 10+)
- macOS, Linux, or FreeBSD

**Optional:**
- PostgreSQL headers (`pg_config` in PATH)
- Meson >= 0.55.0 + Ninja (alternative build system)
- zlib (compressed backup support)

### Build

**Makefile:**
```bash
make
```

**Meson:**
```bash
meson setup builddir
meson compile -C builddir
```

### Install

```bash
sudo make install PREFIX=/usr/local
# or
sudo meson install -C builddir
```

### Usage

```bash
# List all backups
pg_backup_auditor list --backup-dir=/var/lib/pgbackup

# Filter and sort
pg_backup_auditor list -B /var/lib/pgbackup \
  --type=pg_probackup --status=ok --sort-by=start_time --reverse

# Show detailed backup info
pg_backup_auditor info --backup-dir=/var/lib/pgbackup --backup-id=20240101-120000F
pg_backup_auditor info --backup-path=/var/lib/pgbackup/main/20240101-120000F

# Validate (standard level by default)
pg_backup_auditor check --backup-dir=/var/lib/pgbackup

# Full validation with WAL archive
pg_backup_auditor check -B /var/lib/pgbackup --wal-archive=/var/lib/wal --level=full

# Validate single backup
pg_backup_auditor check -B /var/lib/pgbackup --backup-id=20240101-120000F --level=checksums
```

## Commands

### `list`

Display all found backups with timing, storage, and PostgreSQL metadata.

```
pg_backup_auditor list --backup-dir=PATH [OPTIONS]
```

| Option | Description |
|--------|-------------|
| `--backup-dir=PATH, -B PATH` | Backup directory (required) |
| `--type=TYPE` | Filter: `auto`, `pg_basebackup`, `pg_probackup`, `pgbackrest` |
| `--status=STATUS` | Filter: `all`, `ok`, `warning`, `error`, `corrupt`, `orphan` |
| `--sort-by=FIELD` | Sort: `start_time` (default), `end_time`, `name`, `size` |
| `--reverse, -r` | Reverse sort order |
| `--limit=N, -n N` | Limit total output to N backups |
| `--max-depth=N, -d N` | Recursion depth (-1 = unlimited) |

### `check`

Validate backup consistency and WAL availability.

```
pg_backup_auditor check --backup-dir=PATH [OPTIONS]
```

| Option | Description |
|--------|-------------|
| `--backup-dir=PATH, -B PATH` | Backup directory (required) |
| `--backup-id=ID, -i ID` | Validate only specified backup |
| `--wal-archive=PATH, -w PATH` | External WAL archive (for level 3+) |
| `--level=LEVEL, -l LEVEL` | Validation level (default: `standard`) |
| `--skip-wal` | Skip all WAL checks |

**Validation levels** (cumulative):

| Level | Name | Checks |
|-------|------|--------|
| 1 | `basic` | On-disk structure, backup chain integrity |
| 2 | `standard` | Timestamps, LSN range, timeline, pg version *(default)* |
| 3 | `checksums` | Per-file checksums, WAL availability, continuity, restore chain, header validation |
| 4 | `full` | All previous + archive-wide WAL header scan |

### `info`

Show detailed information about a specific backup.

```
pg_backup_auditor info --backup-path=PATH
pg_backup_auditor info --backup-dir=PATH --backup-id=ID
```

## Exit Codes

| Code | Meaning |
|------|---------|
| 0 | Success |
| 1 | General error |
| 2 | Validation failed |
| 3 | Critical error / no backups found |
| 4 | Invalid arguments |

## Supported Platforms

**Tier 1:** macOS 12+ (Intel/Apple Silicon), Ubuntu 20.04/22.04/24.04, Debian 11/12, RHEL/Rocky/AlmaLinux 8/9

**Tier 2:** FreeBSD 13/14, Arch Linux, openSUSE, Alpine

## Known Limitations

- **pgBackRest WAL**: pgBackRest stores WAL in subdirectories with hash-suffixed compressed filenames — this format cannot be scanned by the WAL validator. WAL checks are skipped for pgBackRest backups; a note is shown in output.
- **pg_basebackup tar format**: per-file checksums inside tar archives require unpacking and are not verified. The manifest self-checksum is still validated.
- **pgBackRest compressed backups**: per-file checksums require decompression and are not verified for compressed (`pg_data.gz` etc.) backups.
- **`tool_version`**: the version of the backup tool is not populated (pgBackRest does not expose it in the manifest).
- **pg_basebackup incremental (PG17+)**: incremental backups created with `pg_basebackup --incremental` are detected but chain validation is not yet implemented.

## Testing

```bash
# Makefile
make test

# Meson
meson test -C builddir
```

**217 unit tests, 100% passing.**

Test suite covers: WAL validator (availability, continuity, restore chain, headers, CRC32C, segment size, stream WAL), backup validator (structure, metadata, checksums for all three tools, chain validation), adapters (pg_basebackup, pg_probackup, pgBackRest — scan, metadata, WAL path), common utilities (string_utils, xlog, INI parser, fs_scanner).

## Documentation

- [Installation Guide](docs/INSTALL.md)
- [Developer Guide](docs/DEVELOPER.md)
- [Architecture](docs/ARCHITECTURE.md)
- [Meson Build Guide](docs/MESON_BUILD.md)

## License

GNU General Public License v3 or later (GPL-3.0-or-later).

## Author

Daria Lepikhova <daria.n.lepikhova@gmail.com>
