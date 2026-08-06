# pg_backup_auditor

**Will your PostgreSQL backups actually restore?** See at a glance whether every backup across your fleet is healthy and recoverable — chains intact, checksums valid, WAL coverage in place — with one report across `pg_basebackup`, `pg_probackup`, and `pgBackRest`. No more switching between tool-specific outputs and home-grown scripts to answer the same question.

**Read-only by design.** pg_backup_auditor never writes to the backup catalog, manifests, WAL files, or status flags. Safe to point at production — it only reads what the backup tools have already written.

## Features

- **Three adapters**: pg_basebackup, pg_probackup 2.5, pgBackRest (more coming)
- **Backup listing** with filtering by tool/status, sorting, WAL mode column, tree display for chains
- **Detailed backup info**: timing, storage, PostgreSQL metadata (LSN, timeline, WAL mode)
- **Backup audit**: per-chain recovery points, RPO gap, orphaned backups, WAL coverage, disk usage
- **Backup statistics**: per-(tool, type) counts, average size/duration, success rate, average interval between backups; storage and WAL volume per day; database growth trend from FULL backups; incremental efficiency vs FULL
- **Backup validation** with 4 levels (basic → standard → checksums → full):
  - Level 1 (basic): on-disk structure — required files and directories, backup chain integrity
  - Level 2 (standard): metadata consistency — timestamps, LSN range, timeline, pg version
  - Level 3 (checksums): per-file checksums + WAL availability, continuity, restore chain, header validation
  - Level 4 (full): archive-wide WAL header scan (detects segment swaps outside backup LSN ranges)
- **Per-tool structure checks**:
  - pg_basebackup: `base/`, `global/pg_control`, `PG_VERSION`, `backup_label`/`backup_manifest`; `pg_wal/` or `pg_wal.tar*` (stream mode)
  - pg_probackup: `database/`, `database/database_map`, `database/global/pg_control`, `database/PG_VERSION` (FULL only), `database/backup_label` (archive mode), `database/pg_wal/` (stream mode)
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
- libcheck (`apt install check` / `brew install check`) — required for `make test`

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

# Scan only the top-level directory, no subdirectories
pg_backup_auditor list -B /var/lib/pgbackup --no-recurse

# Show largest 10 backups
pg_backup_auditor list -B /var/lib/pgbackup --sort-by=size --reverse --limit=10

# Show detailed backup info
pg_backup_auditor info --backup-dir=/var/lib/pgbackup --backup-id=20240101-120000F
pg_backup_auditor info --backup-path=/var/lib/pgbackup/main/20240101-120000F

# Validate (standard level by default)
pg_backup_auditor check --backup-dir=/var/lib/pgbackup

# Full validation with WAL archive
pg_backup_auditor check -B /var/lib/pgbackup --wal-archive=/var/lib/wal --level=full

# Validate single backup
pg_backup_auditor check -B /var/lib/pgbackup --backup-id=20240101-120000F --level=checksums

# Audit backup strategy (recovery points, RPO, storage)
pg_backup_auditor audit --backup-dir=/var/lib/pgbackup

# Audit with WAL archive analysis
pg_backup_auditor audit -B /var/lib/pgbackup --wal-archive=/var/lib/wal

# Audit with detection of unusually small backups
pg_backup_auditor audit -B /var/lib/pgbackup --detect-size-small
```

## How to

### Check if all backups across the fleet are healthy

```bash
pg_backup_auditor audit -B /var/lib/pgbackup
```

Look at the Verdict line (OK / WARNING / CRITICAL) and CHAINS status. The Anomalies section flags backups with unusual size or duration.

### Investigate a specific backup after a failed restore

```bash
pg_backup_auditor info  -B /var/lib/pgbackup -i 20260315-020000
pg_backup_auditor check -B /var/lib/pgbackup -i 20260315-020000 --level=checksums
```

`info` shows metadata; `check` with `--level=checksums` validates per-file integrity and WAL availability.

### Find the last valid backup before an incident

```bash
pg_backup_auditor list -B /var/lib/pgbackup --sort-by=start_time --reverse --status=ok
```

Newest OK backups first — scan down to the one just before the incident timestamp, then investigate it with `info` / `check` above. (A direct `--before=<date>` filter is planned for a future release.)

### Plan storage capacity for next quarter

```bash
pg_backup_auditor stat -B /var/lib/pgbackup
```

`DATABASE GROWTH TREND` shows net size change and monthly rate. `STORAGE > WAL Archive Volume` shows WAL bytes per day per instance.

### Compare backup timings before and after a PostgreSQL upgrade

Before the upgrade, save the current baseline:

```bash
pg_backup_auditor stat -B /var/lib/pgbackup > pre-upgrade.txt
```

After several new backups have run post-upgrade:

```bash
pg_backup_auditor stat -B /var/lib/pgbackup
```

Compare `Avg Duration` per (tool, type). A regression there often points to I/O or checksum overhead in the new PostgreSQL version. Make sure the `Interval` column shows the same cadence — otherwise you're comparing different schedules.

### See the shape of the backup history

```bash
pg_backup_auditor list -B /var/lib/pgbackup --sort-by=start_time
```

Tree display shows chains and their WAL mode (stream / archive / mixed).

### Find orphaned backups (incrementals whose FULL is gone)

```bash
pg_backup_auditor audit -B /var/lib/pgbackup
```

Look at the `Orphaned` section in the audit output.

### Validate the entire WAL archive, not just backup ranges

```bash
pg_backup_auditor check -B /var/lib/pgbackup --level=full
```

Level `full` scans every WAL segment in the archive (headers, CRC32C), catching segments swapped or corrupted outside individual backup LSN ranges.

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
| `--status=STATUS` | Filter: `all`, `ok`, `warning`, `error`, `corrupt`, `orphan`, `running` |
| `--sort-by=FIELD` | Sort: `start_time` (default), `end_time`, `name`, `size` |
| `--reverse, -r` | Reverse sort order |
| `--limit=N, -n N` | Limit total output to N backups |
| `--max-depth=N, -d N` | Recursion depth (0 = current dir only, -1 = unlimited) |
| `--no-recurse, -R` | Scan only the specified directory (alias for `--max-depth=0`) |
| `--format=FORMAT, -f FORMAT` | Output format: `table` (only `table` is currently supported) |

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

### `audit`

Assess backup strategy: recovery points, RPO gap, orphaned backups, WAL coverage, storage capacity.

```
pg_backup_auditor audit --backup-dir=PATH [OPTIONS]
```

**Options:**

| Option | Description |
|--------|-------------|
| `--backup-dir=PATH, -B PATH` | Backup directory (required) |
| `--wal-archive=PATH` | External WAL archive for coverage analysis (optional) |
| `--detect-size-small, -s` | Enable detection of unusually small backups as anomalies (optional) |

**Output sections:**

| Section | Description |
|---------|-------------|
| **CHAINS** | Per-chain analysis: status (OK/WARNING/ERROR), WAL mode (stream/archive/mixed), oldest/latest recovery point, RPO gap between backups |
| **Orphaned** | Incremental/differential backups without a parent FULL backup — these cannot be used for recovery |
| **Anomalies** | Backups with unusual patterns (size, duration). Size >2x average always reported; size <0.5x average only with `--detect-size-small` |
| **WAL** | Archive statistics (requires `--wal-archive`): total segments, size, continuity, coverage for backup recovery ranges |
| **STORAGE** | Capacity summary: total backup size, disk usage, in-progress RUNNING backups |
| **Verdict** | Overall assessment: **OK** (healthy chains, good coverage), **WARNING** (RPO gaps exist), or **CRITICAL** (missing backups, WAL gaps) |

**RPO (Recovery Point Objective):**
- Time window between consecutive backups in a chain
- Example: FULL on Monday, INCR on Wednesday = 2-day RPO (lose 1 day of data in a restore)
- Larger RPO = higher risk of data loss; smaller RPO = more backup frequency overhead

**WAL modes:**
- `stream` — WAL embedded in backup (fastest recovery, no external archive needed)
- `archive` — WAL stored separately (requires `--wal-archive` for validation)
- `mixed` — same chain contains both (not recommended; indicates inconsistent backup strategy)

### `stat`

Aggregate statistics across the entire backup collection: counts, sizes, frequency, growth trend, incremental efficiency.

```
pg_backup_auditor stat --backup-dir=PATH [OPTIONS]
```

**Options:**

| Option | Description |
|--------|-------------|
| `--backup-dir=PATH, -B PATH` | Backup directory (required) |
| `--wal-archive=PATH, -W PATH` | Path to WAL archive for pg_probackup (auto-detected from backup directory if omitted) |

**Output sections:**

| Section | Description |
|---------|-------------|
| **Statistics by group** | Per-(tool, type, instance) table: count, average interval between backups, total/avg size, avg duration, OK% |
| **STORAGE** | Total counts and sizes broken down by status (OK, WARNING, ERROR, CORRUPT, ORPHAN, RUNNING); WAL archive volume per day per tool/instance |
| **DATABASE GROWTH TREND** | Per tool/instance: net size change between the first and last FULL backup, plus a monthly rate. Intermediate fluctuations (e.g. stream-mode WAL noise) cancel out. Useful for capacity planning (e.g. "need +X GB/month") |
| **INCREMENTAL EFFICIENCY** | Average size of non-FULL backups as a percentage of FULL size: low % = efficient incrementals; high % = strategy issue |

**Interval column:** average time between consecutive backups within the group, computed as `(max_time − min_time) / (count − 1)`. Shown as `N/A` when fewer than 2 backups exist.

## Historical Analysis

By design, pg_backup_auditor is a **stateless on-demand tool** — it maintains no database of its own, and each run is independent. Historical analysis works on whatever your backup catalog already retains, going back as far as your backup tool's retention policy keeps backups around.

### What you can look back at

- **`list`** — full catalog view with filtering by tool/status and sorting; see the shape of your backup history over the retained window
- **`info`** — deep dive into any past backup: LSN range, timeline, chain position, storage, timings, WAL mode
- **`audit`** — retrospective assessment across all chains:
  - Oldest and latest recovery points per chain
  - RPO gaps between consecutive backups
  - Orphaned backups (incrementals whose FULL is gone)
  - Size and duration anomalies against per-(tool, type) baseline
- **`stat`** — aggregate metrics over the retained history:
  - Backup counts, sizes, durations, success rate per (tool, type)
  - Average interval between backups
  - **Database growth trend**: net size change between the first and last FULL backup plus a monthly rate
  - **Incremental efficiency**: DELTA/PAGE/PTRACK/INCREMENTAL as % of FULL size
  - **WAL archive volume per day** per tool and instance

### Scope and boundaries

- **Bounded by retention.** Once your backup tool deletes a backup, it is gone — no auditor can bring it back. Deep-history analysis needs a matching retention policy.
- **No between-backup data.** The tool sees only what backups recorded. There is no continuous monitoring, no time-series metrics, no data about the periods between backups.
- **Independent runs.** Each invocation reads the catalog fresh. There is no persistent log to consult — if a fact was not visible at run time, it will not be available later.

For long-term trends beyond retention, or for real-time alerting, pair pg_backup_auditor with a general-purpose observability stack (metrics scraper + storage + dashboards). pg_backup_auditor itself intentionally stays focused on on-demand audit.

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

## Supported PostgreSQL Versions

PostgreSQL **14, 15, 16, 17, 18** — tested in CI on every commit.

pg_basebackup and pg_probackup adapters work with the PostgreSQL version that created the backup, regardless of the version installed on the auditing host. pgBackRest backups are version-independent at the metadata level.

## Known Limitations

- **pgBackRest WAL**: pgBackRest stores WAL in subdirectories with hash-suffixed compressed filenames — this format cannot be scanned by the WAL validator. WAL checks are skipped for pgBackRest backups; a note is shown in output.
- **pg_basebackup tar format**: per-file checksums inside tar archives require unpacking and are not verified. The manifest self-checksum is still validated.
- **pgBackRest compressed backups**: per-file checksums require decompression and are not verified for compressed (`pg_data.gz` etc.) backups.
- **`tool_version`**: populated for pgBackRest (`backrest-version` from `backup.info`); not yet implemented for pg_basebackup and pg_probackup.
- **pg_basebackup incremental (PG17+)**: incremental backups created with `pg_basebackup --incremental` are detected and chain-linked via LSN; full chain validation is not yet implemented.
- **pg_combinebackup (PG17+)**: backups produced by `pg_combinebackup` have `backup_manifest` but no `backup_label`. Metadata parsing from `backup_manifest` is not yet implemented; these backups are listed with status ERROR.
- **`node_name`**: always reported as `localhost` for pg_basebackup backups. Extraction from the backup directory name or connection metadata is not yet implemented.
- **pg_probackup custom WAL location**: if `pg_probackup.conf` specifies a non-default WAL archive path, it is ignored. WAL validation always looks in the default location relative to the catalog.
- **pg_probackup tablespace and database map**: entries in `tablespace_map` and `database_map` are not verified against the actual backup file tree.

## Planned for v0.4.0

- **Time-range filters** (`--since=<date>`, `--before=<date>`) for `list`, `audit`, and `stat`: bound analysis to a specific window instead of the full catalog. Enables "find the last valid backup before an incident" without manual scanning.
- **WAL cleanup recommendations** in `audit`: identify oldest WAL segments safe to delete based on the oldest recovery point across all chains.
- **RPO compliance check** in `audit`: compare actual RPO gap against a configurable target (`--rpo-target=24h`).
- **Backup distribution analysis** in `audit`: detect schedule gaps and irregular intervals.
- **JSON/YAML output format**: structured output for integration with monitoring systems.
- **pg_combinebackup full support**: metadata parsing from `backup_manifest` (currently skipped).
- **Retention policy validation**: `audit` will check if backups meet retention requirements.
- **External WAL archive configuration**: for pg_basebackup, support reading WAL archive path from PostgreSQL recovery config.
- **pg_basebackup checkpoint_lsn display**: show checkpoint LSN in `info` command output.

## Testing

```bash
# Makefile
make test

# Meson
meson test -C builddir
```

**244 unit tests, 100% passing.**

CI matrix: Ubuntu 22.04/24.04 + macOS 14, PostgreSQL 14–18 (PG18 Linux only), GCC and Clang.

Test suite covers: WAL validator (availability, continuity, restore chain, headers, CRC32C, segment size, stream WAL), backup validator (structure, metadata, checksums for all three tools, chain validation), adapters (pg_basebackup, pg_probackup, pgBackRest — scan, metadata, WAL path), common utilities (string_utils, xlog, INI parser, fs_scanner).

## Documentation

- [Installation Guide](docs/INSTALL.md)
- [Meson Build Guide](docs/MESON_BUILD.md)

## License

GNU General Public License v3 or later (GPL-3.0-or-later).

## Author

Daria Lepikhova <daria.n.lepikhova@gmail.com>
