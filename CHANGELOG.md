# Changelog

All notable changes to pg_backup_auditor are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [0.3.0] - 2026-06-16

### Added

- **`stat` command**: aggregate statistics across the entire backup collection
  - Per-(tool, type, instance) table: count, average interval between backups, total/avg size, average duration, success rate
  - STORAGE section: counts and sizes by status; WAL archive volume per day per tool/instance (auto-detected pg_probackup archive path)
  - DATABASE GROWTH TREND section: net change between first and last FULL backup plus monthly rate; robust to intermediate WAL noise in stream-mode pg_basebackup; for capacity planning
  - INCREMENTAL EFFICIENCY section: average size of DELTA/PAGE/PTRACK/INCREMENTAL backups as % of FULL size
  - Command example: `pg_backup_auditor stat -B /backup`
- **Anomaly detection in `audit` command**: Identifies backups with unusual sizes or durations (3 new unit tests)
  - Size anomalies: backups >2x average (always enabled)
  - Duration anomalies: backups >2x faster/slower than average
  - Small backups: <0.5x average (optional via `--detect-size-small` flag)
  - Statistics calculated separately per (tool, type) combination
  - Anomalies reported with actual value, average, and multiplier
  - Command example: `pg_backup_auditor audit -B /backup --detect-size-small`
- **`*_from_string` helpers** for `BackupTool`, `BackupStatus`, `ValidationLevel`: case-insensitive parsing paired with the existing `*_to_string` converters
- **`parse_string_option` helper** in arg_parser: collapses the recurring "duplicate check + assign + set seen" boilerplate in CLI option handling
- **`src/common/backup_chain.c`**: shared chain-grouping module extracted from cmd_check and cmd_audit (eliminates ~280 lines of duplication)

### Changed

- `detect_anomalies()` signature updated: added `bool detect_size_small` parameter
- Test count: 247 (added 3 anomaly detection tests, previously 244)
- Logging initialization centralized in `pg_backup_auditor_init()` (removed redundant `log_init()` calls from each command)
- Exit codes unified to standard Unix convention: 0 = success, 1 = general error, 2 = validation failed, 4 = invalid arguments

### Fixed

- pg_probackup metadata parsing: added boundary check after `strncmp()` in `parse_control_line()` so keys like `timeline` no longer match prefixed values like `timelineid`
- Meson test build broken since May 2026: added missing `test_anomaly_detection.c` and its dependencies (`cmd_audit.c`, `cmd_help.c`, `arg_parser.c`) to `tests/unit/meson.build`

---

## [0.2.0] - 2026-04-16

### Added

- **Unit tests for CRC32C and structure validators** — added 13 new tests covering crc32c_update(), pg_basebackup/pg_probackup/pgbackrest structure validation, and file utilities (244 total tests)
- **`audit` command**: per-chain status, oldest/latest recovery point, RPO gap, orphaned backups, WAL archive coverage and size, disk usage, overall verdict (OK / WARNING / CRITICAL)
- **WAL mode in `list`**: new `WAL` column with tool-specific labels — `stream`/`archive` (pg_probackup), `embedded`/`none` (pg_basebackup, derived from `pg_wal/` presence), `archive` (pgBackRest)
- **WAL mode in `info`**: `WAL Mode` field in the POSTGRESQL section
- **WAL mode in `audit`**: per-chain WAL mode (`stream`, `archive`, or `mixed` if members differ)
- **`tool_version` for pgBackRest**: extracted from `backup.info` (`backrest-version` field)
- **PG version column in `list`**
- **Tree display in `list`**: backup chains shown with UTF-8 box-drawing (├─ / └─ / │); orphaned backups in a separate section
- **pg_basebackup 17+ incremental chain linking**: post-scan LSN matching — `INCREMENTAL FROM LSN` in `backup_label` matched against `start_lsn` of parent
- **`--no-recurse` / `-R`**: alias for `--max-depth=0`; directory header shows absolute path
- **`check`: chain-grouped output** — FULL backup with its incrementals displayed as a unit
- **`info`: new fields** — Parent Backup, Backup From (primary/standby), Compression
- **pg_probackup**: `from-replica` field parsed (`true` → standby, `false` → primary); `compress-alg` displayed (suppressed when `none`)
- **PG18 in CI matrix** (Ubuntu 22.04/24.04)

### Fixed

- Directory header showed raw relative path (e.g. `../backup`) — now resolved via `realpath()`
- Spurious `[WARNING] Failed to parse backup metadata` when scanning broad directories — demoted to debug
- `postgresql-server-dev` removed from CI — package pulled llvm/clang (~500 MB) as a hard dependency on Ubuntu 24, `--no-install-recommends` was ineffective
- **Code review fixes**:
  - CRC32C table duplication — extracted to `crc32c.c/crc32c.h`, removed from `file_utils.c` and `wal_validator.c`
  - `get_json_value()` returned pointer to static buffer — now uses caller-supplied buf/bufsize
  - `atoi()` in `ini_get_int()` doesn't handle overflow/non-numeric input — replaced with `strtol()` and proper error checking
  - `popen()` shell injection via tar filename — replaced with `fork()/execvp()` in `pg_basebackup tar_popen()`
  - WAL segment search O(n×m) — replaced with binary search in `segment_exists_in_archive()`
  - `stop_lsn` incorrectly set from `CHECKPOINT LOCATION` — now comes from `backup_manifest` End-LSN only
- **pg_basebackup enhancements**:
  - `backup_manifest` now extracted from tar archives (PG13+) for real `stop_lsn`
  - External WAL archive auto-detection via standard relative paths (../archive, ../wal_archive, etc.)

---

## [0.1.0] - 2026-04-06

First public release.

### Adapters

- **pg_basebackup**: plain and tar format detection (gz, bz2, xz, lz4); metadata from `backup_label`; `backup_manifest` self-checksum validation (SHA256 + CRC32C); `pg_combinebackup` output detected (listed with status ERROR — manifest-only parsing not yet implemented)
- **pg_probackup 2.5**: FULL, DELTA, PAGE, PTRACK backup types; archive and stream modes; metadata from `backup.control`; CRC32C per-file checksums from `backup_content.control`; chain validation (FULL → DELTA/PAGE/PTRACK)
- **pgBackRest**: FULL, DIFF, INCR types; metadata from `backup.manifest` (INI format) and `backup.info` (JSON); SHA1 per-file checksums from `[target:file]`; `backup-error` detection; chain validation via `backup-prior`

### Commands

- **`list`**: scan a backup directory tree, display backups grouped by subdirectory; filter by `--type` and `--status`; sort by `--sort-by` (start_time, end_time, name, size) with `--reverse`; global `--limit`; `--max-depth` recursion control; unknown option values produce an error
- **`check`**: validate backups at four cumulative levels — `basic` (structure + chain), `standard` (metadata consistency), `checksums` (per-file checksums + WAL), `full` (archive-wide WAL header scan); `--backup-id` to validate a single backup; `--wal-archive` for external WAL
- **`info`**: detailed output for a single backup — timing, storage, PostgreSQL metadata (LSN, timeline, WAL range, backup method)
- **`help`**: per-command usage with examples

### Validation

- **Structure checks** — per-tool required files and directories:
  - pg_basebackup: `base/`, `global/pg_control`, `PG_VERSION`, `backup_label`/`backup_manifest`; `pg_wal/` or `pg_wal.tar*` for stream backups
  - pg_probackup: `database/`, `database/database_map`, `database/global/pg_control`, `database/PG_VERSION` (FULL only), `database/backup_label` (archive mode), `database/pg_wal/` (stream mode)
  - pgBackRest: `backup.manifest`, `manifest.copy`, `pg_data/global/pg_control`, `pg_data/PG_VERSION`; `backup-error=y` flagged as error
- **Metadata checks**: backup_id presence, status, timestamps, LSN range (start ≤ stop), timeline
- **Checksum validation**: pg_basebackup SHA256/CRC32C from `backup_manifest`; pg_probackup CRC32C from `backup_content.control`; pgBackRest SHA1 from `backup.manifest [target:file]`
- **Chain validation**: incremental/differential backups verified against their parent; orphaned backups detected

### WAL

- Archive scanner: segment enumeration, LSN arithmetic, timeline handling
- Availability check: required segments present for backup LSN range
- Continuity check: no gaps in archive sequence
- Restore chain check: WAL bridges between consecutive backups
- Header validation: `xlp_magic`, `XLP_LONG_HEADER`, `xlp_tli`, `xlp_pageaddr`
- WAL segment size auto-detected from segment headers (1 MB – 1 GB)
- Stream backups scanned automatically (`database/pg_wal/`, `pg_wal/`, `pg_wal.tar*`)
- Auto-detection of WAL archive path for archive-mode backups

### Build and testing

- Makefile and Meson build systems
- CI: Ubuntu 22.04, Ubuntu 24.04, macOS 14; PostgreSQL 14–18 (PG18 Linux only); GCC and Clang; memory leak check with Valgrind
- 217 unit tests, 100% pass rate
- `--no-color` global flag

### Known Limitations

See [README — Known Limitations](README.md#known-limitations).

## [0.1.0-alpha] - 2026-01-08

Initial infrastructure: project structure, Makefile, logging, string and file utilities, adapter registry, fs_scanner, CLI skeleton (list, check, info), 25 unit tests.
