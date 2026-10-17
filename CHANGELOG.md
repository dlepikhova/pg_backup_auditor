# Changelog

All notable changes to pg_backup_auditor are documented in this file.

The format is based on [Keep a Changelog](https://keepachangelog.com/en/1.0.0/).

## [Unreleased]

### Fixed

- pg_probackup statuses DONE, MERGING, MERGED, DELETING and DELETED are now recognized. Previously they were reported as OK, so a backup with an interrupted merge was counted as restorable.
- A backup whose status or type cannot be read from tool metadata is now reported as UNKNOWN and flagged by `check`, instead of being treated as an OK FULL backup.
- `check --backup-id` with an ID that does not exist now fails with `Backup not found` and exit code 1 instead of reporting `Result: OK`.
- `check` now exits with code 2 when no backup was eligible for validation (previously 0).
- Meson build from a clean clone failed at `meson setup`: `meson.build` installed `docs/DEVELOPER.md`, which is not part of the repository.
- pgBackRest backups with page checksum errors are now detected. The flag is read from `"backup-error":true` in `backup.info`, where pgBackRest records it; the previous check looked for a `backup-error=y` line in `backup.manifest`, which pgBackRest never writes. Such backups are shown as WARNING and reported as an error by `check`.

### Changed

- `check` skips MERGING and MERGED backups with a hint to rerun `pg_probackup merge`, and DELETING / DELETED backups, in addition to ERROR / CORRUPT.
- `audit` and `stat` treat OK, DONE and WARNING as usable backups. In `audit`, a FULL backup in any other status except RUNNING marks its chain BROKEN.
- `list --status` accepts the new statuses; `stat` STORAGE lists every status present in the catalog.
- README exit code table now matches the implementation (code 3 is not used).
- ROADMAP rewritten around milestones: correctness (v0.4.0), per-cluster foundation (v0.5.0), Barman, more backup tools and parallel checks (v0.6.0), cloud storage (v0.7.0). README links to it instead of keeping its own plan.

### Infrastructure

- CI split into a fast set run on every push (Ubuntu 24.04 + GCC, macOS 14 + Clang, meson build, sanitizers, integration tests) and a full set for pull requests to `main`. The PostgreSQL version axis is dropped from CI: the code includes no PostgreSQL headers and the tests do not use a server.
- New Makefile targets: `check-all` (Makefile and meson builds plus unit tests) and `test-sanitize`.
- CLI tests (pytest, `tests/cli`) run the built binary and check exit codes and messages; `make test-cli`, a `cli` test in meson, and a step in the fast CI set. Python and pytest are needed only for these tests.
- PostgreSQL is no longer a build dependency: `pg_config` and PostgreSQL include paths are removed from both the Makefile and meson builds. The code never included PostgreSQL headers. Meson setup previously failed on hosts where `pg_config` reports a server include directory that is not installed (for example the GitHub Ubuntu runner).

## [0.3.1] - 2026-06-22

### Fixed

- `audit` anomaly detection: entries with zero or negative duration (structurally invalid `end_time <= start_time` from broken `backup_label` timestamps) no longer pollute the per-(tool, type) baseline. They previously inflated the average duration to unrealistic values and caused legitimate short backups to display astronomical multipliers (`51742.8x faster than avg`).

### Changed

- README lead rewritten to open with the DBA's question ("Will your PostgreSQL backups actually restore?") instead of a tool-centric description. Adds an explicit "Read-only by design" paragraph so users can safely point the tool at a production catalog.
- New README section **How to** with eight task-shaped recipes: fleet health check, investigating a specific backup, finding the last valid backup before an incident, capacity planning, comparing timings before/after a PostgreSQL upgrade, seeing the shape of backup history, finding orphaned backups, full WAL archive validation.
- New README section **Historical Analysis** documenting what retrospective analysis the tool already provides and its stateless-by-design boundaries.
- Roadmap: added `--since` / `--before` time-range filters as a planned v0.4.0 feature.

### Infrastructure

- New release workflow (`.github/workflows/release.yml`): pre-built binaries for `linux-x86_64` and `macos-arm64` are attached automatically to every published GitHub release. Triggered on `release: published`; also supports `workflow_dispatch` for manual re-uploads or dry-runs against existing tags. Ubuntu 22.04 is used as the Linux build host so the resulting binary's glibc requirement is broad enough for most production servers.

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
