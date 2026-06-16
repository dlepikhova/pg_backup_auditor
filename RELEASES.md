# pg_backup_auditor Release History

## [v0.3.0](https://github.com/daria-lepikhova/pg_backup_auditor/releases/tag/v0.3.0) - 2026-06-16

**Minor release: New `stat` command, anomaly detection in `audit`, and internal cleanup.**

### Enhancements

#### New Command: `stat`

Aggregate statistics across the entire backup collection, complementing `audit` (facts vs. assessment):

- **Statistics by group** — per-(tool, type, instance) table: count, average interval between backups, total/avg size, average duration, success rate
- **STORAGE** — counts and sizes by status (OK / WARNING / ERROR / CORRUPT / ORPHAN / RUNNING); WAL archive volume per day per tool/instance, with auto-detection of the pg_probackup WAL archive path
- **DATABASE GROWTH TREND** — net size change between the first and last FULL backup plus a monthly rate; robust to intermediate WAL noise in stream-mode `pg_basebackup`. Filtered to OK/WARNING backups so stub/corrupt entries do not distort the time and size axes
- **INCREMENTAL EFFICIENCY** — average size of DELTA/PAGE/PTRACK/INCREMENTAL backups as a percentage of FULL size

Example:
```bash
pg_backup_auditor stat -B /var/lib/pgbackup
```

#### Anomaly Detection in `audit`

- Size anomalies: backups >2x average (always enabled)
- Duration anomalies: backups >2x faster/slower than average
- Small backups: <0.5x average (opt-in via `--detect-size-small`)
- Statistics computed separately per (tool, type) combination
- Reported with actual value, average, and multiplier

### Code Quality

- **Shared chain grouping** — extracted `BackupChain` struct and helpers (`build_chains`, `find_chain_root`, `find_backup_in_list`) into `src/common/backup_chain.c`; eliminated ~280 lines duplicated between `cmd_check.c` and `cmd_audit.c`
- **`*_from_string` helpers** — paired with the existing `*_to_string` converters for `BackupTool`, `BackupStatus`, and `ValidationLevel`; case-insensitive
- **`parse_string_option` helper** — collapses the recurring "duplicate check + assign + set seen" boilerplate across all CLI option handlers
- **Logging initialization** centralized in `pg_backup_auditor_init()` (removed redundant `log_init()` calls from each command)
- **Exit codes** unified to standard Unix convention: 0 = success, 1 = general error, 2 = validation failed, 4 = invalid arguments
- **Removed dead `calculate_stddev()`** and its `sum_sizes_sq` field from `StatGroup` along with the per-backup accumulation in the hot loop

### Fixes

- **pg_probackup metadata parsing** — added boundary check after `strncmp()` in `parse_control_line()` so keys like `timeline` no longer match prefixed values such as `timelineid`
- **Meson test build** broken since May 2026 — added missing `test_anomaly_detection.c` and its dependencies (`cmd_audit.c`, `cmd_help.c`, `arg_parser.c`) to `tests/unit/meson.build`
- **`-Wformat-truncation` warning** on Ubuntu GCC for `format_interval()` — bumped buffer size to accommodate the worst-case `long` argument range

### Testing

- **267 unit tests** (up from 244), 100% pass rate
- 23 new tests covering: prefix-collision regression (1), `*_from_string` parsers (9), `parse_string_option` (3), `backup_chain_*` (7), anomaly detection (3)
- Full CI coverage: Ubuntu 22.04/24.04, macOS 14; PostgreSQL 14–18

### Compatibility

- **PostgreSQL**: 14, 15, 16, 17, 18
- **Backup tools**: pg_basebackup, pg_probackup 2.5+, pgBackRest
- **Platforms**: Linux (Ubuntu 22.04+, RHEL, Debian), macOS 14+
- **Build systems**: Makefile, Meson

### Installation

```bash
git clone https://github.com/daria-lepikhova/pg_backup_auditor.git
cd pg_backup_auditor
meson setup builddir && ninja -C builddir
sudo ninja -C builddir install
```

### Migration from v0.2.0

No breaking changes. The new `stat` command and `audit` anomaly detection are additive. The internal refactorings (`backup_chain.c`, `*_from_string`, `parse_string_option`) do not affect user-visible behavior or CLI surface.

**Full Changelog**: [v0.2.0...v0.3.0](https://github.com/daria-lepikhova/pg_backup_auditor/compare/v0.2.0...v0.3.0)

---

## [v0.2.0](https://github.com/daria-lepikhova/pg_backup_auditor/releases/tag/v0.2.0) - 2026-04-16

**Major release: Critical bug fixes, security hardening, and comprehensive test coverage.**

### Critical Fixes

#### Security
- **Shell injection vulnerability in `pg_basebackup` tar extraction** — replaced `popen()` with `fork()/execvp()` to prevent command injection via untrusted filenames
- **Static buffer aliasing in `get_json_value()`** — refactored to use caller-supplied buffers, eliminating stale data bugs across multiple backup tools

#### Data Correctness  
- **Incorrect `stop_lsn` assignment** — was incorrectly reading from `CHECKPOINT LOCATION`; now sourced exclusively from `backup_manifest` End-LSN for correctness
- **WAL segment search O(n²) → O(log n)** — replaced linear search with binary search for archive validation

#### Code Quality
- **CRC32C table duplication** — extracted shared implementation to `crc32c.c`; removed from `file_utils.c` and `wal_validator.c`
- **Unsafe integer parsing** — replaced `atoi()` with `strtol()` + proper overflow/error checking in `ini_parser.c`

### Enhancements

#### New Command: `audit`
- Per-chain backup strategy assessment: recovery points, RPO gaps, orphaned backups
- WAL archive coverage and continuity analysis
- Storage capacity summary with RUNNING backup detection
- Overall verdict: OK / WARNING / CRITICAL

#### WAL Mode Support
- New `WAL` column in `list` command with tool-specific labels
- `WAL Mode` field in `info` command output
- Per-chain WAL mode reporting in `audit` (stream/archive/mixed)

#### pg_basebackup Improvements
- **`backup_manifest` extraction from tar archives** (PG13+) — enables accurate `stop_lsn` for tar-format backups
- **External WAL archive auto-detection** — searches standard relative paths with `realpath()` normalization

#### Testing
- **244 unit tests** (up from 231) — 100% pass rate
- 13 new tests: CRC32C (6), file utilities (12), structure validators (3)
- Full CI coverage: Ubuntu 22.04/24.04, macOS 14; PostgreSQL 14–18

### Compatibility

- **PostgreSQL**: 14, 15, 16, 17, 18
- **Backup tools**: pg_basebackup, pg_probackup 2.5+, pgBackRest
- **Platforms**: Linux (Ubuntu 22.04+, RHEL, Debian), macOS 14+
- **Build systems**: Makefile, Meson

### Installation

```bash
git clone https://github.com/daria-lepikhova/pg_backup_auditor.git
cd pg_backup_auditor
meson setup builddir && ninja -C builddir
sudo ninja -C builddir install
```

### Migration from v0.1.0

No breaking changes. Enhanced validation will catch additional issues previously masked by the `stop_lsn` bug.

---

## [v0.1.0](https://github.com/daria-lepikhova/pg_backup_auditor/releases/tag/v0.1.0) - 2026-04-06

**First public release: Cross-platform PostgreSQL backup auditor.**

### Core Features

#### Adapters (3)
- **pg_basebackup**: plain & tar formats (gz, bz2, xz, lz4); metadata from `backup_label`; `backup_manifest` validation (SHA256 + CRC32C)
- **pg_probackup 2.5**: FULL/DELTA/PAGE/PTRACK types; archive & stream modes; CRC32C validation; chain validation
- **pgBackRest**: FULL/DIFF/INCR types; JSON/INI metadata; SHA1 checksums; chain validation

#### Commands (4)
- **`list`**: scan directory tree, filter by tool/status, sort, limit, tree display with chain grouping
- **`check`**: 4-level validation (basic/standard/checksums/full); per-file checksums; WAL availability & continuity
- **`info`**: detailed backup metadata, timing, storage, PostgreSQL info (LSN, timeline, backup method)
- **`help`**: per-command usage and examples

#### Validation
- **Structure checks**: per-tool required files and directories
- **Metadata checks**: timestamps, LSN ranges, timeline, backup chain integrity
- **Checksum validation**: SHA256/CRC32C (pg_basebackup), CRC32C (pg_probackup), SHA1 (pgBackRest)
- **Chain validation**: incremental/differential verification; orphaned backup detection

#### WAL Support
- Automatic stream backup WAL scanning (`database/pg_wal/`, `pg_wal/`, `pg_wal.tar*`)
- WAL archive scanner: segment enumeration, LSN arithmetic, timeline handling
- Availability check: required segments present for backup LSN ranges
- Continuity validation: detect archive gaps
- Header validation: xlp_magic, timeline, page address, CRC32C
- Segment size auto-detection (1 MB – 1 GB)

### Quality & Testing

- **217 unit tests**, 100% pass rate
- CI matrix: Ubuntu 22.04/24.04, macOS 14; PostgreSQL 14–18 (PG18 Linux only); GCC + Clang; Valgrind leak check
- **Build systems**: Makefile and Meson
- **Global features**: `--no-color` flag for scripting

### Known Limitations (v0.1.0)

- pgBackRest WAL uses hash-suffixed compressed filenames (not scanned)
- pg_basebackup tar archives: per-file checksums require extraction (not verified)
- pgBackRest compressed backups: per-file checksums not verified
- pg_combinebackup (PG17+): manifest-only parsing not implemented (listed as ERROR)
- `tool_version`: only implemented for pgBackRest; missing for pg_basebackup and pg_probackup

---

## Contributors

**Daria Lepikhova** ([@dlepikhova](https://github.com/dlepikhova))

For full changelog, see [CHANGELOG.md](CHANGELOG.md).
