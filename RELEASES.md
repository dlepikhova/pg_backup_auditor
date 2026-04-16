# pg_backup_auditor Release History

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
