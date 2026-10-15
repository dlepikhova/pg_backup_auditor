# Roadmap

Where pg_backup_auditor is heading. Milestones are listed in the order
they are planned; only the next one is firm, later ones may be
reordered as feedback lands.

For what has already shipped, see [CHANGELOG.md](CHANGELOG.md).

Size: S = ~day, M = ~week, L = ~2–3 weeks or more.

---

## Principles that shape the plan

- **Read-only.** The auditor never writes to a backup catalog, its
  metadata or its WAL archive. Anything it derives exists only in the
  output of the current run.
- **Correct before broad.** A wrong "OK" is worse than no answer. Known
  correctness gaps are fixed before new features build on top of them.
- **One cluster at a time.** Backups, WAL archive, timeline history and
  WAL segment size belong to one PostgreSQL cluster; analysis must never
  mix data from different clusters.
- **Say what was not checked.** When a tool, format or storage does not
  allow a check, the output says so instead of reporting success.

---

## v0.4.0 — Correctness

Make the existing analysis trustworthy on real catalogs.

Already done (see `Unreleased` in the changelog): all pg_probackup
statuses recognized, unreadable status/type reported as UNKNOWN,
pgBackRest page checksum errors detected, `check` exit codes for a
missing `--backup-id` and for "nothing validated".

Remaining:

- **Time zones in backup timestamps** — S. Timestamps from
  `backup_label` and `backup.control` are currently read in the
  auditing host's time zone, so durations, RPO gap and intervals shift
  when the server and the host are in different zones.
- **Recovery points in `audit`** — M. The oldest recovery point is the
  end of the oldest usable backup, not its start; the latest one
  requires every ancestor in the chain to be usable; WAL coverage after
  a backup is checked per timeline and with the real segment size.
- **Compressed pg_probackup WAL** — M. Segments stored as `.gz` by
  `archive-push --compress` are currently not seen, which reports
  segments as missing.
- **`--no-color`** — S. Accepted by every command; no color codes when
  output is not a terminal.
- **WAL cleanup line removed from `audit`** — S. The current estimate
  can suggest deleting WAL that a backup of another cluster needs. It
  returns as a recommendation once per-cluster analysis exists (see
  v0.6.0).
- **Time-range filters (`--since` / `--before`)** — S. For `list`,
  `audit` and `stat`: bound analysis to a window, e.g. to find the last
  valid backup before an incident.

## v0.5.0 — Foundation

Groundwork that every new backup tool and the cloud storage depend on.

- **Per-cluster analysis** — L. Backups are grouped by PostgreSQL
  cluster (system identifier from `pg_control`), each with its own WAL
  archive, timeline history and segment size. Fixes chain linking and
  WAL checks on catalogs that hold several instances.
- **Catalog-level adapters** — M. An adapter describes a whole catalog
  (instances, backups, WAL) instead of recognizing one directory at a
  time. Needed for tools whose backups are not plain directories.
- **Storage access layer** — M. All reads go through one interface
  (list, stat, read a range). Local filesystem first; it is the
  prerequisite for S3.
- **Decompression** — M. gzip, lz4, zstd, bzip2, xz as optional build
  dependencies, used for WAL and backup files of every tool.
- **JSON output** — M. Structured output for all commands, for
  monitoring and automation. Comes with separating analysis from
  rendering, which also makes the analysis testable.

## v0.6.0 — More tools, faster checks

- **Barman adapter** — M. Local Barman catalogs: `backup.info`, WAL
  directory, compressed WAL.
- **More backup tools** — further adapters on the same catalog-level
  foundation.
- **Parallel validation (`check -j`)** — L. Files of a backup validated
  in parallel; a single large FULL backup no longer runs in one thread.
- **Policy checks in `audit`** — M each. Built on per-cluster analysis:
  - RPO compliance (`--rpo-target=24h`);
  - retention requirements (minimum number of backups, age of the
    oldest FULL);
  - schedule gaps and irregular intervals;
  - WAL that no usable backup needs any more.

## v0.7.0 — Cloud storage

- **S3 storage backend** — L. Read-only: list, stat, ranged reads.
  Credentials from environment and profile. On S3 the default check
  level covers metadata and WAL; per-file checksums (which download the
  whole backup) are opt-in.
- **wal-g adapter** — L. Metadata, delta chains, compressed WAL.
  Encrypted backups: metadata only, content reported as not checked.
- **barman-cloud adapter** — M.
- **S3 repositories of pgBackRest and pg_probackup** — M.

## Later

Directions without a version yet.

- **Restore-target recommendation** — given a target time, the backup
  (with its chain) and WAL range needed to reach it, or the last
  reachable point if it cannot be reached.
- **Check journal** — optional history of `check` results kept at a
  path the user chooses, never inside or next to the backup catalog.
  Shows files, backups and WAL segments that appeared or disappeared
  since the previous check, and lets unchanged backups skip repeated
  checksum validation.
- **Prometheus text format** (`stat --format=prometheus`).
- **pgmoneta adapter.**
- **pg_combinebackup** — metadata from `backup_manifest` alone.
- **pgBackRest WAL archive** — hash-suffixed, compressed segment files.

## Not planned

- Writing to backup catalogs: changing statuses, deleting or repairing
  backups. Status changes stay with the backup tool itself
  (`pg_probackup validate`, `pgbackrest check`).
- Running restores to test backups.
- Restore time estimation: depends on hardware the auditor cannot see.

---

## How to influence the roadmap

Open a GitHub issue with the use case — real DBA scenarios reorder
priorities faster than anything else.
