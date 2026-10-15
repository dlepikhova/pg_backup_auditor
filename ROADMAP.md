# Roadmap

Where pg_backup_auditor is heading. Organized by direction, not by
version — so it is clear both what is coming next and what is on the
horizon. Concrete version assignments (v0.4.0, v0.5.0, …) are tags
on items, and only the next milestone is firm; later items may be
reordered as feedback lands.

For what has already shipped, see [CHANGELOG.md](CHANGELOG.md).

Legend:
- **[v0.4.0]** — planned for the next release
- **[v0.5+]** — planned but not yet scheduled
- **[future]** — direction, no concrete design yet
- **size**: S = ~day, M = ~week, L = ~2–3 weeks or more

---

## Track 1 — Reach: more backup tools

Broaden coverage beyond the current three adapters (pg_basebackup,
pg_probackup, pgBackRest) so the tool answers "are my backups OK?"
regardless of which backup solution a fleet uses.

- **[v0.5+] Barman adapter** — M
  EnterpriseDB's widely deployed backup tool. Hierarchical layout
  (`<server>/base/<backup_id>/`), INI-style metadata (`backup.info`,
  `server.conf`), local disk via rsync. Well-documented format.
- **[v0.5+] S3 / remote source abstraction** — L
  Data access layer above local filesystem, so adapters can read
  from S3, GCS, and other object stores. **Prerequisite for wal-g
  and for pg_probackup's own S3 backend.**
- **[v0.5+] wal-g adapter** — L
  Cloud-native backup tool. Simple `sentinel.json` metadata per
  backup. Mostly deployed to S3 — depends on the S3 abstraction.
- **[future] pgmoneta adapter** — L
  Newer, RH/Fedora ecosystem, TLS-first, Kubernetes-oriented. May
  need TLS handling and its own protocol support. Strategic bet
  rather than immediate demand.

## Track 2 — Depth: richer analysis of existing tools

Deeper diagnostics and policy checks on catalogs the tool already
understands. Turns the auditor from a snapshot into a strategy
review.

- **[v0.4.0] RPO compliance check** — M
  `audit --rpo-target=24h`: compare the actual RPO gap (largest
  window with no valid backup) against a policy target, flag chains
  that fail.
- **[v0.4.0] WAL cleanup recommendations** — M
  In `audit`: identify the oldest WAL segments safe to delete based
  on the oldest recovery point across all chains. Helps free space
  without breaking PITR.
- **[v0.4.0] Backup distribution analysis** — S
  Detect schedule gaps and irregular intervals. Complements the
  "average interval" number in `stat` with warnings on outliers.
- **[v0.4.0] Retention policy validation** — M
  Check that catalog contents meet a retention requirement (minimum
  number of backups, minimum age of oldest FULL, and so on).
- **[v0.5+] Restore-target recommendation** — L
  Given a target timestamp ("I need to restore to 2026-08-01 14:30"),
  recommend the specific backup (FULL + incremental chain if any)
  and the WAL segment range required. Verify chain integrity and
  WAL continuity from the base backup's stop_lsn up to the target
  point; if recovery is impossible, report the last reachable
  recovery point instead. Interface (new command vs. extension of
  `audit`) TBD. Builds on existing chain and WAL-continuity logic
  plus a new timestamp → LSN mapping.
- **[v0.5+] Parallel validation (`-j`)** — L
  Multi-threaded `check`: validate N backups (or N files within a
  backup) in parallel. Requires thread pool, coordinated progress
  reporting, care around shared state.
- **[v0.5+] JSON / YAML output** — M
  Structured output for all commands, for integration with
  monitoring stacks. Requires a small output-layer refactor so
  every value goes through a formatter.

## Track 3 — Polish: DBA-facing UX

Smaller-but-visible improvements to the day-to-day experience.

- **[v0.4.0] Time-range filters (`--since` / `--before`)** — S
  For `list`, `audit`, and `stat`. Bound analysis to a window
  instead of scanning the full catalog. Enables "find the last
  valid backup before an incident" without manual scrolling.
- **[v0.4.0] pg_combinebackup full support** — M
  Parse metadata from `backup_manifest` alone. Currently these
  backups are listed as ERROR because `backup_label` is absent.
- **[v0.5+] External WAL archive configuration for pg_basebackup**
  — S
  Read WAL archive path from PostgreSQL recovery config or a
  dedicated settings file, so `check` finds the archive without
  `--wal-archive`.
- **[v0.5+] pg_basebackup checkpoint_lsn display** — S
  Show checkpoint LSN in `info` output.
- **[v0.5+] Median / stddev in `stat`** — S
  Add median and standard deviation columns for a more robust
  distribution picture (mean can hide outliers).
- **[v0.5+] Professional output formatting** — M
  Verdict box with colored border, UTF-8 box drawing for section
  separators, status indicators (✓ / ⚠ / ✗), consistent alignment.

---

## v0.4.0 focus

The next release is themed **"analysis vs. policy"** — the tool
already reports the current state; v0.4.0 lets you compare that
state against explicit policy targets:

- RPO compliance check
- WAL cleanup recommendations
- Retention policy validation
- Backup distribution analysis
- Time-range filters
- pg_combinebackup full support

## Ideas without a track (yet)

Things that have come up but need more thought before they belong
in a track:

- Prometheus text-format output (`stat --format=prometheus`) — very
  close to JSON/YAML in scope; may fold into Track 2's output work.
- Backup-tool-specific config auto-detection (pg_probackup.conf,
  pgBackRest.conf) — cross-cutting; could inform several adapters.

---

## How to influence the roadmap

Open a GitHub issue with the use case — real DBA scenarios reorder
priorities faster than anything else. Items in **[v0.5+]** and
**[future]** are the most flexible.
