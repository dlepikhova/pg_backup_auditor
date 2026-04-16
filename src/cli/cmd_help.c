/*
 * cmd_help.c
 *
 * Help and usage message functions
 *
 * Copyright (C) 2026 Daria Lepikhova
 *
 * This program is free software: you can redistribute it and/or modify
 * it under the terms of the GNU General Public License as published by
 * the Free Software Foundation, either version 3 of the License, or
 * (at your option) any later version.
 *
 * This program is distributed in the hope that it will be useful,
 * but WITHOUT ANY WARRANTY; without even the implied warranty of
 * MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
 * GNU General Public License for more details.
 *
 * You should have received a copy of the GNU General Public License
 * along with this program.  If not, see <https://www.gnu.org/licenses/>.
 */


#include "cmd_help.h"
#include <stdio.h>

/*
 * Print general usage/help message
 */
void
print_general_usage(void)
{
	printf("pg_backup_auditor - PostgreSQL backup auditing and validation tool\n\n");
	printf("Usage: pg_backup_auditor COMMAND [OPTIONS]\n\n");
	printf("COMMANDS:\n");
	printf("  list    - List available backups\n");
	printf("  info    - Show detailed backup information\n");
	printf("  check   - Validate backup consistency\n");
	printf("  audit   - Audit backup strategy (recovery points, RPO, storage)\n");
	printf("  help    - Show this help message\n\n");
	printf("Use 'pg_backup_auditor COMMAND --help' for command-specific options.\n\n");
}

/*
 * Print usage for 'list' command
 */
void
print_list_usage(void)
{
	printf("Usage: pg_backup_auditor list [OPTIONS]\n\n");
	printf("List available backups from backup directory.\n\n");

	printf("OPTIONS:\n");
	printf("  -B, --backup-dir=PATH    Path to backup directory (required)\n");
	printf("  -t, --type=TYPE          Filter by tool: auto, pg_basebackup, pg_probackup, pgbackrest\n");
	printf("  -s, --status=STATUS      Filter by status: all, ok, warning, error, corrupt, orphan, running\n");
	printf("      --sort-by=FIELD      Sort by: start_time (default), end_time, name, size\n");
	printf("  -r, --reverse            Reverse sort order\n");
	printf("  -n, --limit=N            Limit number of results (default: unlimited)\n");
	printf("  -d, --max-depth=N        Recursion depth (0 = current dir only, -1 = unlimited, default: -1)\n");
	printf("  -R, --no-recurse         Scan only the specified directory (alias for --max-depth=0)\n");
	printf("  -f, --format=FORMAT      Output format: table (default)\n");
	printf("  -h, --help               Show this help message\n\n");

	printf("EXAMPLES:\n");
	printf("  pg_backup_auditor list -B /backup/pg\n");
	printf("  pg_backup_auditor list -B /backup/pg --type=pg_probackup\n");
	printf("  pg_backup_auditor list -B /backup/pg --status=error\n");
	printf("  pg_backup_auditor list -B /backup/pg --no-recurse\n");
	printf("  pg_backup_auditor list -B /backup/pg --sort-by=size --reverse --limit=10\n\n");
}

/*
 * Print usage for 'info' command
 */
void
print_info_usage(void)
{
	printf("Usage: pg_backup_auditor info [OPTIONS]\n\n");
	printf("Display detailed information about a specific backup.\n\n");

	printf("OPTIONS:\n");
	printf("  -B, --backup-dir=PATH    Path to backup directory (required)\n");
	printf("  -i, --backup-id=ID       Backup ID to inspect (required)\n");
	printf("  -o, --output=FORMAT      Output format: text, json (default: text)\n");
	printf("  -h, --help               Show this help message\n\n");

	printf("INFORMATION DISPLAYED:\n");
	printf("  General      - Backup ID, type, tool, status\n");
	printf("  Timing       - Start time, end time, duration\n");
	printf("  Storage      - Size, path, compression\n");
	printf("  PostgreSQL   - Version, timeline, LSN range\n");
	printf("  WAL          - WAL segments needed for recovery\n\n");
}

/*
 * Print usage for 'check' command
 */
void
print_check_usage(void)
{
	printf("Usage: pg_backup_auditor check [OPTIONS]\n\n");
	printf("Validate backup consistency and check for issues.\n\n");

	printf("OPTIONS:\n");
	printf("  -B, --backup-dir=PATH    Path to backup directory (required)\n");
	printf("  -i, --backup-id=ID       Check specific backup by ID\n");
	printf("  -l, --level=LEVEL        Validation level (default: standard)\n");
	printf("                           Levels: basic, standard, checksums, full\n");
	printf("      --wal-archive=PATH   Path to external WAL archive (optional)\n");
	printf("      --skip-wal           Skip all WAL checking\n");
	printf("  -h, --help               Show this help message\n\n");

	printf("VALIDATION LEVELS (cumulative — each level includes all checks from previous):\n");
	printf("  basic      - Level 1: File structure + chain connectivity + WAL presence\n");
	printf("  standard   - Level 2: Level 1 + metadata validation (default)\n");
	printf("  checksums  - Level 3: Level 2 + WAL availability + WAL header validation\n");
	printf("  full       - Level 4: Level 3 + comprehensive checks (pg_verifybackup)\n\n");

	printf("CHECKS BY LEVEL:\n");
	printf("  Level 1 (basic):\n");
	printf("    - File structure: backup_label/backup_manifest/backup.control presence\n");
	printf("    - Directory structure: base/, global/, database/ as required\n");
	printf("    - Chain connectivity: incremental backups have valid parent\n");
	printf("    - WAL presence: required WAL files exist in backup\n\n");
	printf("  Level 2 (standard):\n");
	printf("    - Required metadata fields: backup_id, start_time, etc.\n");
	printf("    - LSN validity: start_lsn < stop_lsn\n");
	printf("    - Timestamp consistency: start_time < end_time\n");
	printf("    - Timeline and version presence\n\n");
	printf("  Level 3 (checksums):\n");
	printf("    - WAL availability: all segments in [start_lsn, stop_lsn] exist\n");
	printf("    - WAL continuity: no gaps between consecutive archive segments\n");
	printf("    - WAL timeline: .history file present for timeline > 1\n");
	printf("    - WAL header: reads XLogLongPageHeaderData (40 bytes) per segment\n");
	printf("        non-zero magic, XLP_LONG_HEADER flag set\n");
	printf("        timeline matches backup, page address matches segment start\n");
	printf("        segment size is a valid power of two (1 MB - 1 GB)\n");
	printf("        CRC32C of the first XLogRecord\n");
	printf("    - WAL per-record CRC32C: validates CRC32C of every complete\n");
	printf("        single-page XLogRecord across all pages of each segment;\n");
	printf("        multi-page records and xl_crc=0 records are skipped\n\n");
	printf("  Level 4 (full):\n");
	printf("    - pg_verifybackup: if available for pg_basebackup\n");
	printf("    - All comprehensive checks\n\n");

	printf("WAL CHECKING:\n");
	printf("  WAL checks require backup LSN metadata (start_lsn/stop_lsn).\n");
	printf("  For pg_probackup: WAL archive path is auto-detected from the catalog.\n");
	printf("  For other tools: use --wal-archive to specify the archive location.\n");
	printf("  Use --skip-wal to disable all WAL checks.\n\n");

	printf("EXIT CODES:\n");
	printf("  0 - All checks passed successfully\n");
	printf("  1 - General error (cannot scan directory, etc.)\n");
	printf("  2 - Validation issues found (errors or warnings)\n");
	printf("  4 - Invalid arguments\n\n");

	printf("EXAMPLES:\n");
	printf("  # Basic structure check\n");
	printf("  pg_backup_auditor check -B /backup/pg --level=basic\n\n");
	printf("  # Standard validation (default)\n");
	printf("  pg_backup_auditor check -B /backup/pg\n\n");
	printf("  # Full validation with WAL continuity\n");
	printf("  pg_backup_auditor check -B /backup/pg --level=full\n\n");
	printf("  # Check specific backup, skip WAL\n");
	printf("  pg_backup_auditor check -B /backup/pg -i 20240101T120000 --skip-wal\n\n");
	printf("  # Use external WAL archive\n");
	printf("  pg_backup_auditor check -B /backup/pg --wal-archive=/wal/archive\n\n");
}

/*
 * Print usage for 'audit' command
 */
void
print_audit_usage(void)
{
	printf("Usage: pg_backup_auditor audit [OPTIONS]\n\n");
	printf("Assess backup strategy: recovery points, RPO gap, orphaned backups, WAL coverage,\n");
	printf("storage capacity and growth. Provides per-chain status and overall verdict.\n\n");

	printf("OPTIONS:\n");
	printf("  -B, --backup-dir=PATH    Path to backup directory (required)\n");
	printf("      --wal-archive=PATH   Path to external WAL archive for coverage analysis (optional)\n");
	printf("  -h, --help               Show this help message\n\n");

	printf("OUTPUT SECTIONS:\n");
	printf("  CHAINS\n");
	printf("    For each backup chain (FULL + DELTA/PAGE/PTRACK/INCR):\n");
	printf("    - Status: OK, WARNING (gaps/issues), ERROR (critical problems)\n");
	printf("    - WAL Mode: stream/archive/mixed (how WAL is captured)\n");
	printf("    - Oldest recovery point: earliest time you can restore to\n");
	printf("    - Latest recovery point: most recent recovery time\n");
	printf("    - RPO (Recovery Point Objective) gap: time between backups\n\n");
	printf("  Orphaned\n");
	printf("    Incremental/differential backups without a FULL backup parent.\n");
	printf("    These backups cannot be used for recovery.\n\n");
	printf("  WAL\n");
	printf("    Archive statistics and continuity (only with --wal-archive):\n");
	printf("    - Total segments and size\n");
	printf("    - Coverage: are segments available for each backup recovery range?\n");
	printf("    - Continuity: any gaps in the WAL archive?\n\n");
	printf("  STORAGE\n");
	printf("    Backup capacity summary:\n");
	printf("    - Total backup size (all backup data)\n");
	printf("    - Disk usage (actual filesystem usage)\n");
	printf("    - RUNNING backups (incomplete backups in progress)\n\n");
	printf("  Verdict\n");
	printf("    Overall assessment based on all checks:\n");
	printf("    - OK:       All chains healthy, no gaps, good recovery coverage\n");
	printf("    - WARNING:  RPO gaps exist or WAL coverage gaps, but not critical\n");
	printf("    - CRITICAL: Missing backups, orphaned increments, WAL discontinuity\n\n");

	printf("RPO (RECOVERY POINT OBJECTIVE):\n");
	printf("  Time between consecutive backups in a chain.\n");
	printf("  Larger RPO = more data loss if restore to specific point needed.\n");
	printf("  Example: FULL on Mon, INCR on Wed = 2-day RPO (lose 1 day of data)\n\n");

	printf("WAL MODES:\n");
	printf("  stream   - WAL archived within the backup (fast recovery, no external archive needed)\n");
	printf("  archive  - WAL stored separately (flexible, requires --wal-archive for validation)\n");
	printf("  mixed    - Same backup chain contains both stream and archive backups (not recommended)\n\n");

	printf("EXIT CODES:\n");
	printf("  0 - All chains healthy (Verdict: OK)\n");
	printf("  1 - General error (cannot scan directory, invalid path, etc.)\n");
	printf("  2 - Issues detected (Verdict: WARNING or CRITICAL)\n");
	printf("  4 - Invalid arguments\n\n");

	printf("EXAMPLES:\n");
	printf("  # Audit backup directory\n");
	printf("  pg_backup_auditor audit -B /backup/pg\n\n");
	printf("  # Include external WAL archive analysis\n");
	printf("  pg_backup_auditor audit -B /backup/pg --wal-archive=/var/lib/postgresql/wal\n\n");
	printf("  # Short output (exit code tells you verdict)\n");
	printf("  pg_backup_auditor audit -B /backup/pg && echo 'All healthy' || echo 'Issues detected'\n\n");
}
